# flake8: noqa
# command to run in CLI mode is 
# gunicorn -c config.py main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8001
# also need export PROMETHEUS_MULTIPROC_DIR=/home/admini/development/datahub/fastapi/tmp
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from os import environ
import time
from typing import Union
import requests
import uvicorn
import json
import jwt
# from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
# from datahub.emitter.mcp import MetadataChangeProposalWrapper
# from datahub.emitter.rest_emitter import DatahubRestEmitter
# from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
#     DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent, MetadataChangeProposal
from datahub.ingestion.run.pipeline import Pipeline
# from datahub.metadata.schema_classes import (ChangeTypeClass, GlossaryTermAssociationClass, GlossaryTermsClass,
#                                              SystemMetadataClass)
from fastapi import FastAPI
from fastapi import FastAPI, File, UploadFile, Form
import shutil
from pathlib import Path
# when running ingest-api from CLI, need to set some params.
# cos dataset_profile_index name varies depending on ES. If there is an existing index (and datahub is instantiated on top, then it will append a UUID to it)

rest_endpoint = "http://localhost:8080"
api_emitting_port = 8002
# logging - 1 console logger showing info-level+, and 2 logger logging INFO+ AND DEBUG+ levels
# --------------------------------------------------------------
rootLogger = logging.getLogger("ingest")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)
streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.INFO)  #docker logs will show simplified
rootLogger.addHandler(streamLogger)
log = TimedRotatingFileHandler(
    "./logs/fileuploader.log", when="midnight", interval=1, backupCount=730
)
log.setLevel(logging.DEBUG)
log.setFormatter(logformatter)
rootLogger.addHandler(log)

rootLogger.info("started uploader_api!")
# --------------------------------------------------------------

app = FastAPI(
    title="Datahub secret API",
)
origins = [
]
if environ.get("ACCEPT_ORIGINS") is not None:
    new_origin = environ["ACCEPT_ORIGINS"]
    origins.append(new_origin)
    rootLogger.info(f"{new_origin} is added to CORS allow_origins")

# add prometheus monitoring to ingest-api via starlette-exporter
jwt_secret = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94="


@app.get("/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    # how to check that this dataset exist? - curl to GMS?
    # rootLogger.info("hello world is called")
    # rootLogger.info("/custom/hello is called!")
    return {
            "message": "Hello world",
            "timestamp": int(time.time() * 1000)
        }

@app.post("/upload")
async def update_browsepath(user_id: str= Form(), myfile: UploadFile = File()):    
    if not check_entity_exist(user_id=user_id):
        return {"message": "404 user not found"}
    token = impersonate_token(user_id=user_id)
    destination = Path(f"./{str(int(time.time()))}_user_id.json")
    try:
        with destination.open("wb") as buffer:
            shutil.copyfileobj(myfile.file, buffer)
    finally:
        myfile.file.close()
    #save to file
    #open file
    #check valid mcp
    checked_urn={}
    """
    checked_urn = {
        "urn:li:dataset:abcde":{
            "type": "dataset"
            "ownership": True
            "existing_entity": True # already in RDBMS
            "errors"=[],
            "retains_ownership" = True #ensure that no aspect undo ownership of user_id
        }
    }
    """
    path=''    
    with open(destination, "r") as f:
        obj_list = json.load(f)
    for i, obj in enumerate(obj_list):
        item: Union[MetadataChangeEvent, MetadataChangeProposal]
        if "proposedSnapshot" in obj:
            item = MetadataChangeEvent.from_obj(obj)
            if not item.validate():
                #skip current obj
                checked_urn = add_error(checked_urn, "unable_to_infer", f"Item {i} is invalid Snapshot")
                continue            
            snapshot_key = list(obj["proposedSnapshot"].keys())[0] # there should only be 1 key
            item_urn = item.proposedSnapshot.urn
            if snapshot_key!="com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot":
                checked_urn = add_error(checked_urn, item_urn, "Snapshot is not DatasetSnapshot")
                #skip current obj
                continue
            if item_urn not in checked_urn:
                checked_urn = add_new_urn(checked_urn, item_urn, user_id, "dataset")            
            for aspect in item.proposedSnapshot.aspects:
                if "com.linkedin.pegasus2avro.common.Ownership" in aspect:
                    ownership_data = aspect["com.linkedin.pegasus2avro.common.Ownership"]
                    owners_in_data = [item.get("owner","") for item in ownership_data.get("ownership",{})]
                    if f"urn:li:corpuser:{user_id}" not in owners_in_data:
                        checked_urn = update_retain_ownership(checked_urn, item_urn, False)
                    else:
                        checked_urn = update_retain_ownership(checked_urn, item_urn, True)
        elif "aspect" in obj:
            item = MetadataChangeProposal.from_obj(obj)
            if not item.validate():
                checked_urn = add_error(checked_urn, "unable_to_infer", f"Item {i} is invalid Snapshot")
                continue            
            item_urn = item.get("entityUrn")
            if entityType!="dataset" and entityType!="container":
                checked_urn = add_error(checked_urn, item_urn, "aspect is not a dataset or container aspect")
                continue
            if item_urn not in checked_urn:
                checked_urn = add_new_urn(checked_urn, item_urn, user_id, entityType)                        
            entityType=item.get("entityType","")            
            if entityType=="ownership":
                aspect = item["aspect"]["value"]
                owners_in_data = [item.get("owner","") for item in aspect.get("ownership",{})]
                if f"urn:li:corpuser:{user_id}" not in owners_in_data:
                    checked_urn = update_retain_ownership(checked_urn, item_urn, False)
                else:
                    checked_urn = update_retain_ownership(checked_urn, item_urn, True)
        else:
            checked_urn = add_error(checked_urn, "unable_to_infer", f"Item {i} in file is invalid object")
    pre_ingest_check = True
    all_errors=[]
    for item in checked_urn:
        errors = checked_urn[item].get("errors",[])        
        if len(errors)>0:
            pre_ingest_check = False        
            error_line = ".".join(item for item in errors)
            all_errors.append(f"{item}: {error_line}")
            continue        
        if not checked_urn[item].get("retains_ownership", True):
            pre_ingest_check = False
            all_errors.append(f"{item}: Submitter will not own asset if ingestion proceeded")
            continue    
    
    return {"filename": myfile.filename, "value":user_id, "destination": destination}

def start_pipeline(path, token):
    pipeline = Pipeline.create(
        {
            "source":{
                "type": "file",
                "config": {
                    "filename": path, 
                }
            },
            "sink":{
                "type": "datahub-rest",
                "config":{
                    "server": rest_endpoint,
                    "token": token
                }
            },
        }
    )
    pipeline.run()

def update_retain_ownership(existing_dict, item_urn, state):
    if item_urn in existing_dict:
        existing_dict[item_urn]["retain_ownership"] = state
    return existing_dict

def add_new_urn(existing_dict, new_urn: str, user_id: str, entityType:str = "dataset") -> dict:    
    """
    add a new urn to the checklist. 
    If new entity, return immediately
    If existing entity in datahub, confirm that user_id is owner
    """
    print(f"adding {new_urn} to {existing_dict}")
    existing_entity=False
    if check_entity_exist(new_urn):
        existing_entity = True    
    existing_dict[new_urn] = {
        "existing_entity" : existing_entity,
        "type": entityType
    }
    if not existing_entity:
        return existing_dict
    else:
        # existing entity
        ownership = check_curr_ownership(new_urn, user_id, entityType)
        existing_dict[new_urn]["ownership"]=ownership
        existing_dict[new_urn]["retain_ownership"] = ownership
        return existing_dict

def add_error(existing_dict, urn, error) -> dict:
    if urn not in existing_dict:
        existing_dict[urn]={}
    if not existing_dict[urn].get("error"):
        existing_dict[urn]["error"] = [error]
    else:
        curr_error = existing_dict[urn]["error"]
        curr_error.append(error)
        existing_dict[urn]["error"]=curr_error
    return existing_dict  

def check_curr_ownership(item_urn, user_id, entityType):
    if query_dataset_owner(item_urn, user_id, entityType):        
        return True
    return False

def impersonate_token(user_id: str) -> str:    
    """
    since i need to submit as user, i need the token of user
    """
    temp_expiry = int(time.time()) + 600
    impersonated_payload = {
        'actorType': 'USER', 
        'actorId': f"{user_id}", 
        'type': 'PERSONAL', 
        'version': '1', 
        'exp': temp_expiry, 
        'jti': '1', 
        'sub': f'{user_id}', 
        'iss': 'datahub-metadata-service'
    }
    print(impersonated_payload)
    new_token = jwt.encode(impersonated_payload, jwt_secret, algorithm="HS256")
    return new_token

def check_entity_exist(user_id) -> bool:
    """
    ensure that this user exist in Datahub
    """
    query_token = impersonate_token("datahub")
    print(query_token)
    headers={}
    headers["Authorization"] = f"Bearer {query_token}"
    headers["Content-Type"] = "application/json"
    query = """
        query existence($urn: String!){
            entityExists(urn: $urn) 
        }
    """    
    variables = {"urn": f"urn:li:corpuser:{user_id}"}
    resp = requests.post(
        f"{rest_endpoint}/api/graphql", headers=headers, json={"query": query, "variables": variables}
    )    
    if resp.status_code != 200:
        return False
    data_received = json.loads(resp.text)    
    return bool(data_received["data"]['entityExists'])

def query_dataset_owner(dataset_urn: str, user: str, entity_type:str="dataset"):
    """
    Queries for owners of dataset. If there are group owners, then will fire another query to check if user is member of group.        
    """
    # log.debug(f"UI endpoint is {datahub_url}")
    user_urn = f"urn:li:corpuser:{user}"
    query_endpoint = f"{rest_endpoint}/api/graphql"
    query_token = impersonate_token("datahub")
    
    owners_list = query_dataset_ownership(query_token, dataset_urn, query_endpoint, entity_type)    
    individual_owners = [item["owner"]["urn"] for item in owners_list if item["owner"]["__typename"]=="CorpUser"]
    if user_urn in individual_owners:
        log.debug("Individual Ownership Step: True")
        return True    
    group_owners = [item["owner"]["urn"] for item in owners_list if item["owner"]["__typename"]=="CorpGroup"]
    if len(group_owners) > 0:
        groups = query_users_groups(query_token, query_endpoint, user_urn)
        log.debug(f"The list of groups for this user is {groups}")
        groups_urn = [item["entity"]["urn"] for item in groups]
        for item in groups_urn:
            if item in group_owners:
                log.debug(f"Group Ownership Step: True for {item}.")
                return True 
    log.error("Ownership Step: False")
    return False
    
def query_dataset_ownership(token: str, dataset_urn:str, query_endpoint:str, entity_type:str="dataset"):
    headers = {}
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    query = """
        query owner($urn: String!){
            {}(urn: $urn) {
                ownership{
                    owners{
                        __typename
                        owner{
                        ... on CorpUser{
                            __typename
                            urn
                            }
                        ... on CorpGroup{
                            __typename
                            urn
                            }
                        }
                    }
                }
            }
        }
    """.format(entity_type)
    variables = {"urn": dataset_urn}
    resp = requests.post(
        query_endpoint, headers=headers, json={"query": query, "variables": variables}
    )
    log.debug(f"resp.status_code is {resp.status_code}")
    if resp.status_code != 200:
        return []
    data_received = json.loads(resp.text)
    log.error(f"received from graphql ownership info: {data_received}")
    owners_list = data_received["data"][entity_type]["ownership"]["owners"]
    return owners_list

def query_users_groups(token: str, query_endpoint: str, user_urn: str):
    headers = {}
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    query = """
        query test ($urn: String!){
            corpUser(urn:$urn){
                relationships(input:{
                types: "IsMemberOfGroup"
                direction: OUTGOING      
                }){
                count
                relationships
                    {
                        entity{
                            urn
                        }
                    }
                }
            }
        }
    """
    variables = {"urn": user_urn}    
    resp = requests.post(
        query_endpoint, headers=headers, json={"query": query, "variables": variables}
    )
    log.debug(f"group membership resp.status_code is {resp.status_code}")
    if resp.status_code != 200:
        return []
    data_received = json.loads(resp.text)
    if data_received["data"]["corpUser"]["relationships"]["count"]>0:
        groups_list = data_received["data"]["corpUser"]["relationships"]["relationships"]
        return groups_list
    log.debug(f"group membership list is empty")
    return []



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=api_emitting_port)