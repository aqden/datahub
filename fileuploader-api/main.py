import logging
from logging.handlers import TimedRotatingFileHandler
import os
from os import environ
import time
from typing import Union
import requests
import datetime
import uvicorn
import json
import jwt
import pprint
from datahub.metadata.schema_classes import *
from string import Template
from fastapi.responses import JSONResponse

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent, MetadataChangeProposal
from datahub.ingestion.run.pipeline import Pipeline
from fastapi import FastAPI
from fastapi import FastAPI, File, UploadFile, Form
import shutil
from pathlib import Path

rootLogger = logging.getLogger("ingest")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)
streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.INFO)  
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
    title="Datahub 3th Party Uploader API",
)

api_emitting_port = 8002

rest_endpoint = "http://localhost:8080"
if os.environ.get("DATAHUB_BACKEND"):
    rest_endpoint = os.environ.get("DATAHUB_BACKEND")
jwt_secret = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94="
if os.environ.get("JWT_SECRET"):
    jwt_secret = os.environ.get("JWT_SECRET")

@app.get("/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    return {
            "message": "Hello world",
            "timestamp": int(time.time() * 1000)
        }

@app.post("/upload")
async def process_metadata(user_id: str= Form(), myfile: UploadFile = File()):    
    
    start_time = time.time()
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M_%s")
    user_urn = f"urn:li:corpuser:{user_id}"
    if not check_entity_exist(user_urn):
        return JSONResponse(status_code=404, content=f"Uploader {user_id} does not exist in system")
    token = impersonate_token(user_id=user_id)
    destination = f"./submission_{timestamp}_{user_id}.json"
    destination_path = Path(destination)
    try:
        with destination_path.open("wb") as buffer:
            shutil.copyfileobj(myfile.file, buffer)
    finally:
        myfile.file.close()
    urn_checklist={}
    """
    This is the urn_checklist structure. I will refer to this dictionary 
    after all the processing is done, then decide to ingest the file or not.
    The key is the URN of the entity.
    If the urn is unparseable, it is added to the urn="unable_to_infer" key
    type:     

    urn_checklist = {
        "urn:li:dataset:abcde":{
            "type": "dataset"         # dataset or container (helps in generating MCPs)
            "ownership": True         # if uploader is owner of existing asset. Only relevant if existing entity is TRUE
            "existing_entity": True   # already in RDBMS
            "errors"=[],              # any error msg associated with the entity while processing.
            "retain_ownership" = True #ensure that no aspect undo ownership of user_id
            "owners_to_add" = []      # a list of urn that i need to add as owner.
        }, 
        "urn:li:dataset:12345":{
        }....
    }
    """   
    with open(destination, "r") as f:
        obj_list = json.load(f)
    print(f"there are {len(obj_list)} in the jsonfile")
    for i, obj in enumerate(obj_list):
        check_urn: Union[MetadataChangeEvent, MetadataChangeProposal]
        if "proposedSnapshot" in obj:            
            check_urn = MetadataChangeEvent.from_obj(obj)
            if not check_urn.validate():
                #skip current obj
                urn_checklist = add_error(urn_checklist, "unable_to_infer", f"Item {i} is invalid Snapshot")
                continue            
            snapshot_key = list(obj["proposedSnapshot"].keys())[0] # there should only be 1 key
            item_urn = check_urn.proposedSnapshot.urn
            if snapshot_key!="com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot":
                urn_checklist = add_error(urn_checklist, item_urn, "Snapshot is not DatasetSnapshot")
                #skip current obj
                continue
            if item_urn not in urn_checklist:
                urn_checklist = add_new_urn(urn_checklist, item_urn, user_id, "dataset")            
            for ownership_data_aspect in check_urn.proposedSnapshot.aspects:                
                if isinstance(ownership_data_aspect, OwnershipClass):
                    ownership_data_aspect = ownership_data_aspect
                    ownership_data = ownership_data_aspect.owners # list of ownersClass
                    owners_in_data = {item.owner: item.type for item in ownership_data}
                    # owners_in_data = [item.owner for item in ownership_data_aspect.owners]
                    if f"urn:li:corpuser:{user_id}" not in owners_in_data:                        
                        urn_checklist = update_retain_ownership(urn_checklist, item_urn, False, owners_in_data)                        
                    else:
                        urn_checklist = update_retain_ownership(urn_checklist, item_urn, True, owners_in_data)
        elif "aspect" in obj:
            check_urn = MetadataChangeProposal.from_obj(obj)
            if not check_urn.validate():
                urn_checklist = add_error(urn_checklist, "unable_to_infer", f"Item {i} is invalid Snapshot")
                continue            
            item_urn = check_urn.entityUrn
            # item_urn = item.get("entityUrn")
            entityType=check_urn.entityType
            aspectName = check_urn.aspectName
            if entityType!="dataset" and entityType!="container":
                urn_checklist = add_error(urn_checklist, item_urn, "aspect is not a dataset or container aspect")
                continue
            if item_urn not in urn_checklist:
                urn_checklist = add_new_urn(urn_checklist, item_urn, user_id, entityType)                                                
            if aspectName=="ownership":
                ownership_data_aspect = check_urn.value
                ownership_data = ownership_data_aspect.owners
                owners_in_data = {item.owner: item.type for item in ownership_data}
                # owners_in_data = [item.get("owner","") for item in ownership_data_aspect.get("ownership",{})]
                if f"urn:li:corpuser:{user_id}" not in owners_in_data:
                    print("not in owners list")
                    urn_checklist = update_retain_ownership(urn_checklist, item_urn, False, owners_in_data)
                else:
                    urn_checklist = update_retain_ownership(urn_checklist, item_urn, True, owners_in_data)
        else:
            urn_checklist = add_error(urn_checklist, "unable_to_infer", f"Item {i} in file is invalid object")
    pre_ingest_check = True # assume all is ok until otherwise
    for check_urn in urn_checklist:
        # for new assets only; to add uploader as owner
            # if "retain_ownership" has no value then need to create ownership aspect 
        if not urn_checklist[check_urn].get("retains_ownership", False):
            urn_checklist[check_urn]["add_ownership"] = True
        if urn_checklist[check_urn].get("existing_entity",True): 
            # if existing entity and existing ownership state is false, reject
            if not urn_checklist[check_urn].get("ownership", True):
                pre_ingest_check = False
                urn_checklist = add_error(urn_checklist, check_urn, "does not own dataset")                
                continue            
            
        

    pprint.pprint(urn_checklist)
    all_errors=[]
    for check_urn in urn_checklist:
        errors = urn_checklist[check_urn].get("errors",[])        
        if len(errors)>0:
            pre_ingest_check = False        
            error_line = ".".join(item for item in errors)
            all_errors.append(f"{check_urn}: {error_line}")
            continue

    ingestion_checklist_destination = destination.replace(".json", "-ingest-checklist.json")
    if pre_ingest_check: # means we're good to ingest now
        ingested_file_destination = destination.replace(".json", "-ingest-prep.json")
        shutil.copy(destination, f"{ingested_file_destination}")
        additional_mcps=[]
        for urn in urn_checklist:
            if "add_ownership" in urn_checklist[urn]:
                existing_users = urn_checklist[urn].get("owners_to_add",{})
                additional_mcps = insert_owner_mcp(user_id, urn, additional_mcps, existing_users, urn_checklist[urn]["type"])
                print(f"inserted mcp for {urn} for {user_id}")
        if len(additional_mcps)>0:
            with open(destination, "r") as f:
                obj_list = json.load(f)
            obj_list.extend(additional_mcps)
            with open(ingested_file_destination,"w") as f:
                json.dump([item for item in obj_list], f, indent=4)
        mid_time = time.time()-start_time
        print(f"time to validate MCE/MCP is {mid_time}")
        pipeline = create_pipeline(ingested_file_destination, token)
        pipeline.run()
        total_time = time.time()-start_time
        failures = pipeline.sink.get_report().failures
        warnings = pipeline.sink.get_report().warnings
        connection_time = pipeline.sink.get_report().downstream_total_latency_in_seconds
        urn_checklist["gms-warnings"] = warnings
        urn_checklist["gms-failures"] = failures
        with open(ingestion_checklist_destination,"w") as f:
            json.dump(urn_checklist, f, indent=1)
        if len(failures)>0:
            return JSONResponse(status_code=207, 
                content=f"Total Time: {total_time}sec. Sink Time: {connection_time}sec. Partial errors encountered during ingestion: {failures}")
        return JSONResponse(status_code=200, content=f"Total Time: {total_time}sec")
    urn_checklist["errors"] = all_errors
    with open(ingestion_checklist_destination,"w") as f:
        json.dump(urn_checklist, f, indent=1)
    total_time = time.time()-start_time
    return JSONResponse(status_code=400, content=f"Total Time: {total_time}sec. Errors in payload: {all_errors}")

def insert_owner_mcp(user_id, urn, appendlist, other_users_to_add, entityType = "dataset"):
    """
    To insert ownership mcp for an asset. 
    I don't have to be concerned about overwriting an existing owner aspect, 
    cos, by right, if there is such an aspect without user_id, then it will fail the ingest_logic liao

    variables:
    user_id :            the ID for the uploader (not yet in URN form)
    urn:                 the asset's URN whom we're adding this ownership aspect to
    appendlist:          the existing list of mcps that we're consolidating before writing to file
    other_users_to_add:  dict containing {user_urn: ownertype} info
    entityType:          type of asset. (dataset or container)

    returns:
    modified_appendlist:  a modified list containing the owner_aspect for this urn  
    """    
    additional_users=[]    
    if other_users_to_add:
        for additional_user in other_users_to_add:
            user = OwnerClass(
                owner=f"{additional_user}",
                type=other_users_to_add[additional_user],
            )
            additional_users.append(user)
    uploader_ownership = OwnerClass(owner=f"urn:li:corpuser:{user_id}",
        type=OwnershipTypeClass.TECHNICAL_OWNER,
    )
    additional_users.append(uploader_ownership)
    ownership = OwnershipClass(
        owners= additional_users,
    )
    mcp = MetadataChangeProposalWrapper(
            aspect = ownership,
            entityType=entityType,
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=urn,
            aspectName="ownership",
            systemMetadata=SystemMetadataClass(
                runId=f"custom_ownership_{str(int(time.time()))}"
            ),
    )
    appendlist.append(mcp.to_obj())
    return appendlist

def create_pipeline(path, token):
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
    return pipeline

def update_retain_ownership(existing_dict, item_urn, state, new_owners={}):
    """
    This function is called whenever ownership aspect is found in the file.
    If the aspect does not contain the submitter, state= False.
    At the end, assuming everything else is ok, but retain_ownership = False,
    I will create ownership aspect for the urn. any other owners(new_owners) that were spotted
    for this urn will be parked in owners_to_add

    This means that the "bit" can be toggled back and forth if there are multiple 
    ownership aspects for the same urn, but im of the opinion that I will only
    care about the last ownership aspect and whether it has the uploader id or not.
    """
    if item_urn in existing_dict:
        existing_dict[item_urn]["retain_ownership"] = state
        if len(new_owners)>0:
            if "owners_to_add" in existing_dict[item_urn]:
                existing_users = existing_dict[item_urn]["owners_to_add"]
                existing_users.update(new_owners)
                existing_dict[item_urn]["owners_to_add"] = existing_users
            else:
                existing_dict[item_urn]["owners_to_add"] = new_owners
    return existing_dict

def add_new_urn(existing_dict, new_urn: str, user_id: str, entityType:str = "dataset") -> dict:    
    """
    add a new urn to the checklist. 
    If new entity, return immediately
    If existing entity in datahub, confirm that user_id is owner
    """
    # print(f"adding {new_urn} to {existing_dict}")
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
        existing_dict[urn]["errors"] = [error]
    else:
        curr_error = existing_dict[urn]["errors"]
        curr_error.append(error)
        existing_dict[urn]["errors"]=curr_error
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
    # print(impersonated_payload)
    new_token = jwt.encode(impersonated_payload, jwt_secret, algorithm="HS256")
    return new_token

def check_entity_exist(entity_urn) -> bool:
    """
    ensure that this entity exist in Datahub. applicable for any entity type. 
    Returns true even if the entity is "soft-removed".
    """
    query_token = impersonate_token("datahub")
    headers={}
    headers["Authorization"] = f"Bearer {query_token}"
    headers["Content-Type"] = "application/json"
    query = """
        query existence($urn: String!){
            entityExists(urn: $urn) 
        }
    """    
    variables = {"urn": entity_urn}
    resp = requests.post(
        f"{rest_endpoint}/api/graphql", headers=headers, json={"query": query, "variables": variables}
    )    
    if resp.status_code != 200:
        return False
    data_received = json.loads(resp.text)    
    print(data_received)
    print("entity {} exists: {}".format(entity_urn, bool(data_received["data"]['entityExists'])))
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
        rootLogger.debug("Individual Ownership Step: True")
        return True    
    group_owners = [item["owner"]["urn"] for item in owners_list if item["owner"]["__typename"]=="CorpGroup"]
    if len(group_owners) > 0:
        groups = query_users_groups(query_token, query_endpoint, user_urn)
        # rootLogger.debug(f"The list of groups for this user is {groups}")
        groups_urn = [item["entity"]["urn"] for item in groups]
        for item in groups_urn:
            if item in group_owners:
                log.debug(f"Group Ownership Step: True for {item}.")
                return True 
    rootLogger.error("Ownership Step: False")
    return False
    
def query_dataset_ownership(token: str, dataset_urn:str, query_endpoint:str, entity_type:str="dataset"):
    headers = {}
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    query = Template("""
    query owner($urn: String!){
        $entity(urn: $urn) {
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
    """)
    query = query.substitute({"entity": entity_type, "urn":"$urn"})
    variables = {"urn": dataset_urn}
    resp = requests.post(
        query_endpoint, headers=headers, json={"query": query, "variables": variables}
    )
    # rootLogger.debug(f"resp.status_code is {resp.status_code}")
    data_received = json.loads(resp.text)
    # rootLogger.error(f"received from graphql ownership info: {data_received}")
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
    if resp.status_code != 200:
        return []
    data_received = json.loads(resp.text)
    if data_received["data"]["corpUser"]["relationships"]["count"]>0:
        groups_list = data_received["data"]["corpUser"]["relationships"]["relationships"]
        return groups_list
    return []



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=api_emitting_port)