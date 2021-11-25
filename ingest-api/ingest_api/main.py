import os
from os import environ
import logging
from logging.handlers import TimedRotatingFileHandler
import time
from pydantic.main import BaseModel
import uvicorn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from ingest_api.helper.mce_convenience import (generate_json_output,
                                               get_sys_time,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_status_mce,
                                               make_ownership_mce,
                                               make_platform,
                                               make_schema_mce, make_user_urn, 
                                               update_field_param_class, create_new_schema_mce,
                                               derive_platform_name)
from ingest_api.helper.models import (create_dataset_params, prop_params,
                                    schema_params, browsepath_params, echo_param,
                                    dataset_status_params, determine_type)

# when DEBUG = true, im not running ingest_api from container, but from localhost python interpreter, hence need to change the endpoint used.
CLI_MODE = True
if environ.get("DATAHUB_URL") is not None:
    datahub_url = os.environ["DATAHUB_URL"]
else:
    datahub_url = "http://localhost:9002"
api_emitting_port = 80 if not CLI_MODE else 8001
rest_endpoint = "http://datahub-gms:8080" if not CLI_MODE else "http://localhost:8080"

rootLogger = logging.getLogger("__name__")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)

streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.DEBUG)
rootLogger.addHandler(streamLogger)

if not CLI_MODE:
    if not os.path.exists("/var/log/ingest/"):
        os.mkdir("/var/log/ingest/")
    if not os.path.exists("/var/log/ingest/json"):
        os.mkdir("/var/log/ingest/json")    
    log = TimedRotatingFileHandler(
        "/var/log/ingest/ingest_api.log", when="midnight", interval=1, backupCount=14
    )
    log.setLevel(logging.DEBUG)
    log.setFormatter(logformatter)
    rootLogger.addHandler(log)

rootLogger.info("started!")

app = FastAPI(
    title="Datahub secret API",
    description="For generating datasets",
    version="0.0.2",
)
origins = ["http://localhost:9002", "http://172.19.0.1:9002", "http://localhost:3000"]
if environ.get("ACCEPT_ORIGINS") is not None:
    new_origin = environ["ACCEPT_ORIGINS"]
    origins.append(new_origin)
    rootLogger.info(f"{new_origin} is added to CORS allow_origins")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
)


@app.get("/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    ## how to check that this dataset exist? - curl to GMS?
    rootLogger.info("hello world is called")
    return {
        'message':"<b>Forth Announcement</b>", 
        'timestamp': int(time.time()*1000)
        # 'timestamp': 1636964967000
    }

@app.post("/update_browsepath")
async def update_browsepath(item: browsepath_params):
    #i expect the following:
    #name: do not touch
    #schema will generate schema metatdata (not the editable version)
    #properties: get description from graphql and props from form. This will form DatasetProperty (Not EditableDatasetProperty)
    #platform info: needed for schema
    rootLogger.info("update_browsepath_request_received {}".format(item))
    dataset_snapshot = DatasetSnapshot(
            urn=item.dataset_name,
            aspects=[],
    )
    browsepath_aspect = make_browsepath_mce(dataset_urn=item.dataset_name, path = item.browsepath)
    dataset_snapshot.append(browsepath_aspect)
    metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    emit_mce_respond(
        metadata_record=metadata_record,
        owner = item.requestor,        
        event = "UI Update Browsepath"
    )

@app.post("/update_schema")
async def update_schema(item: schema_params):
    #i expect the following:
    #name: do not touch
    #schema will generate schema metatdata (not the editable version)
    #properties: get description from graphql and props from form. This will form DatasetProperty (Not EditableDatasetProperty)
    #platform info: needed for schema
    # rootLogger.info("update_schema_request_received {}".format(item))

    datasetName = item.dataset_name
    dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
    )    
    platformName = derive_platform_name(datasetName)
    rootLogger.info(item.dataset_fields)
    field_params = update_field_param_class(item.dataset_fields)
    rootLogger.info(field_params)
    schemaMetadata_aspect = make_schema_mce(
        platformName=platformName,
        actor=item.requestor,
        fields=field_params,
    )
    rootLogger.info(schemaMetadata_aspect)
    dataset_snapshot.aspects.append(schemaMetadata_aspect)
    metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    emit_mce_respond(
        metadata_record=metadata_record,
        owner = item.requestor,        
        event = "UI Update Schema"
    )

@app.post("/update_properties")
async def update_prop(item: prop_params):
    #i expect the following:
    #name: do not touch
    #schema will generate schema metatdata (not the editable version)
    #properties: get description from graphql and props from form. This will form DatasetProperty (Not EditableDatasetProperty)
    #platform info: needed for schema
    rootLogger.info("update_schema_request_received {}".format(item))
    datasetName = item.dataset_name
    dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
    )
    description = item.get("description", "")
    properties = item.get("properties", {})
    property_aspect = make_dataset_description_mce(
        dataset_name=datasetName,
        description=description,
        customProperties=properties,
    )
    dataset_snapshot.append(property_aspect)
    metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    emit_mce_respond(
        metadata_record=metadata_record,
        owner = item.requestor,        
        event = "UI Update Properties"
    )

def emit_mce_respond(metadata_record:MetadataChangeEvent, owner: str, event: str) -> None:    
    datasetName = metadata_record.proposedSnapshot.urn
    for mce in metadata_record.proposedSnapshot.aspects:
        if not mce.validate():
            rootLogger.error(
                f"{mce.__class__} is not defined properly"
            )
            return Response(
                f"MCE was incorrectly defined. {event} was aborted",
                status_code=400,
            )
            return
        
    if CLI_MODE:
        generate_json_output(metadata_record, "/home/admini/generated/")
    try:
        rootLogger.error('emitting')
        rootLogger.error(metadata_record)
        emitter = DatahubRestEmitter(rest_endpoint, actor=owner)
        emitter.emit_mce(metadata_record)
        emitter._session.close()
    except Exception as e:
        rootLogger.debug(e)
        return Response(
            f"{event} failed because upstream error {e}",
            status_code=500,
        )
        return
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully")
    return Response(
        f"{event} successfully completed".format(
            datasetName),
        status_code=201,
    )


@app.post("/make_dataset")
async def create_item(item: create_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    #todo - to revisit to see if refactoring is needed when make_json is up.
    """
    rootLogger.info("make_dataset_request_received {}".format(item))
    item.dataset_type = determine_type(item.dataset_type)

    item.dataset_name = "{}_{}".format(item.dataset_name, str(get_sys_time()))
    datasetName = make_dataset_urn(item.dataset_type, item.dataset_name)
    platformName = make_platform(item.dataset_type)
    browsePath = "/{}/{}".format(item.dataset_type, item.dataset_name)
        
    requestor = make_user_urn(item.dataset_owner)
    headerRowNum = (
        "n/a" if item.dict().get("hasHeader", "n/a") == "no" else str(item.dict().get("headerLine", "n/a"))
    )
    properties = {
        "dataset_origin": item.dict().get("dataset_origin", ""),
        "dataset_location": item.dict().get("dataset_location", ""),
        "has_header": item.dict().get("hasHeader", "n/a"),
        "header_row_number": headerRowNum,
    }
    if item.dataset_type == "json":  # json has no headers
        properties.pop("has_header")
        properties.pop("header_row_number")
    
    dataset_description = item.dataset_description if item.dataset_description else ""
    dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
    )
    dataset_snapshot.aspects.append(
        make_dataset_description_mce(
            dataset_name=datasetName,
            description=dataset_description,
            customProperties=properties,
        )
    )

    dataset_snapshot.aspects.append(make_ownership_mce(actor=requestor, dataset_urn=datasetName))
    dataset_snapshot.aspects.append(make_browsepath_mce(dataset_urn=datasetName, path=[browsePath]))
    field_params = []
    for existing_field in item.fields:
        current_field = {}
        current_field.update(existing_field.dict())
        current_field["fieldPath"] = current_field.pop("field_name")
        if "field_description" not in current_field:
            current_field["field_description"] = ""
        field_params.append(current_field)

    dataset_snapshot.aspects.append(
        create_new_schema_mce(            
            platformName=platformName,
            actor=requestor,
            fields=field_params,
        )
    )
    metadata_record = MetadataChangeEvent(proposedSnapshot = dataset_snapshot)
    emit_mce_respond(metadata_record=metadata_record, owner=requestor, event='Create Dataset')


@app.post("/update_dataset_status")
async def delete_item(item: dataset_status_params) -> None:
    """
    This endpoint is to support soft delete of datasets. Still require a database/ES chron job to remove the entries though, it only suppresses it from search and UI
    """
    rootLogger.info("remove_dataset_request_received {}".format(item))    
    mce = make_status_mce(dataset_name=item.dataset_name, desired_status=item.desired_state)
    emit_mce_respond(metadata_record=mce, owner= item.requestor, event = f"Status Update removed:{item.desired_state}") 

@app.post("/echo")
async def echo_inputs(item: echo_param):
    rootLogger.info(f"input received {item}")
    return Response(status_code=201)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=api_emitting_port)
