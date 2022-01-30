# flake8: noqa

import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
from os import environ

import requests
import uvicorn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
    DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import ChangeTypeClass
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from ingest_api.helper.mce_convenience import (authenticate_action,
                                               create_new_schema_mce,
                                               derive_platform_name,
                                               generate_json_output_mce,
                                               generate_json_output_mcp,
                                               get_sys_time,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_ownership_mce,
                                               make_platform, make_profile_mcp,
                                               make_schema_mce,
                                               make_status_mce, make_user_urn,
                                               update_field_param_class,
                                               verify_token)
from ingest_api.helper.models import (add_sample_params, browsepath_params,
                                      create_dataset_params,
                                      dataset_status_params,
                                      delete_sample_params, determine_type,
                                      prop_params, schema_params)

CLI_MODE = False if environ.get("RUNNING_IN_DOCKER") else True

api_emitting_port = 8001
rest_endpoint = "http://datahub-gms:8080" if not CLI_MODE else "http://localhost:8080"

rootLogger = logging.getLogger("__name__")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)

streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.DEBUG)
rootLogger.addHandler(streamLogger)
rootLogger.info(f"CLI mode : {CLI_MODE}")

# when running ingest-api from CLI, need to set some params.
if not CLI_MODE:
    for env_var in [
        "ACCEPT_ORIGINS",
        "RUNNING_IN_DOCKER",
        "JWT_SECRET",
        "DATAHUB_AUTHENTICATE_INGEST",
        "DATAHUB_FRONTEND",
        "ELASTIC_HOST",
    ]:
        if not os.environment[env_var]:
            raise Exception(
                f"{env_var} is not defined \
                to operate in Container mode"
            )
    if not os.path.exists("/var/log/ingest/"):
        os.mkdir("/var/log/ingest/")
    if not os.path.exists("/var/log/ingest/json"):
        os.mkdir("/var/log/ingest/json")
    log_path = "/var/log/ingest/ingest_api.log"
else:
    os.environ["ELASTIC_HOST"] = "http://localhost:9200"
    if not os.path.exists("./logs/"):
        os.mkdir(f"{os.getcwd()}/logs/")
    log_path = f"{os.getcwd()}/logs/ingest_api.log"
# I think its fine even if the json and log files get mixed
# in the same folder when running locally

log = TimedRotatingFileHandler(log_path, when="midnight", interval=1, backupCount=365)
log.setLevel(logging.INFO)
log.setFormatter(logformatter)

rootLogger.addHandler(log)
rootLogger.info("started!")

app = FastAPI(
    title="Datahub secret API",
    description="For generating datasets",
    version="0.0.2",
)
origins = [
    "http://localhost:9002",
    "http://172.19.0.1:9002",
    "http://172.19.0.1:3000",
    "http://localhost:3000",
]
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


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    """
    This is meant to log malformed POST requests
    """
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")

    rootLogger.error(exc_str)
    rootLogger.error(
        f"malformed POST request \
        {request.body} from {request.client}"
    )
    return PlainTextResponse(str(exc_str), status_code=400)


@app.get("/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    # how to check that this dataset exist? - curl to GMS?
    # rootLogger.info("hello world is called")
    return JSONResponse(
        content={
            "message": "<b>Hello world</b>",
            "timestamp": int(time.time() * 1000)
            # 'timestamp': 1636964967000
        },
        status_code=200,
    )


@app.post("/update_browsepath")
async def update_browsepath(item: browsepath_params):

    rootLogger.info("update_browsepath_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        dataset_snapshot = DatasetSnapshot(
            urn=item.dataset_name,
            aspects=[],
        )
        all_paths = []
        for path in item.browsePaths:
            all_paths.append(path + "dataset")
        browsepath_aspect = make_browsepath_mce(path=all_paths)
        dataset_snapshot.aspects.append(browsepath_aspect)
        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=item.requestor,
            event="UI Update Browsepath",
            token=item.user_token,
        )
        return JSONResponse(
            content=response.get("message", ""), status_code=response.get("status_code")
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_browsepath) from {user}"
        )
        return JSONResponse(content="Authentication Failed", status_code=401)


@app.post("/update_samples")
async def update_samples(item: add_sample_params):

    rootLogger.info("update_sample_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        generated_mcp = make_profile_mcp(
            sample_values=item.samples,
            timestamp=item.timestamp,
            datasetName=datasetName,
        )
        response = emit_mcp_respond(
            metadata_record=generated_mcp,
            owner=item.requestor,
            event="Update Dataset Profile",
            token=item.user_token,
        )
        return JSONResponse(
            content=response.get("message", ""), status_code=response.get("status_code")
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_samples) from {user}"
        )
        return JSONResponse(content="Authentication Failed", status_code=401)


@app.post("/delete_samples")
async def delete_samples(item: delete_sample_params):

    rootLogger.info("delete_sample_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    headers = {
        "Content-Type": "application/json",
    }
    elastic_host = os.environ["ELASTIC_HOST"]
    if authenticate_action(token=token, user=user, dataset=datasetName):
        data = """{{"query":{{"bool":{{"must":[{{"match":{{"timestampMillis":{timestamp}}}}},
            {{"match":{{"urn":"{urn}"}}}}]}}}}}}""".format(
            urn=datasetName, timestamp=str(item.timestamp)
        )
        response = requests.post(
            "{es_host}/dataset_datasetprofileaspect_v1/_delete_by_query".format(
                es_host=elastic_host
            ),
            headers=headers,
            data=data,
        )
        return JSONResponse(content=response.text, status_code=response.status_code)
    else:
        rootLogger.error(
            f"authentication failed for request\
            (delete_sample) from {user}"
        )
        return JSONResponse(content="Authentication Failed", status_code=401)


@app.post("/update_schema")
async def update_schema(item: schema_params):

    rootLogger.info("update_dataset_schema_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
        )
        rootLogger.info(
            f"token check:\
            {verify_token(item.user_token, item.requestor)}"
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
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=item.requestor,
            event="UI Update Schema",
            token=item.user_token,
        )
        return JSONResponse(
            content=response.get("message", ""), status_code=response.get("status_code")
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_schema) from {user}"
        )
        return JSONResponse(content="Authentication Failed", status_code=401)


@app.post("/update_properties")
async def update_prop(item: prop_params):
    # i expect the following:
    # name: do not touch
    # schema will generate schema metatdata (not the editable version)
    # properties: get description from graphql and props from form.
    # This will form DatasetProperty (Not EditableDatasetProperty)
    # platform info: needed for schema

    rootLogger.info("update_dataset_property_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
        )
        description = item.description
        properties = item.properties
        all_properties = {}
        for prop in properties:
            if "propertyKey" and "propertyValue" in prop:
                all_properties[prop.get("propertyKey")] = prop.get("propertyValue")
        property_aspect = make_dataset_description_mce(
            dataset_name=datasetName,
            description=description,
            customProperties=all_properties,
        )
        dataset_snapshot.aspects.append(property_aspect)
        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=item.requestor,
            event="UI Update Properties",
            token=token,
        )
        return JSONResponse(
            content=response.get("message", ""), status_code=response.get("status_code")
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_props) from {user}"
        )
        return JSONResponse(content="Authentication Failed", status_code=401)


def emit_mce_respond(
    metadata_record: MetadataChangeEvent, owner: str, event: str, token: str
) -> dict():
    datasetName = metadata_record.proposedSnapshot.urn
    for mce in metadata_record.proposedSnapshot.aspects:
        if not mce.validate():
            rootLogger.error(f"{mce.__class__} is not defined properly")
            return {
                "status_code": 400,
                "messsage": f"MCE was incorrectly defined.\
                    {event} was aborted",
            }

    if CLI_MODE:
        generate_json_output_mce(metadata_record, "./logs/")
    else:
        generate_json_output_mce(metadata_record, "/var/log/ingest/json/")
    try:
        rootLogger.info(metadata_record)
        emitter = DatahubRestEmitter(rest_endpoint, token=token)
        emitter.emit_mce(metadata_record)
        emitter._session.close()
    except Exception as e:
        rootLogger.error(e)
        return {
            "status_code": 500,
            "messsage": f"{event} failed because upstream error {e}",
        }
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully"
    )
    return {
        "status_code": 201,
        "messsage": f"{event} completed successfully",
    }


def emit_mcp_respond(
    metadata_record: MetadataChangeProposalWrapper, owner: str, event: str, token: str
) -> dict():
    datasetName = metadata_record.entityUrn
    if CLI_MODE:
        generate_json_output_mcp(metadata_record, "./logs/")
    else:
        generate_json_output_mcp(metadata_record, "/var/log/ingest/json/")
    try:
        rootLogger.info(metadata_record)
        emitter = DatahubRestEmitter(rest_endpoint, token=token)
        emitter.emit_mcp(metadata_record)
        emitter._session.close()
    except Exception as e:
        rootLogger.error(e)
        return {
            "status_code": 500,
            "messsage": f"{event} failed because upstream error {e}",
        }
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully"
    )
    return {
        "status_code": 201,
        "messsage": f"{event} completed successfully",
    }


@app.post("/make_dataset")
async def create_item(item: create_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    #todo - to revisit to see if refactoring is needed when make_json is up.
    """
    rootLogger.info("make_dataset_request_received {}".format(item))
    item.dataset_type = determine_type(item.dataset_type)
    token = item.user_token
    user = item.dataset_owner
    if verify_token(token, user):
        item.dataset_name = "{}_{}".format(item.dataset_name, str(get_sys_time()))
        datasetName = make_dataset_urn(item.dataset_type, item.dataset_name)
        platformName = make_platform(item.dataset_type)
        item.browsepathList = [
            item + "/" for item in item.browsepathList if not item.endswith("//")
        ]
        # this line is in case the endpoint is called by API and not UI,
        # which will enforce ending with /.
        browsepaths = [path + "dataset" for path in item.browsepathList]

        requestor = make_user_urn(item.dataset_owner)
        headerRowNum = (
            "n/a"
            if item.dict().get("hasHeader", "n/a") == "no"
            else str(item.dict().get("headerLine", "n/a"))
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

        dataset_description = (
            item.dataset_description if item.dataset_description else ""
        )
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

        dataset_snapshot.aspects.append(
            make_ownership_mce(actor=requestor, dataset_urn=datasetName)
        )
        dataset_snapshot.aspects.append(make_browsepath_mce(path=browsepaths))
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
        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=requestor,
            event="Create Dataset",
            token=token,
        )
        return JSONResponse(
            content=response.get("message", ""), status_code=response.get("status_code")
        )
    else:
        return JSONResponse(content="Authentication Failed", status_code=401)


@app.post("/update_dataset_status")
async def delete_item(item: dataset_status_params) -> None:
    """
    This endpoint is to support soft delete of datasets. Still require
    a database/ES chron job to remove the entries though, it only
    suppresses it from search and UI
    """
    rootLogger.info("remove_dataset_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        mce = make_status_mce(
            dataset_name=item.dataset_name, desired_status=item.desired_state
        )
        response = emit_mce_respond(
            metadata_record=mce,
            owner=item.requestor,
            event=f"Status Update removed:{item.desired_state}",
            token=item.user_token,
        )
        return JSONResponse(
            content=response.get("message", ""), status_code=response.get("status_code")
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_schema) from {user}"
        )
        return JSONResponse(content="Authentication Failed", status_code=401)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=api_emitting_port)
