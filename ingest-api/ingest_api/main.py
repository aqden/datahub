import logging
import os
from logging.handlers import TimedRotatingFileHandler
from os import environ

import uvicorn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ingest_api.helper.mce_convenience import (
    generate_mce_json_output,
    get_sys_time,
    make_browsepath_mce,
    make_dataprofile,
    make_dataset_description_mce,
    make_dataset_urn,
    make_delete_mce,
    make_ownership_mce,
    make_platform,
    make_recover_mce,
    make_schema_mce,
    make_user_urn,
)
from ingest_api.helper.models import (
    create_dataset_params,
    dataset_status_params,
    determine_type,
)

# when DEBUG = true, im not running ingest_api from container,
# but from localhost python interpreter, hence need to change the
# endpoint used.
DEBUG = False
if environ.get("DATAHUB_URL") is not None:
    datahub_url = os.environ["DATAHUB_URL"]
else:
    datahub_url = "http://localhost:9002"
api_emitting_port = 80 if not DEBUG else 8001
rest_endpoint = "http://datahub-gms:8080" if not DEBUG else "http://localhost:8080"

rootLogger = logging.getLogger("__name__")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)

streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.DEBUG)
rootLogger.addHandler(streamLogger)

if not DEBUG:
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
    version="0.0.3",
)
origins = ["http://localhost:9002", "http://172.19.0.1:9002"]
if environ.get("ACCEPT_ORIGINS") is not None:
    new_origin = environ["ACCEPT_ORIGINS"]
    origins.append(new_origin)
    rootLogger.info(f"{new_origin} is added to CORS allow_origins")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST", "GET"],
)


@app.get("/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    # how to check that this dataset exist? - curl to GMS?
    rootLogger.info("hello world is called")
    return "API is alive"


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

    dataset_snapshot.aspects.append(
        make_ownership_mce(actor=requestor, dataset_urn=datasetName)
    )
    dataset_snapshot.aspects.append(
        make_browsepath_mce(dataset_urn=datasetName, path=[browsePath])
    )
    field_params = []
    for existing_field in item.fields:
        current_field = {}
        current_field.update(existing_field.dict())
        current_field["fieldPath"] = current_field.pop("field_name")
        if "field_description" not in current_field:
            current_field["field_description"] = ""
        field_params.append(current_field)

    dataset_snapshot.aspects.append(
        make_schema_mce(
            dataset_urn=datasetName,
            platformName=platformName,
            actor=requestor,
            fields=field_params,
        )
    )
    data_sample = (
        None
        if not item.dataset_samples
        else make_dataprofile(
            item.dataset_samples, item.dataset_rowcount, item.fields, datasetName
        )
    )
    rootLogger.error(f"data_sample is {data_sample}")
    metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    for mce in metadata_record.proposedSnapshot.aspects:
        if not mce.validate():
            rootLogger.error(f"{mce.__class__} is not defined properly")
            return JSONResponse(
                f"Dataset was not created because dataset definition has"
                f"encountered an error for"
                f"{mce.proposedSnapshot.aspects[0].__class__}",
                status_code=400,
            )
    if data_sample:
        if not data_sample.validate():
            rootLogger.error(f"{data_sample.__class__} is not " f"defined properly")
            return JSONResponse(
                "Dataset was not created because sample definition has "
                "encountered an error",
                status_code=400,
            )
    if not DEBUG:
        generate_mce_json_output(metadata_record, data_sample, "/var/log/ingest/json")
        rootLogger.info("saved to file")
    try:
        emitter = DatahubRestEmitter(rest_endpoint)
        emitter.emit_mce(metadata_record)
        if data_sample:
            emitter.emit(data_sample)
            rootLogger.info("emitted data sample")
        emitter._session.close()
    except Exception as e:
        rootLogger.debug(e)
        return JSONResponse(
            "Dataset was not created because GMS has "
            "encountered an error {}".format(e),
            status_code=500,
        )
    rootLogger.info(
        "Make_dataset_request_completed_for {} requested_by {}".format(
            item.dataset_name, item.dataset_owner
        )
    )
    return JSONResponse(
        "dataset can be found at /dataset/{}".format(
            make_dataset_urn(item.dataset_type, item.dataset_name)
        ),
        status_code=201,
    )


@app.post("/delete_dataset")
async def delete_item(item: dataset_status_params) -> None:
    """
    This endpoint is to support soft delete of datasets.
    Still require a database/ES chron job to remove the entries
    though, it only suppresses it from search and UI
    """
    # how to check that this dataset exist? - curl to GMS?
    rootLogger.info("remove_dataset_request_received {}".format(item))
    datasetName = make_dataset_urn(item.platform, item.dataset_name)
    mce = make_delete_mce(dataset_name=datasetName)
    try:
        emitter = DatahubRestEmitter(rest_endpoint)
        emitter.emit_mce(mce)
        emitter._session.close()
    except Exception as e:
        rootLogger.debug(e)
        return JSONResponse(
            "Request was not fulfilled because upstream "
            "has encountered an error {}".format(e),
            status_code=502,
        )
    rootLogger.info(
        "remove_dataset_request_completed_for {} requested "
        "by {}".format(item.dataset_name, item.requestor)
    )
    return JSONResponse(
        "dataset has been removed from search and UI. please " "refresh the webpage.",
        status_code=201,
    )


@app.post("/recover_dataset")
async def recover_item(item: dataset_status_params) -> None:
    """
    This endpoint is meant for undoing soft deletes.
    """
    rootLogger.info("recover_dataset_request_received {}".format(item))
    datasetName = make_dataset_urn(item.platform, item.dataset_name)
    mce = make_recover_mce(dataset_name=datasetName)
    try:
        emitter = DatahubRestEmitter(rest_endpoint)
        emitter.emit_mce(mce)
        emitter._session.close()
    except Exception as e:
        rootLogger.debug(e)
        return JSONResponse(
            "Request was not fulfilled because upstream has encountered an error {}".format(
                e
            ),
            status_code=502,
        )
    rootLogger.info(
        "recover_dataset_request_completed_for {} requested_by {}".format(
            item.dataset_name, item.requestor
        )
    )
    return JSONResponse("dataset has been restored", status_code=201)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=api_emitting_port)
