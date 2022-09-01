import json
from typing import List
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent, MetadataChangeProposal
from datahub.metadata.schema_classes import *
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
    DatasetSnapshot
import time
import os
def generate_json_output_mce(mce: MetadataChangeEventClass, file_loc: str) -> None:
    """
    Generates the json MCE files that can be ingested via CLI. For debugging
    """
    mce_obj = [mce.to_obj()]
    sys_time = int(time.time() * 1000)
    file_name = mce.proposedSnapshot.urn.replace(
        "urn:li:dataset:(urn:li:dataPlatform:", ""
    ).split(",")[1]
    path = os.path.join(file_loc, f"{file_name}_{sys_time}.json")

    with open(path, "w") as f:
        json.dump([item for item in mce_obj], f, indent=4)


def generate_json_output_mcp(mcp: MetadataChangeProposalWrapper, file_loc: str) -> None:
    """
    Generates the json MCE files that can be ingested via CLI. For debugging
    """
    mcp_obj = [mcp.to_obj()]
    sys_time = int(time.time() * 1000)
    file_name = mcp.entityUrn.replace("urn:li:dataset:(urn:li:dataPlatform:", "").split(
        ","
    )[1]
    path = os.path.join(file_loc, f"{file_name}_{sys_time}.json")

    with open(path, "w") as f:
        json.dump([item for item in mcp_obj], f, indent=4)

requester = "urn:li:corpuser:datahub"
path = ""
platform = "hive"
name = "testdataset1"
dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"
platformUrn = f"urn:li:dataPlatform:{platform}"

browsepath = "/hive/mytest1/dataset"
browse = BrowsePathsClass(paths=[browsepath])
owners = OwnershipClass(
    owners=[
        OwnerClass(
            owner="urn:li:corpuser:abc",
            type=OwnershipTypeClass.DATAOWNER,
        ),
        OwnerClass(
            owner="urn:li:corpuser:datahub",
            type=OwnershipTypeClass.DATAOWNER,
        )
    ],
    
)

fields = []
for field in ["field1", "field2"]:
    fieldschema = SchemaFieldClass(
            fieldPath=field,
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="String",
            description=f"{field} description",
            nullable=None,
    )
    fields.append(fieldschema)
schema = SchemaMetadataClass(
        schemaName="OtherSchema",
        platform=platformUrn,
        version=0,
        created=AuditStampClass(time=int(time.time() * 1000), actor="urn:li:corpuser:cdf"),
        lastModified=AuditStampClass(time=int(time.time() * 1000), actor="urn:li:corpuser:cdf"),
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )
dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
    )

# print(browse.validate())
dataset_snapshot.aspects.append(schema)
# dataset_snapshot.aspects.append(owners)
dataset_snapshot.aspects.append(browse)
mce = MetadataChangeEvent(
    proposedSnapshot=dataset_snapshot,
    systemMetadata=SystemMetadataClass(
        runId=f"{requester}_make_{str(int(time.time()))}"
    ),
)
generate_json_output_mce(mce, path)
