import json
from typing import List
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent, MetadataChangeProposal
from datahub.metadata.schema_classes import *
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
    DatasetSnapshot
import time
import os
# def generate_json_output_mce(mce: MetadataChangeEventClass, file_loc: str) -> None:
#     """
#     Generates the json MCE files that can be ingested via CLI. For debugging
#     """
#     mce_obj = [mce.to_obj()]
#     sys_time = int(time.time() * 1000)
#     file_name = mce.proposedSnapshot.urn.replace(
#         "urn:li:dataset:(urn:li:dataPlatform:", ""
#     ).split(",")[1]
#     path = os.path.join(file_loc, f"{file_name}_{sys_time}.json")

#     with open(path, "w") as f:
#         json.dump([item for item in mce_obj], f, indent=4)


# def generate_json_output_mcp(mcp: MetadataChangeProposalWrapper, file_loc: str) -> None:
#     """
#     Generates the json MCE files that can be ingested via CLI. For debugging
#     """
#     mcp_obj = [mcp.to_obj()]
#     sys_time = int(time.time() * 1000)
#     file_name = mcp.entityUrn.replace("urn:li:dataset:(urn:li:dataPlatform:", "").split(
#         ","
#     )[1]
#     path = os.path.join(file_loc, f"{file_name}_{sys_time}.json")

#     with open(path, "w") as f:
#         json.dump([item for item in mcp_obj], f, indent=4)
output_file = "100mcemcp2.json"
all_objs=[]
for i in range(0,100):
    platform = "kafka"
    name = f"panother_testdataset{i}"
    dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"
    platformUrn = f"urn:li:dataPlatform:{platform}"

    browsepath = "/mass1000/dataset"
    browse = BrowsePathsClass(paths=[browsepath])
    owners = OwnershipClass(
        owners=[
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
    dataset_snapshot.aspects.append(owners)
    dataset_snapshot.aspects.append(browse)
    mce = MetadataChangeEvent(
        proposedSnapshot=dataset_snapshot,
        systemMetadata=SystemMetadataClass(
            runId=f"make_{str(int(time.time()))}"
        ),
    )
    # generate_json_output_mce(mce, path)

    all_objs.append(mce.to_obj())
    props = DatasetPropertiesClass(
        name = name,
        customProperties={"props1":"values1"}, 
        description=f"{name} description",
    )
    mcp = MetadataChangeProposalWrapper(
        aspect = props,
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspectName="datasetProperties",
        systemMetadata=SystemMetadataClass(
            runId=f"custom_ownership_{str(int(time.time()))}"
        ),
    )
    all_objs.append(mcp.to_obj())

# requester = "urn:li:corpuser:datahub"
# path = ""
# platform = "kudu"
# name = "leakset2"
# dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"
# platformUrn = f"urn:li:dataPlatform:{platform}"
# props = DatasetPropertiesClass(
#         name = name,
#         customProperties={"props1":"values1"}, 
#         description=f"{name} description",
#     )
# mcp = MetadataChangeProposalWrapper(
#     aspect = props,
#     entityType="dataset",
#     changeType=ChangeTypeClass.UPSERT,
#     entityUrn=dataset_urn,
#     aspectName="datasetProperties",
#     systemMetadata=SystemMetadataClass(
#         runId=f"custom_ownership_{str(int(time.time()))}"
#     ),
# )

# owners = OwnershipClass(
#         owners=[
#             OwnerClass(
#                 owner=f"urn:li:corpuser:datahub",
#                 type=OwnershipTypeClass.PRODUCER,
#             )            
#         ],        
#     )
# mcp = MetadataChangeProposalWrapper(
#     aspect = owners,
#     entityType="dataset",
#     changeType=ChangeTypeClass.UPSERT,
#     entityUrn=dataset_urn,
#     aspectName="ownership",
#     systemMetadata=SystemMetadataClass(
#         runId=f"custom_ownership_{str(int(time.time()))}"
#     ),
# )

# container_urn = "urn:li:container:temp123"
# container = ContainerPropertiesClass(
#     name= "container123",
#     customProperties={},    
#     description = "container desc",            
# )
# mcp = MetadataChangeProposalWrapper(
#     aspect = container,
#     entityType="container",
#     changeType=ChangeTypeClass.UPSERT,
#     entityUrn=container_urn,
#     aspectName="containerProperties",
#     systemMetadata=SystemMetadataClass(
#         runId=f"container_{str(int(time.time()))}"
#     ),
# )
# file_obj.append(mcp.to_obj())

with open(output_file, "w") as f:
    json.dump([item for item in all_objs], f, indent=4)
