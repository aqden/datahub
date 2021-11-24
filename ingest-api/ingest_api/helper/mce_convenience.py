"""Convenience functions for creating MCEs"""
import datetime
import json
import logging
import os
import time
from typing import Dict, List, Optional, Type, TypeVar, Union

from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import *
from .models import (FieldParamEdited)

log = logging.getLogger(__name__)

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"

T = TypeVar("T")


def get_sys_time() -> int:
    return int(time.time() * 1000)


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def make_path(platform: str, name: str, env: str = DEFAULT_FLOW_CLUSTER) -> str:
    return f"/{env}/{platform}/{name}"


def make_platform(platform: str) -> str:
    return f"urn:li:dataPlatform:{platform}"


def make_user_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def make_tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"

def make_institutionalmemory_mce(
    dataset_urn: str, input_url: List[str], input_description: List[str], actor: str
) -> InstitutionalMemoryClass:
    """
    returns a list of Documents
    """
    sys_time = get_sys_time()
    actor = make_user_urn(actor)
    mce = InstitutionalMemoryClass(
            elements=[
                InstitutionalMemoryMetadataClass(
                    url=url,
                    description=description,
                    createStamp=AuditStampClass(
                        time=sys_time,
                        actor=actor,
                    ),
                )
                for url, description in zip(input_url, input_description)
            ]
        )

    return mce

def make_browsepath_mce(
    dataset_urn: str,
    path: List[str],
) -> BrowsePathsClass:
    """
    Creates browsepath for dataset. By default, if not specified, Datahub assigns it to /prod/platform/datasetname
    """
    sys_time = get_sys_time()
    mce = BrowsePathsClass(paths=path)
    return mce

def derive_platform_name(input: str) -> str:
    """
    derive platform info, needed to create schemaaspect
    urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})
    """
    platform_name_env = input.replace("urn:li:dataset:(urn:li:dataPlatform:","")
    platform = platform_name_env.split(',')[0]
    platform_name = f"urn:li:dataPlatform:{platform}"
    return platform_name

def make_lineage_mce(
    upstream_urns: List[str],
    downstream_urn: str,
    actor: str,
    lineage_type: str = Union[
        DatasetLineageTypeClass.TRANSFORMED,
        DatasetLineageTypeClass.COPY,
        DatasetLineageTypeClass.VIEW,
    ],
) -> MetadataChangeEventClass:
    """
    Specifies Upstream Datasets relative to this dataset. Downstream is always referring to current dataset
    urns should be created using make_dataset_urn
    lineage have to be one of the 3
    """
    sys_time = get_sys_time()
    actor = actor
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=downstream_urn,
            aspects=[
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            auditStamp=AuditStampClass(
                                time=sys_time,
                                actor=actor,
                            ),
                            dataset=upstream_urn,
                            type=lineage_type,
                        )
                        for upstream_urn in upstream_urns
                    ]
                )
            ],
        )
    )
    return mce


def make_dataset_description_mce(
    dataset_name: str,
    description: str,
    externalUrl: str = None,
    tags: List[str] = [],
    customProperties: Optional[Dict[str, str]] = None,
) -> DatasetPropertiesClass:
    """
    Tags and externalUrl doesnt seem to have any impact on UI.
    """
    return DatasetPropertiesClass(
        description=description,
        externalUrl=externalUrl,
        customProperties=customProperties
    )

def update_field_param_class(field_inputs: List[FieldParamEdited]):
    """[summary]
    generate a list of FieldParams that can be used to create metadata schema aspect. 
    This is for the update page call.
    field_name: str
    field_native_type: str
    datahub_type: str
    field_description - need to pull from graphql
    nullable - need to pull from graphql
    This function is different from create_field_param because the field type is different.    
    """
    all_fields =[]

    for field in field_inputs:
        temp = field.dict()
        print(type(temp))
        temp['field_type'] = {
            "BOOLEAN": BooleanTypeClass(),
            "STRING": StringTypeClass(),            
            "BYTES": BytesTypeClass(),
            "NUMBER": NumberTypeClass(),
            "DATE": DateTypeClass(),
            "TIME": TimeTypeClass(),
            "ENUM": EnumTypeClass(),
            "NULL": NullTypeClass(),
            "RECORD": RecordTypeClass(),
            "ARRAY": ArrayTypeClass(),
            "UNION": UnionTypeClass(),
            "MAP": MapTypeClass(),
            "FIXED": FixedTypeClass(),            
        }.get(temp["datahubType"])
        field_class = SchemaFieldClass(
                fieldPath=temp["fieldName"],
                type=SchemaFieldDataTypeClass(type=temp["field_type"]),
                nativeDataType=temp.get("nativeDataType", ""),
                description=temp.get("fieldDescription", ""),
                nullable=temp.get("nullable", None),
        )
        all_fields.append(field_class)
    return all_fields

def create_field_param_class(inputs):
    """
    generate a list of FieldParams that can be used to create metadata schema aspect. 
    This is for the create page call.
    Args:
        inputs ([type]): [description]
    """
    all_fields =[]
    for field in inputs:
        field['field_type'] = {
            "boolean": BooleanTypeClass(),
            "string": StringTypeClass(),
            "bool": BooleanTypeClass(),
            "bytes": BytesTypeClass(),
            "number": NumberTypeClass(),
            "num": NumberTypeClass(),
            "integer": NumberTypeClass(),
            "date": DateTypeClass(),
            "time": TimeTypeClass(),
            "enum": EnumTypeClass(),
            "null": NullTypeClass(),
            "object": RecordTypeClass(),
            "array": ArrayTypeClass(),
            "union": UnionTypeClass(),
            "map": MapTypeClass(),
            "fixed": FixedTypeClass(),
            "double":NumberTypeClass(),
            "date-time":TimeTypeClass(),
        }.get(field["field_type"])
        field_class = SchemaFieldClass(
                fieldPath=field["fieldPath"],
                type=SchemaFieldDataTypeClass(type=field["field_type"]),
                nativeDataType=field.get("nativeType", ""),
                description=field.get("field_description", ""),
                nullable=field.get("nullable", None),
        )
        all_fields.append(field_class)
    return all_fields

def create_new_schema_mce(
    platformName: str,
    actor: str,
    fields: List[Dict[str, str]],
    system_time: int = None,
) -> MetadataChangeEventClass:
    if system_time:
        try:
            datetime.datetime.fromtimestamp(system_time / 1000)
            sys_time = system_time
        except ValueError as e:
            log.error("specified_time is out of range")
            sys_time = get_sys_time()
    else:
        sys_time = get_sys_time()    
    field_schemas = create_field_param_class(fields)
    mce = make_schema_mce(         
        platformName=platformName,
        actor=actor,
        fields= field_schemas,
        system_time = sys_time)
    return mce

def make_schema_mce(    
    platformName: str,
    actor: str,
    fields: List[SchemaFieldClass],
    system_time: int = None,
) -> MetadataChangeEventClass:
    if system_time:
        try:
            datetime.datetime.fromtimestamp(system_time / 1000)
            sys_time = system_time
        except ValueError as e:
            log.error("specified_time is out of range")
            sys_time = get_sys_time()
    else:
        sys_time = 0

    mce = SchemaMetadataClass(
        schemaName="OtherSchema",
        platform=platformName,
        version=0,
        created=AuditStampClass(time=sys_time, actor=actor),
        lastModified=AuditStampClass(time=sys_time, actor=actor),
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )
    return mce

def make_ownership_mce(actor: str, dataset_urn: str) -> OwnershipClass:
    return OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=actor,
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ],
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000),
                    actor=make_user_urn(actor),
                ),
            )
            
def generate_json_output(mce: MetadataChangeEventClass, file_loc: str) -> None:
    """
    Generates the json MCE files that can be ingested via CLI. For debugging
    """    
    mce_obj = mce.to_obj()
    sys_time = int(time.time() * 1000)
    file_name = mce.proposedSnapshot.urn.replace("urn:li:dataset:(urn:li:dataPlatform:", "").split(",")[1]
    path = os.path.join(file_loc, f"{file_name}_{sys_time}.json")

    with open(path, "w") as f:
        json.dump(mce_obj, f, indent=4)


def make_status_mce(
    dataset_name: str,
    desired_status: bool
) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name, aspects=[StatusClass(removed=desired_status)]
        )
    )


def make_recover_mce(
    dataset_name: str,
) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name, aspects=[StatusClass(removed=False)]
        )
    )
