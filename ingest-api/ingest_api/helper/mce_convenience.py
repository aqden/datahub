"""Convenience functions for creating MCEs"""
import datetime
import json
import logging
import os
import time
from typing import Dict, List, Optional, TypeVar, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    AuditStampClass,
    BooleanTypeClass,
    BrowsePathsClass,
    BytesTypeClass,
    ChangeTypeClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    MapTypeClass,
    MetadataChangeEventClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

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
    Creates browsepath for dataset. By default,
    if not specified, Datahub assigns it to
    /prod/platform/datasetname
    """
    mce = BrowsePathsClass(paths=path)
    return mce


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
    Specifies Upstream Datasets relative to this dataset.
    Downstream is always referring to current dataset
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
        customProperties=customProperties,
    )


def make_schema_mce(
    dataset_urn: str,
    platformName: str,
    actor: str,
    fields: List[Dict[str, str]],
    primaryKeys: List[str] = None,
    foreignKeysSpecs: List[str] = None,
    system_time: int = None,
) -> MetadataChangeEventClass:
    if system_time:
        try:
            datetime.datetime.fromtimestamp(system_time / 1000)
            sys_time = system_time
        except:
            log.error("specified_time is out of range")
            sys_time = get_sys_time()
    else:
        sys_time = get_sys_time()

    for item in fields:
        item["nativeType"] = item.get("field_type", "")
        item["field_type"] = {
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
            "double": NumberTypeClass(),
            "date-time": TimeTypeClass(),
        }.get(item["field_type"])

    mce = SchemaMetadataClass(
        schemaName="OtherSchema",
        platform=platformName,
        version=0,
        created=AuditStampClass(time=sys_time, actor=actor),
        lastModified=AuditStampClass(time=sys_time, actor=actor),
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=item["fieldPath"],
                type=SchemaFieldDataTypeClass(type=item["field_type"]),
                nativeDataType=item.get("nativeType", ""),
                description=item.get("field_description", ""),
                nullable=item.get("nullable", None),
            )
            for item in fields
        ],
        primaryKeys=primaryKeys,  # no visual impact in UI
        foreignKeysSpecs=None,
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


def make_dataprofile(
    samples: Dict[str, List[str]],
    data_rowcount: int,
    fields: List[Dict[str, str]],
    dataset_name: str,
    specified_time: int,
) -> MetadataChangeProposalWrapper:

    dataset_profile = DatasetProfileClass(
        timestampMillis=int(time.time() * 1000)
        if not specified_time
        else specified_time,
        rowCount=data_rowcount if data_rowcount > 0 else None,
        columnCount=len(fields) if len(fields) > 0 else None,
        fieldProfiles=[
            DatasetFieldProfileClass(
                fieldPath=key, sampleValues=[str(item) for item in samples[key]]
            )
            for key in samples.keys()
        ],
    )
    metadata_proposal = MetadataChangeProposalWrapper(
        entityType="dataset",
        aspectName="datasetProfile",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_name,
        aspect=dataset_profile,
    )
    return metadata_proposal


def generate_mce_json_output(
    mce: MetadataChangeEventClass,
    data_sample: Union[MetadataChangeProposalWrapper, None],
    file_loc: str,
) -> None:
    """
    Generates the json MCE files that can be ingested via CLI. For debugging
    """
    if data_sample:
        dataset_obj = [mce.to_obj(), data_sample.to_obj()]
    else:
        dataset_obj = [mce.to_obj()]
    file_name = mce.proposedSnapshot.urn.replace(
        "urn:li:dataset:(urn:li:dataPlatform:", ""
    ).split(",")[1]
    path = os.path.join(file_loc, f"{file_name}.json")

    with open(path, "w") as f:
        json.dump(dataset_obj, f, indent=4)


def make_delete_mce(
    dataset_name: str,
) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name, aspects=[StatusClass(removed=True)]
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
