import json
import logging
import pathlib
from typing import Iterable, Union

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)


def _to_obj_for_file(
    obj: Union[
        MetadataChangeEvent,
        MetadataChangeProposal,
        MetadataChangeProposalWrapper,
    ],
    simplified_structure: bool = True,
) -> dict:
    if isinstance(obj, MetadataChangeProposalWrapper):
        return obj.to_obj(simplified_structure=simplified_structure)
    return obj.to_obj()


class FileSinkConfig(ConfigModel):
    filename: str

    legacy_nested_json_string: bool = False


class FileSink(Sink[FileSinkConfig, SinkReport]):
    def __post_init__(self) -> None:
        fpath = pathlib.Path(self.config.filename)
        self.file = fpath.open("w")
        self.file.write("[\n")
        self.wrote_something = False

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        record = record_envelope.record
        obj = _to_obj_for_file(
            record, simplified_structure=not self.config.legacy_nested_json_string
        )

        if self.wrote_something:
            self.file.write(",\n")

        json.dump(obj, self.file, indent=4)
        self.wrote_something = True

        self.report.report_record_written(record_envelope)
        if write_callback:
            write_callback.on_success(record_envelope, {})

    def close(self):
        self.file.write("\n]")
        self.file.close()


def write_metadata_file(
    file: pathlib.Path,
    records: Iterable[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ],
) -> None:
    # This simplified version of the FileSink can be used for testing purposes.
    with file.open("w") as f:
        f.write("[\n")
        for i, record in enumerate(records):
            if i > 0:
                f.write(",\n")
            obj = _to_obj_for_file(record)
            json.dump(obj, f, indent=4)
        f.write("\n]")
