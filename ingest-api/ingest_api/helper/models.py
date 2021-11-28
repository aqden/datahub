from typing import Any, Dict, List, Optional, TypeVar, Union

from pydantic import BaseModel, validator
from typing_extensions import TypedDict


class FieldParam(BaseModel):
    field_name: str
    field_type: str
    field_description: str = None

class FieldParamEdited(BaseModel):
    key: int
    fieldName: str
    nativeDataType: Optional[str]
    datahubType: str
    fieldTags: Optional[List[str]]
    fieldGlossaryTerms: Optional[List[str]]
    fieldDescription: Optional[str]
    editKey: str

class create_dataset_params(BaseModel):
    dataset_name: str
    dataset_type: Union[str, Dict[str, str]]
    fields: List[FieldParam]
    dataset_owner: str = "no_owner"
    dataset_description: str = ""
    dataset_location: str = ""
    dataset_origin: str = ""
    hasHeader: str = "n/a"
    headerLine: int = 1

    class Config:
        schema_extra = {
            "example": {
                "dataset_name": "name of dataset",
                "dataset_type": "text/csv",
                "dataset_description": "What this dataset is about...",
                "dataset_owner": "12345",
                "dataset_location": "the file can be found here @...",
                "dataset_origin": "this dataset found came from... ie internet",
                "hasHeader": "no",
                "headerLine": 1,
                "dataset_fields": [
                    {
                        "field_name": "columnA",
                        "field_type": "string",
                        "field_description": "what is column A about",
                    },
                    {
                        "field_name": "columnB",
                        "field_type": "num",
                        "field_description": "what is column B about",
                    },
                ],
            }
        }

    # @validator('dataset_name')
    # def dataset_name_alphanumeric(cls, v):
    #     assert len(set(v).difference(ascii_letters+digits+' -_/\\'))==0, 'dataset_name must be alphanumeric/space character only'
    #     return v
    # @validator('dataset_type')
    # def dataset_type_alphanumeric(cls, v):
    #     assert v.isalpha(), 'dataset_type must be alphabetical string only'
    #     return v


class dataset_status_params(BaseModel):
    dataset_name: str
    requestor: str
    desired_state: bool

class browsepath_params(BaseModel):
    dataset_name: str
    requestor: str
    browsePaths: List[str]

class schema_params(BaseModel):
    dataset_name: str
    requestor: str    
    dataset_fields: List[FieldParamEdited]

class prop_params(BaseModel):
    dataset_name: str
    requestor: str
    description: str
    # properties: 

class echo_param(BaseModel):
    user_input: Any
    class Config:
        arbitary_types_allowed = True


def determine_type(type_input: Union[str, Dict[str, str]]) -> str:
    """
    this list will grow when we have more dataset types in the form
    """
    if isinstance(type_input, Dict):
        type_input = type_input.get("dataset_type", "")
    if (type_input.lower() == "text/csv") or (
        type_input.lower() == "application/octet-stream"
    ):
        return "csv"
    if type_input.lower() == "json":
        return "json"
    else:
        return "undefined"
