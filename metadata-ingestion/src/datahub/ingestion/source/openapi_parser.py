import json
import logging
import re
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
import yaml
from requests.auth import HTTPBasicAuth

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchemaClass,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass, StringTypeClass

logger = logging.getLogger(__name__)


def flatten(d: dict, prefix: str = "") -> Generator:
    for k, v in d.items():
        if isinstance(v, dict):
            yield from flatten(v, f"{prefix}.{k}")
        else:
            yield f"{prefix}.{k}".strip(".")


def flatten2list(d: dict) -> list:
    """
    This function explodes dictionary keys such as:
        d = {"first":
            {"second_a": 3, "second_b": 4},
         "another": 2,
         "anotherone": {"third_a": {"last": 3}}
         }

    yeilds:

        ["first.second_a",
         "first.second_b",
         "another",
         "anotherone.third_a.last"
         ]
    """
    fl_l = list(flatten(d))
    return [d[1:] if d[0] == "-" else d for d in fl_l]


def request_call(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> requests.Response:
    headers = {"accept": "application/json"}

    if username is not None and password is not None:
        return requests.get(
            url, headers=headers, auth=HTTPBasicAuth(username, password)
        )

    elif token is not None:
        headers["Authorization"] = f"Bearer {token}"
        return requests.get(url, headers=headers)
    else:
        return requests.get(url, headers=headers)


def get_swag_json(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    swagger_file: str = "",
) -> Dict:
    tot_url = url + swagger_file
    if token is not None:
        response = request_call(url=tot_url, token=token)
    else:
        response = request_call(url=tot_url, username=username, password=password)

    if response.status_code != 200:
        raise Exception(f"Unable to retrieve {tot_url}, error {response.status_code}")
    try:
        dict_data = json.loads(response.content)
    except json.JSONDecodeError:  # it's not a JSON!
        dict_data = yaml.safe_load(response.content)
    return dict_data


def get_url_basepath(sw_dict: dict) -> str:
    try:
        return sw_dict["basePath"]
    except KeyError:  # no base path defined
        return ""


def check_sw_version(sw_dict: dict) -> int:
    if "swagger" in sw_dict:
        v_split = sw_dict["swagger"].split(".")
    else:
        v_split = sw_dict["openapi"].split(".")

    version = [int(v) for v in v_split]

    if version[0] == 3 and version[1] > 0:
        raise NotImplementedError(
            "This plugin is not compatible with Swagger version >3.0"
        )

    return version[0]


def get_endpoints(sw_dict: dict) -> dict:  # noqa: C901
    """
    Get all the URLs accepting the "GET" method, together with their description and the tags
    """
    url_details = {}

    sw_version = check_sw_version(sw_dict)

    for p_k, p_o in sw_dict["paths"].items():
        # will track only the "get" methods, which are the ones that give us data
        if "get" in p_o.keys():
            if "200" in p_o["get"]["responses"].keys():
                base_res = p_o["get"]["responses"]["200"]
            elif 200 in p_o["get"]["responses"].keys():
                # if you read a plain yml file the 200 will be an integer
                base_res = p_o["get"]["responses"][200]
            else:
                # the endpoint does not have a 200 response
                continue

            if "description" in p_o["get"].keys():
                desc = p_o["get"]["description"]
            elif "summary" in p_o["get"].keys():
                desc = p_o["get"]["summary"]
            else:  # still testing
                desc = ""

            try:
                tags = p_o["get"]["tags"]
            except KeyError:
                tags = []

            url_details[p_k] = {"description": desc, "tags": tags}

            # trying if dataset is defined in swagger...
            if "content" in base_res.keys():
                res_cont = base_res["content"]
                if "application/json" in res_cont.keys():
                    ex_field = None
                    if "example" in res_cont["application/json"]:
                        ex_field = "example"
                    elif "examples" in res_cont["application/json"]:
                        ex_field = "examples"

                    if ex_field:
                        if sw_version == 3:
                            for k, o in res_cont["application/json"][ex_field].items():
                                if "value" in o.keys():
                                    url_details[p_k]["data"] = o["value"]
                        else:
                            if isinstance(res_cont["application/json"][ex_field], dict):
                                url_details[p_k]["data"] = res_cont["application/json"][
                                    ex_field
                                ]
                            elif isinstance(
                                res_cont["application/json"][ex_field], list
                            ):
                                # taking the first example
                                url_details[p_k]["data"] = res_cont["application/json"][
                                    ex_field
                                ][0]
                    else:
                        logger.warning(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "text/csv" in res_cont.keys():
                    url_details[p_k]["data"] = res_cont["text/csv"]["schema"]
            elif "examples" in base_res.keys():
                url_details[p_k]["data"] = base_res["examples"]["application/json"]

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o["get"].keys():
                url_details[p_k]["parameters"] = p_o["get"]["parameters"]

    return dict(sorted(url_details.items()))


def guessing_url_name(url: str, examples: dict) -> str:
    """
    given a url and dict of extracted data, we try to guess a working URL. Example:
    url2complete = "/advancedcomputersearches/name/{name}/id/{id}"
    extr_data = {"advancedcomputersearches": {'id': 202, 'name': '_unmanaged'}}
    -->> guessed_url = /advancedcomputersearches/name/_unmanaged/id/202'
    """
    url2op = url[1:] if url[0] == "/" else url
    divisions = url2op.split("/")

    # the very first part of the url should stay the same.
    root = url2op.split("{")[0]

    needed_n = [
        a for a in divisions if not a.find("{")
    ]  # search for stuff like "{example}"
    cleaned_needed_n = [
        name[1:-1] for name in needed_n
    ]  # no parenthesis, {example} -> example

    # in the cases when the parameter name is specified, we have to correct the root.
    # in the example, advancedcomputersearches/name/ -> advancedcomputersearches/
    for field in cleaned_needed_n:
        if field in root:
            div_pos = root.find(field)
            if div_pos > 0:
                root = root[: div_pos - 1]  # like "base/field" should become "base"

    if root in examples:
        # if our root is contained in our samples examples...
        ex2use = root
    elif root[:-1] in examples:
        ex2use = root[:-1]
    elif root.replace("/", ".") in examples:
        ex2use = root.replace("/", ".")
    elif root[:-1].replace("/", ".") in examples:
        ex2use = root[:-1].replace("/", ".")
    else:
        return url

    # we got our example! Let's search for the needed parameters...
    guessed_url = url  # just a copy of the original url

    # substituting the parameter's name w the value
    for name, clean_name in zip(needed_n, cleaned_needed_n):
        if clean_name in examples[ex2use].keys():
            guessed_url = re.sub(name, str(examples[ex2use][clean_name]), guessed_url)

    return guessed_url


def compose_url_attr(raw_url: str, attr_list: list) -> str:
    """
    This function will compose URLs based on attr_list.
    Examples:
    asd = compose_url_attr(raw_url="http://asd.com/{id}/boh/{name}",
                           attr_list=["2", "my"])
    asd == "http://asd.com/2/boh/my"

    asd2 = compose_url_attr(raw_url="http://asd.com/{id}",
                           attr_list=["2",])
    asd2 == "http://asd.com/2"
    """
    splitted = re.split(r"\{[^}]+\}", raw_url)
    if splitted[-1] == "":  # it can happen that the last element is empty
        splitted = splitted[:-1]
    composed_url = ""

    for i_s, split in enumerate(splitted):
        try:
            composed_url += split + attr_list[i_s]
        except IndexError:  # we already ended to fill the url
            composed_url += split
    return composed_url


def maybe_theres_simple_id(url: str) -> str:
    dets = re.findall(r"(\{[^}]+\})", url)  # searching the fields between parenthesis
    if len(dets) == 0:
        return url
    dets_w_id = [det for det in dets if "id" in det]  # the fields containing "id"
    if len(dets) == len(dets_w_id):
        # if we only have fields containing IDs, we guess to use "1"s
        return compose_url_attr(url, ["1" for _ in dets_w_id])
    else:
        return url


def try_guessing(url: str, examples: dict) -> str:
    """
    We will guess the content of the url string...
    Any non-guessed name will stay as it was (with parenthesis{})
    """
    url_guess = guessing_url_name(url, examples)  # try to fill with known informations
    return maybe_theres_simple_id(url_guess)


def clean_url(url: str) -> str:
    protocols = ["http://", "https://"]
    for prot in protocols:
        if prot in url:
            parts = url.split(prot)
            return prot + parts[1].replace("//", "/")
    raise Exception(f"Unable to understand URL {url}")


def extract_fields(
    response: requests.Response, dataset_name: str
) -> Tuple[List[Any], Dict[Any, Any]]:
    """
    Given a URL, this function will extract the fields contained in the
    response of the call to that URL, supposing that the response is a JSON.

    The list in the output tuple will contain the fields name.
    The dict in the output tuple will contain a sample of data.
    """
    try:
        dict_data = json.loads(response.content)
    except json.JSONDecodeError:  # it's not a JSON!
        logger.warning(f"It is not valid JSON response --- {dataset_name}")
        return [], {}

    if isinstance(dict_data, str):
        # no sense
        logger.warning(f"Empty data --- {dataset_name}")
        return [], {}
    elif isinstance(dict_data, list):
        # it's maybe just a list
        if len(dict_data) == 0:
            logger.warning(f"Empty data --- {dataset_name}")
            return [], {}
        # so we take the fields of the first element,
        # if it's a dict
        if isinstance(dict_data[0], dict):
            return flatten2list(dict_data[0]), dict_data[0]
        elif isinstance(dict_data[0], str):
            # this is actually data
            return ["contains_a_string"], {"contains_a_string": dict_data[0]}
        else:
            raise ValueError("unknown format")
    if len(dict_data.keys()) > 1:
        # the elements are directly inside the dict
        return flatten2list(dict_data), dict_data
    dst_key = list(dict_data.keys())[
        0
    ]  # the first and unique key is the dataset's name

    try:
        return flatten2list(dict_data[dst_key]), dict_data[dst_key]
    except AttributeError:
        # if the content is a list, we should treat each element as a dataset.
        # ..but will take the keys of the first element (to be improved)
        if isinstance(dict_data[dst_key], list):
            if len(dict_data[dst_key]) > 0:
                return flatten2list(dict_data[dst_key][0]), dict_data[dst_key][0]
            else:
                return [], {}  # it's empty!
        else:
            logger.warning(f"Unable to get the attributes --- {dataset_name}")
            return [], {}


def get_tok(
    url: str,
    username: str = "",
    password: str = "",
    tok_url: str = "",
    method: str = "post",
) -> str:
    """
    Trying to post username/password to get auth.
    """
    token = ""
    url4req = url + tok_url
    if method == "post":
        # this will make a POST call with username and password
        data = {"username": username, "password": password}
        # url2post = url + "api/authenticate/"
        response = requests.post(url4req, data=data)
        if response.status_code == 200:
            cont = json.loads(response.content)
            token = cont["tokens"]["access"]
    elif method == "get":
        # this will make a GET call with username and password
        response = requests.get(url4req)
        if response.status_code == 200:
            cont = json.loads(response.content)
            token = cont["token"]
    else:
        raise ValueError(f"Method unrecognised: {method}")
    if token != "":
        return token
    else:
        raise Exception(f"Unable to get a valid token: {response.text}")


def set_metadata(
    dataset_name: str, fields: List, platform: str = "api"
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []

    for column in fields:
        field = SchemaField(
            fieldPath=column,
            nativeDataType="str",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            description="",
            recursive=False,
        )
        canonical_schema.append(field)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=canonical_schema,
    )
    return schema_metadata
