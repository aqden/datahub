import requests

# files = {'myfile': open('bootstrap_mce.json','rb')}
# values = {'user_id': '123456'}
# url = 'http://localhost:8002/upload'
# r = requests.post(url, files=files, data=values)
# print(r.status_code)
# print(r.text)

import jwt

token="eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRlbW8iLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY2MTgzMDEyMywianRpIjoiZGRlMmFjYWYtYWU0Mi00NTYxLTk2MWUtMGFjZWM2YWZhMDhlIiwic3ViIjoiZGVtbyIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.hR_Jo53-DusWE1sXHrQz9liGcbQ_FElBBf5zvcitne0"
# payload = jwt.decode(token, secret, algorithms="HS256")

# print(payload)

# secret = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94="
# generated_payload = {
#     'actorType': 'USER', 
#     'actorId': 'demo', 
#     'type': 'PERSONAL', 
#     'version': '1', 
#     'exp': 1661913556, #or any expiry time actually
#     'jti': '1', 
#     'sub': 'demo', 
#     'iss': 'datahub-metadata-service'
# }
# new_token = jwt.encode(generated_payload, secret, algorithm="HS256")
# print(new_token)
import json
import requests

jwt_secret = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94="
rest_endpoint = "http://localhost:8080"
import time
def impersonate_token(user_id: str) -> str:    
    """
    since i need to submit as user, i need the token of user
    """
    temp_expiry = int(time.time()) + 600
    impersonated_payload = {
        'actorType': 'USER', 
        'actorId': f"{user_id}", 
        'type': 'PERSONAL', 
        'version': '1', 
        'exp': temp_expiry, 
        'jti': '1', 
        'sub': f'{user_id}', 
        'iss': 'datahub-metadata-service'
    }
    new_token = jwt.encode(impersonated_payload, jwt_secret, algorithm="HS256")
    return new_token
def check_user_exist(user_id:str) -> bool:
    """
    ensure that this user exist in Datahub
    """
    query_token = impersonate_token("datahub")
    print(f"query token {query_token}")
    headers={}
    headers["Authorization"] = f"Bearer {query_token}"
    headers["Content-Type"] = "application/json"
    query = """
    query existence($urn: String!){
        entityExists(urn: $urn) 
    }
    """    
    variables = {"urn": f"urn:li:corpuser:{user_id}"}
    
    # print(headers)
    # print(query)    
    # print(variables)
    
    resp = requests.post(
        f"{rest_endpoint}/api/graphql", headers=headers, json={"query": query, "variables": variables}
    )
    
    data_received = json.loads(resp.text)
    print(data_received)    
    doesExists = bool(data_received["data"]['entityExists'])
    return doesExists

print(check_user_exist("syste2"))
