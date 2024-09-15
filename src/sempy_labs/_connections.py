import sempy.fabric as fabric
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from typing import Optional
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._helper_functions import pagination


principal_types = ['Group', 'ServicePrincipal', 'ServicePrincipalProfile', 'User']
connection_roles = ['Owner', 'User', 'UserWithReshare']

def list_connections() -> pd.DataFrame:
    """
    Lists all available connections.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all available connections.
    """

    client = fabric.FabricRestClient()
    response = client.get("/v1/connections")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Connection Id",
            "Connection Name",
            "Gateway Id",
            "Connectivity Type",
            "Connection Path",
            "Connection Type",
            "Privacy Level",
            "Credential Type",
            "Single Sign on Type",
            "Connection Encyrption",
            "Skip Test Connection",
        ]
    )

    for i in response.json().get("value", []):
        connection_details = i.get("connectionDetails", {})
        credential_details = i.get("credentialDetails", {})

        new_data = {
            "Connection Id": i.get("id"),
            "Connection Name": i.get("displayName"),
            "Gateway Id": i.get("gatewayId"),
            "Connectivity Type": i.get("connectivityType"),
            "Connection Path": connection_details.get("path"),
            "Connection Type": connection_details.get("type"),
            "Privacy Level": i.get("privacyLevel"),
            "Credential Type": (
                credential_details.get("credentialType") if credential_details else None
            ),
            "Single Sign On Type": (
                credential_details.get("singleSignOnType")
                if credential_details
                else None
            ),
            "Connection Encryption": (
                credential_details.get("connectionEncryption")
                if credential_details
                else None
            ),
            "Skip Test Connection": (
                credential_details.get("skipTestConnection")
                if credential_details
                else None
            ),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    bool_cols = ["Skip Test Connection"]
    df[bool_cols] = df[bool_cols].astype(bool)

    return df


def create_connection_cloud(
    connection_name: str,
    server_name: str,
    database_name: str,
    credential_type: str,
    user_name: str,
    password: str,
    privacy_level: str,
    connectivity_type: Optional[str] = "ShareableCloud",
    single_sign_on_type: Optional[str] = 'None',
    encryption: Optional[str] = 'Encyrpted',
):
    """
    Creates a cloud connection.

    Parameters
    ----------
    connection_name: str
        Name of the connection.
    server_name : str
    database_name : str
    credential_type : str
        The `credential type <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#credentialtype>`_.
    user_name : str
    password : str
    privacy_level : str
        The `privacy level <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#privacylevel>`_.
    connectivity_type : str, default="ShaeableCloud"
        The `connectivity type <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#connectivitytype>`_.
    single_sign_on_type: str, default="None"
        The `single sign on type <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#singlesignontype>`_.
    encryption : str, defualt="Encrypted"
        The `connection encryption <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection?tabs=HTTP#connectionencryption>`_.
    """

    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection?branch=features%2Fdmts&tabs=HTTP

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": connectivity_type,
        "name": connection_name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
                {"name": "server", "value": server_name},
                {"name": "database", "value": database_name},
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": single_sign_on_type,
            "connectionEncryption": encryption,
            "skipTestConnection": False,
            "credentials": {
                "credentialType": credential_type,
                "username": user_name,
                "password": password,
            },
        },
    }

    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)


def create_connection_on_prem(
    connection_name: str,
    gateway_id: str,
    server_name: str,
    database_name: str,
    credential_type: str,
    credentials: str,
    privacy_level: str,
    connectivity_type: Optional[str] = "OnPremisesDataGateway",
    single_sign_on_type: Optional[str] = 'None',
    encryption: Optional[str] = 'Encyrpted',
):
    """
    Creates an on premises connection.

    Parameters
    ----------
    connection_name: str
        Name of the connection.
    """

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": connectivity_type,
        "gatewayId": gateway_id,
        "name": connection_name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
                {"name": "server", "value": server_name},
                {"name": "database", "value": database_name},
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": single_sign_on_type,
            "connectionEncryption": encryption,
            "skipTestConnection": False,
            "credentials": {
                "credentialType": credential_type,
                "values": [{"gatewayId": gateway_id, "credentials": credentials}],
            },
        },
    }

    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)


def create_connection_vnet(
    connection_name: str,
    gateway_id: str,
    server_name: str,
    database_name: str,
    user_name: str,
    password: str,
    credential_type: str,
    privacy_level: str,
    connectivity_type: Optional[str] = "VirtualNetworkDataGateway",
    single_sign_on_type: Optional[str] = 'None',
    encryption: Optional[str] = 'Encyrpted',
):
    """
    Creates an virtual network gateway connection.

    Parameters
    ----------
    connection_name: str
        Name of the connection.
    """

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": connectivity_type,
        "gatewayId": gateway_id,
        "name": connection_name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
                {"name": "server", "value": server_name},
                {"name": "database", "value": database_name},
            ],
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": single_sign_on_type,
            "connectionEncryption": encryption,
            "skipTestConnection": False,
            "credentials": {
                "credentialType": credential_type,
                "username": user_name,
                "password": password,
            },
        },
    }

    response = client.post("/v1/connections", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)


def add_connection_role_assignemnt(connection_name: str, email_address: str, principal_type: str, role: str):

    from sempy_labs._helper_functions import resolve_connection_id

    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/add-connection-role-assignment?branch=drafts%2Fdev%2Fabvarshney%2Fgw_serviceprincipalprofiles&tabs=HTTP

    connection_id = resolve_connection_id(connection_name=connection_name)

    if principal_type not in principal_types:
        raise ValueError(f"{icons.red_dot} '{principal_type}' is not a valid 'principal_type'. Valid options: {principal_types}.")
    if role not in connection_roles:
        raise ValueError(f"{icons.red_dot} '{role}' is not a valid 'role'. Valid options: {connection_roles}.")

    payload = {
        "principal": {
            "id": email_address,
            "type": principal_type,
        },
        "role": role
    }
    client = fabric.FabricRestClient()
    response = client.post(f"/v1/connections/{connection_id}/roleAssignments", payload=payload)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{email_address}' user was added as a '{role}' role to the '{connection_name}' connection.")


def delete_connection(connection_name: str):

    from sempy_labs._helper_functions import resolve_connection_id

    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/delete-connection?branch=drafts%2Fdev%2Fabvarshney%2Fgw_serviceprincipalprofiles&tabs=HTTP

    connection_id = resolve_connection_id(connection_name=connection_name)

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/connections/{connection_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    
    print(f"{icons.green_dot} The '{connection_name}' connection has been deleted.")
    

#def delete_connection_role_assignment(connection_name: str, user_name: str):
    
    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/delete-connection-role-assignment?branch=drafts%2Fdev%2Fabvarshney%2Fgw_serviceprincipalprofiles&tabs=HTTP

#    connection_id = resolve_connection_id(connection_name=connection_name)
#    connection_role_assignment_id = ''

#    client = fabric.FabricRestClient()
#    response = client.delete(f"/v1/connections/{connection_id}/roleAssignments/{connection_role_assignment_id}")

#    if response.status_code != 200:
#        raise FabricHTTPException(response)

#    print(f"{icons.green_dot} The '{user_name}' user's role assignment has been deleted from the '{connection_name}' connection.")

def list_connection_role_assignments(connection_name: str):

    from sempy_labs._helper_functions import resolve_connection_id
    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connection-role-assignments?branch=drafts%2Fdev%2Fabvarshney%2Fgw_serviceprincipalprofiles&tabs=HTTP
    connection_id = resolve_connection_id(connection_name=connection_name)

    df = pd.DataFrame(
        columns=[
            "Connection Role Assignment Id",
            "Principal Id",
            "Principal Type",
            "Role",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/connections/{connection_id}/roleAssignments")
    if response.status_code != 200:
        raise FabricHTTPException(response)
    
    responses = pagination(client, response)
    
    for r in responses:
        for v in r.get('value', []):
            new_data = {
                "Connection Role Assignment Id": v.get("id"),
                "Principal Id": v.get('princiapl', {}).get('id'),
                "Princiapl Type": v.get('principal', {}).get('type'),
                "Role": v.get('role'),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    
    return df


def update_connection():

    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/update-connection?branch=drafts%2Fdev%2Fabvarshney%2Fgw_serviceprincipalprofiles&tabs=HTTP


#def update_connection_role_assignment():

    # https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/update-connection-role-assignment?branch=drafts%2Fdev%2Fabvarshney%2Fgw_serviceprincipalprofiles&tabs=HTTP