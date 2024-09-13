# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install gql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vGraphQLEndPoint = "https://api.fabric.microsoft.com/v1/workspaces/0fc26c38-56b5-4506-95d6-d24363c97398/graphqlapis/23a05d25-10b4-44a9-876a-cc5acd105e7f/graphql"  # GraphQL API Endpoint
vScope = "https://analysis.windows.net/powerbi/api" 
vAccessToken  = mssparkutils.credentials.getToken(vScope)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# import requests
# 
# # Define the GraphQL endpoint
# url = "https://api.fabric.microsoft.com/v1/workspaces/0fc26c38-56b5-4506-95d6-d24363c97398/graphqlapis/23a05d25-10b4-44a9-876a-cc5acd105e7f/graphql"
# 
# # Define the query
# query = """
# {
#    suppliers (first: 100){
#    items {
#       s_suppkey
#       s_name
#       s_address
#       s_nationkey
#       s_phone
#       s_acctbal
#       s_comment
#       }  
#    }
# }  
# """
# 
# # Send the request to the GraphQL endpoint
# response = requests.post(url, json={'query': query})
# 
# # Check for errors
# if response.status_code == 200:
#     # Parse the response as JSON
#     result = response.json()
#     # Print the result
#     print(result)
# else:
#     print(f"Query failed to run by returning code of {response.status_code}. {response.text}")


# CELL ********************

import asyncio
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

async def main():

    transport = AIOHTTPTransport(url=vGraphQLEndPoint, headers={"Authorization": f"Bearer {vAccessToken}"})
    client = Client(transport=transport, fetch_schema_from_transport=False)

    # GraphQL query
    query = gql("""
        query {
            suppliers (first: 100){
            items {
                s_suppkey
                s_name
                s_address
                s_nationkey
                s_phone
                s_acctbal
                s_comment
                }  
            }
        }
    """)

    # Execute query
    response = await client.execute_async(query)
    print(response)


# if __name__ == "__main__":
#     asyncio.run(main())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    loop = asyncio.get_running_loop()
except RuntimeError:  # 'RuntimeError: There is no current event loop...'
    loop = None

if loop and loop.is_running():
    print('Async event loop already running. Adding coroutine to the event loop.')
    tsk = loop.create_task(main())
    # ^-- https://docs.python.org/3/library/asyncio-task.html#task-object
    # Optionally, a callback function can be executed when the coroutine completes
    tsk.add_done_callback(
        lambda t: print(f'Task done with result={t.result()}  << return val of main()'))
else:
    print('Starting new event loop')
    result = asyncio.run(main())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
