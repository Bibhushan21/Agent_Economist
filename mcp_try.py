# # import requests

# # # Define the URL for the MCP fetch endpoint
# # url = "http://localhost:5000/mcp/fetch"

# # # Define the payload for the request
# # payload = {
# #     "query": "Give me GDP per capita of India from 2000 to 2025"
# # }

# # # Send a POST request to the MCP server
# # response = requests.post(url, json=payload)

# # # Print the response from the server
# # print(response.json())


# import json
# import subprocess

# # Use a raw string to avoid Unicode escape issues
# config_path = r'C:\Users\subed\.cursor\mcp.json'

# # Load the configuration file
# try:
#     with open(config_path, 'r') as file:
#         config = json.load(file)
# except FileNotFoundError:
#     print(f"Configuration file not found: {config_path}")
#     exit(1)

# # Get the command and arguments for the agent
# agent_config = config['mcpServers']['documentation']
# command = agent_config['command']
# args = agent_config['args']

# # Start the server
# subprocess.run([command] + args)
#########################################################
# import requests
# import csv

# # World Bank Indicator API
# url = "https://api.worldbank.org/v2/indicator?format=json&per_page=50000000"

# # Fetch data
# response = requests.get(url)
# data = response.json()

# # CSV header
# header = ["id", "name", "source", "sourceNote", "sourceOrganization"]

# # Prepare rows
# rows = []
# for indicator in data[1]:
#     rows.append([
#         indicator["id"],
#         indicator["name"],
#         indicator.get("source", {}).get("value", ""),
#         indicator.get("sourceNote", ""),
#         indicator.get("sourceOrganization", "")
#     ])

# # Save to CSV
# with open("world_bank_indicators.csv", "w", newline='', encoding="utf-8") as f:
#     writer = csv.writer(f)
#     writer.writerow(header)
#     writer.writerows(rows)

# print("Saved as world_bank_indicators.csv")

import requests
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup session with retries and timeouts
session = requests.Session()
retry = Retry(connect=5, backoff_factor=1, status_forcelist=[502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

# IMF datasets to fetch
dataset_ids = [
    "IFS", "WEO", "BOP", "GFS", "DOT", "QNA", "FSI", "CPI", "CDIS", "IIP", "GGX"
]

base_structure_url = "https://dataservices.imf.org/REST/SDMX_JSON.svc/DataStructure/"
all_indicators = []

for dataset_id in dataset_ids:
    try:
        print(f"Fetching structure for dataset: {dataset_id}")
        response = session.get(base_structure_url + dataset_id, timeout=10)
        structure = response.json()

        dimensions = structure['Structure']['KeyFamilies'][0]['Components']['Dimension']
        target_dimension = None

        for dim in dimensions:
            if dim['id'] in ['INDICATOR', 'INDICATORS', 'SUBJECT']:
                target_dimension = dim
                break

        if not target_dimension:
            print(f"No indicator-like dimension found for {dataset_id}, skipping.")
            continue

        code_list_id = target_dimension['CodeList']['@id']
        code_list_url = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CodeList/{code_list_id}"
        code_response = session.get(code_list_url, timeout=10)
        code_data = code_response.json()

        codes = code_data['Structure']['CodeLists'][0]['CodeList']['Code']
        for code in codes:
            code_value = code['@value']
            description = code['Description']['#text']
            all_indicators.append([dataset_id, code_value, description])

    except Exception as e:
        print(f"⚠️  Error processing dataset {dataset_id}: {e}")

# Save to CSV
csv_path = "imf_all_indicators.csv"
with open(csv_path, "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["dataset", "indicator_code", "description"])
    writer.writerows(all_indicators)

print(f"\n✅ Saved {len(all_indicators)} indicators to {csv_path}")
