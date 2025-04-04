"""Filters latest campaign and line item data from Beeswax API (used for 3P mapping)."""

import requests
import csv
import time
from datetime import datetime
import os
import logging
from dotenv import load_dotenv
import copy

load_dotenv("./input_folder/beeswax_input_filter.env")

session = requests.Session()

def get_login_credentials():
    return {
        "email": os.getenv('LOGIN_EMAIL'),
        "password": os.getenv('PASSWORD'),
        "keep_logged_in": True
    }

def get_cookies():
    print("✅ Getting authentication cookies...")
    data = get_login_credentials()
    url = os.getenv('LOGIN_URL')
    response = session.post(url, data=data)
    print("✅ Authentication successful!")
    return session.cookies.get_dict()

def get_payload_response(api_url, cookies, output_file):
    print(f"✅ Fetching data from API: {api_url}")
    payload_list = []
    
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = None
        
        for i in range(0, 20):  # Implementing pagination
            offset_url = api_url + f"&offset={i * 10000}"
            response_data = requests.get(offset_url, cookies=cookies)
            
            if response_data.status_code != 200:
                response_data = requests.get(offset_url, cookies=get_cookies())
            
            if response_data.status_code != 200:
                logging.error("Wrong URL: %s", offset_url)
                return payload_list
            
            response_json = response_data.json().get("payload", [])
            if response_json:
                if writer is None:
                    writer = csv.DictWriter(file, fieldnames=response_json[0].keys())
                    writer.writeheader()
                writer.writerows(response_json)
                payload_list += response_json
                print(f"Fetched {len(response_json)} records from {offset_url}")
            else:
                print("No more data to fetch.")
                break  # Stop if no more data
    
    return payload_list

def get_custom_column_names():
    return {
        "campaign_columns": ["campaign_id", "campaign_name"],
        "lineitem_columns": ["line_item_id", "line_item_name"],
        "creative_columns": ["creative_id", "creative_name", "pixels", "scripts", "creative_content_munge"]
    }

def get_column_values(payload, column_names_list, key_name=None):
    return {f"{key_name}_{col}": payload[col] for col in column_names_list if col in payload}

def write_consolidated_data_into_csv(payload):
    if not payload:
        print("No data to write!")
        return
    
    # ✅ Get absolute path for the output folder
    output_folder = os.path.abspath("./Beeswax_Data")
    os.makedirs(output_folder, exist_ok=True)  # ✅ Ensure folder exists
    
    output_file_path = os.path.join(output_folder, f"beeswax_filtered_report_{datetime.now().strftime('%m%d%Y%H%M%S')}.csv")
    
    print(f"Writing data to {output_file_path}...")
    with open(output_file_path, 'w', newline='', encoding='utf-8') as file:
        fieldnames = payload[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(payload)
    
    print(f"✅ File successfully written: {output_file_path}")

def generate_consolidated_report():
    logging.basicConfig(filename='app.log', level=logging.ERROR, format='%(levelname)s:%(asctime)s:%(message)s')
    start_time = time.time()
    
    custom_column_names = get_custom_column_names()
    consolidated_reports_list = []
    cookies = get_cookies()

    report_path = os.getenv('REPORT_PATH', './Beeswax_Data/raw/')
    print(f"🛠️ REPORT_PATH from .env: {report_path}")
    os.makedirs(report_path, exist_ok=True)  # ✅ Ensure folder exists
    
    print("Fetching campaigns...")
    campaign_list = get_payload_response(os.getenv('CAMPAIGN_URL'), cookies, os.path.join(report_path, "campaigns.csv"))
    print(f"Total campaigns fetched: {len(campaign_list)}")
    
    print("✅ Fetching line items...")
    lineitem_list = get_payload_response(os.getenv('LINEITEM_URL'), cookies, os.path.join(report_path, "line_items.csv"))
    print(f"Total line items fetched: {len(lineitem_list)}")
    
    print("✅ Fetching filtered creative-line item mappings...")
    creative_lineitem_list = get_payload_response(os.getenv('CREATIVE_LINEITEM_URL'), cookies, os.path.join(report_path, "creative_line_items.csv"))
    print(f"Total filtered creative-line item mappings fetched: {len(creative_lineitem_list)}")
    
    print("✅ Fetching filtered creatives...")
    creative_list = get_payload_response(os.getenv('CREATIVE_URL'), cookies, os.path.join(report_path, "creatives.csv"))
    print(f"Total filtered creatives fetched: {len(creative_list)}")
    
    print("🔄 Processing data...")
    for _campaign in campaign_list:
        consolidated_data = get_column_values(_campaign, custom_column_names["campaign_columns"], key_name="campaign")
        
        lineitems = [li for li in lineitem_list if li['campaign_id'] == _campaign["campaign_id"]]
        for _lineitem in lineitems:
            consolidated_data.update(get_column_values(_lineitem, custom_column_names["lineitem_columns"], key_name="line_item"))
            
            creatives = [cl for cl in creative_lineitem_list if cl['line_item_id'] == _lineitem["line_item_id"]]
            for _creative_link in creatives:
                creative_data = next((c for c in creative_list if c['creative_id'] == _creative_link["creative_id"]), None)
                if creative_data:
                    consolidated_data.update(get_column_values(creative_data, custom_column_names["creative_columns"], key_name="creative"))
                    consolidated_reports_list.append(copy.deepcopy(consolidated_data))

    write_consolidated_data_into_csv(consolidated_reports_list)
    print("Filtered Report Created Successfully")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    print(f"⏳ Script execution completed in {int(minutes)} minutes and {int(seconds)} seconds!")

def main():
    print("🔹 Starting script...")
    generate_consolidated_report()

if __name__ == '__main__':
    main()
