import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import calendar
import pytz
import re

# Load environment variables
load_dotenv("./input_folder/beeswax_input_report.env")

# Beeswax API Credentials
USERNAME = os.getenv('LOGIN_EMAIL')
PASSWORD = os.getenv('PASSWORD')
BASE_URL = "https://catalina.api.beeswax.com/rest/v2/authenticate"

# Get current time in UTC
utc_now = datetime.now(pytz.UTC)
# Format for display
today = utc_now.strftime('%Y-%m-%d')
print(f"Current UTC time: {today}")

# Load individual start dates from .env, with defaults if missing
START_DATE_SPEND = os.getenv("START_DATE_SPEND", "2024-01-01")
START_DATE_REACH_LI = os.getenv("START_DATE_REACH_LI", "2024-01-01")
START_DATE_REACH_C = os.getenv("START_DATE_REACH_C", "2024-01-01")

# Always set END_DATE to today
END_DATE = today

# Custom function to parse timezone lists from environment variables
def parse_timezone_list(env_var_name, default_timezone="America/New_York"):
    """Parse a list of timezones from an environment variable."""
    # Get raw string from environment
    raw_value = os.getenv(env_var_name)
    
    if not raw_value:
        return [default_timezone]
    
    # Remove whitespace and extract timezone names
    # This handles formats like [America/New_York, America/Los_Angeles]
    try:
        # Remove brackets and split by comma
        value = raw_value.strip('[]')
        timezones = [tz.strip() for tz in value.split(',')]
        
        # Validate timezones
        valid_timezones = []
        for tz in timezones:
            try:
                pytz.timezone(tz)
                valid_timezones.append(tz)
            except pytz.exceptions.UnknownTimeZoneError:
                print(f"⚠️ Unknown timezone: {tz}. Skipping.")
        
        if not valid_timezones:
            print(f"⚠️ No valid timezones found for {env_var_name}. Using default: {default_timezone}")
            return [default_timezone]
            
        return valid_timezones
        
    except Exception as e:
        print(f"⚠️ Error parsing timezone list from {env_var_name}: {e}")
        print(f"⚠️ Using default timezone: {default_timezone}")
        return [default_timezone]

# Get timezone configurations using custom parser
TIMEZONES_SPEND = parse_timezone_list("BEESWAX_SPEND_TZ", "America/New_York")
TIMEZONES_REACH_LI = parse_timezone_list("BEESWAX_REACH_LI_TZ", "America/New_York")
TIMEZONES_REACH_C = parse_timezone_list("BEESWAX_REACH_C_TZ", "America/New_York")

# Print configured timezones
print(f"🌐 Beeswax_Spend timezones: {TIMEZONES_SPEND}")
print(f"🌐 Beeswax_Reach_LI timezones: {TIMEZONES_REACH_LI}")
print(f"🌐 Beeswax_Reach_C timezones: {TIMEZONES_REACH_C}")

# Function to get timezone-specific start date
def get_timezone_specific_start_date(report_type, timezone, default_start_date):
    """Get timezone-specific start date if available, otherwise use default."""
    # Format the env var name: START_DATE_REPORT_TYPE_TIMEZONE
    # Replace "/" with "_" in timezone name for env var
    tz_env_var = f"START_DATE_{report_type}_{timezone.replace('/', '_')}"
    return os.getenv(tz_env_var, default_start_date)

# Define report configs with timezone-specific start dates
report_configs = {
    "Beeswax_Spend": {
        "default_start_date": START_DATE_SPEND,
        "timezones": TIMEZONES_SPEND,
        "split_by_month": True  # Flag to indicate whether to split by month
    },
    "Beeswax_Reach_LI": {
        "default_start_date": START_DATE_REACH_LI,
        "timezones": TIMEZONES_REACH_LI,
        "split_by_month": False  # Don't split Reach_LI reports
    },
    "Beeswax_Reach_C": {
        "default_start_date": START_DATE_REACH_C,
        "timezones": TIMEZONES_REACH_C,
        "split_by_month": False  # Don't split Reach_C reports
    }
}

# Print the selected date ranges for verification
for report_name, config in report_configs.items():
    report_type = report_name.replace("Beeswax_", "")
    for timezone in config["timezones"]:
        start_date = get_timezone_specific_start_date(report_type, timezone, config["default_start_date"])

# Fix folder path to absolute
data_folder = os.path.abspath(f"Beeswax_reports/{today.split()[0]}")

# Create folder for backup
backup_folder = os.path.abspath(f"Beeswax_reports/{today}")
os.makedirs(backup_folder, exist_ok=True)

# Create folders if they don't exist
try:
    os.makedirs(f"{data_folder}/beeswax_raw", exist_ok=True)
    print("✔ Directory structure set up successfully!")
except Exception as e:
    print(f"❌ Error creating directories: {e}")

# Delete old files in the data folder before starting a new run
for root, dirs, files in os.walk(data_folder):
    for file in files:
        file_path = os.path.join(root, file)
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"❌ Error deleting file {file_path}: {e}")

# Start session
session = requests.Session()

def get_login_credentials():
    return {
        "email": USERNAME,
        "password": PASSWORD,
        "keep_logged_in": "true"
    }

def authenticate_beeswax():
    """ Authenticate with Beeswax and return session cookies & CSRF token. """
    login_url = BASE_URL
    data = get_login_credentials()
    headers = {
        'User-Agent': 'python-requests/2.32.3',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
    }

    print("🔄 Attempting Beeswax Authentication...")
    try:
        response = session.post(login_url, data=data, headers=headers)
        print(f"🔍 Authentication Response Status: {response.status_code}")
        print(f"🔍 Authentication Response Text: {response.text}")
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Authentication request failed: {e}")
        return None, None

    if response.status_code == 200:
        print("✅ Beeswax Authentication Successful!")
        session.cookies.update(response.cookies)
        csrf_token = session.cookies.get('csrftoken', '')
        print("🔑 Extracted CSRF Token:", csrf_token)
        return session.cookies.get_dict(), csrf_token
    else:
        print("❌ Authentication Failed! Response:", response.text)
        return None, None

def get_payload(report_type, start_period, end_period, timezone=None):
    """Return the appropriate payload for each report type with optional timezone."""
    payloads = {
        "Beeswax_Spend": {
            "fields": ["campaign_id", "line_item_id", "bid_day", "campaign_name", "line_item_name", "spend", "impression", "clicks"],
        },
        "Beeswax_Reach_LI": {
            "fields": ["campaign_id", "line_item_id", "campaign_name", "line_item_name", "reach_standard_fallback"],
        },
        "Beeswax_Reach_C": {
            "fields": ["campaign_name", "reach_standard_fallback"],
        }
    }
    
    payload = {
        "fields": payloads[report_type]["fields"],
        "filters": {"bid_day": f"{start_period} to {end_period}"},
        "result_format": "csv",
        "limit": 30000,
        "view": "performance_agg"
    }
    
    # Add query_timezone if provided
    if timezone:
        payload["query_timezone"] = timezone

    return payload

def request_report(cookies, csrf_token, report_type, start_date, end_date, timezone=None, split_by_month=True):
    """ Request a performance report from Beeswax API in parallel to speed up execution. """
    if not cookies or not csrf_token:
        print("❌ Missing authentication credentials, aborting request.")
        return None

    headers = {
        'User-Agent': 'python-requests/2.32.3',
        'Accept': 'application/json',
        'X-CSRFToken': csrf_token,
        'Content-Type': 'application/json'
    }

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    task_ids = []

    def send_report_request(start_period, end_period, retries=3):
        payload = get_payload(report_type, start_period, end_period, timezone)
        report_endpoint = "https://catalina.api.beeswax.com/rest/v2/reporting/run-query"

        # Include timezone in log message if specified
        tz_info = f" [TZ: {timezone}]" if timezone else ""
        
        for attempt in range(retries):
            response = session.post(report_endpoint, json=payload, headers=headers, cookies=cookies)
            if response.status_code == 200:
                report_data = response.json()
                task_id = report_data.get("task_id")
                if task_id:
                    return (task_id, start_period, end_period, timezone)
            else:
                print(f"⚠️ Attempt {attempt+1}/{retries} failed. Retrying in 5s...")
                time.sleep(5)

        print(f"❌ Final attempt failed for {start_period} - {end_period}{tz_info}")
        return None

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_tasks = []

        # Different handling based on whether we need to split by month
        if split_by_month:
            # Original logic for Beeswax_Spend - split by month
            current = start
            while current <= end:
                month_start = current.strftime("%Y-%m-%d")
                next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
                month_end = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")

                if next_month > end:
                    month_end = end.strftime("%Y-%m-%d")

                total_days_in_month = calendar.monthrange(current.year, current.month)[1]
                mid_month = total_days_in_month // 2
                first_half_end = (current.replace(day=mid_month) + timedelta(days=1)).strftime("%Y-%m-%d")
                second_half_start = first_half_end

                current_month_str = current.strftime("%Y-%m")
                today_month_str = datetime.strptime(today, "%Y-%m-%d").strftime("%Y-%m")

                today_day = int(datetime.now().strftime("%d"))

                if current_month_str == today_month_str:
                    if today_day <= mid_month:
                        future_tasks.append(executor.submit(send_report_request, month_start, today))
                    else:
                        future_tasks.append(executor.submit(send_report_request, month_start, first_half_end))
                        future_tasks.append(executor.submit(send_report_request, second_half_start, today))
                else:
                    next_month_start = next_month.strftime("%Y-%m-%d")
                    future_tasks.append(executor.submit(send_report_request, month_start, first_half_end))
                    future_tasks.append(executor.submit(send_report_request, second_half_start, next_month_start))

                current = next_month
        else:
            # Simplified logic for Reach reports - pull entire date range at once
            future_tasks.append(executor.submit(send_report_request, start_date, end_date))

        for future in future_tasks:
            result = future.result()
            if result:
                task_ids.append(result)

        return task_ids

def fetch_report(cookies, tid, s_date, e_date, report_type, timezone=None):
    """ Fetch and download a report from Beeswax API """
    report_status_url = f"https://catalina.api.beeswax.com/rest/v2/reporting/async-results/{tid}"
    headers = {'User-Agent': 'python-requests/2.32.3', 'Accept': 'application/json'}
    download_folder = os.path.join(data_folder, "beeswax_raw")

    # Include timezone in file name and log message if specified
    tz_suffix = f"_tz_{timezone.replace('/', '_')}" if timezone else ""
    tz_info = f" [TZ: {timezone}]" if timezone else ""

    for attempt in range(10):
        try:
            response = session.get(report_status_url, headers=headers, cookies=cookies)
            response.raise_for_status()

            if response.status_code == 200 and response.content:
                file_name = f"{report_type.lower()}_{s_date}_to_{e_date}{tz_suffix}_{tid}.csv"
                file_path = os.path.join(download_folder, file_name)

                if len(response.content) > 10:
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    return file_path
                else:
                    print(f"⚠️ Received an empty report for Task ID {tid} ({s_date} to {e_date}){tz_info}")
                    return None

            print(f"⚠️ Unexpected response ({response.status_code}): {response.text}")
            time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error checking report status for Task ID {tid}: {e}")

    print(f"❌ Failed to download report for Task ID: {tid} after multiple attempts.")
    return None

def download_report(cookies, task_ids, report_type):
    """ Download reports in parallel to speed up execution. """
    if not task_ids:
        print(f"⚠️ No valid task IDs for {report_type}. Skipping download.")
        return

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_downloads = [
            executor.submit(fetch_report, cookies, tid, s_date, e_date, report_type, timezone)
            for tid, s_date, e_date, timezone in task_ids
        ]

        for future in future_downloads:
            result = future.result()
            if not result:
                print(f"⚠️ A report failed to download.")

def merge_reports(report_name, timezone=None):
    """ Merge all downloaded reports into one file with a dynamic name. """
    # Include timezone in file name if specified
    tz_suffix = f"_tz_{timezone.replace('/', '_')}" if timezone else ""
    
    merged_file_name = f"{report_name}{tz_suffix}_{today.replace('-', '')}.csv"
    merged_file_path = os.path.join(data_folder, merged_file_name)

    csv_folder = os.path.join(data_folder, "beeswax_raw")
    
    # Adjust file pattern to match timezone if specified
    file_pattern = report_name.replace("Beeswax_", "").lower()
    if timezone:
        file_pattern_tz = f"{file_pattern}_.*_tz_{timezone.replace('/', '_')}"
        csv_files = [f for f in os.listdir(csv_folder) if file_pattern in f.lower() and 
                    f"_tz_{timezone.replace('/', '_')}" in f and f.endswith('.csv')]
    else:
        csv_files = [f for f in os.listdir(csv_folder) if file_pattern in f.lower() and f.endswith('.csv')]

    if not csv_files:
        tz_info = f" for timezone {timezone}" if timezone else ""
        print(f"❌ No reports found to merge for {report_name}{tz_info}. Files in folder:")
        print(os.listdir(csv_folder))
        return None

    tz_info = f" ({timezone})" if timezone else ""
    print(f"🔄 Found {len(csv_files)} reports to merge for {report_name}{tz_info}.")

    df_list = []
    for file in csv_files:
        file_path = os.path.join(csv_folder, file)

        while not os.path.exists(file_path):
            print(f"⏳ Waiting for file: {file_path} to be fully written...")
            time.sleep(2)

        try:
            df = pd.read_csv(file_path)
            df_list.append(df)
        except Exception as e:
            print(f"❌ Error reading {file}: {e}")

    if not df_list:
        print(f"❌ No valid data found in reports for {report_name}{tz_info}.")
        return None

    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df.to_csv(merged_file_path, index=False)
    print(f"✅ Merged report saved as {merged_file_path}")

    return merged_file_path

def main():
    start_time = time.time()
    print("🚀 Starting Beeswax API Automation...")
    
    cookies, csrf_token = authenticate_beeswax()
    if cookies and csrf_token:
        print("🔄 Proceeding to request reports...")

        for report_name, config in report_configs.items():
            report_type = report_name.replace("Beeswax_", "")
            timezones = config["timezones"]
            split_by_month = config["split_by_month"]
            
            # If there are multiple timezones configured, process each one
            for timezone in timezones:
                # Get timezone-specific start date
                start_date = get_timezone_specific_start_date(report_type, timezone, config["default_start_date"])
                end_date = END_DATE
                
                split_info = " (split by month)" if split_by_month else " (full date range)"
                task_ids = request_report(cookies, csrf_token, report_name, start_date, end_date, timezone, split_by_month)

                if task_ids:
                    download_report(cookies, task_ids, report_name)
                    merged_report_path = merge_reports(report_name, timezone)

                    if merged_report_path:
                        print(f"✅ Merged report created for {report_name} [TZ: {timezone}]")
                    else:
                        print(f"⚠️ Merging failed for {report_name} [TZ: {timezone}]")

    end_time = time.time()
    total_time = end_time - start_time
    minutes, seconds = divmod(total_time, 60)
    print(f"🎯 Script execution completed in {int(minutes)} minutes and {int(seconds)} seconds!")

if __name__ == "__main__":
    main()