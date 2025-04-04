import pandas as pd
import os
import logging
import csv
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def detect_delimiter(file_path, num_lines=5):
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        sample_lines = [next(file) for _ in range(num_lines)]
    sniffer = csv.Sniffer()
    try:
        return sniffer.sniff(''.join(sample_lines)).delimiter
    except csv.Error:
        return ','

def custom_csv_parse(line, delimiter):
    fields = []
    current_field = ""
    in_quotes = False
    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == delimiter and not in_quotes:
            fields.append(current_field.strip())
            current_field = ""
        else:
            current_field += char
    fields.append(current_field.strip())
    return fields

def find_required_columns(header_rows, required_columns):
    column_indices = {}
    for col in required_columns:
        column_indices[col] = None
        for row in header_rows:
            try:
                lower_row = [item.lower() for item in row]
                index = lower_row.index(col)
                column_indices[col] = index
                break
            except ValueError:
                continue
    return column_indices

def merged_dcm_report(folder_path, output_file):
    all_data = []
    required_columns = ["date", "placement id", "impressions"]
    optional_columns = ["clicks", "video completions"]
    processed_files = []
    skipped_files = []

    all_files = os.listdir(folder_path)
    total_files = len([f for f in all_files if f.endswith(".csv")])

    for filename in all_files:
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            try:
                delimiter = ','  # Force comma delimiter
                with open(file_path, 'r', encoding='utf-8-sig') as file:
                    lines = file.readlines()

                report_fields_row = None
                for i, line in enumerate(lines):
                    if "Report Fields" in line:
                        report_fields_row = i
                        break

                if report_fields_row is None:
                    logging.warning(f"Warning: 'Report Fields' not found in {filename}")
                    print(f"Warning: 'Report Fields' not found in {filename}")
                    skipped_files.append(filename)
                    continue

                header_rows = [custom_csv_parse(lines[i].strip(), delimiter) for i in range(report_fields_row + 1, report_fields_row + 7)]
                column_indices = find_required_columns(header_rows, required_columns + optional_columns)
                data_rows = [custom_csv_parse(line.strip(), delimiter) for line in lines[report_fields_row + 7:]]

                for row in data_rows:
                    if row and row[0].startswith("Grand Total:"):
                        continue

                    if not any(row):
                        print("Skipping empty row")
                        continue

                    date_index = column_indices.get("date", None)
                    placement_id_index = column_indices.get("placement id", None)
                    impressions_index = column_indices.get("impressions", None)

                    if date_index is None or placement_id_index is None or impressions_index is None:
                        continue

                    new_row = {
                        "Report Name": filename,
                        "Date": datetime.strptime(row[date_index], "%Y-%m-%d").strftime("%Y-%m-%d") if date_index is not None else None,
                        "Placement ID": str(row[placement_id_index]) if placement_id_index is not None else None,
                        "Impressions": int(row[impressions_index]) if impressions_index is not None and row[impressions_index] else None,
                        "Clicks": int(row[column_indices.get("clicks", None)]) if column_indices.get("clicks", None) is not None and column_indices.get("clicks", None) < len(row) and row[column_indices.get("clicks", None)] else None,
                        "Video Completions": int(row[column_indices.get("video completions", None)]) if column_indices.get("video completions", None) is not None and column_indices.get("video completions", None) < len(row) and row[column_indices.get("video completions", None)] else None,
                    }

                    all_data.append(new_row)

                processed_files.append(filename)

            except (FileNotFoundError, ValueError, IndexError, Exception) as e:
                logging.error(f"Error processing {filename}: {e}")
                print(f"Error processing {filename}: {e}")
                skipped_files.append(filename)

    if all_data:  # Check if all_data has any elements
        df = pd.DataFrame(all_data)
        # Remove duplicate rows based on all relevant columns
        df = df.drop_duplicates(subset=["Date", "Placement ID", "Impressions", "Clicks", "Video Completions"])

        # Remove "Report Name" and de-duplicate again
        df_no_report_name = df.drop(columns=["Report Name"])
        df_final = df_no_report_name.drop_duplicates(subset=["Date", "Placement ID", "Impressions", "Clicks", "Video Completions"])

        df_final.to_csv(output_file, index=False)
        logging.info(f"Merged and de-duplicated report saved to {output_file}")
        print(f"✅Merged and de-duplicated report saved to {output_file}")
    else:
        logging.warning("No valid data found to create a report.")
        print("No valid data found to create a report.")

    print(f"Total files found: {total_files}")

    if skipped_files:
        print("Skipped files:")
        for skipped_file in skipped_files:
            print(f"- {skipped_file}")

folder_path = r"C:\Catalina_auto_report\third_party_reports\dcm_folder\dcm_email_reports"
output_file = r"C:\Catalina_auto_report\third_party_reports\dcm_folder\merged_dcm_report.csv"
merged_dcm_report(folder_path, output_file)
