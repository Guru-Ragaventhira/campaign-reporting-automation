import pandas as pd
import os
import logging
import csv
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_date(date_str):
    """Parse date string in various formats to YYYY-MM-DD format."""
    if not date_str:
        return None
    
    date_formats = [
        '%Y-%m-%d',  # YYYY-MM-DD - Standard ISO format
        '%m/%d/%Y',  # MM/DD/YYYY - Common US format
        '%d/%m/%Y',  # DD/MM/YYYY - Common European format
        '%Y/%m/%d',  # YYYY/MM/DD - Alternative standard format
        '%m-%d-%Y',  # MM-DD-YYYY - Alternative US format
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    logging.warning(f"Could not parse date: {date_str}")
    return None

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

def load_placement_mapping(mapping_file_path):
    """Load the placement ID mapping from the CSV file."""
    try:
        mapping_df = pd.read_csv(mapping_file_path)
        # Create a dictionary mapping names to placement IDs
        mapping_dict = dict(zip(mapping_df['Name'], mapping_df['Placement ID']))
        logging.info(f"Successfully loaded {len(mapping_dict)} placement mappings")
        return mapping_dict
    except Exception as e:
        logging.error(f"Error loading placement mapping file: {e}")
        return {}

def process_report_without_placement_id(file_path, mapping_dict):
    """Process a report that doesn't have a Placement ID column."""
    try:
        original_filename = os.path.basename(file_path)
        
        # First, read the file line by line to find the actual data structure
        with open(file_path, 'r', encoding='utf-8-sig') as file:
            lines = file.readlines()
            
        # Find Report Fields row and actual headers
        report_fields_row = None
        for i, line in enumerate(lines):
            if "Report Fields" in line:
                report_fields_row = i
                break
                
        if report_fields_row is None:
            logging.error(f"No 'Report Fields' row found in {original_filename}")
            return None
            
        # Skip Report Fields section and find the actual data headers
        data_start = report_fields_row + 1
        while data_start < len(lines):
            if any(keyword in lines[data_start].lower() for keyword in ['campaign', 'placement', 'date']):
                break
            data_start += 1
            
        if data_start >= len(lines):
            logging.error(f"Could not find data headers in {original_filename}")
            return None
            
        # Get the actual headers and data
        header_row = custom_csv_parse(lines[data_start].strip(), ',')
        header_row = [h.strip() for h in header_row]
        
        # Create a list to store the processed data
        processed_data = []
        
        # Process each data row
        for line in lines[data_start + 1:]:
            if line.strip() and not line.strip().startswith('Grand Total:'):
                values = custom_csv_parse(line.strip(), ',')
                if len(values) >= len(header_row):
                    row_dict = dict(zip(header_row, values))
                    processed_data.append(row_dict)
        
        if not processed_data:
            logging.warning(f"No data rows found in {original_filename}")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(processed_data)
        
        # Check for various possible column names (case-insensitive)
        df.columns = df.columns.str.strip().str.lower()
        placement_columns = ['placement', 'placement name', 'placement_name', 'placement id', 'placement_id']
        
        # Find matching placement column
        found_placement_col = next((col for col in df.columns if col in placement_columns), None)
        
        if found_placement_col:
            # Create mapping using case-insensitive matching
            name_to_id_map = {str(name).lower(): id for name, id in mapping_dict.items()}
            df['placement id'] = df[found_placement_col].astype(str).str.lower().map(name_to_id_map)
            
            # Validate mapping results
            unmapped_count = df['placement id'].isna().sum()
            if unmapped_count > 0:
                logging.warning(f"Warning: {unmapped_count} placements could not be mapped to IDs in {original_filename}")
                # Log some sample unmapped values for debugging
                unmapped_values = df[df['placement id'].isna()][found_placement_col].unique()[:5]
                logging.warning(f"Sample unmapped placements: {unmapped_values}")
            
            # Drop the original column if it's different from 'placement id'
            if found_placement_col != 'placement id':
                df = df.drop(columns=[found_placement_col])
        else:
            logging.warning(f"No placement column found in {original_filename}")
            return None
            
        # Restore original case for standard columns
        column_case_map = {
            'campaign': 'Campaign',
            'ad': 'Ad',
            'advertiser': 'Advertiser',
            'date': 'Date',
            'placement id': 'Placement ID',
            'impressions': 'Impressions',
            'clicks': 'Clicks',
            'video completions': 'Video Completions'
        }
        df = df.rename(columns=column_case_map)
        
        # Create new filename with _with_placement_id suffix
        file_dir = os.path.dirname(file_path)
        file_name = os.path.splitext(original_filename)[0]
        new_file_path = os.path.join(file_dir, f"{file_name}_with_placement_id.csv")
        
        # Reorder columns to match DCM format
        desired_order = ['Campaign', 'Ad', 'Advertiser', 'Date', 'Placement ID', 'Impressions', 'Clicks', 'Video Completions']
        current_cols = df.columns.tolist()
        final_cols = [col for col in desired_order if col in current_cols]
        final_cols.extend([col for col in current_cols if col not in desired_order])
        df = df[final_cols]
        
        # Create the content with Report Fields
        with open(new_file_path, 'w', newline='', encoding='utf-8') as f:
            f.write('Report Fields\n')  # Add Report Fields header
            # Write the column headers
            f.write(','.join(df.columns) + '\n')
            # Write the data
            df.to_csv(f, index=False, header=False)
            
        logging.info(f"Created processed file: {new_file_path}")
        return new_file_path
            
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        return None

def process_processed_file(file_path):
    """Process a file that has already been processed (has _with_placement_id suffix)."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Columns in processed file {file_path}: {', '.join(df.columns)}")
        
        # Verify required columns exist
        required_columns = {'Date', 'Placement ID', 'Impressions'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            logging.error(f"Processed file {file_path} missing required columns: {missing_columns}")
            return None
            
        # Convert numeric columns
        if 'Impressions' in df.columns:
            df['Impressions'] = pd.to_numeric(df['Impressions'], errors='coerce')
        if 'Clicks' in df.columns:
            df['Clicks'] = pd.to_numeric(df['Clicks'], errors='coerce')
        if 'Video Completions' in df.columns:
            df['Video Completions'] = pd.to_numeric(df['Video Completions'], errors='coerce')
            
        return df
    except Exception as e:
        logging.error(f"Error reading processed file {file_path}: {e}")
        return None

def merged_dcm_report(folder_path, output_file):
    all_data = []
    required_columns = ["date", "placement id", "impressions"]
    optional_columns = ["clicks", "video completions"]
    processed_files = []
    skipped_files = []
    
    # Track reports missing placement IDs
    reports_missing_placement_ids = []
    
    # Track which placement IDs appear in which reports
    placement_id_to_reports = defaultdict(list)

    # Load placement ID mapping
    mapping_file = r"C:\Catalina_auto_report\input_folder\Data_Mapping\DCM_ID_Mapping.csv"
    placement_mapping = load_placement_mapping(mapping_file)

    all_files = os.listdir(folder_path)
    # First process files that don't have _with_placement_id suffix
    original_files = [f for f in all_files if f.endswith(".csv") and not f.endswith("_with_placement_id.csv")]
    logging.info(f"Found {len(original_files)} original files to process")

    # Process each file
    for filename in all_files:
        if not filename.endswith(".csv"):
            continue
            
        # Skip files that have already been processed
        if filename.endswith("_with_placement_id.csv"):
            continue
            
        file_path = os.path.join(folder_path, filename)
        logging.info(f"Processing file: {filename}")
        
        try:
            delimiter = ','  # Force comma delimiter
            with open(file_path, 'r', encoding='utf-8-sig') as file:
                lines = file.readlines()

            # Find Report Fields row
            report_fields_row = None
            for i, line in enumerate(lines):
                if "Report Fields" in line:
                    report_fields_row = i
                    break

            if report_fields_row is None:
                logging.warning(f"Warning: 'Report Fields' not found in {filename}")
                skipped_files.append(filename)
                continue

            # Find the actual data headers
            data_start = report_fields_row + 1
            while data_start < len(lines):
                if any(keyword in lines[data_start].lower() for keyword in ['campaign', 'placement', 'date']):
                    break
                data_start += 1

            if data_start >= len(lines):
                logging.error(f"Could not find data headers in {filename}")
                skipped_files.append(filename)
                continue

            # Get headers and data
            headers = custom_csv_parse(lines[data_start].strip(), delimiter)
            headers = [h.strip() for h in headers]
            header_lower = [h.lower() for h in headers]

            # Check if this is an original file that needs processing
            if "placement id" not in [h.lower() for h in headers]:
                reports_missing_placement_ids.append(filename)
                logging.warning(f"Warning: 'placement id' column not found in {filename}")
                
                # Process the file to add Placement ID
                processed_file = process_report_without_placement_id(file_path, placement_mapping)
                if processed_file:
                    # Process the newly created file
                    file_path = processed_file
                    filename = os.path.basename(processed_file)
                    # Re-read the file with the new path
                    with open(file_path, 'r', encoding='utf-8-sig') as file:
                        lines = file.readlines()
                    # Find Report Fields row again
                    report_fields_row = None
                    for i, line in enumerate(lines):
                        if "Report Fields" in line:
                            report_fields_row = i
                            break
                    if report_fields_row is None:
                        logging.warning(f"Warning: 'Report Fields' not found in processed file {filename}")
                        continue
                    # Get headers again
                    data_start = report_fields_row + 1
                    while data_start < len(lines):
                        if any(keyword in lines[data_start].lower() for keyword in ['campaign', 'placement', 'date']):
                            break
                        data_start += 1
                    if data_start >= len(lines):
                        logging.error(f"Could not find data headers in processed file {filename}")
                        continue
                    headers = custom_csv_parse(lines[data_start].strip(), delimiter)
                    headers = [h.strip() for h in headers]
                    header_lower = [h.lower() for h in headers]
                else:
                    logging.error(f"Failed to create processed file for {filename}")
                    continue

            # Process the data rows
            for line in lines[data_start + 1:]:
                if line.strip() and not line.strip().startswith('Grand Total:'):
                    values = custom_csv_parse(line.strip(), delimiter)
                    if len(values) >= len(headers):
                        row_data = dict(zip(headers, values))
                        
                        # Find the correct column names (case-insensitive)
                        date_col = next((h for h in headers if h.lower() == 'date'), None)
                        placement_id_col = next((h for h in headers if h.lower() == 'placement id'), None)
                        impressions_col = next((h for h in headers if h.lower() == 'impressions'), None)
                        clicks_col = next((h for h in headers if h.lower() == 'clicks'), None)
                        video_comp_col = next((h for h in headers if h.lower() == 'video completions'), None)

                        if date_col and placement_id_col and impressions_col:
                            new_row = {
                                "Report Name": filename,
                                "Date": parse_date(row_data[date_col]) if date_col else None,
                                "Placement ID": row_data[placement_id_col] if placement_id_col else None,
                                "Impressions": int(row_data[impressions_col]) if impressions_col and row_data[impressions_col] else None,
                                "Clicks": int(row_data[clicks_col]) if clicks_col and row_data.get(clicks_col) else None,
                                "Video Completions": int(row_data[video_comp_col]) if video_comp_col and row_data.get(video_comp_col) else None
                            }
                            
                            # Add to placement_id_to_reports mapping
                            if new_row["Placement ID"] and filename not in placement_id_to_reports[new_row["Placement ID"]]:
                                placement_id_to_reports[new_row["Placement ID"]].append(filename)
                                
                            all_data.append(new_row)

            processed_files.append(filename)
            logging.info(f"Successfully processed file: {filename}")

        except Exception as e:
            logging.error(f"Error processing file {filename}: {e}")
            skipped_files.append(filename)

    # Generate timestamp for Excel file
    current_time = datetime.now()
    timestamp = current_time.strftime("%m_%d_%Y_%I.%M%p")
    
    # Get base path and file name
    base_dir = os.path.dirname(output_file)
    base_name = os.path.basename(output_file)
    base_name_without_ext = os.path.splitext(base_name)[0]
    
    # Create Excel file name with timestamp
    excel_output_file = os.path.join(base_dir, f"{base_name_without_ext}_check_{timestamp}.xlsx")

    if all_data:  # Check if all_data has any elements
        df = pd.DataFrame(all_data)
        # Remove duplicate rows based on all relevant columns
        df = df.drop_duplicates(subset=["Date", "Placement ID", "Impressions", "Clicks", "Video Completions"])

        # Remove "Report Name" and de-duplicate again
        df_no_report_name = df.drop(columns=["Report Name"])
        df_final = df_no_report_name.drop_duplicates(subset=["Date", "Placement ID", "Impressions", "Clicks", "Video Completions"])
        
        # Add aggregation logic here
        # Group by Date and Placement ID and sum the metrics
        numeric_cols = ['Impressions', 'Clicks', 'Video Completions']
        df_final = df_final.groupby(['Date', 'Placement ID'])[numeric_cols].sum().reset_index()
        
        # Save CSV file (keeping original name)
        df_final.to_csv(output_file, index=False)
        
        # Create a DataFrame for reports missing placement IDs
        missing_placement_df = pd.DataFrame({
            "Report Name": reports_missing_placement_ids if reports_missing_placement_ids else ["No reports missing placement IDs"],
            "Issue": ["Missing Placement ID column"] * len(reports_missing_placement_ids) if reports_missing_placement_ids else ["N/A"]
        })
        
        # Create a DataFrame for duplicate placement IDs
        duplicate_data = []
        for placement_id, reports in placement_id_to_reports.items():
            if len(reports) > 1:  # Only include if placement ID appears in multiple reports
                duplicate_data.append({
                    "Placement ID": placement_id,
                    "Appears In Reports": ", ".join(reports),
                    "Number of Reports": len(reports)
                })
        
        # If no duplicates found, add a placeholder row
        if not duplicate_data:
            duplicate_data.append({
                "Placement ID": "No duplicate placement IDs found",
                "Appears In Reports": "N/A",
                "Number of Reports": 0
            })
        
        duplicate_df = pd.DataFrame(duplicate_data)
        
        # Write all three dataframes to the Excel file
        with pd.ExcelWriter(excel_output_file) as writer:
            df_final.to_excel(writer, sheet_name='Merged Report', index=False)
            missing_placement_df.to_excel(writer, sheet_name='Missing Placement IDs', index=False)
            duplicate_df.to_excel(writer, sheet_name='Duplicate Placement IDs', index=False)
        
        logging.info(f"Merged and de-duplicated report saved to {output_file}")
        logging.info(f"Report checks saved to {excel_output_file}")
        print(f"✅ Merged and de-duplicated report saved to {output_file}")
        print(f"✅ Report checks saved to {excel_output_file}")
        
        # Print detailed summary of issues
        print("\n==== REPORT SUMMARY ====")
        print(f"Total files processed: {len(processed_files)} of {len(original_files)}")
        
        if reports_missing_placement_ids:
            print(f"\n🚨 {len(reports_missing_placement_ids)} reports are missing Placement ID column:")
            for report in reports_missing_placement_ids:
                print(f"  - {report}")
        else:
            print(f"\n✅ All reports have Placement ID columns")
        
        duplicate_count = len([pid for pid, reports in placement_id_to_reports.items() if len(reports) > 1])
        if duplicate_count > 0:
            print(f"\n⚠️ Found {duplicate_count} placement IDs that appear in multiple reports")
            print(f"    (See Excel report for full details)")
        else:
            print(f"\n✅ No duplicate placement IDs found across reports")
        
        if skipped_files:
            print(f"\n⚠️ {len(skipped_files)} files were skipped due to errors:")
            for skipped_file in skipped_files:
                print(f"  - {skipped_file}")
        else:
            print(f"\n✅ All files were processed successfully")
    else:
        logging.warning("No valid data found to create a report.")
        print("No valid data found to create a report.")

def update_dcm_main_data(merged_report_path, main_data_path):
    """
    Update the DCM_Main_Data report with new data from the merged report.
    Args:
        merged_report_path: Path to the merged report
        main_data_path: Path to the DCM_Main_Data report
    """
    try:
        # Read both reports
        merged_df = pd.read_csv(merged_report_path)
        main_df = pd.read_csv(main_data_path)
        
        # Ensure Date columns are in datetime format
        merged_df['Date'] = pd.to_datetime(merged_df['Date'])
        main_df['Date'] = pd.to_datetime(main_df['Date'])
        
        # Create a copy of the main data for backup
        backup_path = main_data_path.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        main_df.to_csv(backup_path, index=False)
        logging.info(f"Created backup of DCM_Main_Data at: {backup_path}")
        
        # Convert numeric columns to float for comparison
        numeric_cols = ['Impressions', 'Clicks', 'Video Completions']
        for col in numeric_cols:
            if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
            if col in main_df.columns:
                main_df[col] = pd.to_numeric(main_df[col], errors='coerce')
        
        # Create a key for comparison
        merged_df['key'] = merged_df['Date'].astype(str) + '_' + merged_df['Placement ID'].astype(str)
        main_df['key'] = main_df['Date'].astype(str) + '_' + main_df['Placement ID'].astype(str)
        
        # Find rows that need to be updated (same Date and Placement ID but different metrics)
        update_mask = merged_df['key'].isin(main_df['key'])
        update_df = merged_df[update_mask].copy()
        
        # Find new rows to add (different Date and Placement ID combinations)
        new_rows_mask = ~merged_df['key'].isin(main_df['key'])
        new_rows_df = merged_df[new_rows_mask].copy()
        
        # Update existing rows in main_df
        for _, row in update_df.iterrows():
            mask = (main_df['Date'] == row['Date']) & (main_df['Placement ID'] == row['Placement ID'])
            for col in numeric_cols:
                if col in row and col in main_df.columns:
                    main_df.loc[mask, col] = row[col]
        
        # Add new rows
        if not new_rows_df.empty:
            main_df = pd.concat([main_df, new_rows_df], ignore_index=True)
        
        # Remove the temporary key column
        main_df = main_df.drop(columns=['key'])
        
        # Sort by Date and Placement ID
        main_df = main_df.sort_values(['Date', 'Placement ID'])
        
        # Save the updated main data
        main_df.to_csv(main_data_path, index=False)
        
        # Log summary of changes
        logging.info(f"Updated DCM_Main_Data report:")
        logging.info(f"- Rows updated: {len(update_df)}")
        logging.info(f"- New rows added: {len(new_rows_df)}")
        logging.info(f"- Total rows in updated report: {len(main_df)}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error updating DCM_Main_Data: {e}")
        return False

# Update the main function call at the bottom
if __name__ == "__main__":
    folder_path = r"C:\Catalina_auto_report\third_party_reports\dcm_folder\dcm_email_reports"
    output_file = r"C:\Catalina_auto_report\third_party_reports\dcm_folder\merged_dcm_report.csv"
    main_data_path = r"C:\Catalina_auto_report\third_party_reports\dcm_folder\DCM_Main_Data.csv"
    
    # First create the merged report
    merged_dcm_report(folder_path, output_file)
    
    # Then update the main data file
    if os.path.exists(main_data_path):
        update_dcm_main_data(output_file, main_data_path)
    else:
        logging.warning(f"DCM_Main_Data file not found at {main_data_path}. Creating new file...")
        merged_df = pd.read_csv(output_file)
        merged_df.to_csv(main_data_path, index=False)
        logging.info(f"Created new DCM_Main_Data file at {main_data_path}")
