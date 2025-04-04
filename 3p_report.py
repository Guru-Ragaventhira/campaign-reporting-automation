import os
import pandas as pd
import time
from datetime import datetime

def process_reports():
    """Generates the Third_Party_Data report and saves it locally before uploading to Google Sheets."""
    start_time = time.time()  # Start Timer
    print("🔍 Starting process...")

    dcm_folder = r"C:\Catalina_auto_report\third_party_reports\dcm_folder"
    beeswax_folder = r"C:\Catalina_auto_report\Beeswax_Data"
    output_folder = dcm_folder
    output_file = f"Third_Party_Data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Load DCM report
    dcm_report_path = os.path.join(dcm_folder, "merged_dcm_report.csv")
    if not os.path.exists(dcm_report_path):
        print(f"❌ Error: DCM report not found at {dcm_report_path}")
        return
    print("📥 Loading DCM report...")
    dcm_df = pd.read_csv(dcm_report_path, dtype=str)

    # Find the latest Beeswax report
    print("🔍 Searching for latest Beeswax report...")
    beeswax_files = [f for f in os.listdir(beeswax_folder) if f.startswith("beeswax_filtered_report_")]
    if not beeswax_files:
        print("❌ Error: No Beeswax reports found.")
        return

    latest_beeswax_file = max(beeswax_files, key=lambda f: datetime.strptime(f.split("report_")[1].split(".")[0], "%m%d%Y%H%M%S"))
    beeswax_report_path = os.path.join(beeswax_folder, latest_beeswax_file)
    print(f"✅ Found latest Beeswax report: {latest_beeswax_file}")

    if not os.path.exists(beeswax_report_path):
        print(f"❌ Error: Beeswax report not found at {beeswax_report_path}")
        return
    print("📥 Loading Beeswax report...")
    beeswax_df = pd.read_csv(beeswax_report_path, dtype=str)

    # Define the correct columns
    search_columns = ["creative_pixels", "creative_scripts", "creative_creative_content_munge"]
    available_columns = [col for col in search_columns if col in beeswax_df.columns]

    if not available_columns:
        print("❌ Error: None of the expected search columns found in Beeswax report.")
        return

    print(f"✅ Using columns for search: {available_columns}")

    # Combine only the available columns
    beeswax_df["combined_check"] = beeswax_df[available_columns].fillna("").agg(" ".join, axis=1)

    # Search function
    def find_match(placement_id):
        return beeswax_df[beeswax_df["combined_check"].str.contains(placement_id, na=False)]

    print("🚀 Processing DCM placements... (this might take some time)")
    results = []
    total_rows = len(dcm_df)

    for idx, dcm_row in dcm_df.iterrows():
        if idx % 100 == 0:
            print(f"📝 Processed {idx}/{total_rows} placements...")

        placement_id = dcm_row["Placement ID"]
        dcm_date = dcm_row["Date"]
        dcm_impressions = dcm_row["Impressions"]
        dcm_clicks = dcm_row["Clicks"]
        dcm_video_completions = dcm_row["Video Completions"]

        matched_rows = find_match(placement_id)

        if not matched_rows.empty:
            for _, beeswax_row in matched_rows.iterrows():
                creative_name = beeswax_row.get("creative_creative_name", "Unknown")
                campaign_id = beeswax_row.get("campaign_campaign_id", "Unknown")

                creative_type = (
                    "Mobile" if creative_name.startswith("MO") else
                    "Desktop" if creative_name.startswith("DE_") else
                    "CTV" if creative_name.startswith("CTV_") else "Unknown"
                )

                creative_format = (
                    "Banner" if "_BA_" in creative_name or "_RM_" in creative_name else
                    "Video" if "_VI_" in creative_name else "Unknown"
                )

                bees_name = f"{campaign_id}_{creative_type}_{creative_format}"
                results.append([placement_id, bees_name, dcm_date, dcm_impressions, dcm_clicks, dcm_video_completions])
        else:
            results.append([placement_id, "Placement ID not found in Beeswax", dcm_date, dcm_impressions, dcm_clicks, dcm_video_completions])

    print("✅ Finished processing placements. Saving output...")

    output_df = pd.DataFrame(results, columns=["Placement ID", "Bees_Name", "Date", "Impressions", "Clicks", "Video Completions"])
    output_df["Date"] = pd.to_datetime(output_df["Date"], errors="coerce").dt.strftime("%B %e, %Y")

    # Deduplicate based on all columns
    output_df.drop_duplicates(subset=["Placement ID", "Bees_Name", "Date", "Impressions", "Clicks", "Video Completions"], inplace=True)

    output_path = os.path.join(output_folder, output_file)
    output_df.to_csv(output_path, index=False)

    print(f"🎉 Report generated successfully: {output_path}")

if __name__ == "__main__":
    process_reports()