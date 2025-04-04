# campaign-reporting-automation
A collection of Python scripts to fully automate 1st and 3rd-party campaign reporting for digital advertising, saving hours of manual work every week.

---
## Script Descriptions (Each script is standalone)

| Script              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `beeswax_report.py` | Downloads Beeswax performance report based on timezone and date inputs.     |
| `beeswax_filter.py` | Filters latest campaign and line item data from Beeswax API (used for 3P).  |
| `dcm_report.py`     | Merges all 3rd-party reports (e.g., DCM) into a single report.              |
| `3p_report.py`      | Matches creative data from Beeswax with 3P data and generates final report. |

# Notes:
- All scripts are built for Python 3.x.
- Modify file paths as needed depending on your working environment.
- Merge logic is based on matching placement ID and date.
- Beeswax creative names are mapped to standardized formats (MO, CTV, DE, etc.). This is company specific taxanomy formats.

### Install dependencies (required libraries)
pip install -r requirements.txt

## Folder Structure
- scripts/ # All main automation scripts 
- config/ # .env config files with API keys, user info, etc. 
- docs/ # Instruction notes or documentation 
-> requirements.txt # Python packages needed to run the scripts

''' edit the config to "input_folder" to match with the scripts.'''

## How to Run
1. Clone the repo
2. Install dependencies:  
   `pip install -r requirements.txt`
3. Add `.env` files to the `config/` folder (format already provided)
4. Run the script using:  
   `python scripts/beeswax_report.py`
