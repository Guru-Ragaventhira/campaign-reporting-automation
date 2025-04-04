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

### Install dependencies (required libraries)
pip install -r requirements.txt

''' edit the config to "input_folder" to match with the scripts.'''
