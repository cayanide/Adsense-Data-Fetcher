# AdSense Data Fetcher

A Python tool that automates fetching Google AdSense reports from multiple accounts and uploads the consolidated data to Google BigQuery. The tool handles OAuth authentication and credential management for seamless operation.

## Prerequisites

- Python 3.x
- Google Cloud Project with BigQuery enabled
- Google AdSense API access
- Google Cloud service account with BigQuery permissions

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd adsense-data-fetcher
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install required packages:
   ```bash
   pip install google-auth google-auth-oauthlib google-api-python-client google-cloud-bigquery pandas pandas-gbq prettytable
   ```

## Required Files Setup

1. **Create Project Directory Structure:**
   ```
   adsense-data-fetcher/
   ├── main.py
   ├── credentials/
   │   ├── client_secrets/
   │   ├── adsense/
   │   └── bigquery/
   ```

2. **OAuth Client Secret (`client_secret.json`)**:
   - Go to Google Cloud Console > APIs & Services > Credentials
   - Create OAuth 2.0 Client ID (Application Type: Desktop)
   - Download JSON and save as `credentials/client_secrets/client_secret.json`

3. **BigQuery Service Account Key**:
   - In Google Cloud Console > IAM & Admin > Service Accounts
   - Create service account with BigQuery Admin role
   - Create key (JSON) and save as `credentials/bigquery/service_account_key.json`

4. **Update Configuration in `main.py`**:
   ```python
   # Update these paths
   CLIENT_SECRETS_FILE = "credentials/client_secrets/client_secret.json"
   CREDENTIALS_FILE = "credentials/adsense/adsense_credentials.json"
   CREDENTIALS_FILE_2 = "credentials/adsense/adsense_credentials_2.json"
   CREDENTIALS_FILE_3 = "credentials/adsense/adsense_credentials_3.json"
   CREDENTIALS_FILE_4 = "credentials/adsense/adsense_credentials_4.json"
   
   BIGQUERY_SERVICE_ACCOUNT_KEY_FILE = "credentials/bigquery/service_account_key.json"
   
   # Update BigQuery settings
   BIGQUERY_PROJECT_ID = 'your-project-id'
   BIGQUERY_DATASET_ID = 'adsense_reports'
   BIGQUERY_TABLE_ID = 'adsense'
   ```

## BigQuery Setup

1. Create dataset and table:
   ```sql
   -- Create dataset
   CREATE DATASET your-project-id.adsense_reports;
   
   -- Create table
   CREATE TABLE your-project-id.adsense_reports.adsense (
     account_id STRING,
     date DATE,
     domain STRING,
     country STRING,
     ESTIMATED_EARNINGS FLOAT64,
     PAGE_VIEWS INT64,
     PAGE_VIEWS_RPM FLOAT64,
     CLICKS INT64,
     ctr FLOAT64,
     cpc FLOAT64,
     TOTAL_IMPRESSIONS INT64,
     AD_REQUESTS INT64,
     MATCHED_AD_REQUESTS INT64,
     IMPRESSIONS INT64,
     INDIVIDUAL_AD_IMPRESSIONS INT64
   );
   ```

## First-Time Setup

1. Run the script:
   ```bash
   python main.py
   ```

2. For each AdSense account:
   - Browser window will open for OAuth authentication
   - Log in with the Google account associated with AdSense
   - Grant permissions
   - Credentials will be saved automatically

## Scheduling

### Linux (Cron)

1. Make script executable:
   ```bash
   chmod +x /path/to/adsense-data-fetcher/main.py
   ```

2. Add cron job (runs daily at midnight):
   ```bash
   0 0 * * * cd /path/to/adsense-data-fetcher && ./venv/bin/python main.py >> logs/adsense_fetcher.log 2>&1
   ```

### Windows (Task Scheduler)

1. Create batch file `run_adsense.bat`:
   ```batch
   @echo off
   cd /d "C:\path\to\adsense-data-fetcher"
   call venv\Scripts\activate
   python main.py >> logs\adsense_fetcher.log 2>&1
   ```

2. Create task in Task Scheduler:
   - Action: Start a program
   - Program: `C:\path\to\adsense-data-fetcher\run_adsense.bat`
   - Schedule: Daily at midnight

## Troubleshooting

### OAuth Issues
- Delete corresponding credentials file in `credentials/adsense/`
- Re-run script to trigger new OAuth flow

### BigQuery Errors
- Verify service account has BigQuery Admin role
- Check if BigQuery API is enabled
- Verify project ID, dataset, and table names

### No Data in Reports
- Verify date range in `fetch_report` method
- Check if AdSense account has active ads
- Verify OAuth scope includes `https://www.googleapis.com/auth/adsense.readonly`

## Data Flow

1. Script authenticates with each AdSense account
2. Fetches previous day's data for each account
3. Deletes existing data in BigQuery for the same date
4. Uploads new data to BigQuery
5. Logs operations and any errors

## Customization

- Modify date range in `fetch_report` method
- Add/remove metrics in the API call
- Adjust BigQuery schema as needed
- Configure additional AdSense accounts by adding new credential file paths

## Support

For issues:
- Check logs in `logs/adsense_fetcher.log`
- Verify all credentials and permissions
- Ensure all required APIs are enabled in Google Cloud Console
