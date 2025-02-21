#!/usr/bin/env python3
"""
AdSense-to-BigQuery Loader Tool
--------------------------------

This tool automates the process of fetching Google AdSense reports and uploading them
to Google BigQuery. It uses OAuth2 to authenticate with the AdSense API and a service
account for BigQuery access. Once run, the tool saves the authentication details so
that subsequent executions do not require manual login.

Usage:
    python main.py

Requirements:
    - Python 3.x
    - google-auth, google-auth-oauthlib, google-api-python-client,
      google-cloud-bigquery, pandas, pandas-gbq, prettytable

Configuration:
    - Replace the placeholder file paths and project IDs below with your own.
    - Ensure that the OAuth client secrets file and BigQuery service account key file are
      correctly configured and accessible.
"""

import os
import json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from prettytable import PrettyTable
from datetime import datetime, timedelta

# Configuration Constants (Update these with your own paths and project details)
SCOPES = ['https://www.googleapis.com/auth/adsense.readonly',
          'https://www.googleapis.com/auth/bigquery']

# Path to your OAuth client secrets file (downloaded from Google API Console)
CLIENT_SECRETS_FILE = "path/to/your/client_secret.json"

# Paths to store/retrieve AdSense credentials
CREDENTIALS_FILE = "path/to/your/adsense_credentials.json"
CREDENTIALS_FILE_2 = "path/to/your/adsense_credentials_2.json"
CREDENTIALS_FILE_3 = "path/to/your/adsense_credentials_3.json"
CREDENTIALS_FILE_4 = "path/to/your/adsense_credentials_4.json"
# CREDENTIALS_FILE_5 = "path/to/your/adsense_credentials_5.json"  # Optional additional credential

# BigQuery settings (update these with your BigQuery project information)
BIGQUERY_PROJECT_ID = 'your_bigquery_project_id'
BIGQUERY_DATASET_ID = 'your_bigquery_dataset'
BIGQUERY_TABLE_ID = 'your_bigquery_table'
BIGQUERY_SERVICE_ACCOUNT_KEY_FILE = "path/to/your/bigquery_service_account_key.json"
TABLE_FULL_NAME = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"


class BigQueryClient:
    """
    Client for interacting with Google BigQuery.

    Provides methods to delete yesterday's data (to avoid duplicates) and
    push new AdSense report data into a BigQuery table.
    """
    def __init__(self):
        print("Initializing BigQuery client...")
        self.credentials = service_account.Credentials.from_service_account_file(
            BIGQUERY_SERVICE_ACCOUNT_KEY_FILE,
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        self.client = bigquery.Client(credentials=self.credentials,
                                      project=BIGQUERY_PROJECT_ID)
        print("BigQuery client initialized.")

    def delete_yesterday_data(self):
        """
        Delete data from yesterday in the target BigQuery table.
        This prevents duplicate entries when new data is inserted.
        """
        current_date = datetime.now()
        previous_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
        query = f"""
        DELETE FROM `{TABLE_FULL_NAME}`
        WHERE date >= '{previous_date}'
        """
        print(f"Deleting yesterday's data ({previous_date}) from BigQuery...")
        query_job = self.client.query(query)
        query_job.result()  # Wait for the query to complete
        print("Yesterday's data deleted successfully.")

    def push_to_bigquery(self, df):
        """
        Push the given DataFrame to BigQuery.
        
        Before insertion, certain columns are converted to integers and yesterday's
        data is deleted from the target table.
        """
        if df.empty:
            print("No data to insert into BigQuery.")
            return

        # Define columns that should be converted to integer type
        int_cols = [
            'CLICKS', 'PAGE_VIEWS', 'TOTAL_IMPRESSIONS', 'IMPRESSIONS',
            'INDIVIDUAL_AD_IMPRESSIONS', 'MATCHED_AD_REQUESTS', 'AD_REQUESTS'
        ]
        df[int_cols] = df[int_cols].fillna(0).astype(int)

        # Rename columns to match your BigQuery schema (if needed)
        df = df.rename(columns={'AD_REQUESTS_CTR': 'ctr', 'COST_PER_CLICK': 'cpc'})

        # Remove yesterday's data to avoid duplicates
        self.delete_yesterday_data()

        print(f"Pushing {len(df)} rows to BigQuery...")
        pandas_gbq.to_gbq(df, TABLE_FULL_NAME, project_id=BIGQUERY_PROJECT_ID,
                            if_exists='append', credentials=self.credentials)
        print("Data successfully inserted into BigQuery.")


class AdSenseAPI:
    """
    Class to interact with the Google AdSense API.

    Handles authentication via OAuth2 and provides methods to list AdSense
    accounts and fetch reports.
    """
    def __init__(self, client_secrets_file, credentials_file):
        self.service = None
        self.credentials = None
        self.client_secrets_file = client_secrets_file
        self.credentials_file = credentials_file
        self.authenticate()

    def authenticate(self):
        """
        Authenticate using saved credentials if available. If credentials are
        expired or missing, the OAuth flow is initiated.
        """
        print(f"Authenticating with credentials file: {self.credentials_file}")
        if os.path.exists(self.credentials_file):
            self.credentials = Credentials.from_authorized_user_file(self.credentials_file, SCOPES)
            print(f"Using existing credentials from {self.credentials_file}")

        if self.credentials and self.credentials.expired and self.credentials.refresh_token:
            print("Refreshing expired credentials...")
            self.credentials.refresh(Request())

        if not self.credentials or not self.credentials.valid:
            print("No valid credentials found. Starting OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(self.client_secrets_file, SCOPES)
            self.credentials = flow.run_local_server(port=0)
            os.makedirs(os.path.dirname(self.credentials_file), exist_ok=True)
            with open(self.credentials_file, 'w') as token:
                token.write(self.credentials.to_json())
            print("OAuth flow completed, credentials saved.")

        self.service = build('adsense', 'v2', credentials=self.credentials)
        print("AdSense service initialized.")

    def list_accounts(self):
        """
        List all AdSense accounts associated with the authenticated user.
        
        Returns:
            A list of account IDs.
        """
        try:
            print("Fetching AdSense accounts...")
            accounts = self.service.accounts().list().execute()
            account_ids = []
            table = PrettyTable()
            table.field_names = ["Account ID", "Display Name"]

            for account in accounts.get('accounts', []):
                account_id = account['name']
                display_name = account['displayName']
                table.add_row([account_id, display_name])
                account_ids.append(account_id)

            print("\nAvailable AdSense Accounts:")
            print(table)
            return account_ids
        except Exception as e:
            print(f"Error listing AdSense accounts: {e}")
            return []

    def fetch_report(self, account_id):
        """
        Fetch the AdSense report for a given account.

        Retrieves metrics for the period from yesterday to today and returns the data
        as a Pandas DataFrame.

        Returns:
            DataFrame containing the report data.
        """
        previous_date = datetime.now() - timedelta(days=1)
        current_dt = datetime.now()
        print(f"Fetching report for account: {account_id} for dates: {previous_date.date()} to {current_dt.date()}")
        try:
            report = self.service.accounts().reports().generate(
                account=account_id,
                dateRange='CUSTOM',
                startDate_year=previous_date.year,
                startDate_month=previous_date.month,
                startDate_day=previous_date.day,
                endDate_year=current_dt.year,
                endDate_month=current_dt.month,
                endDate_day=current_dt.day,
                metrics=[
                    'ESTIMATED_EARNINGS', 'PAGE_VIEWS', 'PAGE_VIEWS_RPM', 'CLICKS',
                    'AD_REQUESTS_CTR', 'COST_PER_CLICK', 'TOTAL_IMPRESSIONS',
                    'AD_REQUESTS', 'MATCHED_AD_REQUESTS', 'IMPRESSIONS',
                    'INDIVIDUAL_AD_IMPRESSIONS'
                ],
                dimensions=['DATE', 'DOMAIN_NAME', 'COUNTRY_CODE']
            ).execute()

            if 'rows' not in report:
                print("No rows found in the report.")
                return pd.DataFrame()

            rows = []
            for row in report['rows']:
                report_date = row['cells'][0]['value']
                domain = row['cells'][1]['value']
                country = row['cells'][2]['value']
                metrics = {
                    header['name']: float(cell['value'])
                    for header, cell in zip(report['headers'][3:], row['cells'][3:])
                }
                row_data = {
                    "account_id": account_id,
                    "date": report_date,
                    "domain": domain,
                    "country": country,
                    **metrics
                }
                rows.append(row_data)

            return pd.DataFrame(rows)
        except Exception as e:
            print(f"Error fetching AdSense report: {e}")
            return pd.DataFrame()


class AdSenseReportProcessor:
    """
    Processes AdSense reports from multiple accounts and uploads the consolidated data to BigQuery.
    """
    def __init__(self):
        # Initialize multiple AdSense API instances if you have more than one set of credentials.
        self.adsense_api_1 = AdSenseAPI(CLIENT_SECRETS_FILE, CREDENTIALS_FILE)
        self.adsense_api_2 = AdSenseAPI(CLIENT_SECRETS_FILE, CREDENTIALS_FILE_2)
        self.adsense_api_3 = AdSenseAPI(CLIENT_SECRETS_FILE, CREDENTIALS_FILE_3)
        self.adsense_api_4 = AdSenseAPI(CLIENT_SECRETS_FILE, CREDENTIALS_FILE_4)
        # self.adsense_api_5 = AdSenseAPI(CLIENT_SECRETS_FILE, CREDENTIALS_FILE_5)  # Optional

        self.bigquery_client = BigQueryClient()

    def process_reports(self):
        """
        Fetch reports from all configured AdSense accounts, consolidate the data,
        and upload the result to BigQuery.
        """
        all_data = pd.DataFrame()

        # Process accounts from the first set of credentials
        for adsense_api in [self.adsense_api_1]:
            for account_id in adsense_api.list_accounts():
                df = adsense_api.fetch_report(account_id)
                all_data = pd.concat([all_data, df], ignore_index=True)

        # Process accounts from the second set of credentials
        for adsense_api in [self.adsense_api_2]:
            for account_id in adsense_api.list_accounts():
                df = adsense_api.fetch_report(account_id)
                all_data = pd.concat([all_data, df], ignore_index=True)

        # Process accounts from the third set of credentials
        for adsense_api in [self.adsense_api_3]:
            for account_id in adsense_api.list_accounts():
                df = adsense_api.fetch_report(account_id)
                all_data = pd.concat([all_data, df], ignore_index=True)

        # Process accounts from the fourth set of credentials
        for adsense_api in [self.adsense_api_4]:
            for account_id in adsense_api.list_accounts():
                df = adsense_api.fetch_report(account_id)
                all_data = pd.concat([all_data, df], ignore_index=True)

        # Uncomment the block below if you wish to process an additional set of credentials.
        # for adsense_api in [self.adsense_api_5]:
        #     for account_id in adsense_api.list_accounts():
        #         df = adsense_api.fetch_report(account_id)
        #         all_data = pd.concat([all_data, df], ignore_index=True)

        if not all_data.empty:
            self.bigquery_client.push_to_bigquery(all_data)
        else:
            print("No data fetched from AdSense accounts.")


if __name__ == '__main__':
    print("Starting multi-account AdSense report processing...")
    processor = AdSenseReportProcessor()
    processor.process_reports()
    print("Multi-account AdSense report processing completed.")
