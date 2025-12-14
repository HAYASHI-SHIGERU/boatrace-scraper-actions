import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

class GoogleSheetsClient:
    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = self._get_credentials()
        self.client = gspread.authorize(self.creds)
        self.spreadsheet_id = os.environ.get('SPREADSHEET_ID')
        if not self.spreadsheet_id:
            raise ValueError("Environment variable SPREADSHEET_ID is not set")
        self.sheet = self.client.open_by_key(self.spreadsheet_id)

    def _get_credentials(self):
        # Prefer environment variable containing the JSON string
        json_creds = os.environ.get('GCP_SA_KEY')
        if json_creds:
            try:
                creds_dict = json.loads(json_creds)
                return ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, self.scope)
            except json.JSONDecodeError:
                raise ValueError("GCP_SA_KEY environment variable is not valid JSON")
        
        # Fallback to file if needed (mostly for local dev if configured)
        # keyfile = 'path/to/keyfile.json' 
        # return ServiceAccountCredentials.from_json_keyfile_name(keyfile, self.scope)
        
        raise ValueError("Environment variable GCP_SA_KEY is not set")

    def write_dataframe(self, worksheet_name, df, append=True):
        """
        Write a pandas DataFrame to a specific worksheet.
        If append is True, it appends to the existing sheet.
        If the sheet doesn't exist, it creates it.
        """
        try:
            worksheet = self.sheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = self.sheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            # Add header if new sheet
            worksheet.append_row(df.columns.tolist())

        # Prepare values
        # Convert all to string to avoid serialization issues, or handle types carefully
        # gspread handles basic types.
        # Handle NaN/None
        
        # Convert dataframe to list of lists
        # data_to_upload = df.fillna('').values.tolist()
        
        # Better: use df.fillna('')
        data_to_upload = df.fillna('').values.tolist()
        
        if append:
            worksheet.append_rows(data_to_upload)
        else:
            # Overwrite or Clear and Write? 
            # Usually for accumulation we want append.
            # If we wanted to clear: worksheet.clear() then write headers + data
             worksheet.append_rows(data_to_upload)
        
        print(f"Written {len(data_to_upload)} rows to '{worksheet_name}'")
