

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def main():
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            print("✅ Token refreshed automatically")
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "/Users/nera_thomas/Downloads/credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open("/Users/nera_thomas/Downloads/token.json", "w") as token:
            token.write(creds.to_json())

        print("✅ token.json saved")

if __name__ == "__main__":
    main()

