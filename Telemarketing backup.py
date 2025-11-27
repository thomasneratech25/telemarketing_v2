import os
import re
import time
import json
import socket
import atexit
import requests
import subprocess
from dotenv import load_dotenv
from pymongo import MongoClient
from urllib.parse import urlencode
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta, timezone
from google_auth_oauthlib.flow import InstalledAppFlow
from playwright.sync_api import sync_playwright, expect

# Load MongoDB API Key 
load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/api/mongodb/.env")
MONGODB_URI = os.getenv("MONGODB_API_KEY")

# Chrome Settings
class Automation:

    # Chrome CDP 
    chrome_proc = None
    @classmethod
    def chrome_CDP(cls):

        # User Profile
        USER_DATA_DIR = f"/Users/nera_thomas/Library/Application Support/Google/Chrome/Profile 9"
        
        # Start Chrome normally
        cls.chrome_proc = subprocess.Popen([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "--remote-debugging-port=9222",
            "--disable-session-crashed-bubble",
            "--hide-crash-restore-bubble",
            "--no-first-run",
            "--no-default-browser-check",
            f"--user-data-dir={USER_DATA_DIR}",  # User Profile
            # "--headless=new",                    # ------> if want to use headless mode, use --windows-size together, due to headless mode small screen size
            "--window-size=1920,1080",           # ✅ simulate full HD
            "--force-device-scale-factor=1",     # ✅ ensure no zoom scalin
        ],
        stdout=subprocess.DEVNULL,  # ✅ hide chrome cdp logs
        stderr=subprocess.DEVNULL   # ✅ hide chrome cdp logs
        )
    
        # wait for Chrome CDP launch...
        cls.wait_for_cdp_ready()

        atexit.register(cls.cleanup)

    # Close Chrome CDP
    @classmethod
    def cleanup(cls):
        try:
            cls.chrome_proc.terminate()
        except Exception as e:
            print(f"Error terminating Chrome: {e}")
    
    # Wait for Chrome CDP to be ready
    @staticmethod
    def wait_for_cdp_ready(timeout=10):
        """Wait until Chrome CDP is ready at http://localhost:9222/json"""
        for _ in range(timeout):
            try:
                res = requests.get("http://localhost:9222/json")
                if res.status_code == 200:
                    return True
            except:
                pass
            time.sleep(1)
        raise RuntimeError("Chrome CDP is not ready after waiting.")

# BO Account
class BO_Account:

   # Load the .env file
   # Format to use in other Class ===>             cls.accounts["gc99"]["merchant_code"]
    load_dotenv()

    accounts = {
        "super_swan": {
            "acc_ID": os.getenv("ACC_ID_SUPERSWAN"),
            "acc_PASS": os.getenv("ACC_PASS_SUPERSWAN")
        },
        "wdb1": {
            "acc_ID": os.getenv("ACC_ID_WDB1"),
            "acc_PASS": os.getenv("ACC_PASS_WDB1")
        },
        "22fun": {
            "merchant_code": os.getenv("MERCHANT_CODE_22FUN"),
            "acc_ID": os.getenv("ACC_ID_22FUN"),
            "acc_PASS": os.getenv("ACC_PASS_22FUN")
        },
        "gc99": {
            "merchant_code": os.getenv("MERCHANT_CODE_GC99"),
            "acc_ID": os.getenv("ACC_ID_GC99"),
            "acc_PASS": os.getenv("ACC_PASS_GC99")
        },
        "828": {
            "merchant_code": os.getenv("MERCHANT_CODE_828"),
            "acc_ID": os.getenv("ACC_ID_828"),
            "acc_PASS": os.getenv("ACC_PASS_828")
        },
        "jw8": {
            "merchant_code": os.getenv("MERCHANT_CODE_JW8"),
            "acc_ID": os.getenv("ACC_ID_JW8"),
            "acc_PASS": os.getenv("ACC_PASS_JW8")
        },
        "n191": {
            "merchant_code": os.getenv("MERCHANT_CODE_N191"),
            "acc_ID": os.getenv("ACC_ID_N191"),
            "acc_PASS": os.getenv("ACC_PASS_N191")
        },
        "slot": {
            "merchant_code": os.getenv("MERCHANT_CODE_SLOT"),
            "acc_ID": os.getenv("ACC_ID_SLOT"),
            "acc_PASS": os.getenv("ACC_PASS_SLOT")
        },
        "s345": {
            "merchant_code": os.getenv("MERCHANT_CODE_S345"),
            "acc_ID": os.getenv("ACC_ID_S345"),
            "acc_PASS": os.getenv("ACC_PASS_S345")
        },
        "s369": {
            "merchant_code": os.getenv("MERCHANT_CODE_S369"),
            "acc_ID": os.getenv("ACC_ID_S369"),
            "acc_PASS": os.getenv("ACC_PASS_S369")
        },
        "s66": {
            "merchant_code": os.getenv("MERCHANT_CODE_S66"),
            "acc_ID": os.getenv("ACC_ID_S66"),
            "acc_PASS": os.getenv("ACC_PASS_S66")
        },
        "s855": {
            "merchant_code": os.getenv("MERCHANT_CODE_S855"),
            "acc_ID": os.getenv("ACC_ID_S855"),
            "acc_PASS": os.getenv("ACC_PASS_S855")
        },
        "s55": {
            "merchant_code": os.getenv("MERCHANT_CODE_S55"),
            "acc_ID": os.getenv("ACC_ID_S55"),
            "acc_PASS": os.getenv("ACC_PASS_S55")
        },
        "aw8": {
            "merchant_code": os.getenv("MERCHANT_CODE_AW8"),
            "acc_ID": os.getenv("ACC_ID_AW8"),
            "acc_PASS": os.getenv("ACC_PASS_AW8")
        },
        "n789": {
            "merchant_code": os.getenv("MERCHANT_CODE_N789"),
            "acc_ID": os.getenv("ACC_ID_N789"),
            "acc_PASS": os.getenv("ACC_PASS_N789")
        },
        "s212": {
            "merchant_code": os.getenv("MERCHANT_CODE_S212"),
            "acc_ID": os.getenv("ACC_ID_S212"),
            "acc_PASS": os.getenv("ACC_PASS_S212")
        },
        "mf191": {
            "merchant_code": os.getenv("MERCHANT_CODE_MF191"),
            "acc_ID": os.getenv("ACC_ID_MF191"),
            "acc_PASS": os.getenv("ACC_PASS_MF191")
        },
        "n855": {
            "merchant_code": os.getenv("MERCHANT_CODE_N855"),
            "acc_ID": os.getenv("ACC_ID_N855"),
            "acc_PASS": os.getenv("ACC_PASS_N855")
        },
        "g855": {
            "merchant_code": os.getenv("MERCHANT_CODE_G855"),
            "acc_ID": os.getenv("ACC_ID_G855"),
            "acc_PASS": os.getenv("ACC_PASS_G855")
        },
        "g345": {
            "merchant_code": os.getenv("MERCHANT_CODE_G345"),
            "acc_ID": os.getenv("ACC_ID_G345"),
            "acc_PASS": os.getenv("ACC_PASS_G345")
        },
        "r66": {
            "merchant_code": os.getenv("MERCHANT_CODE_R66"),
            "acc_ID": os.getenv("ACC_ID_R66"),
            "acc_PASS": os.getenv("ACC_PASS_R66")
        },
        "r99": {
            "merchant_code": os.getenv("MERCHANT_CODE_R99"),
            "acc_ID": os.getenv("ACC_ID_R99"),
            "acc_PASS": os.getenv("ACC_PASS_R99")
        },
        "22W": {
            "merchant_code": os.getenv("MERCHANT_CODE_22W"),
            "acc_ID": os.getenv("ACC_ID_22W"),
            "acc_PASS": os.getenv("ACC_PASS_22W")
        },
        "i88": {    
            "merchant_code": os.getenv("MERCHANT_CODE_I88"),
            "acc_ID": os.getenv("ACC_ID_I88"),
            "acc_PASS": os.getenv("ACC_PASS_I88")
        },
        "k88": {
            "merchant_code": os.getenv("MERCHANT_CODE_K88"),
            "acc_ID": os.getenv("ACC_ID_K88"),
            "acc_PASS": os.getenv("ACC_PASS_K88")
        },
        "jaya11": {
            "merchant_code": os.getenv("MERCHANT_CODE_JAYA11"),
            "acc_ID": os.getenv("ACC_ID_JAYA11"),
            "acc_PASS": os.getenv("ACC_PASS_JAYA11")
        },
        "joli": {
            "merchant_code": os.getenv("MERCHANT_CODE_JOLI"),
            "acc_ID": os.getenv("ACC_ID_JOLI"),
            "acc_PASS": os.getenv("ACC_PASS_JOLI")
        },
        "uea8": {
            "merchant_code": os.getenv("MERCHANT_CODE_UEA8"),
            "acc_ID": os.getenv("ACC_ID_UEA8"),
            "acc_PASS": os.getenv("ACC_PASS_UEA8")
        },
    }

# MongoDB <-> Google Sheet
class mongodb_2_gs:

    # Handles Google Sheets operations for the Telemarketing project.
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    TOKEN_PATH = "./api/google/token.json"
    CREDS_PATH = "./api/google/credentials.json"

    # Google API Authentication
    @classmethod
    def googleAPI(cls):
        """Authenticate and return Google credentials."""
        creds = None

        # Load token if exists
        if os.path.exists(cls.TOKEN_PATH):
            creds = Credentials.from_authorized_user_file(cls.TOKEN_PATH, cls.SCOPES)

        # Refresh or create new credentials if invalid
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(cls.CREDS_PATH, cls.SCOPES)
                creds = flow.run_local_server(port=0)

            # Save refreshed credentials
            with open(cls.TOKEN_PATH, "w") as token:
                token.write(creds.to_json())

        return creds

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= SSBO DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_ssbo_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["Telemarketing"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field are Unique Data
        collection.create_index(
            [("memberLogin", 1), ("confirmedAmount", 1), ("lastModifiedDate", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []

        batch = []

        # for each rows in a list of JSON objects return
        for row in rows:
            # Extract only the fields you want (Extract Data from json file)
            memberLogin = row.get("memberLogin")
            confirmedAmount = row.get("confirmedAmount")
            lastModifiedDate = row.get("lastModifiedDate")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            if lastModifiedDate:
                try:
                    dt = datetime.fromisoformat(lastModifiedDate.replace("Z", "+00:00"))
                    lastModifiedDate_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    lastModifiedDate_fmt = lastModifiedDate
            else:
                lastModifiedDate_fmt = ""

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "memberLogin": memberLogin,
                "confirmedAmount": confirmedAmount,
                "lastModifiedDate": lastModifiedDate_fmt
            }

            # Keep the version without _id for uploading later
            cleaned_docs.append(doc.copy())
            batch.append(doc)

            if len(batch) == 500:
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception as exc:
                    if hasattr(exc, "details"):
                        skipped += len(exc.details.get("writeErrors", []))
                        inserted += len(batch) - len(exc.details.get("writeErrors", []))
                    else:
                        skipped += 0
                batch = []

        # Insert any remaining documents in batch
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
            except Exception as exc:
                if hasattr(exc, "details"):
                    skipped += len(exc.details.get("writeErrors", []))
                    inserted += len(batch) - len(exc.details.get("writeErrors", []))
                else:
                    skipped += 0

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}\n")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB
    @classmethod
    def upload_to_google_sheet_ssbo_DL_PID(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!{start_column}3:{end_column}"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; keep completed_at unchanged."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    pid = str(r.get("player_id", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    sanitized.append([
                        pid,
                        str(r.get("amount", "")),
                        r.get("completed_at", ""),  # do not coerce to str
                    ])
                elif isinstance(r, (list, tuple)):
                    pid = str(r[0]) if len(r) > 0 else ""
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"
                    sanitized.append([
                        pid,
                        str(r[1]) if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",  # do not coerce to str
                    ])
                else:
                    sanitized.append([str(r), "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            if not MONGODB_URI:
                raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
            client = MongoClient(MONGODB_URI)

            db = client["Telemarketing"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("completed_at", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")
        # Clear previous data then write the fresh rows
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.")

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["Telemarketing"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("player_id", 1), ("amount", 1), ("completed_at", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []

        batch = []

        # for each rows in a list of JSON objects return
        for row in rows:
            # Extract only the fields you want (Extract Data from json file)
            player_id = row.get("player_id")
            amount = row.get("amount")
            completed_at = row.get("completed_at")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            dt = datetime.fromisoformat(completed_at)
            completed_at_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "player_id": player_id,
                "amount": amount,
                "completed_at": completed_at_fmt,
            }

            # Keep the version without _id for uploading later
            cleaned_docs.append(doc.copy())
            batch.append(doc)

            if len(batch) == 500:
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception as exc:
                    if hasattr(exc, "details"):
                        skipped += len(exc.details.get("writeErrors", []))
                        inserted += len(batch) - len(exc.details.get("writeErrors", []))
                    else:
                        skipped += 0
                batch = []

        # Insert any remaining documents in batch
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
            except Exception as exc:
                if hasattr(exc, "details"):
                    skipped += len(exc.details.get("writeErrors", []))
                    inserted += len(batch) - len(exc.details.get("writeErrors", []))
                else:
                    skipped += 0

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}\n")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB
    @classmethod
    def upload_to_google_sheet_DL_PID(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!{start_column}3:{end_column}"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; keep completed_at unchanged."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    pid = str(r.get("player_id", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    sanitized.append([
                        pid,
                        str(r.get("amount", "")),
                        r.get("completed_at", ""),  # do not coerce to str
                    ])
                elif isinstance(r, (list, tuple)):
                    pid = str(r[0]) if len(r) > 0 else ""
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"
                    sanitized.append([
                        pid,
                        str(r[1]) if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",  # do not coerce to str
                    ])
                else:
                    sanitized.append([str(r), "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            if not MONGODB_URI:
                raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
            client = MongoClient(MONGODB_URI)

            db = client["Telemarketing"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("completed_at", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")
        # Clear previous data then write the fresh rows
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.")

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= DEPOSIT LIST (USERNAME) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================
    
    # MongoDB Database 
    def mongodbAPI_DL_USERNAME(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["Telemarketing"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("username", 1), ("amount", 1), ("completed_at", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []

        batch = []

        # for each rows in a list of JSON objects return
        for row in rows:
            # Extract only the fields you want (Extract Data from json file)
            username = row.get("username")
            amount = row.get("amount")
            completed_at = row.get("completed_at")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            dt = datetime.fromisoformat(completed_at)
            completed_at_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "username": username,
                "amount": amount,
                "completed_at": completed_at_fmt,
            }

            # Keep the version without _id for uploading later
            cleaned_docs.append(doc.copy())
            batch.append(doc)

            if len(batch) == 500:
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception as exc:
                    if hasattr(exc, "details"):
                        skipped += len(exc.details.get("writeErrors", []))
                        inserted += len(batch) - len(exc.details.get("writeErrors", []))
                    else:
                        skipped += 0
                batch = []

        # Insert any remaining documents in batch
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
            except Exception as exc:
                if hasattr(exc, "details"):
                    skipped += len(exc.details.get("writeErrors", []))
                    inserted += len(batch) - len(exc.details.get("writeErrors", []))
                else:
                    skipped += 0

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}\n")
        return cleaned_docs

        # Update Data to Google Sheet from MongoDB (Deposit List) (Username)
    
    # Upload Google Sheet
    @classmethod
    def upload_to_google_sheet_DL_USERNAME(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!{start_column}3:{end_column}"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; keep completed_at unchanged."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    pid = str(r.get("username", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    sanitized.append([
                        pid,
                        str(r.get("amount", "")),
                        r.get("completed_at", ""),  # do not coerce to str
                    ])
                elif isinstance(r, (list, tuple)):
                    pid = str(r[0]) if len(r) > 0 else ""
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"
                    sanitized.append([
                        pid,
                        str(r[1]) if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",  # do not coerce to str
                    ])
                else:
                    sanitized.append([str(r), "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            if not MONGODB_URI:
                raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
            client = MongoClient(MONGODB_URI)

            db = client["Telemarketing"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("completed_at", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")
        # Clear previous data then write the fresh rows
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.")

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # MongoDB Database (Member Info)
    def mongodbAPI_MI(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["Telemarketing"]
        collection = db[collection]

        # Ensure deposit-specific unique index does not block member inserts
        try:
            collection.drop_index("player_id_1_amount_1_completed_at_1")
        except Exception:
            pass

       # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("username", 1), ("register_info_date", 1), ("mobileno", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []

        batch = []

        # for each rows in a list of JSON objects return
        for row in rows:

            # Skip null or invalid rows
            if not isinstance(row, dict):
                continue

            # Extract only the fields you want (Extract Data from json file)
            username= row.get("username")
            first_name = row.get("first_name")
            register_info_date = row.get("register_info_date")
            mobileno = row.get("mobileno")
            member_id = row.get("member_id")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            dt = datetime.fromisoformat(register_info_date)
            register_info_date_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "username": username,
                "first_name": first_name,
                "register_info_date": register_info_date_fmt,
                "mobileno": mobileno,
                "member_id": member_id,
            }

            # Keep the version without _id for uploading later
            cleaned_docs.append(doc.copy())
            batch.append(doc)

            if len(batch) == 500:
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception as exc:
                    if hasattr(exc, "details"):
                        skipped += len(exc.details.get("writeErrors", []))
                        inserted += len(batch) - len(exc.details.get("writeErrors", []))
                    else:
                        skipped += 0
                batch = []

        # Insert any remaining documents in batch
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
            except Exception as exc:
                if hasattr(exc, "details"):
                    skipped += len(exc.details.get("writeErrors", []))
                    inserted += len(batch) - len(exc.details.get("writeErrors", []))
                else:
                    skipped += 0

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB (MemberInfo)
    @classmethod
    def upload_to_google_sheet_MI(cls, collection, gs_id, gs_tab, rows=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!A3:E"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for Member Info."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get("username", "")),
                        str(r.get("first_name", "")),
                        str(r.get("register_info_date", "")),
                        str(r.get("mobileno", "")),
                        str(r.get("member_id", "")),
                    ])
                else:
                    sanitized.append(["", "", "", "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            client = MongoClient(MONGODB_URI)

            db = client["Telemarketing"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("register_info_date", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")
        # Clear previous data then write the fresh rows
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.")

    # Update Data to Google Sheet from MongoDB (MemberInfo) Split Data (Odd/Even)
    @classmethod
    def upload_to_google_sheet_MI_SPLIT_DATA(cls, collection, gs_ids, gs_tab, rows=None):
        """
        Split Member Info rows into odd/even:
        - odd rows → gs_ids[0]
        - even rows → gs_ids[1]
        """

        # Authenticate
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Convert MongoDB docs → Google Sheets rows
        def sanitize_rows(raw_rows):
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get("username", "")),
                        str(r.get("first_name", "")),
                        str(r.get("register_info_date", "")),
                        str(r.get("mobileno", "")),
                        str(r.get("member_id", "")),
                    ])
                else:
                    sanitized.append(["", "", "", "", ""])
            return sanitized

        # Load from MongoDB if not provided
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            client = MongoClient(MONGODB_URI)
            db = client["Telemarketing"]
            col = db[collection]

            documents = list(col.find({}, {"_id": 0}).sort("register_info_date", 1))
            rows = documents

        # Format rows for GS API
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload.")
            return

        # Split odd/even
        odd_rows = []
        even_rows = []

        for i, row in enumerate(rows):
            if (i + 1) % 2 == 0:
                even_rows.append(row)  # row #2,4,6,8...
            else:
                odd_rows.append(row)   # row #1,3,5,7...

        chunks = [odd_rows, even_rows]

        # Upload each split dataset to each Sheet ID
        for idx, chunk in enumerate(chunks):
            if idx >= len(gs_ids):
                break

            SPREADSHEET_ID = gs_ids[idx]
            RANGE_NAME = f"{gs_tab}!A3:E"

            print(f"Uploading {len(chunk)} rows → Sheet {idx+1} ({SPREADSHEET_ID})")

            body = {"values": chunk, "majorDimension": "ROWS"}

            # Clear existing rows
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME
            ).execute()

            # Upload
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()

        print("All split uploads completed.")

# Fetch Data
class Fetch(Automation, BO_Account, mongodb_2_gs):
    
    # =========================== GET Cookies ===========================

    # Get IBS Cookies incase Cookies expired
    @classmethod
    def _get_cookies(cls, bo_link, merchant_code, acc_id, acc_pass, cookies_path):
        with sync_playwright() as p:
            # Wait for Chrome CDP to be ready
            cls.wait_for_cdp_ready()

            # Connect to running Chrome
            browser = p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            # Clean Cookies
            context.clear_cookies()

            # Open a new browser page
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(f"https://v3-bo.{bo_link}", wait_until="load", timeout=0)

            # Delay 2 seconds
            page.wait_for_timeout(2000)

            # if is in Login Page, then Login, else Skip
            try:
                # Check whether "Back Office Login" appear, else pass
                expect(page.locator("//div[@class='lg:mb-10 mb-6 text-lg lg:text-2xl text-center text-primary']")).to_be_visible(timeout=2000)
                # Wait for captcha to appear
                page.locator(".text-2xl > span:first-child").wait_for(state="visible", timeout=0)
                # Get Captcha Code
                captcha_code = page.locator("//div[@class='font-normal cursor-pointer tracking-normal space-x-3 text-2xl']").inner_text()
                # Fill in Merchant Code
                page.locator("//input[@placeholder='Merchant Code']").fill(merchant_code)
                # Fill in Username
                page.locator("//input[@placeholder='Username']").fill(acc_id)
                # Fill in Password
                page.locator("//input[@placeholder='Password']").fill(acc_pass)
                # Fill in Captcha Code
                page.locator("//input[@placeholder='Captcha Code']").fill(captcha_code)
                # Button click "Login"
                page.click("//button[normalize-space()='Login']", force=True)
            except (TimeoutError, Exception):
                pass

            # Wait for "Dashboard" appear
            page.wait_for_selector("//span[normalize-space()='Dashboard']", state="visible", timeout=300000)

            time.sleep(1)

            # Extract all cookies from the current context
            cookies = context.cookies()

            # Convert to Json Format
            cookies_json = json.dumps(cookies, indent=4, ensure_ascii=False)

            # Using regex to get cookies
            get_cookies = re.search(r'"name"\s*:\s*"user".*?"value"\s*:\s*"([^"]+)"', cookies_json, re.S)

            # Save to JSON file
            data = {"user_cookie": get_cookies.group(1) if get_cookies else ""}
            with open(cookies_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            # Print dynamic message based on merchant_code or output_path
            merchant_name = None
            for k, v in cls.accounts.items():
                if isinstance(v, dict) and v.get("merchant_code") == merchant_code:
                    merchant_name = k
                    break
            if merchant_name:
                print(f"✅ Get {merchant_name} Cookies Successful ...")
            else:
                print(f"✅ Get BO Cookies Successful ...")

            # Browser Quit
            browser.close()
            Automation.cleanup()

    # Get SSBO Cookies incase Cookies expired
    @classmethod
    def _ssbo_get_cookies(cls):
        with sync_playwright() as p:  
            
            # Load Env
            load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/.env")

            # Wait for Chrome CDP to be ready
            cls.wait_for_cdp_ready()

            # Connect to running Chrome
            browser = p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0] if browser.contexts else browser.new_context()    

            # Open a new browser page
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://aw8.premium-bo.com/#/member/member-info", wait_until="load", timeout=0)

            # if announment appear, then click close
            try:
                # Wait for "Member" appear
                expect(page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[4]/div[1]/div[1]/a[1]/ul[1]/li[2]")).to_be_visible(timeout=30000)
                # Check whether "Merchant credit balance is low" appear, else pass
                expect(page.locator("//div[normalize-space()='Merchant credit balance is low.']")).to_be_visible(timeout=1500)
                # Click checkbox
                page.locator("//div[@class='disable-low-merchant-credit-balance']//input[@type='checkbox']").click()
                time.sleep(1)
                # Click Close
                page.locator("//button[normalize-space()='Close']").click()
            except:
                pass

            # if is in Login Page, then Login, else Skip
            try:
                # Check whether "Sign In" appear, else pass
                expect(page.locator("//h5[normalize-space()='Sign In?']")).to_be_visible(timeout=2000)
                # Fill in Username
                page.locator("//input[@placeholder='Username:']").fill(cls.accounts["super_swan"]["acc_ID"])
                # Fill in Password
                page.locator("//input[@id='password-input']").fill(cls.accounts["super_swan"]["acc_PASS"])
                # Login 
                page.click("//jhi-form-shared-component[@ng-reflect-disabled='false']//button[@class='btn btn-primary btn-form btn-submit login-label-color'][normalize-space()='Login']", force=True)
                # Delay 2 second
                page.wait_for_timeout(2000)
                # 
            except:
                pass

            # if is in Login Page, then Login, else Skip
            try:
                # Check whether "Sign In" appear, else pass
                expect(page.locator("//body[1]/ngb-modal-window[11]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[1]/h5[1]")).to_be_visible(timeout=4000)
                # Fill in Username
                page.locator("//body[1]/ngb-modal-window[11]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[2]/div[2]/jhi-text-shared-component[1]/div[1]/div[1]/div[1]/input[1]").fill(cls.accounts["super_swan"]["acc_ID"])
                # Fill in Password
                page.locator("//body[1]/ngb-modal-window[11]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[3]/div[2]/jhi-password-shared-component[1]/div[1]/div[1]/div[1]/input[1]").fill(cls.accounts["super_swan"]["acc_PASS"])
                # Login 
                page.click("//jhi-form-shared-component[@ng-reflect-disabled='false']//button[@class='btn btn-primary btn-form btn-submit login-label-color'][normalize-space()='Login']", force=True)
                # Delay 2 second
                page.wait_for_timeout(2000)
                # Click Member
                page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[4]/div[1]/div[1]/a[1]/ul[1]/li[2]").click()
                # Click Member Info
                page.locator("//a[@ng-reflect-router-link='/member/member-info']//li[@class='parent-nav-item'][normalize-space()='1.3. Member Info']").click()
            except:
                pass
            
            # Click All
            page.locator("//button[normalize-space()='All']").click()
            # Delay 1 second
            page.wait_for_timeout(1000)

            # Extract all cookies
            cookies = context.cookies()

            # # Convert to Json Format
            # cookies_json = json.dumps(cookies, indent=4, ensure_ascii=False)
            # print(cookies_json)

            # Extract all cookies starting with "_ga"
            ga_cookies = [c for c in cookies if c.get("name", "").startswith("_ga")]

            # Initialize parts list
            parts = []

            # ============== Get GA1 Cookies ===============

            for c in ga_cookies:
                name = c.get("name")
                value = c.get("value")
                if name == "_ga":  # main GA cookie
                    parts.append(value)
                else:
                    parts.append(f"{name}={value}")

            # Join everything with "; "
            combined_ga = "; ".join(parts)
            # print(f"✅ Combined GA cookies: {combined_ga}")

            # =============== Get Bearer Token =================

            bearer_token = ""
            try:
                keys = page.evaluate("() => Object.keys(localStorage)")
                for key in keys:
                    value = page.evaluate(f"() => window.localStorage.getItem('{key}')")
                    # Find "jwt":"xxxxx" inside JSON text
                    match = re.search(r'"jwt"\s*:\s*"([^"]+)"', value or "")
                    if match:
                        bearer_token = match.group(1)
                        # print(f"✅ Found Bearer Token:\nBearer {bearer_token}")
                        break
            except Exception as e:
                print(f"⚠️ Error extracting bearer token: {e}")

            # Save cookie and bearer token to JSON file
            data = {
                "user_cookie": combined_ga,
                "bearer_token": bearer_token
            }
            output_path = "/Users/nera_thomas/Desktop/Telemarketing/get_cookies/superswan.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            print(f"✅ Get Super Swan Cookies + Bearer Token Successful ...")

            # Browser Quit
            browser.close()
            Automation.cleanup()

    # =========================== DEPOSIT LIST ===========================

    # Deposit List (Player ID)
    @classmethod
    def deposit_list_PID(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column):

        # Tell ultra listener which collection + sheet columns to watch
        cls.notify_listener(collection, gs_id, gs_tab, start_column, end_column, mode="deposit")

        # Get today date and time
        today = datetime.now()
        today = today.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")
        
        url = f"https://v3-bo.{bo_link}/api/be/finance/get-deposit"

        payload = {
        "paginate": 100,
        "page": 1,
        "currency": [
            currency
        ],
        "status": "approved",
        "start_date": "2025-11-01",
        "end_date": today,
        "gmt": gmt_time,
        "merchant_id": 1,
        "admin_id": 337,
        "aid": 337
        }
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': gmt_time,
        'lang': 'en-US',
        'loggedin': 'true',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us/finance-management/deposit',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/finance-management/deposit',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'POST',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }

        # Post Response 
        response = requests.post(url, headers=headers, json=payload)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            # Get Cookies
            Automation.chrome_CDP()
            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.deposit_list_PID(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column)


        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request
            response = requests.post(url, headers=headers, json=payload)
            
            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            rows = data.get("data", [])
            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_DL_PID(data["data"], collection)
            else:
                print("No data returned from API.")

        # # Upload Data to Google Sheet by reading from MongoDB
        # mongodb_2_gs.upload_to_google_sheet_DL_PID(collection, gs_id, gs_tab, start_column, end_column)

        # Python Requests Method
    
    # Deposit List (Username)
    @classmethod
    def deposit_list_USERNAME(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column):

        # Get today date and time
        today = datetime.now()
        today = today.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")
        
        url = f"https://v3-bo.{bo_link}/api/be/finance/get-deposit"

        payload = {
        "paginate": 100,
        "page": 1,
        "currency": [
            currency
        ],
        "status": "approved",
        "start_date": "2025-11-01",
        "end_date": today,
        "gmt": gmt_time,
        "merchant_id": 1,
        "admin_id": 337,
        "aid": 337
        }
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': gmt_time,
        'lang': 'en-US',
        'loggedin': 'true',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us/finance-management/deposit',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/finance-management/deposit',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'POST',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }

        # Post Response 
        response = requests.post(url, headers=headers, json=payload)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            # Get Cookies
            Automation.chrome_CDP()
            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.deposit_list_USERNAME(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab)


        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request (CORRECT WAY)
            response = requests.post(url, headers=headers, json=payload)

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            rows = data.get("data", [])
            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_DL_USERNAME(data["data"], collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_DL_USERNAME(cls, collection, gs_id, gs_tab, start_column, end_column)

    # SSBO Deposit List (Player ID)
    @classmethod
    def ssbo_deposit_list_PID(cls, merchants, currency, collection, gs_id, gs_tab):

        # Get today and yesterday date
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/superswan.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        user_cookie = cookie_data.get("user_cookie", "")
        bearer_token = cookie_data.get("bearer_token", "")

        # ======================================= PART 1: Get the ID ==========================================
        
        ids = []
        page = 0
        
        while True:

            url = "https://aw8.premium-bo.com/cashmarket/api/sbo/deposit-withdrawal-management/transactions-deposit"

            params = {
                "page": page,
                "size": 2000,
                "sort": ["membershipLevel,DESC", "createdDate,DESC"],
                "tenantId": 35,
                "merchants": merchants,
                "merchant": merchants,
                "merchantCode": merchants,
                "currencies": currency,
                "start": f"{yesterday}T16:00:00.000Z",
                "startTime": f"{yesterday}T16:00:00.000Z",
                "startCreatedTime": f"{yesterday}T16:00:00.000Z",
                "end": f"{today}T15:59:59.000Z",
                "endTime": f"{today}T15:59:59.000Z",
                "endCreatedTime": f"{today}T15:59:59.000Z",
                "transType": "D",
                "approved": "true",
                "rejected": "false",
                "pending": "false",
                "inProgress": "false",
                "risk": "false",
                "kyc": "false",
                "isProcessingTime": "false",
                "hasDepositHideProcessingFees": "false",
                "isAllowViewAccountNumber": "false",
                "maskReloadAccountNumberCurrencyList": "",
                "firstDeposit": "ALL",
                "hiddenColumns": [
                    "merchant",
                    "region",
                    "affiliateGroupCategory",
                    "affiliateLogin",
                    "processingFee",
                    "bankStatus"
                ],
                "isSeamlessWalletMerchant": "null",
                "cacheBuster": str(int(time.time() * 1000))
            }


            headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': f'Bearer {bearer_token}',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://aw8.premium-bo.com/',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Cookie': f'_ga={user_cookie}',
            }

            # Get Post Response
            query = urlencode(params, doseq=True)
            full_url = f"{url}?{query}"

            response = requests.get(full_url, headers=headers)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                Automation.chrome_CDP()
                cls._ssbo_get_cookies()
                print("⚠️ Cookies + bearer token refreshed ... Retrying request...\n")
                cls.ssbo_member_info(merchants, currency, collection, gs_id, gs_tab)
                return
            
            # Try to parse JSON safely
            try:
                data = response.json()
            except ValueError:
                print("⚠️ Response is not valid JSON.")
                print("Status code:", response.status_code)
                print("Text preview:", response.text[:500])
                return

            # List store all Member ID
            ids = []
            try:
                data = response.json()  # Try converting to JSON
                if isinstance(data, list):
                    ids = [item.get("id") for item in data if isinstance(item, dict) and "id" in item]
                    # print("All IDs:", ids)
                else:
                    print("⚠️ Unexpected JSON format:", data)
            except ValueError:
                print("⚠️ Response not valid JSON:")
                print(response.text)

            print(f"✅ Page {page}: Total IDs found:", len(ids))
            
            # ======================================= PART 2: Use IDs to GET DATA  (funny lol) ==========================================
            
            if not ids:
                print("⚠️ No transaction IDs found — break")
                break

            base_url = "https://aw8.premium-bo.com/cashmarket/api/sbo/deposit-withdrawal-management/transactions-deposit-details"
            params = {
                "page": 0,
                "size": 2000,
                "sort": ["membershipLevel,ASC", "createdDate,ASC"],
                "tenantId": 35,
                "merchants": merchants,
                "merchant": merchants,
                "merchantCode": merchants,
                "currencies": currency,
                "start": f"{yesterday}T16:00:00.000Z",
                "startTime": f"{yesterday}T16:00:00.000Z",
                "startCreatedTime": f"{yesterday}T16:00:00.000Z",
                "end": f"{today}T15:59:59.000Z",
                "endTime": f"{today}T15:59:59.000Z",
                "endCreatedTime": f"{today}T15:59:59.000Z",
                "transType": "D",
                "approved": "true",
                "rejected": "false",
                "pending": "false",
                "inProgress": "false",
                "risk": "false",
                "kyc": "false",
                "isProcessingTime": "false",
                "hasDepositHideProcessingFees": "false",
                "isAllowViewAccountNumber": "false",
                "maskReloadAccountNumberCurrencyList": "",
                "firstDeposit": "ALL",
                "hiddenColumns": [
                    "merchant",
                    "region",
                    "affiliateGroupCategory",
                    "affiliateLogin",
                    "processingFee",
                    "bankStatus"
                ],
                "isSeamlessWalletMerchant": "null",
                "cacheBuster": str(int(time.time() * 1000))
            }

            base_query = urlencode(params, doseq=True)
            transaction_ids_query = "transactionIds=" + "%2C".join(str(i) for i in ids)
            url = f"{base_url}?{base_query}&{transaction_ids_query}"

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': f'Bearer {bearer_token}',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'referer': 'https://aw8.premium-bo.com/',
                'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                'Cookie': f'_ga={user_cookie}',
            }

            response = requests.get(url, headers=headers)
            print(response.json())

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            rows = data.get("data", [])
            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_ssbo_DL_PID(data["data"], collection)
            else:
                print("No data returned from API.")


    # =========================== Member Info ===========================

    # Member Info
    @classmethod
    def member_info(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column):

        # Tell ultra listener which collection + sheet columns to watch
        cls.notify_listener(collection, gs_id, gs_tab, start_column, end_column, mode="memberinfo")

        # Get today date and time
        today = datetime.now()
        today = today.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")
    

        url = f"https://v3-bo.{bo_link}/api/be/member/get-list"

        payload = {
        "paginate": 100,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "register_from": "2025-11-01",
        "register_to": today,
        "merchant_id": 1,
        "admin_id": 581,
        "aid": 581
        }
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': gmt_time,
        'lang': 'en-US',
        'loggedin': 'true',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us/member/list',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/member/list',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-arch': '"arm"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"142.0.7444.162"',
        'sec-ch-ua-full-version-list': '"Chromium";v="142.0.7444.162", "Google Chrome";v="142.0.7444.162", "Not_A Brand";v="99.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-platform-version': '"15.5.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'POST',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }

        # Post Response 
        response = requests.post(url, headers=headers, json=payload)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            # Get Cookies
            Automation.chrome_CDP()
            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.member_info(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column)

        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request (CORRECT WAY)
            response = requests.post(url, headers=headers, json=payload)

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            rows = data.get("data", [])
            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_MI(data["data"], collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_no_duplicate(collection, gs_id, gs_tab)
        # mongodb_2_gs.upload_to_google_sheet_MI(collection, gs_id, gs_tab)

    # Member Info (Split data)
    @classmethod
    def member_info_SPLIT_DATA(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab):

        # Get today date and time
        today = datetime.now()
        today = today.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")

        url = f"https://v3-bo.{bo_link}/api/be/member/get-list"

        payload = {
        "paginate": 100,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "register_from": "2025-11-01",
        "register_to": today,
        "merchant_id": 1,
        "admin_id": 581,
        "aid": 581
        }
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': gmt_time,
        'lang': 'en-US',
        'loggedin': 'true',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us/member/list',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/member/list',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-arch': '"arm"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"142.0.7444.162"',
        'sec-ch-ua-full-version-list': '"Chromium";v="142.0.7444.162", "Google Chrome";v="142.0.7444.162", "Not_A Brand";v="99.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-platform-version': '"15.5.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'POST',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }
        
        # Post Response 
        response = requests.post(url, headers=headers, json=payload)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            # Get Cookies
            Automation.chrome_CDP()
            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.member_info_SPLIT_DATA(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab)
            
        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request (CORRECT WAY)
            response = requests.post(url, headers=headers, json=payload)

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            rows = data.get("data", [])
            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_MI(data["data"], collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_MI_SPLIT_DATA(collection, gs_id, gs_tab)

    # SSBO Member Info (merchants name = aw8, ip9, uea)
    @classmethod
    def ssbo_member_info(cls, merchants, currency, collection, gs_id, gs_tab):

        # Get today and yesterday date
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/superswan.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        user_cookie = cookie_data.get("user_cookie", "")
        bearer_token = cookie_data.get("bearer_token", "")

        # ======================================= PART 1: Get the Member ID ==========================================

        ids = []
        page = 0

        while True:

            url = f"https://aw8.premium-bo.com/cashmarket/api/sbo/member-management/get-member-list-by-filter?page={page}&size=200&sort=m.id,DESC&cacheBuster=1762790852830"

            payload = json.dumps({
            "tenantId": 35,
            "merchants": [
                merchants
            ],
            "merchant": merchants,
            "merchantCode": merchants,
            "currencies": currency,
            "loginID": None,
            "matchFullPhone": True,
            "status": None,
            "name": None,
            "bankAccount": None,
            "cryptoAddress": None,
            "id": None,
            "affiliate": None,
            "contactType": None,
            "phone": None,
            "group": None,
            "kyc": None,
            "kycStatus": None,
            "applicantLevel": None,
            "approvedKycLevel": None,
            "referralSource": None,
            "provider": None,
            "playID": None,
            "risk": None,
            "referrerLogin": None,
            "refCode": None,
            "fingerprint": None,
            "created_date_from": f"{yesterday}T16:00:00.000Z",
            "created_date": f"{today}T15:59:59.000Z",
            "registerDate": None,
            "last_login_date_from": None,
            "last_login_date_to": None,
            "bankGroupId": None,
            "complianceViewMemberSocialMediaDetails": True,
            "tagIds": None,
            "keyword": None,
            "multiple": None,
            "affiliateGroupCategoryId": None,
            "hiddenColumns": [
                "disableWithdrawalOneTimeTurnover"
            ],
            "socialMediaProvider": None,
            "flagLegends": None,
            "disableWithdrawalOneTimeTurnover": None,
            "enableShowTotalWallet": False,
            "bankGroupFeature": None,
            "statusMemberWithdrawLimit": None,
            "dobDateFrom": None,
            "dobDateTo": None,
            "mask": False
            })
            headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': f'Bearer {bearer_token}',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'origin': 'https://aw8.premium-bo.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://aw8.premium-bo.com/',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'Cookie': f'_ga={user_cookie}'
            }

            # Get Post Response
            response = requests.request("POST", url, headers=headers, data=payload)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                Automation.chrome_CDP()
                cls._ssbo_get_cookies()
                print("⚠️ Cookies + bearer token refreshed ... Retrying request...\n")
                cls.ssbo_member_info(merchants, currency, collection, gs_id, gs_tab)
                return
            
            # Try to parse JSON safely
            try:
                data = response.json()
            except ValueError:
                print("⚠️ Response is not valid JSON.")
                print("Status code:", response.status_code)
                print("Text preview:", response.text[:500])
                return

            # List store all Member ID
            ids = []
            try:
                data = response.json()  # Try converting to JSON
                if isinstance(data, list):
                    ids = [item.get("id") for item in data if isinstance(item, dict) and "id" in item]
                    # print("All IDs:", ids)
                else:
                    print("⚠️ Unexpected JSON format:", data)
            except ValueError:
                print("⚠️ Response not valid JSON:")
                print(response.text)

            print(f"✅ Page {page}: Total IDs found:", len(ids))


            # ======================================= PART 2: Use Member ID to GET DATA  (funny lol) ==========================================
            
            if not ids:
                print("⚠️ No transaction IDs found — break")
                break


            # Build URL dynamically with all memberIds as repeated params
            base_url = "https://aw8.premium-bo.com/cashmarket/api/sbo/member-management/get-member-info-details-by-ids"
            params = {
                "page": 0,
                "size": 200,
                "sort": "m.id,DESC",
                "tenantId": 35,
                "currencies": currency,
                "matchFullPhone": "true",
                "createdDateFrom": f"{yesterday}T16:00:00.000Z",
                "createdDate": f"{today}T15:59:59.000Z",
                "complianceViewMemberSocialMediaDetails": "true",
                "enableShowTotalWallet": "false",
                "mask": "false",
                "merchants": merchants,
                "merchant": merchants,
                "merchantCode": merchants
            }
            # Build repeated memberIds params
            member_ids_query = "&".join([f"memberIds={mid}" for mid in ids])
            base_query = urlencode(params, doseq=True)
            url = f"{base_url}?{base_query}&{member_ids_query}"

            payload = {}
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': f'Bearer {bearer_token}',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'referer': 'https://aw8.premium-bo.com/',
                'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                'Cookie': f'_ga={user_cookie}',
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            # Normalize rows from list or dict
            if isinstance(response.json(), list):
                rows = response.json()            # SSBO returns list of dicts
            elif isinstance(response.json(), dict):
                rows = response.json().get("data", [])
            else:
                rows = []

            # Insert into MongoDB ONLY if rows contain SSBO fields
            valid_rows = [
                r for r in rows
                if isinstance(r, dict) and (
                    "login" in r or
                    "name" in r or
                    "registerDate" in r or
                    "phone" in r
                )
            ]

            if valid_rows:
                cls.mongodbAPI_ssbo_MI(valid_rows, collection)
            else:
                print("No valid SSBO rows returned from API.")
            
            page+=1

 


###############=================================== CODE RUN HERE =======================================############

### ==== README YO!!!! ==== ####
# member_info format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# member_info_2 format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# deposit_list format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name, google sheet start column, google sheet end column)

# # =============== SSBO MEMBER INFO ==============================

# Fetch.ssbo_member_info("aw8", ["MYR"], "SSBO_A8M", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "joli")

# # =============== MEMBER INFO ==============================

# # S55 (S5T) 
# Fetch.member_info("s55bo.com", "s55", "THB", "+07:00", "S55_S5T_MI", "12Eu4ZGeRkcqgUWscQ-ZNetBq-Xz0xQGWvifWRbzXqL4", "S5T", "A", "E")
# mongodb_2_gs.sync_append_no_overwrite(collection="S55_S5T_MI", gs_id="12Eu4ZGeRkcqgUWscQ-ZNetBq-Xz0xQGWvifWRbzXqL4", gs_tab="S5T", start_column="A", end_column="E")


# # IBS J8M MY 
# Fetch.member_info("jw8bo.com", "jw8", "MYR", "+08:00", "J8M_MI", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "J8M")
# # IBS J8S SG
# Fetch.member_info("jw8bo.com", "jw8", "SGD", "+08:00", "J8S_MI", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "J8S")
# # IBS A8M MY 
# Fetch.member_info("aw8bo.com", "aw8", "MYR", "+08:00", "A8M_MI", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "A8M")
# # IBS A8S SG
# Fetch.member_info("aw8bo.com", "aw8", "SGD", "+08:00", "A8S_MI", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "A8S")


# # IBS UEA8 MY 
# Fetch.member_info("29018465.asia", "uea8", "MYR", "+08:00", "UM_MI", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "UM")
# # IBS UEA8 SG 
# Fetch.member_info("29018465.asia", "uea8", "SGD", "+08:00", "US_MI", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "US")

# =============== SSBO DEPOSIT LIST PLAYER ID ============================

# S55 (S5T) 
# Fetch.ssbo_deposit_list_PID("aw8", ["MYR"], "SSBO_A8M_DL", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "DEPOSIT LIST")

# =============== DEPOSIT LIST PLAYER ID ============================

# # S55 (S5T) 
# Fetch.deposit_list_PID("s55bo.com", "s55", "THB", "+07:00", "S55_S5T_DL", "1PLzkJ_vfg6DvylV0N_WgQSfUB_ClR5ojVeOAbzXbXEM", "S5T DEPOSIT LIST", "A", "C")

# # EMILIA
# # IBS J8M MY 
# Fetch.deposit_list_PID("jw8bo.com", "jw8", "MYR", "+08:00", "J8M_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "A", "C")
# # IBS J8S SG 
# Fetch.deposit_list_PID("jw8bo.com", "jw8", "SGD", "+08:00", "J8S_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "E", "G")
# # IBS A8M MY 
# Fetch.deposit_list_PID("aw8bo.com", "aw8", "MYR", "+08:00", "A8M_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "I", "K")
# # IBS A8S SG  
# Fetch.deposit_list_PID("aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "M", "O")

# # KAYREEN
# # IBS J8M MY 
# Fetch.deposit_list_PID("jw8bo.com", "jw8", "MYR", "+08:00", "J8M_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "A", "C")
# # IBS J8S SG 
# Fetch.deposit_list_PID("jw8bo.com", "jw8", "SGD", "+08:00", "J8S_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "E", "G")
# # IBS A8M MY 
# Fetch.deposit_list_PID("aw8bo.com", "aw8", "MYR", "+08:00", "A8M_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "I", "K")
# # IBS A8S SG  
# Fetch.deposit_list_PID("aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "M", "O")

# # YVONNE
# # IBS J8M MY 
# Fetch.deposit_list_PID("jw8bo.com", "jw8", "MYR", "+08:00", "J8M_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "A", "C")
# # IBS J8S SG 
# Fetch.deposit_list_PID("jw8bo.com", "jw8", "SGD", "+08:00", "J8S_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "E", "G")
# # IBS A8M MY 
# Fetch.deposit_list_PID("aw8bo.com", "aw8", "MYR", "+08:00", "A8M_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "I", "K")
# # IBS A8S SG  
# Fetch.deposit_list_PID("aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL", "1arO3gXAlfF7_CmKJ94RclmiPTLOqxKm3iS-gHWBTFbI", "A8MS J8MS DEPOSIT LIST", "M", "O")


# # IBS UEA8 MY 
# Fetch.deposit_list_PID("29018465.asia", "uea8", "MYR", "+08:00", "UM_DL", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "UM DEPOSIT LIST", "A", "C")
# # IBS UEA8 SG
# Fetch.deposit_list_PID("29018465.asia", "uea8", "SGD", "+08:00", "UM_DL_2", "1UVMhE2ciVawmBhi3yFDW12TiLz4BK1mbX9b1YTZdlYY", "US DEPOSIT LIST", "A", "C")


# # =============== DEPOSIT LIST USERNAME ============================

# # S55 (S5T) 
# Fetch.deposit_list_PID("s55bo.com", "s55", "THB", "+07:00", "S55_S5T_DL", "1PLzkJ_vfg6DvylV0N_WgQSfUB_ClR5ojVeOAbzXbXEM", "S5T DEPOSIT LIST 2", "A", "C")


# # IBS J8M MY 
# Fetch.deposit_list_USERNAME("jw8bo.com", "jw8", "MYR", "+08:00", "J8M_DL_2", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "A8MS J8MS DEPOSIT LIST 2", "A", "C")
# # IBS J8S SG 
# Fetch.deposit_list_USERNAME("jw8bo.com", "jw8", "SGD", "+08:00", "J8S_DL_2", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "A8MS J8MS DEPOSIT LIST 2", "E", "G")
# # IBS A8M MY 
# Fetch.deposit_list_USERNAME("aw8bo.com", "aw8", "MYR", "+08:00", "A8M_DL_2", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "A8MS J8MS DEPOSIT LIST 2", "I", "K")
# # IBS A8S SG  
# Fetch.deposit_list_USERNAME("aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL_2", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "A8MS J8MS DEPOSIT LIST 2", "M", "O")


# # IBS UEA8 MY 
# Fetch.deposit_list_USERNAME("29018465.asia", "uea8", "MYR", "+08:00", "UM_DL", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "UM DEPOSIT LIST 2", "A", "C")
# # IBS UEA8 SG 
# Fetch.deposit_list_USERNAME("29018465.asia", "uea8", "SGD", "+08:00", "US_DL", "1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "US DEPOSIT LIST 2", "A", "C")


# # =============== Jolibee MEMBER INFO ======================
# # Input 2 Google Sheet ID
# gs_ids = ["1Lh8HI7YSz7I2XvwQ63lYK_b1b09AMeIvEBwRAxFXchA", "1JBHT3uCvRjmX5ruxkkGDfgH7c19KFjyd0qQl_iJVaCs"]
# # Jolibeee GMT+8
# Fetch.member_info_SPLIT_DATA("jolibetbo.com", "joli", "PHP", "+08:00", "JOLI_MI", gs_ids, "S5T")
