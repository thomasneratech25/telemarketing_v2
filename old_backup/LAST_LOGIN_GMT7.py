import os
import re
import sys
import time
import json
import pytz
import atexit
import logging
import requests
import subprocess
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from colorama import Fore, Style
from urllib.parse import urlencode
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from playwright.sync_api import sync_playwright, expect

# Load MongoDB API Key 
load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/api/mongodb/.env")
MONGODB_URI = os.getenv("MONGODB_API_KEY")

# Logging configuration
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("log_files")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(LOG_DIR / "CONVERSION_errors.log", encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def safe_call(func, *args, description=None, retries=500, delay=60, **kwargs):
    """
    Call a function safely with retries.

    Parameters:
        func: callable to execute.
        description: Optional string used in logs for readability.
        retries: Number of attempts before giving up.
        delay: Seconds to wait between retries.
    """
    attempt = 1
    label = description or getattr(func, "__name__", "callable")
    while True:
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Error during %s (attempt %s/%s)", label, attempt, retries)
            if attempt >= retries:
                logger.error("Giving up on %s after %s attempts.", label, retries)
                return None
            attempt += 1
            time.sleep(delay)

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
        "22f": {
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
        "22w": {
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
        "nex8": {
            "merchant_code": os.getenv("MERCHANT_CODE_NEX8"),
            "acc_ID": os.getenv("ACC_ID_NEX8"),
            "acc_PASS": os.getenv("ACC_PASS_NEX8")
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

    @classmethod
    def _find_first_empty_row(cls, sheet, spreadsheet_id, gs_tab, start_column, end_column, start_row=3):
        """
        Return the first empty row (starting from `start_row`) within the target range.
        Uses values().get + values().update, not append.
        """
        range_name = cls._build_a1_range(gs_tab, start_column, start_row, end_column)
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        existing_rows = result.get("values", [])
        for idx, row in enumerate(existing_rows, start=start_row):
            if not row or all(str(cell).strip() == "" for cell in row):
                return idx

        return start_row + len(existing_rows)

    @staticmethod
    def _sort_rows_by_datetime(rows, field_name, descending=False):
        """Sort list of dict rows in-place by a datetime field."""
        def parse_dt(raw):
            if not raw:
                return datetime.min
            if isinstance(raw, datetime):
                return raw
            try:
                # handle "YYYY-MM-DD HH:MM:SS"
                return datetime.fromisoformat(str(raw))
            except ValueError:
                try:
                    return datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return datetime.min

        def sort_key(row):
            if isinstance(row, dict):
                return parse_dt(row.get(field_name))
            return datetime.min

        rows.sort(key=sort_key, reverse=descending)

    @staticmethod
    def _column_letter_to_index(column):
        """Convert column letters (e.g. 'A', 'AA') to zero-based index."""
        if not column:
            raise ValueError("Column letter cannot be empty.")
        column = column.strip().upper()
        result = 0
        for ch in column:
            if not ("A" <= ch <= "Z"):
                raise ValueError(f"Invalid column letter: {column}")
            result = result * 26 + (ord(ch) - ord("A") + 1)
        return result - 1

    @staticmethod
    def _quote_sheet_title(sheet_title):
        """Wrap sheet/tab names that contain spaces/special chars in single quotes."""
        if sheet_title is None:
            raise ValueError("Sheet/tab name cannot be empty.")
        title = str(sheet_title).strip()
        if not title:
            raise ValueError("Sheet/tab name cannot be empty.")
        if title.startswith("'") and title.endswith("'"):
            return title
        safe_title = title.replace("'", "''")
        return f"'{safe_title}'"

    @classmethod
    def _build_a1_range(cls, sheet_title, start_column, start_row=None, end_column=None, end_row=None):
        """Construct an A1 notation range with safe sheet quoting."""
        if not start_column:
            raise ValueError("Start column is required for A1 range.")
        start_col = str(start_column).strip()
        if not start_col:
            raise ValueError("Start column is required for A1 range.")
        start_part = f"{start_col}{start_row or ''}"
        sheet_ref = cls._quote_sheet_title(sheet_title)
        if end_column:
            end_col = str(end_column).strip()
            if not end_col:
                raise ValueError("End column is required when provided.")
            end_part = f"{end_col}{end_row or ''}"
            return f"{sheet_ref}!{start_part}:{end_part}"
        return f"{sheet_ref}!{start_part}"

    @classmethod
    def _sort_range_by_column(cls, service, spreadsheet_id, sheet_title, start_column, end_column, start_row, end_row, sort_column_letter=None, descending=False):
        """Use Sheets API sortRange to sort a block by a column."""
        if end_row < start_row:
            return

        sort_column_letter = sort_column_letter or end_column

        metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))"
        ).execute()

        sheet_id = None
        for sh in metadata.get("sheets", []):
            props = sh.get("properties", {})
            if props.get("title") == sheet_title:
                sheet_id = props.get("sheetId")
                break

        if sheet_id is None:
            raise RuntimeError(f"Sheet '{sheet_title}' not found in spreadsheet {spreadsheet_id}.")

        start_col_idx = cls._column_letter_to_index(start_column)
        end_col_idx = cls._column_letter_to_index(end_column) + 1  # exclusive
        sort_col_idx = cls._column_letter_to_index(sort_column_letter)

        # Convert to 0-based indices; Sheets expects exclusive row end.
        range_body = {
            "sheetId": sheet_id,
            "startRowIndex": max(start_row - 1, 0),
            "endRowIndex": end_row,
            "startColumnIndex": start_col_idx,
            "endColumnIndex": end_col_idx
        }

        sort_col_in_range = sort_col_idx - start_col_idx
        if sort_col_in_range < 0 or sort_col_in_range >= (end_col_idx - start_col_idx):
            raise ValueError("sort_column_letter must fall within the specified range.")

        sort_order = "DESCENDING" if descending else "ASCENDING"

        body = {
            "requests": [
                {
                    "sortRange": {
                        "range": range_body,
                        "sortSpecs": [
                            {
                                "dimensionIndex": sort_col_in_range,
                                "sortOrder": sort_order
                            }
                        ]
                    }
                }
            ]
        }

        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

    @staticmethod
    def _ensure_sheet_row_capacity(service, spreadsheet_id, sheet_title, required_last_row):
        """
        Ensure the sheet has at least `required_last_row` rows.
        If not, append rows via batchUpdate.
        """
        if required_last_row <= 0:
            return

        metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount)))"
        ).execute()

        sheet_id = None
        current_rows = None
        for sh in metadata.get("sheets", []):
            props = sh.get("properties", {})
            if props.get("title") == sheet_title:
                sheet_id = props.get("sheetId")
                current_rows = props.get("gridProperties", {}).get("rowCount", 0)
                break

        if sheet_id is None or current_rows is None:
            raise RuntimeError(f"Sheet '{sheet_title}' not found in spreadsheet {spreadsheet_id}.")

        if required_last_row > current_rows:
            increase = required_last_row - current_rows
            body = {
                "requests": [
                    {
                        "appendDimension": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "length": increase
                        }
                    }
                ]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

    @staticmethod
    def _normalize_pid_rows(rows):
        """Return list of dicts with player_id/amount/completed_at keys."""
        normalized = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            player_id = row.get("player_id") or row.get("memberLogin") or row.get("username") or ""
            amount = row.get("amount", "")
            if amount == "":
                amount = row.get("confirmedAmount", "")
            completed = row.get("completed_at") or row.get("lastModifiedDate") or row.get("completedAt") or ""
            normalized.append({
                "player_id": str(player_id),
                "amount": str(amount),
                "completed_at": str(completed),
            })
        return normalized

    @classmethod
    def _fetch_extra_pid_rows(cls, collection_names):
        """Load and normalize PID docs from extra Mongo collections."""
        if not collection_names:
            return []
        load_dotenv()
        MONGODB_URI = os.getenv("MONGODB_API_KEY")
        if not MONGODB_URI:
            raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
        client = MongoClient(MONGODB_URI)
        db = client["UEA8"]
        combined = []
        for col_name in collection_names:
            col = db[col_name]
            docs = list(col.find({}, {"_id": 0}))
            combined.extend(cls._normalize_pid_rows(docs))
        return combined

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # =-=-=--=-=-=-=-=-= IBS =-=-=-=-=-=-=-=-=-=-=-=-=
    # MongoDB Database
    def mongodbAPI_Last_Login(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["LastLogin"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("username", 1), ("last_login_info_date", 1)],
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
            username = row.get("username")
            last_login_info_date = row.get("last_login_info_date")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            dt = datetime.fromisoformat(last_login_info_date)
            last_login_info_date_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "username": username,
                "last_login_info_date": last_login_info_date_fmt,
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
    def upload_to_google_sheet_Last_Login(cls, collection, gs_id, gs_tab, rows=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!A2:B"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for Member Info."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get("username", "")),
                        str(r.get("last_login_info_date", "")),
                    ])
                else:
                    sanitized.append(["", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            client = MongoClient(MONGODB_URI)

            db = client["LastLogin"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("last_login_info_date", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")

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

    # =-=-=--=-=-=-=-=-= SSBO =-=-=-=-=-=-=-=-=-=-=-=-=
    # MongoDB Database (Member Info)
    def mongodbAPI_ssbo_Last_Login (rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["LastLogin"]
        collection = db[collection]

       # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("login", 1), ("lastLoginDate", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []
        # for each rows in a list of JSON objects return
        for row in rows:
            # Skip null or invalid rows
            if not isinstance(row, dict):
                continue
            # Extract only the fields you want (Extract Data from json file)
            login = row.get("login")
            lastLoginDate = row.get("lastLoginDate")

            if not lastLoginDate:
                continue

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS) in GMT+7
            try:
                dt = datetime.fromisoformat(str(lastLoginDate).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(timezone(timedelta(hours=7)))
                lastLoginDate_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                lastLoginDate_fmt = str(lastLoginDate)

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "login": login,
                "lastLoginDate": lastLoginDate_fmt,
                
            }
            # Upsert: overwrite if username exists
            cleaned_docs.append(doc.copy())
            try:
                result = collection.update_one(
                    {"login": login},
                    {"$set": doc},
                    upsert=True
                )
                if result.upserted_id is not None or result.modified_count > 0:
                    inserted += 1
            except Exception:
                skipped += 1
        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}\n")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB (MemberInfo)
    @classmethod
    def upload_to_google_sheet_ssbo_Last_Login(cls, collection, gs_id, gs_tab, rows=None, extra_mongo_collections=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = cls._build_a1_range(gs_tab, "A", 3, "C")

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for Member Info."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    lastLoginDate = (
                        r.get("lastLoginDate")
                        or ""
                    )
                    id = (
                        r.get("login")
                        or ""
                    )
                    sanitized.append([
                        str(id),
                        str(lastLoginDate),
                    ])
                else:
                    sanitized.append(["", ""])
            return sanitized

        load_dotenv()
        MONGODB_URI = os.getenv("MONGODB_API_KEY")
        if not MONGODB_URI:
            raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
        client = MongoClient(MONGODB_URI)
        db = client["LastLogin"]

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            collection_ref = db[collection]
            documents = list(
                collection_ref.find({}, {"_id": 0}).sort("lastLoginDate", 1)
            )
            rows = documents
        if extra_mongo_collections:
            for extra_col in extra_mongo_collections:
                extra_ref = db[extra_col]
                extra_docs = list(
                    extra_ref.find({}, {"_id": 0}).sort("lastLoginDate", 1)
                )
                rows.extend(extra_docs)
        
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")

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
        print("Uploaded MongoDB data to Google Sheet.\n")

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

    # =========================== Last Login Info ===========================

    # Last Login Info
    @classmethod
    def last_Login(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab):
        
        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} Last Login Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        today_dt = datetime.now()                 # Dec 17 (tomorrow)
        start_dt = today_dt - relativedelta(months=1)

        start_day = start_dt.strftime("%Y-%m-%d")
        end_day = today_dt.strftime("%Y-%m-%d")

        print("Start:", start_day)
        print("End:", end_day)

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
        "paginate": 1000,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "vip_level": [],
        "last_login_from": start_day,
        "last_login_to": end_day,
        "merchant_id": 1,
        "admin_id": 205,
        "aid": 205
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
            return cls.last_Login(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab)

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

            # STOP only after double-checking the next page
            if not rows:
                print(f"⚠️ Page {page} returned 0 rows — double checking Page {page + 1}...")

                # Prepare next-page payload
                next_payload = payload.copy()
                next_payload["page"] = page + 1

                # Request next page
                next_response = requests.post(url, headers=headers, json=next_payload)
                try:
                    next_data = next_response.json()
                    next_rows = next_data.get("data", [])
                except Exception:
                    print("⚠️ Invalid JSON while double checking next page.")
                    next_rows = []

                print(f"Page {page + 1} → {len(next_rows)} rows (double check)")

                # Only break if next page *also* returns 0
                if not next_rows:
                    print(f"No transaction ID found on Page {page} and Page {page + 1}. Breaking loop.")
                    break
                else:
                    print(f"Page {page} was empty but Page {page + 1} has data → continuing...")
                    continue


            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_Last_Login(data["data"], collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_Last_Login(collection, gs_id, gs_tab)

    # SSBO Last Login Info (merchants name = aw8, ip9, uea)
    @classmethod
    def ssbo_last_Login(cls, merchants, currency, collection, gs_id, gs_tab, extra_mongo_collections=None):

        # Get today's date
        today = datetime.now()

        # --- 2nd day of THIS month ---
        second_this_month = today.replace(day=3)
        second_this_month_str = second_this_month.strftime("%Y-%m-%d")

        # --- 2nd day of LAST month ---
        # Step 1: go to the first day of this month
        first_this_month = today.replace(day=1)
        # Step 2: subtract one day → last day of previous month
        last_day_prev_month = first_this_month - timedelta(days=1)
        # Step 3: replace with day=2
        second_last_month = last_day_prev_month.replace(day=2)
        second_last_month_str = second_last_month.strftime("%Y-%m-%d")


        # Normalize currency input to list form for API payloads
        if isinstance(currency, (list, tuple, set)):
            currency_list = list(currency)
        else:
            currency_list = [currency]

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
            "currencies": currency_list,
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
            "created_date_from": None,
            "created_date": None,
            "registerDate": None,
            "last_login_date_from": f"{second_last_month_str}T17:00:00.000Z",
            "last_login_date_to": f"{second_this_month_str}T16:59:59.000Z",
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
                cls.ssbo_last_Login(merchants, currency, collection, gs_id, gs_tab, extra_mongo_collections=extra_mongo_collections)
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

            print(f"Page {page}: Total IDs found:", len(ids))


            # ======================================= PART 2: Use Member ID to GET DATA  (funny lol) ==========================================
            
            if not ids:
                print("⚠️ No transaction IDs found — break\n")
                break


            # Build URL dynamically with all memberIds as repeated params
            base_url = "https://aw8.premium-bo.com/cashmarket/api/sbo/member-management/get-member-info-details-by-ids"
            params = {
                "page": 0,
                "size": 200,
                "sort": "m.id,DESC",
                "tenantId": 35,
                "currencies": currency_list,
                "matchFullPhone": "true",
                "lastLoginDateFrom": f"{second_last_month_str}T17:00:00.000Z",
                "lastLoginDateTo": f"{second_this_month_str}T16:59:59.000Z",
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
                    "id" in r or
                    "lastLoginDate" in r
                )
            ]

            if valid_rows:
                cls.mongodbAPI_ssbo_Last_Login(valid_rows, collection)
            else:
                print("No valid SSBO rows returned from API.")
            
            page+=1
        
        # upload to google sheet
        cls.upload_to_google_sheet_ssbo_Last_Login(collection, gs_id, gs_tab, extra_mongo_collections=extra_mongo_collections)


while True:
    try:

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= Last Login Info =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # J1B
        safe_call(Fetch.last_Login, "batsman88.com", "jaya11", "J1B", "BDT", "+07:00", "JAYA11_J1B_LAST_LOGIN", "1vAmjUff6yxKyBgZGqu0JZzAihOUOIMyKpx4Gpxmtf5U", "J1B", description="J1B LAST LOGIN")
        # J1N
        safe_call(Fetch.last_Login, "batsman88.com", "jaya11", "J1N", "NPR", "+07:00", "JAYA11_J1N_LAST_LOGIN", "1vAmjUff6yxKyBgZGqu0JZzAihOUOIMyKpx4Gpxmtf5U", "J1N", description="J1N LAST LOGIN")
        # K8N
        safe_call(Fetch.last_Login, "6668889.store", "k88", "K8N", "NPR", "+07:00", "K88_K8N_LAST_LOGIN", "1vAmjUff6yxKyBgZGqu0JZzAihOUOIMyKpx4Gpxmtf5U", "K8N", description="K8N LAST LOGIN")
        # I8N
        safe_call(Fetch.last_Login, "6668889.site", "i88", "I8N", "NPR", "+07:00", "I88_I8N_LAST_LOGIN", "1vAmjUff6yxKyBgZGqu0JZzAihOUOIMyKpx4Gpxmtf5U", "I8N", description="I8N LAST LOGIN")
        
        # SSBO
        # safe_call(Fetch.ssbo_last_Login, "aw8", ["MYR"], "SSBO_A8M_LAST_LOGIN", "1vAmjUff6yxKyBgZGqu0JZzAihOUOIMyKpx4Gpxmtf5U", "LAST LOGIN", description="SSBO A8M MY LAST LOGIN")

        time.sleep(1800)

    except KeyboardInterrupt:
            logger.info("Execution interrupted by user.")
            break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
