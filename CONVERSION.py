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
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION"]
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

            db = client["CONVERSION"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("completed_at", 1))
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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # MongoDB Database (Member Info)
    def mongodbAPI_MI(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("member_id", 1)],
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
            email = row.get("email")

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
                "email": email,
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
        RANGE_NAME = f"{gs_tab}!A3:F"

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
                        str(r.get("email", "")),
                    ])
                else:
                    sanitized.append(["", "", "", "", "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            client = MongoClient(MONGODB_URI)

            db = client["CONVERSION"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("register_info_date", 1))
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
                        str(r.get("email", "")),
                    ])
                else:
                    sanitized.append(["", "", "", "", "", ""])
            return sanitized

        # Load from MongoDB if not provided
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            client = MongoClient(MONGODB_URI)
            db = client["CONVERSION"]
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
            RANGE_NAME = f"{gs_tab}!A3:F"

            print(f"Uploading {len(chunk)} rows → Sheet {idx+1} ({SPREADSHEET_ID})")

            body = {"values": chunk, "majorDimension": "ROWS"}

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

    # =========================== DEPOSIT LIST ===========================

    # Deposit List (Player ID)
    @classmethod
    def deposit_list_PID(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column):
        
        # Print Color Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} DEPOSIT LIST Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        
        # Get TimeZone (GMT+7)
        gmt7 = pytz.timezone("Asia/Bangkok")
        now_gmt7 = datetime.now(gmt7)

        # Get Current Time (GMT +7)    
        current_time = now_gmt7.time()
        print(current_time, "GMT+7")
        
        # Today & Yesterday Date
        today = now_gmt7.strftime("%Y-%m-%d")
        yesterday = (now_gmt7 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → use yesterday
        # 00:15 onward → use today
        if current_time < datetime.strptime("00:15", "%H:%M").time():
            start_date = yesterday
            end_date = yesterday
        else:
            start_date = today
            end_date = today

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
        "start_date":  start_date,
        "end_date": end_date,
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
            return cls.deposit_list_PID(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column)


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
        mongodb_2_gs.upload_to_google_sheet_DL_PID(collection, gs_id, gs_tab, start_column, end_column)

        # Python Requests Method
    
    # =========================== Member Info ===========================

    # Member Info
    @classmethod
    def member_info(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab):
        
        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} MEMBER INFO Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)


        # Get TimeZone (GMT+7)
        gmt7 = pytz.timezone("Asia/Bangkok")
        now_gmt7 = datetime.now(gmt7)

        # Get Current Time (GMT +7)    
        current_time = now_gmt7.time()
        print(current_time, "GMT+7")
        
        # Today & Yesterday Date
        today = now_gmt7.strftime("%Y-%m-%d")
        yesterday = (now_gmt7 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → use yesterday
        # 00:15 onward → use today
        if current_time < datetime.strptime("01:00", "%H:%M").time():
            start_date = yesterday
            end_date = yesterday
        else:
            start_date = today
            end_date = today

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
        "paginate": 10000,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "register_from": start_date,
        "register_to": end_date,
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
            return cls.member_info(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab)

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
        mongodb_2_gs.upload_to_google_sheet_MI(collection, gs_id, gs_tab)

    # Member Info (Split data)
    @classmethod
    def member_info_SPLIT_DATA(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab):
        
        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} MEMBER INFO Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get TimeZone (GMT+7)
        gmt7 = pytz.timezone("Asia/Bangkok")
        now_gmt7 = datetime.now(gmt7)

        # Get Current Time (GMT +7)    
        current_time = now_gmt7.time()
        print(current_time, "GMT+7")
        
        # Today & Yesterday Date
        today = now_gmt7.strftime("%Y-%m-%d")
        yesterday = (now_gmt7 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → use yesterday
        # 00:15 onward → use today
        if current_time < datetime.strptime("01:00", "%H:%M").time():
            start_date = yesterday
            end_date = yesterday
        else:
            start_date = today
            end_date = today

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
        "paginate": 10000,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "register_from": start_date,
        "register_to": end_date,
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
            return cls.member_info_SPLIT_DATA(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab)
            
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


###############=================================== CODE RUN HERE =======================================############

### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####
# member_info format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# member_info_2 format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# deposit_list format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name, google sheet start column, google sheet end column)

# # =============== DEPOSIT LIST USERNAME ============================

while True:
    try:

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S5T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S5T (MEMBER INFO)
        safe_call(Fetch.member_info, "s55bo.com", "s55", "S5T", "THB", "+07:00", "S55_S5T_MI", "1tcUSfgT_Locmmm8M8QUeq7HXzPxJ-NqHvX34N1JLCFc", "New Register", description="S5T member info")
        
        # # S5T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "s55bo.com", "s55", "S5T", "THB", "+07:00", "S55_S5T_DL", "1tcUSfgT_Locmmm8M8QUeq7HXzPxJ-NqHvX34N1JLCFc", "DEPOSIT LIST", "A", "C", description="S5T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= JOLIBEE MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=
        # ==========================================================================

        # Krisitian & Amber (MEMBER INFO)
        gs_ids = ["1u7OV_KckatfkrAqPVTvGk5v3AXSzgFIs2v48exUFFk8", "1sOpeTRBxjQ1aEEgnhGIUkj7sfTM44zj70zEd-x6lGSk"]
        safe_call(Fetch.member_info_SPLIT_DATA, "jolibetbo.com", "joli", "Kristian & Amber", "PHP", "+08:00", "JOLI_MI", gs_ids, "New Register", description="Jolibet Kristian & Amber member info")

        # # Krisitian & Amber (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "jolibetbo.com", "joli", "Joli Kristian", "PHP", "+08:00", "JOLI_DL", "1u7OV_KckatfkrAqPVTvGk5v3AXSzgFIs2v48exUFFk8", "DEPOSIT LIST", "A", "C", description="Jolibet Kristian deposit list")
        # safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "JOLI_DL", "1sOpeTRBxjQ1aEEgnhGIUkj7sfTM44zj70zEd-x6lGSk", "DEPOSIT LIST", "A", "C")
        
        # ==========================================================================
        # =-=-=-=-==-=-=-=-= N8Y MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # N8Y (MEMBER INFO)
        safe_call(Fetch.member_info, "nex8bo.com", "nex8", "N8Y", "MMK", "+07:00", "NEX8_N8Y_MI", "1wu2CuhTZyddAq0Xj5xENzMDFxvVr8Zykw5gVKBR1PVI", "New Register", description="N8Y member info")
        
        # # N8Y (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "nex8bo.com", "nex8", "N8Y", "MMK", "+07:00", "NEX8_N8Y_DL", "1wu2CuhTZyddAq0Xj5xENzMDFxvVr8Zykw5gVKBR1PVI", "DEPOSIT LIST", "A", "C", description="N8Y deposit list")


        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S345T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S345T (MEMBER INFO)
        safe_call(Fetch.member_info, "57249022.asia", "s345", "S345T", "THB", "+07:00", "S345_S345T_MI", "10sLnd4QBztBNdFqnzIkebJn-ytyEBz1unuQJUCCWpsY", "New Register", description="S345T member info")
        
        # # S345T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "57249022.asia", "s345", "S345T", "THB", "+07:00", "S345_S345T_DL", "10sLnd4QBztBNdFqnzIkebJn-ytyEBz1unuQJUCCWpsY", "DEPOSIT LIST", "A", "C", description="S345T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S2T MEMBER INFO & DEPOSIT LIST (CHING)=-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S2T (MEMBER INFO)
        safe_call(Fetch.member_info, "m3v5r6cx.com", "s212", "S2T", "THB", "+07:00", "S212_S2T_MI", "1kRwQnlCz6ZiNNbjwF8O6UacgIhF2kzirqmK05cl_M_s", "New Register", description="S2T member info")
        
        # # S2T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "m3v5r6cx.com", "s212", "S2T", "THB", "+07:00", "S212_S2T_DL", "1kRwQnlCz6ZiNNbjwF8O6UacgIhF2kzirqmK05cl_M_s", "DEPOSIT LIST", "A", "C", description="S2T deposit list")


        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S369T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S369T (MEMBER INFO)
        safe_call(Fetch.member_info, "uhy3umx.com", "s369", "S369T", "THB", "+07:00", "S369_S369T_MI", "1O3cnGleSipLATH_c00GmGFe152qFgAPCBYS_CWM8mHk", "New Register", description="S369T member info")
        
        # # S369T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "uhy3umx.com", "s369", "S369T", "THB", "+07:00", "S369_S369T_DL", "1O3cnGleSipLATH_c00GmGFe152qFgAPCBYS_CWM8mHk", "DEPOSIT LIST", "A", "C", description="S369T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S6T MEMBER INFO & DEPOSIT LIST (MEI) =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S6T (MEMBER INFO)
        safe_call(Fetch.member_info, "siam66bo.com", "s66", "S6T", "THB", "+07:00", "S66_S6T_MI", "1bHKEfVvunQ4OfU_weF-6CTuMAkcQXqwnSehSZyhMClo", "New Register", description="S6T member info")
        
        # # S6T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "siam66bo.com", "s66", "S6T", "THB", "+07:00", "S66_S6T_DL", "1bHKEfVvunQ4OfU_weF-6CTuMAkcQXqwnSehSZyhMClo", "DEPOSIT LIST", "A", "C", description="S6T deposit list")


        # ==========================================================================
        # =-=-=-=-==-=-=-=-= A8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # # A8T  (MEMBER INFO)
        # safe_call(Fetch.member_info, "aw8bo.com", "aw8", "A8T", "THB", "+07:00", "AW8_A8T_MI", "1fL_qVhAKC8BmbPYrUlxSv4iT8CuFJk3zn4jv_xe8rjM", "New Register", description="A8T member info")
        
        # # A8T  (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "aw8bo.com", "aw8", "A8T", "THB", "+07:00", "AW8_A8T_DL", "1fL_qVhAKC8BmbPYrUlxSv4iT8CuFJk3zn4jv_xe8rjM", "DEPOSIT LIST", "A", "C", description="A8T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= J8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # J8T (MEMBER INFO)
        safe_call(Fetch.member_info, "jw8bo.com", "jw8", "J8T", "THB", "+07:00", "JW8_J8T_MI", "1gwXlJymukzhSqJbz4PAaRoMuSHPszhdXpNmsc15nKdw", "New Register", description="J8T member info")
        
        # # J8T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "jw8bo.com", "jw8", "J8T", "THB", "+07:00", "JW8_J8T_DL", "1gwXlJymukzhSqJbz4PAaRoMuSHPszhdXpNmsc15nKdw", "DEPOSIT LIST", "A", "C", description="J8T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= MST MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # MST (MEMBER INFO)
        safe_call(Fetch.member_info, "bo-msslot.com", "slot", "MST", "THB", "+07:00", "SLOT_MST_MI", "1U8Bzy_UsoQhOJh1ETr3ChK1PIc5QzDbPhiQ3aN7EQEI", "New Register", description="MST member info")
        
        # # MST (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "bo-msslot.com", "slot", "MST", "THB", "+07:00", "SLOT_MST_DL", "1U8Bzy_UsoQhOJh1ETr3ChK1PIc5QzDbPhiQ3aN7EQEI", "DEPOSIT LIST", "A", "C", description="MST deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= I8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # I8T (MEMBER INFO)
        safe_call(Fetch.member_info, "i828.asia", "i88", "I8T", "THB", "+07:00", "I828_I8T_MI", "1lelEpA64CynKqfnpcZEpQhGu7w_7bwzDUHpoozVWmwo", "New Register", description="I8T member info")
        
        # # I8T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "i828.asia", "i88", "I8T", "THB", "+07:00", "I828_I8T_DL", "1lelEpA64CynKqfnpcZEpQhGu7w_7bwzDUHpoozVWmwo", "DEPOSIT LIST", "A", "C", description="I8T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= G855T MEMBER INFO & DEPOSIT LIST (MEME) =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================
        
        # G855T (MEMBER INFO)
        safe_call(Fetch.member_info, "god855.asia", "g855", "G855T", "THB", "+07:00", "G855_G855T_MI", "154-8gltDhGt1gAZM6KrhFXGmnRaoOPGzPps8v4aJnd4", "New Register", description="G855T member info")
        
        # # G855T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "god855.asia", "g855", "G855T", "THB", "+07:00", "G855_G855T_DL", "154-8gltDhGt1gAZM6KrhFXGmnRaoOPGzPps8v4aJnd4", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= 2FT MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # 2FT (MEMBER INFO)
        safe_call(Fetch.member_info, "22funbo.com", "22f", "2FT", "THB", "+07:00", "22F_2FT_MI", "1j-dyM2v6mjfaWP2BF77_PUhwnHo2D9mrxt8bX1nAXcM", "New Register", description="2FT member info")
        
        # # 2FT (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "22funbo.com", "22f", "2FT", "THB", "+07:00", "22F_2FT_DL", "1j-dyM2v6mjfaWP2BF77_PUhwnHo2D9mrxt8bX1nAXcM", "DEPOSIT LIST", "A", "C", description="2FT deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= M1T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # M1T (MEMBER INFO)
        safe_call(Fetch.member_info, "zupra7x.com", "mf191", "M1T", "THB", "+07:00", "MF191_M1T_MI", "1JkMSXyKmAUjqBt5V0MZ8OfUJIEJ9K4Wfrm2qFVcw3fs", "New Register", description="M1T member info")
        
        # # M1T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "zupra7x.com", "mf191", "M1T", "THB", "+07:00", "MF191_M1T_DL", "1JkMSXyKmAUjqBt5V0MZ8OfUJIEJ9K4Wfrm2qFVcw3fs", "DEPOSIT LIST", "A", "C", description="M1T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= 2WT MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # 2WT (MEMBER INFO)
        safe_call(Fetch.member_info, "w8c4n9be.com", "22w", "2WT", "THB", "+07:00", "22W_2WT_MI", "1ofTw7dzHL7gr7Nr-29H6997O8zspX4_LVcTxngaXSso", "New Register", description="2WT member info")
        
        # # 2WT (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "w8c4n9be.com", "22w", "2WT", "THB", "+07:00", "22W_2WT_DL", "1ofTw7dzHL7gr7Nr-29H6997O8zspX4_LVcTxngaXSso", "DEPOSIT LIST", "A", "C", description="2WT deposit list")
        
        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S8T (MEMBER INFO)
        safe_call(Fetch.member_info, "siam855bo.com", "s855", "S8T", "THB", "+07:00", "S855_S8T_MI", "1zlB2V3yi9HyVJLKBomkGX0wKkgKfrSV2G8jQa-dzhgI", "New Register", description="S8T member info")
        
        # # S8T (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "siam855bo.com", "s855", "S8T", "THB", "+07:00", "S855_S8T_DL", "1zlB2V3yi9HyVJLKBomkGX0wKkgKfrSV2G8jQa-dzhgI", "DEPOSIT LIST", "A", "C", description="S8T deposit list")


        time.sleep(300)

    except KeyboardInterrupt:
            logger.info("Execution interrupted by user.")
            break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
