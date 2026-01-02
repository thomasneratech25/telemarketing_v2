import os
import re
import sys
import time
import pytz
import json
import atexit
import shutil
import zipfile
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

    file_handler = logging.FileHandler(LOG_DIR / "RETENTION_errors.log", encoding="utf-8")
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
            "--headless=new",                    # ------> if want to use headless mode, use --windows-size together, due to headless mode small screen size
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
        db = client["Telemarketing"]
        combined = []
        for col_name in collection_names:
            col = db[col_name]
            docs = list(col.find({}, {"_id": 0}))
            combined.extend(cls._normalize_pid_rows(docs))
        return combined

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS AMR =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # Unzip File (ssbo)
    @classmethod
    def unzip(cls, file_name):
        base_dir = "/Users/nera_thomas/Desktop/Telemarketing/excel_file"
        zip_path = os.path.join(base_dir, f"{file_name}.zip")

        if not os.path.exists(zip_path):
            print(f"❌ Zip file not found: {zip_path}")
            return

        # ✅ Use dedicated temp folder
        extract_to = os.path.join(base_dir, f"{file_name}_temp")
        os.makedirs(extract_to, exist_ok=True)

        new_csv_path = os.path.join(base_dir, f"{file_name}.csv")
        if os.path.exists(new_csv_path):
            os.remove(new_csv_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"✅ Unzipped into folder: {extract_to}")

        csv_files = []
        for root, _, files in os.walk(extract_to):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        if not csv_files:
            print("❌ No CSV found inside ZIP.")
            shutil.rmtree(extract_to, ignore_errors=True)
            return

        shutil.move(csv_files[0], new_csv_path)
        shutil.rmtree(extract_to, ignore_errors=True)

    # MongoDB Database (All Member Report)
    def mongodbAPI_AMR(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["Telemarketing"]
        collection = db[collection]

        # Drop any legacy indexes that might conflict
        for idx in (
            "player_id_1_amount_1_completed_at_1",
            "all_member_unique",
            "username_1_first_name_1_register_info_date_1",
        ):
            try:
                collection.drop_index(idx)
            except Exception:
                pass

        # Ensure username+member_id+register_date unique to avoid duplicates
        collection.create_index(
            [("username", 1), ("fullname", 1), ("member_id", 1)],
            name="all_member_unique",
            unique=True,
            partialFilterExpression={
                "username": {"$type": "string"},
                "fullname": {"$type": "string"},
                "member_id": {"$type": "string"},
            },
        )

        inserted = 0
        skipped = 0
        cleaned_docs = []

        ordered_fields = [
            "none",
            "currency",
            "member_group",
            "username",
            "fullname",
            "member_id",
            "phone_no",
            "email",
            "vip_level",
            "aff_username",
            "member_tag",
            "deposit_count",
            "affiliate_transfer_in_count",
            "affiliate_transfer_in_amount",
            "total_deposit",
            "processing_fee",
            "received_deposit",
            "withdraw_count",
            "affiliate_transfer_out_count",
            "affiliate_transfer_out_amount",
            "total_withdraw",
            "adjust_count",
            "adjust_in",
            "adjust_out",
            "total_adjust",
            "bet_count",
            "total_valid_bet",
            "total_bonus_bet",
            "total_turnover",
            "total_valid_bonus_bet",
            "win_loss",
            "player_bonus_winloss",
            "total_bonus",
            "total_cancel_bonus",
            "total_rebate",
            "total_referral",
            "total_referral_comm",
            "total_reward",
            "total_win_loss",
        ]

        for row in rows:

            if not isinstance(row, dict):
                continue

            doc = {}
            for field in ordered_fields:
                value = row.get(field, "")
                if value is None:
                    value = ""
                doc[field] = value

            username = row.get("username") or row.get("memberLogin")
            member_id = row.get("member_id") or row.get("player_id")

            if username:
                doc["username"] = str(username)
            if member_id:
                doc["member_id"] = str(member_id)

            cleaned_docs.append(doc.copy())

            # === Upsert by member_id (replace existing document when member_id matches) ===
            key_value = doc.get("member_id")

            # If we have a member_id, use it as the unique key and replace existing doc
            if key_value:
                try:
                    result = collection.replace_one(
                        {"member_id": key_value},
                        doc,
                        upsert=True
                    )
                    # New insert vs update existing
                    if result.upserted_id:
                        inserted += 1
                    else:
                        # treated as "skipped" in the sense of "updated existing"
                        skipped += 1
                except Exception:
                    # any failure to write is treated as skipped
                    skipped += 1
            else:
                # fallback: no member_id, behave like a simple insert
                try:
                    collection.insert_one(doc)
                    inserted += 1
                except Exception:
                    skipped += 1

        return cleaned_docs, inserted, skipped

    # Update Data to Google Sheet from MongoDB (All Member Report) IBS
    @classmethod
    def upload_to_google_sheet_AMR(cls, collection, gs_id, gs_tab, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        SPREADSHEET_ID = gs_id
        RANGE_NAME = cls._build_a1_range(gs_tab, "A", 2, "AM")

        ordered_fields = [
            "none",
            "currency",
            "member_group",
            "username",
            "fullname",
            "member_id",
            "phone_no",
            "email",
            "vip_level",
            "aff_username",
            "member_tag",
            "deposit_count",
            "affiliate_transfer_in_count",
            "affiliate_transfer_in_amount",
            "total_deposit",
            "processing_fee",
            "received_deposit",
            "withdraw_count",
            "affiliate_transfer_out_count",
            "affiliate_transfer_out_amount",
            "total_withdraw",
            "adjust_count",
            "adjust_in",
            "adjust_out",
            "total_adjust",
            "bet_count",
            "total_valid_bet",
            "total_bonus_bet",
            "total_turnover",
            "total_valid_bonus_bet",
            "win_loss",
            "player_bonus_winloss",
            "total_bonus",
            "total_cancel_bonus",
            "total_rebate",
            "total_referral",
            "total_referral_comm",
            "total_reward",
            "total_win_loss",
        ]

        def sanitize_rows(raw_rows):
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get(field, "")) if r.get(field, "") is not None else ""
                        for field in ordered_fields
                    ])
                else:
                    sanitized.append([""] * len(ordered_fields))
            return sanitized

        load_dotenv()
        MONGODB_URI = os.getenv("MONGODB_API_KEY")
        if not MONGODB_URI:
            raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
        client = MongoClient(MONGODB_URI)
        db = client["Telemarketing"]

        if not rows:
            collection_ref = db[collection]
            documents = list(collection_ref.find({}, {"_id": 0}).sort("username", 1))
            rows = documents

        rows = sanitize_rows(rows)
        row_count = len(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n")

    # Upload to Google Sheet (From JSON List) (All Member Report) SSBO
    @classmethod
    def upload_to_google_sheet_SSBO_AMR(cls, file_name, g_sheet_tab, g_sheet_ID):
        """Upload CSV data to Google Sheet starting from row 2 (overwrite existing cells)."""
        import os, pandas as pd
        from googleapiclient.discovery import build

        creds = cls.googleAPI()
        sheet = build("sheets", "v4", credentials=creds).spreadsheets()

        csv_path = f"/Users/nera_thomas/Desktop/Telemarketing/excel_file/{file_name}.csv"
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            return

        df = pd.read_csv(csv_path, encoding="utf-8-sig").fillna("")
        values = df.values.tolist()   # skip header row on sheet

        sheet.values().update(
            spreadsheetId=g_sheet_ID,
            range=f"'{g_sheet_tab}'!A2",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

        print(f"✅ Uploaded {len(df)} data rows to '{g_sheet_tab}' starting from row 2.")

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

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}")
        return cleaned_docs

        # Update Data to Google Sheet from MongoDB (Deposit List) (Username)
    
    # Upload Google Sheet
    @classmethod
    def upload_to_google_sheet_DL_USERNAME(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None, extra_mongo_collections=None, overwrite=False, upload_to_sheet=True):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; keep completed_at unchanged."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    username = (
                        r.get("username")
                        or r.get("player_id")
                        or r.get("memberLogin")
                        or ""
                    )
                    username = str(username)
                    if username and not username.startswith("'"):
                        username = f"'{username}"  # force Google Sheets to treat as text
                    # --- BEGIN PATCHED AMOUNT HANDLING ---
                    amount_raw = str(r.get("amount", ""))

                    # Force 2 decimal places WITHOUT rounding
                    if "." in amount_raw:
                        whole, frac = amount_raw.split(".", 1)
                        frac = (frac + "00")[:2]   # pad then truncate
                        amount_clean = f"{whole}.{frac}"
                    else:
                        amount_clean = f"{amount_raw}.00"
                    # --- END PATCHED AMOUNT HANDLING ---

                    sanitized.append([
                        username,
                        amount_clean,
                        r.get("completed_at", ""),
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
        if not upload_to_sheet:
            print(f"upload_to_sheet=False → Skipping Google Sheet update for tab '{gs_tab}'.")
            return

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
            
        combined_rows = cls._normalize_pid_rows(rows)
        if extra_mongo_collections:
            combined_rows.extend(cls._fetch_extra_pid_rows(extra_mongo_collections))

        cls._sort_rows_by_datetime(combined_rows, "completed_at")
        rows = sanitize_rows(combined_rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        first_empty_row = 4
        end_row = first_empty_row + len(rows) - 1
        target_range = cls._build_a1_range(gs_tab, start_column, first_empty_row, end_column, end_row)

        body = {"values": rows, "majorDimension": "ROWS"}

        cls._ensure_sheet_row_capacity(service, SPREADSHEET_ID, gs_tab, end_row)

        print(f"Uploading {len(rows)} rows to Google Sheet range {target_range}")

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=target_range,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n\n")

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= SSBO DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_ssbo_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]
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

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS) in GMT+8
            if lastModifiedDate:
                try:
                    # Convert Z → UTC datetime
                    dt = datetime.fromisoformat(lastModifiedDate.replace("Z", "+00:00"))

                    # Convert UTC → GMT+8 (Malaysia Time)
                    myt = dt.astimezone(timezone(timedelta(hours=8)))

                    # Format output
                    lastModifiedDate_fmt = myt.strftime("%Y-%m-%d %H:%M:%S")
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

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}")
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

        # Convert from MongoDB (dicts) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; SSBO mapping."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    pid = str(r.get("memberLogin", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    # --- BEGIN PATCHED AMOUNT HANDLING ---
                    amount_raw = str(r.get("confirmedAmount", ""))

                    # Force 2 decimal places WITHOUT rounding
                    if "." in amount_raw:
                        whole, frac = amount_raw.split(".", 1)
                        frac = (frac + "00")[:2]   # pad then truncate
                        amount_clean = f"{whole}.{frac}"
                    else:
                        amount_clean = f"{amount_raw}.00"

                    # --- END PATCHED AMOUNT HANDLING ---
                    sanitized.append([
                        pid,
                        amount_clean,
                        r.get("lastModifiedDate", ""),
                    ])
                elif isinstance(r, (list, tuple)):
                    pid = str(r[0]) if len(r) > 0 else ""
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"
                    sanitized.append([
                        pid,
                        str(r[1]) if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",
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

            db = client["J8MS_A8MS"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("lastModifiedDate", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        first_empty_row = 4
        end_row = first_empty_row + len(rows) - 1
        target_range = cls._build_a1_range(gs_tab, start_column, first_empty_row, end_column, end_row)

        body = {"values": rows, "majorDimension": "ROWS"}

        cls._ensure_sheet_row_capacity(service, SPREADSHEET_ID, gs_tab, end_row)

        print(f"Uploading {len(rows)} rows to Google Sheet range {target_range}")

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=target_range,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        if (start_column, end_column) in {("I", "K"), ("M", "O")}:
            cls._sort_range_by_column(
                service,
                SPREADSHEET_ID,
                gs_tab,
                start_column,
                end_column,
                start_row=first_empty_row,
                end_row=end_row,
                sort_column_letter=end_column,
                descending=False
            )

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n\n")

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

    # =========================== ALL MEMBER REPORT ===========================

    # BO All Member Report
    @classmethod
    def allmemberReport(cls, team, bo_link, bo_name, currency, gmt_time, collection, g_sheet_ID, g_sheet_tab):

        # Print Color Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} All MEMBER REPORT Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get today date
        today = datetime.now().strftime("%Y-%m-%d")

        # Get every month of first 1st
        today2 = datetime.now()
        # Replace day with 1
        first_day_of_month = today2.replace(day=1)
        # Format to YYYY-MM-DD
        first_day_of_month_str = first_day_of_month.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")

        url = f"https://v3-bo.{bo_link}/api/be/report/get-all-member-report"

        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'domain': f'v3-bo.{bo_link}',
            'gmt': gmt_time,
            'lang': 'en-US',
            'loggedin': 'true',
            'origin': f'https://v3-bo.{bo_link}',
            'page': '/en-us/report/personnel-report/all-member-report',
            'priority': 'u=1, i',
            'referer': f'https://v3-bo.{bo_link}/en-us/report/personnel-report/all-member-report',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'type': 'POST',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }

        page = 1
        paginate = 50000
        total_inserted = 0
        total_skipped = 0
        last_page_with_data = 0
        finish_reason = None

        while True:

            payload = {
                "paginate": paginate,
                "page": page,
                "from_date": f"{first_day_of_month_str}T00:00:00+08:00",
                "to_date": f"{today}T23:59:59+08:00",
                "currency": [currency],
                "merchant_id": 1,
                "admin_id": 295,
                "aid": 295
            }

            response = requests.post(url, headers=headers, json=payload)

            try:
                data = response.json()
            except ValueError:
                print("⚠️ Response is not valid JSON.")
                print("Status code:", response.status_code)
                print("Text preview:", response.text[:500])
                finish_reason = "invalid_json"
                break

            if data.get("statusCode") == 401:
                print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")
                Automation.chrome_CDP()
                cls._get_cookies(
                    bo_link,
                    cls.accounts[f"{bo_name}"]["merchant_code"],
                    cls.accounts[f"{bo_name}"]["acc_ID"],
                    cls.accounts[f"{bo_name}"]["acc_PASS"],
                    f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"
                )
                print("⚠️  Cookies refreshed ... Retrying request...")
                return cls.allmemberReport(team, bo_link, bo_name, currency, gmt_time, collection, g_sheet_ID, g_sheet_tab)

            rows = data.get("data", [])
            pagination = data.get("pagination", {})

            print(f"\nPage {page} → {len(rows)} rows")

            if not isinstance(rows, list):
                rows = []

            if not rows:
                finish_reason = "no_data" if page == 1 else "completed"
                if page > 1:
                    last_page_with_data = page - 1
                break

            _, inserted, skipped = cls.mongodbAPI_AMR(rows, collection)
            total_inserted += inserted
            total_skipped += skipped
            last_page_with_data = page

            pagination_last_page = None
            if isinstance(pagination, dict):
                pagination_last_page = (
                    pagination.get("last_page")
                    or pagination.get("lastPage")
                )
                if pagination_last_page is not None:
                    try:
                        pagination_last_page = int(pagination_last_page)
                    except (TypeError, ValueError):
                        pagination_last_page = None

            if (
                pagination_last_page is not None
                and page >= pagination_last_page
            ):
                finish_reason = "completed"
                break

            if pagination_last_page is None and len(rows) < paginate:
                finish_reason = "completed"
                break

            page += 1

        if finish_reason is None:
            finish_reason = "completed"

        print(f"MongoDB Summary → Inserted: {total_inserted}, Skipped: {total_skipped}")
        if finish_reason == "no_data":
            print("⚠️ No All Member rows returned — break")
        elif finish_reason == "invalid_json":
            print("⚠️ Unable to continue due to invalid JSON response.")
        else:
            print(f"Finished. Last page = {last_page_with_data}")

        cls.upload_to_google_sheet_AMR(collection, g_sheet_ID, g_sheet_tab)

    # BO All Member Report (Extract Data like Postman/API and save as json file)
    # (merchants name = Acewin8, Ivip9, UEABET)
    @classmethod
    def ssbo_allmemberReport(cls, merchant, currency, file_name, g_sheet_tab, g_sheet_ID):
        with sync_playwright() as p:  

 
            # Get Deposit List Data (effect for fun only)
            msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{file_name}] {Fore.YELLOW}All Member Report...{Style.RESET_ALL}\n"
            for ch in msg:
                sys.stdout.write(ch)
                sys.stdout.flush()
                time.sleep(0.02)

            # Wait for Chrome CDP to be ready
            cls.wait_for_cdp_ready()

            # Connect to running Chrome
            browser = p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0] if browser.contexts else browser.new_context()    

            # Open a new browser page
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://aw8.premium-bo.com/", wait_until="load", timeout=0)

            try:
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
                
                # Click Report
                page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[7]/div[1]/div[1]/a[1]/ul[1]/li[2]").click()
                # Click All Member Report
                page.locator("//a[@ng-reflect-router-link='/report/all-member-report']//li[@class='parent-nav-item'][normalize-space()='10.2. All Member Report']").click() 
                # Delay 5 seconds
                page.wait_for_timeout(5000)
                
                # if is in Login Page, then Login, else Skip
                try:
                    # Check whether "Sign In" appear, else pass
                    expect(page.locator("//h5[normalize-space()='Sign In?']")).to_be_visible(timeout=5000)
                    # Fill in Username
                    page.locator("//input[@placeholder='Username:']").fill(cls.accounts["super_swan"]["acc_ID"])
                    # Fill in Password
                    page.locator("//input[@id='password-input']").fill(cls.accounts["super_swan"]["acc_PASS"])
                    # Login 
                    page.click("//button[normalize-space()='Login']", force=True)
                    # Delay 2 second
                    page.wait_for_timeout(2000)
                    # Click Report
                    page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[7]/div[1]/div[1]/a[1]/ul[1]/li[2]").click()
                    # Click All Member Report
                    page.locator("//a[@ng-reflect-router-link='/report/all-member-report']//li[@class='parent-nav-item'][normalize-space()='10.2. All Member Report']").click()    
                except:
                    pass
                
                # Click Region dropdown
                page.locator("ng-select .ng-select-container").nth(0).click()
                page.locator(".ng-dropdown-panel .ng-option").filter(has_text=merchant).click()
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Click Region dropdown
                page.locator("ng-select .ng-select-container").nth(1).click()
                page.locator(".ng-dropdown-panel .ng-option").filter(has_text=currency).click()
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Button Click "This Month"
                page.locator("//button[normalize-space()='This Month']").click()
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Button Click "Search"
                page.locator("//button[normalize-space()='Search']").click()
                # wait for the first row data appear 
                page.locator("tbody tr").first.wait_for(state="visible", timeout=10000)
                # Delay 3 seconds
                page.wait_for_timeout(3000)

                # Download .CSV Report File
                with page.expect_download(timeout=600000) as download_info:
                    page.locator("//button[normalize-space()='Export']").click(timeout=10000)   # ---> button click downnload csv file
                download = download_info.value

                # Save the file manually
                base_dir = "/Users/nera_thomas/Desktop/Telemarketing/excel_file"
                os.makedirs(base_dir, exist_ok=True)
                download_path = os.path.join(base_dir, f"{file_name}.zip")
                download.save_as(download_path)

                # Unzip file
                cls.unzip(file_name)
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Upload to Google Sheet
                cls.upload_to_google_sheet_SSBO_AMR(file_name, g_sheet_tab, g_sheet_ID)
            finally:
                try:
                    browser.close()
                except:
                    pass  # Browser already closed or failed to close
                Automation.cleanup()
                # Run Chrome Browser
                Automation.chrome_CDP()
                
    # =========================== DEPOSIT LIST USERNAME ===========================

    # Deposit List (Username)
    @classmethod
    def deposit_list_USERNAME(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column, extra_mongo_collections=None):

        # Get current time in GMT+8
        gmt8 = pytz.timezone("Asia/Singapore")   # GMT+8
        now_gmt8 = datetime.now(gmt8)

        current_time = now_gmt8.time()
        print(current_time, "GMT+8")

        # Get today and yesterday date
        today = now_gmt8.strftime("%Y-%m-%d")
        yesterday = (now_gmt8 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → yesterday
        # 00:15 onward → today
        cutoff_time = datetime.strptime("01:00", "%H:%M").time()

        if current_time < cutoff_time:
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
        "start_date": start_date,
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
            return cls.deposit_list_USERNAME(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column, extra_mongo_collections=extra_mongo_collections)

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
        cls.upload_to_google_sheet_DL_USERNAME(collection, gs_id, gs_tab, start_column, end_column, rows=None, extra_mongo_collections=extra_mongo_collections)

    # SSBO Deposit List (Player ID)
    @classmethod
    def ssbo_deposit_list_PID(cls, merchants, currency, collection, gs_id, gs_tab, start_column, end_column, upload_to_sheet=True):
        
        # Get current time in GMT+8
        gmt8 = pytz.timezone("Asia/Singapore")   # GMT+8
        now_gmt8 = datetime.now(gmt8)

        current_time = now_gmt8.time()
        print(current_time, "GMT+8")

        # Get today and yesterday date
        today = now_gmt8.strftime("%Y-%m-%d")
        yesterday = (now_gmt8 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → yesterday
        # 00:15 onward → today
        cutoff_time = datetime.strptime("01:00", "%H:%M").time()

        if current_time < cutoff_time:
            start_date = yesterday
            end_date = yesterday
        else:
            start_date = today
            end_date = today

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
                "start": f"{end_date}T16:00:00.000Z",
                "startTime": f"{end_date}T16:00:00.000Z",
                "startCreatedTime": f"{end_date}T16:00:00.000Z",
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
                cls.ssbo_deposit_list_PID(merchants, currency, collection, gs_id, gs_tab, start_column, end_column)
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

            # print(f"✅ Page {page}: Total IDs found:", len(ids))
            
            # ======================================= PART 2: Use IDs to GET DATA  (funny lol) ==========================================
            
            if not ids:
                print("⚠️ No transaction IDs found — break\n")
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
                "start": f"{end_date}T16:00:00.000Z",
                "startTime": f"{end_date}T16:00:00.000Z",
                "startCreatedTime": f"{end_date}T16:00:00.000Z",
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
            # print(response.json())

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            # Normalize data: SSBO sometimes returns a LIST, sometimes a DICT
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                rows = data.get("data", [])
            else:
                print("⚠️ Unexpected response type:", type(data))
                return

            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if rows:
                cls.mongodbAPI_ssbo_DL_PID(rows, collection)
            else:
                print("No valid data returned from API.")

            page+=1

        if upload_to_sheet:
            cls.upload_to_google_sheet_ssbo_DL_PID(collection, gs_id, gs_tab, start_column, end_column)
        

###############=================================== CODE RUN HERE =======================================############

### ==== README YO!!!! ==== ####
# member_info format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# member_info_2 format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# deposit_list format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name, google sheet start column, google sheet end column)


while True:
    try:

        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        # ================================== IBS BO J8MS A8MS ALL MEMBER REPORT & DEPOSIT LIST ========================================================================
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        

        print(">>== IBS J8M MY (AVA) ==<<")
        safe_call(Fetch.allmemberReport, "IBS J8M", "jw8bo.com", "jw8", "MYR", "+08:00", "J8M_AMR", "1KJyt3V3a15VuOzu98eimVXENnSr1BDn4qdi-b_TnpeU", "TM - All Member Report")
        print("======================================================\n")

        print(">>== IBS J8M MY (EUNICE) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_AMR, "J8M_AMR", "1cR5mkwhauH4tjmEC0pMm9BiTUazECZhcWFftrE4gJ4s", "TM - All Member Report")
        print("======================================================\n")

        print(">>== IBS J8M MY (XY) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_AMR,"J8M_AMR", "1zopk6PLsHeWFpDn2C6odkweJMH9S6Up-hq15m4Sdfes", "TM - All Member Report")
        print("======================================================\n")

        print("\n>>== IBS J8M MY (AVA) ==<<")
        safe_call(Fetch.deposit_list_USERNAME, "jw8bo.com", "jw8", "MYR", "+08:00", "J8M_DL_USERNAME", "1KJyt3V3a15VuOzu98eimVXENnSr1BDn4qdi-b_TnpeU", "Deposit List", "A", "C")
        print("======================================================\n")

        print("\n>>== IBS J8M MY (EUNICE) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_USERNAME, "J8M_DL_USERNAME", "1cR5mkwhauH4tjmEC0pMm9BiTUazECZhcWFftrE4gJ4s", "Deposit List", "A", "C")
        print("======================================================\n")

        print("\n>>== IBS J8M MY (XY) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_USERNAME, "J8M_DL_USERNAME", "1zopk6PLsHeWFpDn2C6odkweJMH9S6Up-hq15m4Sdfes", "Deposit List", "A", "C")
        print("======================================================\n")
        
        print(">>== IBS J8S SG (CINDY) ==<<")
        safe_call(Fetch.allmemberReport, "IBS J8S", "jw8bo.com", "jw8", "SGD", "+08:00", "J8S_AMR", "11IBUD7F91W2KkLFm6pLarDoiYmTutMcOJaoyeF92yW8", "TM - All Member Report")
        print("======================================================\n")

        print(">>== IBS J8S SG (CINDY) ==<<")
        safe_call(Fetch.deposit_list_USERNAME, "jw8bo.com", "jw8", "SGD", "+08:00", "J8S_DL_USERNAME", "11IBUD7F91W2KkLFm6pLarDoiYmTutMcOJaoyeF92yW8", "Deposit List", "A", "C")
        print("======================================================\n")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS A8M ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================
        
        print(">>== IBS A8M MY (ANGIE) ==<<")
        safe_call(Fetch.allmemberReport, "IBS A8M", "aw8bo.com", "aw8", "MYR", "+08:00", "A8M_AMR", "1vX5xRodP7-n8zNlFDtZi0ZEjrCfOl8uEJsR_DRixYKc", "TM - All Member Report (IBS)")
        print("======================================================\n")
        
        print(">>== IBS A8M MY (ANGIE 2) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_AMR, "A8M_AMR", "11XnxY2XnOPrQ7hXwJB9rpAR57lZXPv98O4czq7I1m9g", "TM - All Member Report (IBS)")
        print("======================================================\n")

        print(">>== IBS A8M MY (AVA) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_AMR, "A8M_AMR", "19y-e_M5IcWASZ7SR7jgJa0DeMJRvRXiiVuYZnfBgO2k", "TM - All Member Report (IBS)")
        print("======================================================\n")

        print("\n>>== SSBO A8M MY (ANGIE) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_ssbo_DL_PID, "SSBO_A8M_DL", "1vX5xRodP7-n8zNlFDtZi0ZEjrCfOl8uEJsR_DRixYKc", "Deposit List", "A", "C")
        safe_call(Fetch.deposit_list_USERNAME, "aw8bo.com", "aw8", "MYR", "+08:00", "A8M_DL_USERNAME", "1vX5xRodP7-n8zNlFDtZi0ZEjrCfOl8uEJsR_DRixYKc", "Deposit List", "E", "G")
        print("======================================================\n")

        print(">>== IBS A8M MY (ANGIE 2) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_ssbo_DL_PID, "SSBO_A8M_DL", "11XnxY2XnOPrQ7hXwJB9rpAR57lZXPv98O4czq7I1m9g", "Deposit List", "A", "C")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_USERNAME, "A8M_DL_USERNAME", "11XnxY2XnOPrQ7hXwJB9rpAR57lZXPv98O4czq7I1m9g", "Deposit List", "E", "G")
        print("======================================================\n")

        print("\n>>== SSBO A8M MY (AVA) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_ssbo_DL_PID, "SSBO_A8M_DL", "19y-e_M5IcWASZ7SR7jgJa0DeMJRvRXiiVuYZnfBgO2k", "Deposit List", "A", "C")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_USERNAME, "A8M_DL_USERNAME", "19y-e_M5IcWASZ7SR7jgJa0DeMJRvRXiiVuYZnfBgO2k", "Deposit List", "E", "G")
        print("======================================================\n")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS A8S ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        print(">>== IBS A8S SG (AVA) ==<<")
        safe_call(Fetch.allmemberReport, "IBS A8S", "aw8bo.com", "aw8", "SGD", "+08:00", "A8S_AMR", "19jfd2yS2cItX2UUfZewXk-qr91eNIJzMxjPTM1tz2KU", "TM - All Member Report (IBS)")
        print("======================================================\n")

        print(">>== IBS A8S SG (CINDY) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_AMR, "A8S_AMR", "1tZW0CKCCx6espAQgFRLmGnL5f5rXsLlMB0UkVVPdzYs", "TM - All Member Report (IBS)")
        print("======================================================\n")

        print(">>== IBS A8S SG (AVA) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_ssbo_DL_PID, "SSBO_A8S_DL", "19jfd2yS2cItX2UUfZewXk-qr91eNIJzMxjPTM1tz2KU", "Deposit List", "A", "C")
        safe_call(Fetch.deposit_list_USERNAME, "aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL_USERNAME", "19jfd2yS2cItX2UUfZewXk-qr91eNIJzMxjPTM1tz2KU", "Deposit List", "E", "G")
        print("======================================================\n")

        print(">>== IBS A8S SG (CINDY) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_ssbo_DL_PID, "SSBO_A8S_DL", "1tZW0CKCCx6espAQgFRLmGnL5f5rXsLlMB0UkVVPdzYs", "Deposit List", "A", "C")
        safe_call(Fetch.deposit_list_USERNAME, "aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL_USERNAME", "1tZW0CKCCx6espAQgFRLmGnL5f5rXsLlMB0UkVVPdzYs", "Deposit List", "E", "G")
        print("======================================================\n")

        
        # # =============================================================================================================================
        # # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J1B ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # # =============================================================================================================================

        # # print(">>== J1B (TOP 1000 RABBIT FILE) ==<<")
        # # safe_call(Fetch.allmemberReport, "IBS J1B", "batsman88.com", "jaya11", "BDT", "+07:00", "J1B_AMR", "1vjybD6v2I0sewy_LKYDMkU8e3tuacgg0g3-vPU4YmNY", "AMR")
        # # print("======================================================\n")

        # print(">>== J1B (TOP 1000 RABBIT FILE) ==<<")
        # safe_call(Fetch.deposit_list_USERNAME, "batsman88.com", "jaya11", "BDT", "+07:00", "J1B_DL_USERNAME", "1vjybD6v2I0sewy_LKYDMkU8e3tuacgg0g3-vPU4YmNY", "Deposit List", "A", "C")
        # print("======================================================\n")

        
        # # =============================================================================================================================
        # # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS D8M ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # # =============================================================================================================================

        # # print(">>== D8M RETENTION LIST ==<<")
        # # safe_call(Fetch.allmemberReport, "IBS D8M", "dis88bo.com", "dis88", "MYR", "+08:00", "D8M_AMR", "1BV_FRD3T4LquzhZjYZPlidRsNJBComs73UXzTvaqlko", "TM - All Member Report")
        # # print("======================================================\n")

        # # print(">>== D8M VIP LIST ==<<")
        # # safe_call(mongodb_2_gs.upload_to_google_sheet_AMR, "D8M_AMR", "1VqKTphZ7CsFMHzASskQoN6yhjsWxtwQ0RbIduc1Q9rc", "TM - All Member Report")
        # # print("======================================================\n")

        # print(">>== D8M RETENTION LIST ==<<")
        # safe_call(Fetch.deposit_list_USERNAME, "dis88bo.com", "dis88", "MYR", "+08:00",  "D8M_DL_USERNAME", "1BV_FRD3T4LquzhZjYZPlidRsNJBComs73UXzTvaqlko", "DEPOSIT LIST", "A", "C")
        # print("======================================================\n")

        # print(">>== D8M VIP LIST ==<<")
        # safe_call(mongodb_2_gs.upload_to_google_sheet_DL_USERNAME, "D8M_DL_USERNAME", "1VqKTphZ7CsFMHzASskQoN6yhjsWxtwQ0RbIduc1Q9rc", "DEPOSIT LIST", "A", "C")
        # print("======================================================\n")

        
        

        # # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-
        # # # ============================================== SSBO A8MS USING EXPORT METHOD ====================================================================================
        # # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-
        
        
        # Run Chrome Browser
        Automation.chrome_CDP()

        print(">>== SSBO A8M MY (ANGIE) ==<<")
        safe_call(Fetch.ssbo_allmemberReport, "Acewin8", "Malaysia", "SSBO_A8M_AMR", "TM - All Member Report (SS)", "1vX5xRodP7-n8zNlFDtZi0ZEjrCfOl8uEJsR_DRixYKc")

        print(">>== SSBO A8M MY (ANGIE 2) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_SSBO_AMR, "SSBO_A8M_AMR", "TM - All Member Report (SS)", "11XnxY2XnOPrQ7hXwJB9rpAR57lZXPv98O4czq7I1m9g")

        print(">>== SSBO A8M MY (AVA) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_SSBO_AMR, "SSBO_A8M_AMR", "TM - All Member Report (SS)", "19y-e_M5IcWASZ7SR7jgJa0DeMJRvRXiiVuYZnfBgO2k")

        print(">>== SSBO A8S SG (AVA) ==<<")
        safe_call(Fetch.ssbo_allmemberReport, "Acewin8", "Singapore", "SSBO_A8S_AMR", "TM - All Member Report (SS)", "19jfd2yS2cItX2UUfZewXk-qr91eNIJzMxjPTM1tz2KU")

        print(">>== SSBO A8S SG (CINDY) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_SSBO_AMR, "SSBO_A8S_AMR", "TM - All Member Report (SS)", "1tZW0CKCCx6espAQgFRLmGnL5f5rXsLlMB0UkVVPdzYs")

        # Close Browser
        Automation.cleanup()

        # Delay 5 minutes
        time.sleep(300)

    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
