import os
import re
import sys
import time
import pytz
import json
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
from runtime import logger, safe_call, MONGODB_URI

PROXIES = {
    "http":  "http://127.0.0.1:10809",
    "https": "http://127.0.0.1:10809",
}

def create_session():
    s = requests.Session()
    s.proxies.update(PROXIES)
    s.trust_env = False   # VERY IMPORTANT
    return s

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
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]
        combined = []
        for col_name in collection_names:
            col = db[col_name]
            docs = list(col.find({}, {"_id": 0}))
            combined.extend(cls._normalize_pid_rows(docs))
        return combined

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================
    
    # =-=-=--=-=-=-=-=-= IBS =-=-=-=-=-=-=-=-=-=-=-=-=
    # MongoDB Database (Member Info)
    def mongodbAPI_MI(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]
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
        RANGE_NAME = f"{gs_tab}!B6:F"

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

            client = MongoClient(MONGODB_URI)
            db = client["J8MS_A8MS"]
            collection = db[collection]
            
            # Decide MongoDB query window based on current system time (GMT+8)
            now = datetime.now(pytz.timezone("Asia/Kuala_Lumpur"))
            current_time = now.time()

            is_day_window = current_time >= datetime.strptime("10:00:00", "%H:%M:%S").time()

            # Build WHERE condition (IGNORE DATE COMPLETELY)
            if is_day_window:
                # 10:00 - 23:59
                where_condition = {
                    "$expr": {
                        "$and": [
                            {
                                "$gte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "10:00:00"
                                ]
                            },
                            {
                                "$lte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "23:59:59"
                                ]
                            }
                        ]
                    }
                }
            else:
                # 00:00 - 09:59
                where_condition = {
                    "$expr": {
                        "$and": [
                            {
                                "$gte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "00:00:00"
                                ]
                            },
                            {
                                "$lte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "09:59:59"
                                ]
                            }
                        ]
                    }
                }

            documents = list(
                collection.find(where_condition, {"_id": 0})
                .sort("register_info_date", 1)
            )

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
    def mongodbAPI_ssbo_MI(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]
        collection = db[collection]

       # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("member_id", 1)],
            name="ssbo_member_unique",
            unique=True,
            partialFilterExpression={
                "member_id": {"$type": "string"}
            },
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
            username = row.get("username") or row.get("login")
            first_name = row.get("first_name") or row.get("name")
            register_info_date = row.get("register_info_date") or row.get("registerDate")
            mobileno = row.get("mobileno") or row.get("phone")
            member_id = row.get("member_id") or row.get("id")
            if not register_info_date:
                continue
            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS) in GMT+8
            try:
                dt = datetime.fromisoformat(str(register_info_date).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(timezone(timedelta(hours=8)))
                register_info_date_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                register_info_date_fmt = str(register_info_date)
            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "username": username,
                "first_name": first_name,
                "register_info_date": register_info_date_fmt,
                "mobileno": mobileno,
                "member_id": member_id,
            }
            # Upsert: overwrite if member_id exists
            cleaned_docs.append(doc.copy())
            try:
                result = collection.update_one(
                    {"member_id": member_id},
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
    def upload_to_google_sheet_ssbo_MI(cls, collection, gs_id, gs_tab, rows=None, extra_mongo_collections=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = cls._build_a1_range(gs_tab, "B", 6, "F")

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for Member Info."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    username = (
                        r.get("username")
                        or r.get("login")
                        or ""
                    )
                    first_name = (
                        r.get("first_name")
                        or r.get("name")
                        or ""
                    )
                    register_date = (
                        r.get("register_info_date")
                        or r.get("registerDate")
                        or ""
                    )
                    phone = (
                        r.get("mobileno")
                        or r.get("phone")
                        or ""
                    )
                    member_id = (
                        r.get("member_id")
                        or r.get("id")
                        or ""
                    )
                    sanitized.append([
                        str(username),
                        str(first_name),
                        str(register_date),
                        str(phone),
                        str(member_id),
                    ])
                else:
                    sanitized.append(["", "", "", "", ""])
            return sanitized

        if not MONGODB_URI:
            raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            collection = db[collection]

            # Decide MongoDB query window based on current system time (GMT+8)
            now = datetime.now(pytz.timezone("Asia/Kuala_Lumpur"))
            current_time = now.time()

            is_day_window = current_time >= datetime.strptime("10:00:00", "%H:%M:%S").time()

            # Build WHERE condition (IGNORE DATE COMPLETELY)
            if is_day_window:
                # 10:00 - 23:59
                where_condition = {
                    "$expr": {
                        "$and": [
                            {
                                "$gte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "10:00:00"
                                ]
                            },
                            {
                                "$lte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "23:59:59"
                                ]
                            }
                        ]
                    }
                }
            else:
                # 00:00 - 09:59
                where_condition = {
                    "$expr": {
                        "$and": [
                            {
                                "$gte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "00:00:00"
                                ]
                            },
                            {
                                "$lte": [
                                    {"$substrCP": ["$register_info_date", 11, 8]},
                                    "09:59:59"
                                ]
                            }
                        ]
                    }
                }

            documents = list(
                collection.find(where_condition, {"_id": 0})
                .sort("register_info_date", 1)
            )

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
        print("Uploaded MongoDB data to Google Sheet.\n")

# Fetch Data
class Fetch(BO_Account, mongodb_2_gs):
    
    # =========================== GET Cookies ===========================

    # Get IBS Cookies incase Cookies expired
    @classmethod
    def _get_cookies(cls, bo_link, merchant_code, acc_id, acc_pass, cookies_path):
        
        session = create_session()

        url = f"https://v3-bo.{bo_link}/api/be/auth/loginV2"

        payload = json.dumps({
        "mer_code": merchant_code,
        "username": acc_id,
        "password": acc_pass,
        })
        
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': '+08:00',
        'lang': 'en-US',
        'loggedin': 'false',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-arch': '"arm"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"143.0.7499.147"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="143.0.7499.147", "Chromium";v="143.0.7499.147", "Not A(Brand";v="24.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-platform-version': '"15.5.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'CUSTOM',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

        # Get session
        session.get(f"https://v3-bo.{bo_link}/en-us", headers=headers)

        # Return Response
        response = session.post(url, headers=headers, data=payload)
        print(response.json())

        # to get Authentication Cookies
        user_cookie = session.cookies.get("user")

        # Save cookies to "get_cookies" path
        file_path = cookies_path

        # data format
        data = {
            "user_cookie": user_cookie
        }

        # write to json file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print("Saved user cookie to:", file_path)

    # Get SSBO Cookies incase Cookies expired
    @classmethod
    def _ssbo_get_cookies(cls, acc_id, acc_pass):

        session = create_session()

        url = "https://aw8.premium-bo.com/api/authenticate?noBlockUI=true"

        payload = json.dumps({
        "tenantCode": None,
        "login": acc_id,
        "password": acc_pass,
        "rememberMe": True,
        "authenticatorAppEncoded": None,
        "authenticatorAppCode": None
        })

        headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'origin': 'https://aw8.premium-bo.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://aw8.premium-bo.com/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

        # Session
        session.get("https://aw8.premium-bo.com/", headers=headers)

        # Response
        response = session.post(url, headers=headers, data=payload)
        data = response.json()
        jwt_token = data.get("jwt")

        # Save cookies to "get_cookies" path
        file_path = f"/home/thomas/get_cookies/superswan.json"

        # data format you want
        data = {"jwt_token": jwt_token}

        # write to json file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print("Saved user cookie to:", file_path)

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # Member Info
    @classmethod
    def member_info(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, upload_to_sheet=True):

        session = create_session()

        # Print Title
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{collection}] {Fore.YELLOW} MEMBER INFO Data ... {Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

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
        cookie_file = f"/home/thomas/get_cookies/{bo_link}.json"

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
        response = session.post(url, headers=headers, json=payload, timeout=30)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            # Get Cookies
            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/home/thomas/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.member_info(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, upload_to_sheet=upload_to_sheet)

        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request (CORRECT WAY)
            response = session.post(url, headers=headers, json=payload, timeout=30)

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
        if upload_to_sheet:
            mongodb_2_gs.upload_to_google_sheet_MI(collection, gs_id, gs_tab)

    # SSBO Member Info (merchants name = aw8, ip9, uea)
    @classmethod
    def ssbo_member_info(cls, merchants, currency, collection, gs_id, gs_tab, extra_mongo_collections=None):
        
        session = create_session()

        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{collection}] {Fore.YELLOW} MEMBER INFO Data ... {Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get TimeZone (GMT+8)
        gmt8 = pytz.timezone("Asia/Kuala_Lumpur")
        now_gmt8 = datetime.now(gmt8)

        # Get Current Time (GMT +8)    
        current_time = now_gmt8.time()
        print(current_time, "GMT+8")
        
        # Today & Yesterday Date
        today = now_gmt8.strftime("%Y-%m-%d")
        yesterday = (now_gmt8 - timedelta(days=1)).strftime("%Y-%m-%d")
        yes_yesterday = (now_gmt8 - timedelta(days=2)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 01:00 → use yesterday
        # after 01:00 onward → use today
        if current_time < datetime.strptime("01:00", "%H:%M").time():
            start_date = yes_yesterday
            end_date = yesterday
        else:
            start_date = yesterday
            end_date = today

        # Normalize currency input to list form for API payloads
        if isinstance(currency, (list, tuple, set)):
            currency_list = list(currency)
        else:
            currency_list = [currency]

        # Cookie File
        cookie_file = f"/home/thomas/get_cookies/superswan.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"jwt_token": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        user_cookie = cookie_data.get("jwt_token", "")

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
            "created_date_from": f"{start_date}T16:00:00.000Z",
            "created_date": f"{end_date}T15:59:59.000Z",
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
            'authorization': f'Bearer {user_cookie}',
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
            }

            # Post Response 
            response = session.post(url, headers=headers, json=payload, timeout=30)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                
                # Error 401 or 403, get cookies
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                cls._ssbo_get_cookies(cls.accounts["super_swan2"]["acc_ID"], cls.accounts["super_swan2"]["acc_PASS"])

                # Retrying requests...
                print("⚠️ Cookies + bearer token refreshed ... Retrying request...\n")
                cls.ssbo_member_info(merchants, currency, collection, gs_id, gs_tab, extra_mongo_collections=extra_mongo_collections)
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
                "createdDateFrom": f"2025-12-31T16:00:00.000Z",
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
                'authorization': f'Bearer {user_cookie}',
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
            }

            # Post Response 
            response = session.get(url, headers=headers, timeout=30)

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
        
        # upload to google sheet
        cls.upload_to_google_sheet_ssbo_MI(collection, gs_id, gs_tab, extra_mongo_collections=extra_mongo_collections)

###############=================================== CODE RUN HERE =======================================############

# ================== AUTO STOP DATE CONFIG ==================

gmt8 = pytz.timezone("Asia/Singapore")
now = datetime.now(gmt8)

# Calculate next month (safe for December)
if now.month == 12:
    stop_year = now.year + 1
    stop_month = 1
else:
    stop_year = now.year
    stop_month = now.month + 1

# Stop at 1st day of next month, 00:00 GMT+8
STOP_DATETIME = gmt8.localize(datetime(stop_year, stop_month, 1, 0, 0, 0))
print(f"Bot will stop automatically at: {STOP_DATETIME}")

# ================== AUTO STOP DATE CONFIG ==================

while True:
    try:

        # Auto-stop check
        current_time = datetime.now(gmt8)
        if current_time >= STOP_DATETIME:
            logger.info(f"Reached stop date ({STOP_DATETIME}). Bot is stopping.")
            break
        
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        # ============================================================== J8MS A8MS EMILLIA =============================================================================
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        # Check current time to use correct google sheet tab to upload data
        current_time = datetime.now().time()
        print(current_time)
        
        # 00:00 - 09:59
        if current_time < datetime.strptime("09:59", "%H:%M").time():
            
            # IBS J8M MY
            print("\n>>== IBS J8M ==<<")
            safe_call(Fetch.member_info, "jw8bo.com", "jw8", "MYR", "+08:00", "J8M_MI_00_10", "1GR3NwHoD6niRcXAbdssPIrxzJIdlSea9gya8W7MkTb4", "J8M IBS", description="IBS J8M MY MEMBER INFO")
        
            # IBS J8S SG
            print("\n>>== IBS J8S ==<<")
            safe_call(Fetch.member_info, "jw8bo.com", "jw8", "SGD", "+08:00", "J8S_MI_00_10", "1GR3NwHoD6niRcXAbdssPIrxzJIdlSea9gya8W7MkTb4", "J8S IBS", description="IBS J8S SG MEMBER INFO")
            
            # IBS A8M MY
            print("\n>>== IBS A8M ==<<")
            safe_call(Fetch.member_info, "aw8bo.com", "aw8", "MYR", "+08:00", "A8M_MI_00_10", "1GR3NwHoD6niRcXAbdssPIrxzJIdlSea9gya8W7MkTb4", "A8M IBS", description="IBS A8M MY MEMBER INFO")

            # SSBO A8M MY
            print("\n>>== SSBO A8M MY ==<<")
            safe_call(Fetch.ssbo_member_info, "aw8", ["MYR"], "SSBO_A8M_MI_00_10", "1GR3NwHoD6niRcXAbdssPIrxzJIdlSea9gya8W7MkTb4", "A8M SS", description="SSBO A8M MY MEMBER INFO")

            # IBS A8S SG
            print("\n>>== IBS A8S ==<<")
            safe_call(Fetch.member_info, "aw8bo.com", "aw8", "SGD", "+08:00", "A8S_MI_00_10", "1GR3NwHoD6niRcXAbdssPIrxzJIdlSea9gya8W7MkTb4", "A8S IBS", description="IBS A8S SG MEMBER INFO")

            # SSBO A8S SG
            print("\n>>== SSBO A8S SG ==<<")
            safe_call(Fetch.ssbo_member_info, "aw8", ["SGD"], "SSBO_A8S_MI_00_10", "1GR3NwHoD6niRcXAbdssPIrxzJIdlSea9gya8W7MkTb4", "A8S SS", description="SSBO A8S SG MEMBER INFO")
        
        # 10:00 - 23:59
        else:

            # IBS J8M MY
            safe_call(Fetch.member_info, "jw8bo.com", "jw8", "MYR", "+08:00", "J8M_MI_10_00", "1IAer3P0IUWNH69itVLlJ8gcZumaHFIQVGchOTkHiMFs", "J8M IBS", description="IBS J8M MY MEMBER INFO")
        
            # IBS J8S SG
            safe_call(Fetch.member_info, "jw8bo.com", "jw8", "SGD", "+08:00", "J8S_MI_10_00", "1IAer3P0IUWNH69itVLlJ8gcZumaHFIQVGchOTkHiMFs", "J8S IBS", description="IBS J8S SG MEMBER INFO")
            
            # IBS A8M MY
            safe_call(Fetch.member_info, "aw8bo.com", "aw8", "MYR", "+08:00", "A8M_MI_10_00", "1IAer3P0IUWNH69itVLlJ8gcZumaHFIQVGchOTkHiMFs", "A8M IBS", description="IBS A8M MY MEMBER INFO")

            # SSBO A8M MY
            safe_call(Fetch.ssbo_member_info, "aw8", ["MYR"], "SSBO_A8M_MI_10_00", "1IAer3P0IUWNH69itVLlJ8gcZumaHFIQVGchOTkHiMFs", "A8M SS", description="SSBO A8M MY MEMBER INFO")

            # IBS A8S SG
            safe_call(Fetch.member_info, "aw8bo.com", "aw8", "SGD", "+08:00", "A8S_MI_10_00", "1IAer3P0IUWNH69itVLlJ8gcZumaHFIQVGchOTkHiMFs", "A8S IBS", description="IBS A8S SG MEMBER INFO")

            # SSBO A8S SG
            safe_call(Fetch.ssbo_member_info, "aw8", ["SGD"], "SSBO_A8S_MI_10_00", "1IAer3P0IUWNH69itVLlJ8gcZumaHFIQVGchOTkHiMFs", "A8S SS", description="SSBO A8S SG MEMBER INFO")
            

    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
