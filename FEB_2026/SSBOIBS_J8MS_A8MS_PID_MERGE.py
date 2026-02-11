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
    TOKEN_PATH = "/home/thomas/api/google7/token.json"
    CREDS_PATH = "/home/thomas/api/google7/credentials.json"

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
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # -_-_-_-_-_-_-_- SSBO -_-_-_-_-_-_-_-

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

            client = MongoClient(MONGODB_URI)
            db = client["J8MS_A8MS"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("lastModifiedDate", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        first_empty_row = 3
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

    # -_-_-_-_-_-_-_- IBS -_-_-_-_-_-_-_-

    # MongoDB Database
    def mongodbAPI_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]
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

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB
    @classmethod
    def upload_to_google_sheet_DL_PID(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None, extra_mongo_collections=None, overwrite=False, upload_to_sheet=True):

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
                    pid = str(r.get("player_id", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    # --- BEGIN PATCHED AMOUNT HANDLING ---
                    amount_raw = r.get("amount", "")

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

            client = MongoClient(MONGODB_URI)
            db = client["J8MS_A8MS"]
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

        if overwrite or extra_mongo_collections or (start_column, end_column) in {("A", "C"), ("E", "G")}:
            first_empty_row = 3
        else:
            first_empty_row = cls._find_first_empty_row(
                sheet,
                SPREADSHEET_ID,
                gs_tab,
                start_column,
                end_column,
                start_row=3
            )
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
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS DEPOSIT LIST =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================# 

    # Deposit List (Player ID)
    @classmethod
    def deposit_list_PID(cls, bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column, extra_mongo_collections=None):
        
        session = create_session()

        # Get current time in GMT+8
        gmt8 = pytz.timezone("Asia/Singapore")   # GMT+8
        now_gmt8 = datetime.now(gmt8)

        current_time = now_gmt8.time()
        print(current_time, "GMT+8")

        # Get today and yesterday date
        today = now_gmt8.strftime("%Y-%m-%d")
        yesterday = (now_gmt8 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 01:00 → use yesterday
        # 01:00 onward → use today
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
        response = session.post(url, headers=headers, json=payload, timeout=30)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.deposit_list_PID(bo_link, bo_name, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column, extra_mongo_collections)


        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request
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

            # ====================== EMPTY PAGE SAFETY RETRY ======================
            # ⚠️ IMPORTANT:
            # This API is unstable and may randomly return 0 rows even when data exists.
            # A single (or even double) empty response MUST NOT be trusted.
            #
            # Strategy:
            # - Retry the SAME page up to MAX_EMPTY_RETRIES times
            # - Sleep briefly between retries to avoid backend race conditions
            # - If ANY retry returns data → treat page as valid
            # - Only stop pagination if ALL retries return 0 rows
            
            if not rows:
                MAX_EMPTY_RETRIES = 5  # Set to 5 retries
                empty_attempts = 0
                
                print(f"⚠️ Page {page} returned 0 rows — Starting safety retries...")

                while empty_attempts < MAX_EMPTY_RETRIES:
                    # Incrementing attempt number for display (1 to 8)
                    current_attempt = empty_attempts + 1
                    
                    # Print this BEFORE the request so you see it immediately
                    print(f"🔄 [Retry {current_attempt}/{MAX_EMPTY_RETRIES}] Re-requesting Page {page}...")
                    
                    time.sleep(1.5)  

                    try:
                        # Use session.post to ensure proxies/headers are maintained
                        retry_response = session.post(url, headers=headers, json=payload, timeout=30)
                        
                        if retry_response.status_code == 401:
                            print(f"❌ Attempt {current_attempt}: Unauthorized (401). Stopping retries.")
                            break

                        retry_data = retry_response.json()
                        rows = retry_data.get("data", []) 
                        
                        if rows:
                            print(f"✅ Success! Page {page} recovered on attempt {current_attempt} ({len(rows)} rows found).")
                            break
                        else:
                            print(f"Empty: Attempt {current_attempt} still returned 0 rows.")
                            
                    except Exception as e:
                        # This catches connection timeouts or proxy errors
                        print(f"⚠️ Attempt {current_attempt} failed with error: {e}")
                        rows = []

                    empty_attempts += 1

                if not rows:
                    print(f"⛔ Page {page} confirmed empty after {MAX_EMPTY_RETRIES} retries. Stopping pagination.")
                    break

            # Insert into MongoDB
            rows_to_insert = rows if isinstance(rows, list) else []

            if rows_to_insert:
                cls.mongodbAPI_DL_PID(rows_to_insert, collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        cls.upload_to_google_sheet_DL_PID(collection, gs_id, gs_tab, start_column, end_column, rows=None, extra_mongo_collections=extra_mongo_collections)

    # SSBO Deposit List (Player ID)
    @classmethod
    def ssbo_deposit_list_PID(cls, merchants, currency, collection, gs_id, gs_tab, start_column, end_column, upload_to_sheet=True):
        
        session = create_session()

        # Get today and yesterday date
        # Get current time in GMT+8
        gmt8 = pytz.timezone("Asia/Singapore")   # GMT+8
        now_gmt8 = datetime.now(gmt8)

        current_time = now_gmt8.time()
        print(current_time, "GMT+8")

        # Get today and yesterday date
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Get today and yesterday date
        today_minus1 = (now_gmt8 - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_minus2 = (now_gmt8 - timedelta(days=2)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 01:00 → use yesterday
        # 01:01 onward → use today
        cutoff_time = datetime.strptime("01:00", "%H:%M").time()

        if current_time < cutoff_time:
            start_date = yesterday_minus2
            end_date = today_minus1
        else:
            start_date = yesterday
            end_date = today

        print(f"Start Date: {start_date}, End Date: {end_date}")
        
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
                "start": f"{start_date}T16:00:00.000Z",
                "startTime": f"{start_date}T16:00:00.000Z",
                "startCreatedTime": f"{start_date}T16:00:00.000Z",
                "end": f"{end_date}T15:59:59.000Z",
                "endTime": f"{end_date}T15:59:59.000Z",
                "endCreatedTime": f"{end_date}T15:59:59.000Z",
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

            # Get Post Response
            query = urlencode(params, doseq=True)
            full_url = f"{url}?{query}"

            # Post Response 
            response = session.get(full_url, headers=headers, timeout=30)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):

                # Check if 401 or 403 error, Reget Cookies
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                cls._ssbo_get_cookies(cls.accounts["super_swan2"]["acc_ID"], cls.accounts["super_swan2"]["acc_PASS"])

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
                "start": f"{start_date}T16:00:00.000Z",
                "startTime": f"{start_date}T16:00:00.000Z",
                "startCreatedTime": f"{start_date}T16:00:00.000Z",
                "end": f"{end_date}T15:59:59.000Z",
                "endTime": f"{end_date}T15:59:59.000Z",
                "endCreatedTime": f"{end_date}T15:59:59.000Z",
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

# Stop at next month of 1st, 00:00 GMT+8
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
        
        # SSBO A8M MY
        print("\n\033[1;31mEMILLIA\033[0m \033[1;36mSSBO A8M MY DL USERNAME\033[0m \033[2m(UPLOAD FALSE)\033[0m")
        safe_call(Fetch.ssbo_deposit_list_PID, "aw8", ["MYR"], "SSBO_A8M_DL", "1SGZL94lvPZDeAR1Au3JxiINBySPPjVgr18hDbPsDQAE", "DEPOSIT LIST", "I", "K", upload_to_sheet=False)
        # SSBO A8S SG
        print("\n\033[1;31mEMILLIA\033[0m \033[1;36mSSBO A8S SG DL USERNAME\033[0m \033[2m(UPLOAD FALSE)\033[0m")
        safe_call(Fetch.ssbo_deposit_list_PID, "aw8", ["SGD"], "SSBO_A8S_DL", "1SGZL94lvPZDeAR1Au3JxiINBySPPjVgr18hDbPsDQAE", "DEPOSIT LIST", "M", "O", upload_to_sheet=False)
        # IBS J8M MY
        print("\n\033[1;31mEMILLIA\033[0m \033[1;36mIBS J8M MY DL PID\033[0m")
        safe_call(Fetch.deposit_list_PID, "jw8bo.com", "jw8", "MYR", "+08:00", "J8M_DL", "1SGZL94lvPZDeAR1Au3JxiINBySPPjVgr18hDbPsDQAE", "DEPOSIT LIST", "A", "C")
        # IBS J8S SG 
        print("\n\033[1;31mEMILLIA\033[0m \033[1;36mIBS J8S SG DL PID\033[0m")
        safe_call(Fetch.deposit_list_PID, "jw8bo.com", "jw8", "SGD", "+08:00", "J8S_DL", "1SGZL94lvPZDeAR1Au3JxiINBySPPjVgr18hDbPsDQAE", "DEPOSIT LIST", "E", "G")
        # IBS A8M MY 
        print("\n\033[1;31mEMILLIA\033[0m \033[1;36mIBS A8M MY DL PID + SSBO A8M MY DL USERNAME\033[0m \033[2m(COMBINE)\033[0m")
        safe_call(Fetch.deposit_list_PID, "aw8bo.com", "aw8", "MYR", "+08:00", "A8M_DL", "1SGZL94lvPZDeAR1Au3JxiINBySPPjVgr18hDbPsDQAE", "DEPOSIT LIST", "I", "K", extra_mongo_collections=["SSBO_A8M_DL"])
        # IBS A8S SG  
        print("\n\033[1;31mEMILLIA\033[0m \033[1;36mIBS A8S SG DL PID + SSBO A8S SG DL USERNAME\033[0m \033[2m(COMBINE)\033[0m")
        safe_call(Fetch.deposit_list_PID, "aw8bo.com", "aw8", "SGD", "+08:00", "A8S_DL", "1SGZL94lvPZDeAR1Au3JxiINBySPPjVgr18hDbPsDQAE", "DEPOSIT LIST", "M", "O", extra_mongo_collections=["SSBO_A8S_DL"])
        

        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        # ============================================================== J8MS A8MS KAYREEN =============================================================================
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        # SSBO A8M MY
        print("\n\033[1;31mKAYREEN\033[0m \033[1;36mSSBO A8M MY DL USERNAME\033[0m \033[2m(UPLOAD FALSE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "SSBO_A8M_DL", "1dLvLwC_2HrXl0elshVsQg-koRE23FEozf4aiJQVzTmE", "DEPOSIT LIST", "I", "K",  upload_to_sheet=False)
        # SSBO A8S SG
        print("\n\033[1;31mKAYREEN\033[0m \033[1;36mSSBO A8S SG DL USERNAME\033[0m \033[2m(UPLOAD FALSE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "SSBO_A8S_DL", "1dLvLwC_2HrXl0elshVsQg-koRE23FEozf4aiJQVzTmE", "DEPOSIT LIST", "M", "O", upload_to_sheet=False)
        # IBS J8M MY
        print("\n\033[1;31mKAYREEN\033[0m \033[1;36mIBS J8M MY DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "J8M_DL", "1dLvLwC_2HrXl0elshVsQg-koRE23FEozf4aiJQVzTmE", "DEPOSIT LIST", "A", "C")
        # IBS J8S SG 
        print("\n\033[1;31mKAYREEN\033[0m \033[1;36mIBS J8S SG DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "J8S_DL", "1dLvLwC_2HrXl0elshVsQg-koRE23FEozf4aiJQVzTmE", "DEPOSIT LIST", "E", "G")
        # IBS A8M MY 
        print("\n\033[1;31mKAYREEN\033[0m \033[1;36mIBS A8M MY DL PID + SSBO A8M MY DL USERNAME\033[0m \033[2m(COMBINE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "A8M_DL", "1dLvLwC_2HrXl0elshVsQg-koRE23FEozf4aiJQVzTmE", "DEPOSIT LIST", "I", "K", extra_mongo_collections=["SSBO_A8M_DL"])
        # IBS A8S SG  
        print("\n\033[1;31mKAYREEN\033[0m \033[1;36mIBS A8S SG DL PID + SSBO A8S SG DL USERNAME\033[0m \033[2m(COMBINE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "A8S_DL", "1dLvLwC_2HrXl0elshVsQg-koRE23FEozf4aiJQVzTmE", "DEPOSIT LIST", "M", "O", extra_mongo_collections=["SSBO_A8S_DL"])

        # # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        # # # ============================================================== J8MS A8MS YVONNE =============================================================================
        # # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        # SSBO A8M MY
        print("\n\033[1;31mYVONNE\033[0m \033[1;36mSSBO A8M MY DL USERNAME\033[0m \033[2m(UPLOAD FALSE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "SSBO_A8M_DL", "17o9McEnqSmUZNb45dGsWlMThpn0WbwDl5ZQsdYxM-Ig", "DEPOSIT LIST", "I", "K", upload_to_sheet=False)
        # SSBO A8S SG
        print("\n\033[1;31mYVONNE\033[0m \033[1;36mSSBO A8S SG DL USERNAME\033[0m \033[2m(UPLOAD FALSE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "SSBO_A8S_DL", "17o9McEnqSmUZNb45dGsWlMThpn0WbwDl5ZQsdYxM-Ig", "DEPOSIT LIST", "M", "O", upload_to_sheet=False)
        # IBS J8M MY
        print("\n\033[1;31mYVONNE\033[0m \033[1;36mIBS J8M MY DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "J8M_DL", "17o9McEnqSmUZNb45dGsWlMThpn0WbwDl5ZQsdYxM-Ig", "DEPOSIT LIST", "A", "C")
        # IBS J8S SG 
        print("\n\033[1;31mYVONNE\033[0m \033[1;36mIBS J8S SG DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "J8S_DL", "17o9McEnqSmUZNb45dGsWlMThpn0WbwDl5ZQsdYxM-Ig", "DEPOSIT LIST", "E", "G")
        # IBS A8M MY 
        print("\n\033[1;31mYVONNE\033[0m \033[1;36mIBS A8M MY DL PID + SSBO A8M MY DL USERNAME\033[0m \033[2m(COMBINE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "A8M_DL", "17o9McEnqSmUZNb45dGsWlMThpn0WbwDl5ZQsdYxM-Ig", "DEPOSIT LIST", "I", "K", extra_mongo_collections=["SSBO_A8M_DL"])
        # IBS A8S SG  
        print("\n\033[1;31mYVONNE\033[0m \033[1;36mIBS A8S SG DL PID + SSBO A8S SG DL USERNAME\033[0m \033[2m(COMBINE)\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "A8S_DL", "17o9McEnqSmUZNb45dGsWlMThpn0WbwDl5ZQsdYxM-Ig", "DEPOSIT LIST", "M", "O", extra_mongo_collections=["SSBO_A8S_DL"])
    
        # Delay 5 minutes
        time.sleep(300)
        
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
