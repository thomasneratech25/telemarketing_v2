import os
import sys
import time
import pytz
import json
import requests
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
        "super_swan2": {
            "acc_ID": os.getenv("ACC_ID_SUPERSWAN2"),
            "acc_PASS": os.getenv("ACC_PASS_SUPERSWAN2")
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
        "nex8": {
            "merchant_code": os.getenv("MERCHANT_CODE_NEX8"),
            "acc_ID": os.getenv("ACC_ID_NEX8"),
            "acc_PASS": os.getenv("ACC_PASS_NEX8")
        },
        "828": {
            "merchant_code": os.getenv("MERCHANT_CODE_828"),
            "acc_ID": os.getenv("ACC_ID_828"),
            "acc_PASS": os.getenv("ACC_PASS_828")
        },
        "dis88": {
            "merchant_code": os.getenv("MERCHANT_CODE_DIS88"),
            "acc_ID": os.getenv("ACC_ID_DIS88"),
            "acc_PASS": os.getenv("ACC_PASS_DIS88")
        },
        "s191": {
            "merchant_code": os.getenv("MERCHANT_CODE_S191"),
            "acc_ID": os.getenv("ACC_ID_S191"),
            "acc_PASS": os.getenv("ACC_PASS_S191")
        },
        "gojudi": {
            "merchant_code": os.getenv("MERCHANT_CODE_GOJUDI"),
            "acc_ID": os.getenv("ACC_ID_GOJUDI"),
            "acc_PASS": os.getenv("ACC_PASS_GOJUDI")
        },
    }

# MongoDB <-> Google Sheet
class mongodb_2_gs:

    # Handles Google Sheets operations for the Telemarketing project.
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    TOKEN_PATH = "/home/thomas/api/google3/token.json"
    CREDS_PATH = "/home/thomas/api/google3/credentials.json"

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
        mongo_uri = os.getenv("MONGODB_API_KEY") or MONGODB_URI
        if not mongo_uri:
            raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
        client = MongoClient(mongo_uri)
        db = client["CONVERSION_0226"]
        combined = []
        for col_name in collection_names:
            col = db[col_name]
            docs = list(col.find({}, {"_id": 0}))
            print(f"Extra collection '{col_name}': {len(docs)} rows")
            combined.extend(cls._normalize_pid_rows(docs))
        return combined
    
    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS FTD/STD =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # =-=-=--=-=-=-=-=-= IBS =-=-=-=-=-=-=-=-=-=-=-=-=
    # MongoDB Database (FTD/STD)
    def mongodbAPI_FTD_STD(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION_0226"]
        collection = db[collection]

       # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("player_id", 1)],
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
            username = (
                row.get("username")
            )
            name = (
                row.get("name")
                or ""
            )
            completed_date = (
                row.get("completed_date")
            )
            mobileno = (
                row.get("mobileno")
                or ""
            )
            player_id = (
                row.get("player_id")
            )

            if not username or not completed_date:
                continue

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            try:
                completed_date_str = str(completed_date).replace("Z", "+00:00")
                dt = datetime.fromisoformat(completed_date_str)
            except ValueError:
                try:
                    dt = datetime.strptime(str(completed_date), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            completed_date_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "username": str(username),
                "name": name,
                "completed_date": completed_date_fmt,
                "mobileno": mobileno,
                "player_id": str(player_id) if player_id is not None else "",
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

        return cleaned_docs, inserted, skipped

    # Update Data to Google Sheet from MongoDB (FTD/STD)
    @classmethod
    def upload_to_google_sheet_FTD_STD(cls, collection, gs_id, gs_tab, rows=None, extra_mongo_collections=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = cls._build_a1_range(gs_tab, "A", 3, "E")

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for Member Info."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get("username", "")),
                        str(r.get("name", "")),
                        str(r.get("completed_date", "")),
                        str(r.get("mobileno", "")),
                        str(
                            r.get("player_id", "")
                            or r.get("member_id", "")
                        ),
                    ])
                else:
                    sanitized.append(["", "", "", "", ""])
            return sanitized


        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION_0226"]

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            collection_ref = db[collection]
            documents = list(collection_ref.find({}, {"_id": 0}).sort("completed_date", 1))
            rows = documents
        
        if extra_mongo_collections:
            for extra_col in extra_mongo_collections:
                extra_ref = db[extra_col]
                extra_docs = list(extra_ref.find({}, {"_id": 0}).sort("completed_date", 1))
                rows.extend(extra_docs)

        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        # Sort locally to avoid sortRange timeouts on large sheets.
        rows.sort(key=lambda r: r[2] if len(r) > 2 else "")

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

    # =-=-=--=-=-=-=-=-= SSBO =-=-=-=-=-=-=-=-=-=-=-=-=
    # MongoDB Database (Member Info)
    def mongodbAPI_ssbo_FTD_STD(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION_0226"]
        collection = db[collection]


       # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("username", 1), ("first_name", 1), ("register_info_date", 1), ("mobileno", 1)],
            name="ssbo_member_unique",
            unique=True,
            partialFilterExpression={
                "username": {"$type": "string"},
                "first_name": {"$type": "string"},
                "register_info_date": {"$type": "string"},
                "mobileno": {"$type": "string"},
            },
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
            username = row.get("username") or row.get("login")
            first_name = row.get("first_name") or row.get("name") or row.get("memberName") or ""
            register_info_date = row.get("register_info_date") or row.get("registerDate") or row.get("firstDepositDate")
            mobileno = row.get("mobileno") or row.get("phone") or ""
            member_id = row.get("member_id") or row.get("id") or row.get("player_id") or ""

            if not username or not register_info_date:
                continue

            username = str(username)
            first_name = str(first_name)
            member_id = str(member_id)
            mobileno = str(mobileno)

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

        return cleaned_docs, inserted, skipped

    # Update Data to Google Sheet from MongoDB (MemberInfo)
    @classmethod
    def upload_to_google_sheet_ssbo_FTD_STD(cls, collection, gs_id, gs_tab, rows=None, extra_mongo_collections=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = cls._build_a1_range(gs_tab, "A", 3, "E")

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for FTD/STD."""
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
                        or r.get("firstDepositDate")
                        or r.get("completed_date")
                        or r.get("completed_at")
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
                        or r.get("player_id")
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
        
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION_0226"]

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            collection_ref = db[collection]
            documents = list(
                collection_ref.find({}, {"_id": 0}).sort("register_info_date", 1)
            )
            rows = documents
        if extra_mongo_collections:
            for extra_col in extra_mongo_collections:
                extra_ref = db[extra_col]
                extra_docs = list(
                    extra_ref.find({}, {"_id": 0}).sort("register_info_date", 1)
                )
                rows.extend(extra_docs)
        
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        # Sort locally to avoid sortRange timeouts on large sheets.
        rows.sort(key=lambda r: r[2] if len(r) > 2 else "")

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
    
    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- GET COOKIES =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

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
        try:
            login_data = response.json()
        except Exception:
            login_data = {}
        print(login_data)

        # to get Authentication Cookies
        user_cookie = session.cookies.get("user")

        # Save cookies to "get_cookies" path
        file_path = cookies_path

        admin_id = None
        merchant_id = None
        if isinstance(login_data, dict):
            admin_info = login_data.get("data", {}).get("admin")
            if isinstance(admin_info, dict):
                admin_id = (
                    admin_info.get("id")
                    or admin_info.get("admin_id")
                    or admin_info.get("adminId")
                )
                merchant_id = (
                    admin_info.get("merchant_id")
                    or admin_info.get("merchantId")
                    or admin_info.get("m")
                )

        # data format
        data = {"user_cookie": user_cookie}
        if admin_id is not None:
            data["admin_id"] = admin_id
        if merchant_id is not None:
            data["merchant_id"] = merchant_id

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
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS FTD/STD =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # IBS FTD/STD Report
    @classmethod
    def ftd_stdReport(cls, bo_link, bo_name, currency, gmt_time, collection, g_sheet_ID, g_sheet_tab, upload_to_sheet=True):
        
        session = create_session()

        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{collection}] {Fore.YELLOW} FTD/STD Data ... {Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get today date and time
        today = datetime.now()

        # Yesterday & Day before yesterday
        yesterday = today - timedelta(days=1)

        # Format to string YYYY-MM-DD
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/home/thomas/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        user_cookie = cookie_data.get("user_cookie", "")

        def _to_int(value):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        admin_id = _to_int(cookie_data.get("admin_id"))
        merchant_id = _to_int(cookie_data.get("merchant_id"))

        base_url = f"https://v3-bo.{bo_link}"
        url = f"{base_url}/api/be/report/get-first-time-deposit-report"

        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': gmt_time,
        'lang': 'en-US',
        'loggedin': 'true',
        'origin': base_url,
        'page': '/en-us/report/finance-report/report-first-time-deposit',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'{base_url}/en-us/report/finance-report/report-first-time-deposit',
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

        page = 1
        total_inserted = 0
        total_skipped = 0
        last_page_with_data = 0
        finish_reason = None

        while True:
            payload = {
                "paginate": 100,
                "page": page,
                "currency": [currency],
                "timeZone": gmt_time,
                "ftd": 1,
                "start_date": yesterday_str,
                "end_date": yesterday_str,
                # "register_from": "2025-11-01",
                # "register_to": today,
                "merchant_id": merchant_id if merchant_id is not None else 1,
                "admin_id": admin_id if admin_id is not None else 152,
                "aid": admin_id if admin_id is not None else 152
            }

            # Post Response 
            response = session.post(url, headers=headers, json=payload, timeout=30)

            data = None
            try:
                data = response.json()
            except Exception:
                data = None

            # Check if return unauthorized (401/403)
            if response.status_code in (401, 403) or (
                isinstance(data, dict) and data.get("statusCode") in (401, 403)
            ):
                print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

                # Get Cookies
                cls._get_cookies(
                    bo_link,
                    cls.accounts[bo_name]["merchant_code"],
                    cls.accounts[bo_name]["acc_ID"],
                    cls.accounts[bo_name]["acc_PASS"],
                    f"/home/thomas/get_cookies/{bo_link}.json"
                )
                return cls.ftd_stdReport(bo_link, bo_name, currency, gmt_time, collection, g_sheet_ID, g_sheet_tab, upload_to_sheet=upload_to_sheet)

            if data is None:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                break

            rows = []
            pagination_info = None
            data_block = data.get("data", {})
            currency_candidates = []
            if isinstance(currency, str):
                base = str(currency)
                currency_candidates = [base, base.upper(), base.lower()]
            elif isinstance(currency, (list, tuple, set)):
                for cur in currency:
                    base = str(cur)
                    currency_candidates.extend([base, base.upper(), base.lower()])

            if isinstance(data_block, dict):
                currency_payload = None
                for candidate in currency_candidates:
                    candidate_value = data_block.get(candidate)
                    if isinstance(candidate_value, dict):
                        currency_payload = candidate_value
                        break
                if currency_payload is None:
                    for payload_info in data_block.values():
                        if isinstance(payload_info, dict):
                            currency_payload = payload_info
                            break
                if isinstance(currency_payload, dict):
                    rows = currency_payload.get("data", [])
                    pagination_info = currency_payload.get("pagination")
                    if isinstance(rows, dict):
                        rows = rows.get("data", [])
                elif isinstance(currency_payload, list):
                    rows = currency_payload
            elif isinstance(data_block, list):
                rows = data_block
            if not isinstance(rows, list):
                rows = []

            print(f"\nPage {page} → {len(rows)} rows")

            if not rows:
                finish_reason = "no_data" if page == 1 else "completed"
                if page > 1:
                    last_page_with_data = page - 1
                break

            _, inserted, skipped = cls.mongodbAPI_FTD_STD(rows, collection)
            total_inserted += inserted
            total_skipped += skipped
            last_page_with_data = page

            pagination_last_page = None
            pagination_current = None
            if isinstance(pagination_info, dict):
                pagination_last_page = (
                    pagination_info.get("last_page")
                    or pagination_info.get("lastPage")
                )
                pagination_current = (
                    pagination_info.get("current_page")
                    or pagination_info.get("currentPage")
                )
            if pagination_last_page is not None:
                try:
                    pagination_last_page = int(pagination_last_page)
                except (TypeError, ValueError):
                    pagination_last_page = None
            if pagination_current is not None:
                try:
                    pagination_current = int(pagination_current)
                except (TypeError, ValueError):
                    pagination_current = page
            else:
                pagination_current = page

            if (
                pagination_last_page is not None
                and pagination_current >= pagination_last_page
            ):
                finish_reason = "completed"
                break

            if pagination_last_page is None and len(rows) < payload["paginate"]:
                finish_reason = "completed"
                break

            page += 1

        if finish_reason is None:
            finish_reason = "completed"

        print(f"MongoDB Summary → Inserted: {total_inserted}, Skipped: {total_skipped}")
        if finish_reason == "no_data":
            print("⚠️ No transaction IDs found — break")
        else:
            print(f"Finished. Last page = {last_page_with_data}")

        if upload_to_sheet:
            cls.upload_to_google_sheet_FTD_STD(collection, g_sheet_ID, g_sheet_tab)
        
    # SSBO FTD/STD Report
    @classmethod
    def ssbo_ftd_stdReport(cls, merchants, currency, collection, g_sheet_ID, g_sheet_tab, extra_mongo_collections=None):
        
        session = create_session()
        
        # Print Title
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{collection}] {Fore.YELLOW} FTD/STD Data ... {Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get today date
        today = datetime.now().strftime("%Y-%m-%d")

        # Get this/last year, last month, last month of day
        today2 = date.today()
        last_month_date = today2.replace(day=1) - timedelta(days=1)

        # Normalize currency input to list form for API payloads
        if isinstance(currency, (list, tuple, set)):
            currency = list(currency)
        else:
            currency = [currency]

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

        page = 0
        max_pages = 1000
        total_inserted = 0
        total_skipped = 0
        last_page_with_data = -1
        finish_reason = None

        while page < max_pages:

            url = "https://aw8.premium-bo.com/cashmarket/api/sbo/report-management/get-first-time-deposit-report"

            params = {
                "page": page,
                "size": 200,
                "sort": "eventTime,DESC",
                "tenantId": 35,
                "merchants": "uea",
                "merchantCode": "uea",
                "currencies": currency,
                "start": f"{last_month_date}T17:00:00.000Z",
                "startTime": f"{last_month_date}T17:00:00.000Z",
                "startCreatedTime": f"{last_month_date}T17:00:00.000Z",
                "end": f"{today}T16:59:59.000Z",
                "endTime": f"{today}T16:59:59.000Z",
                "endCreatedTime": f"{today}T16:59:59.000Z",
                "timezone": 8,
                "cacheBuster": int(time.time() * 1000)
            }

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': f'Bearer {user_cookie}',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'referer': 'https://aw8.premium-bo.com/',
                'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            }

            # Post Response 
            response = session.get(url, headers=headers, params=params, timeout=30)
            # print("Status:", response.status_code)
            # print("Content-Type:", response.headers.get("Content-Type"))
            # print("Text preview:", response.text[:300])

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                
                # Error 401 or 403, get cookies
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                cls._ssbo_get_cookies(cls.accounts["super_swan"]["acc_ID"], cls.accounts["super_swan"]["acc_PASS"])

                # Retrying requests...
                print("⚠️ Cookies + bearer token refreshed ... Retrying request...\n")
                return cls.ssbo_ftd_stdReport(merchants, currency, collection, g_sheet_ID, g_sheet_tab, extra_mongo_collections=extra_mongo_collections)
            
            # Try to parse JSON safely
            try:
                data = response.json()
            except ValueError:
                print("⚠️ Response is not valid JSON.")
                print("Status code:", response.status_code)
                print("Text preview:", response.text[:500])
                finish_reason = "invalid_json"
                break
            
            # Normalize data: SSBO sometimes returns a LIST, sometimes a DICT
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                payload_block = data.get("data")
                if isinstance(payload_block, list):
                    rows = payload_block
                elif isinstance(payload_block, dict):
                    # some responses wrap rows inside "content" or "records"
                    if "data" in payload_block and isinstance(payload_block["data"], list):
                        rows = payload_block["data"]
                    elif "records" in payload_block and isinstance(payload_block["records"], list):
                        rows = payload_block["records"]
                    else:
                        extracted = None
                        for value in payload_block.values():
                            if isinstance(value, dict) and isinstance(value.get("data"), list):
                                extracted = value.get("data", [])
                                break
                        rows = extracted or []
                else:
                    rows = []
            else:
                print("⚠️ Unexpected response type:", type(data))
                finish_reason = "invalid_json"
                break

            if not isinstance(rows, list):
                rows = []

            print(f"\nPage {page} → {len(rows)} rows")
            print(f"MongoDB Summary → Inserted: {total_inserted}, Skipped: {total_skipped}")

            # STOP when no data
            if not rows:
                finish_reason = "no_data" if page == 0 else "completed"
                if page > 0:
                    last_page_with_data = page - 1
                break

            _, inserted, skipped = cls.mongodbAPI_ssbo_FTD_STD(rows, collection)
            total_inserted += inserted
            total_skipped += skipped
            last_page_with_data = page

            page += 1

        if finish_reason is None and page >= max_pages:
            finish_reason = "safety_stop"

        if finish_reason == "no_data":
            print("⚠️ No transaction IDs found — break")
        elif finish_reason == "safety_stop":
            print(f"⚠️ Stopped after {page} pages to avoid infinite loop.")
        elif finish_reason == "invalid_json":
            print("⚠️ Unable to continue due to invalid JSON response.")
        else:
            print(f"Finished. Last page = {last_page_with_data}")
        
        # upload to google sheet
        cls.upload_to_google_sheet_ssbo_FTD_STD(collection, g_sheet_ID, g_sheet_tab, extra_mongo_collections=extra_mongo_collections)

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

# Stop at 1st day of next month, 00:00 GMT+8
STOP_DATETIME = gmt8.localize(datetime(stop_year, stop_month, 1, 0, 0, 0))
print(f"Bot will stop automatically at: {STOP_DATETIME}")

# ================== AUTO STOP DATE CONFIG ==================

last_run_date = None

while True:
    try:

        # Auto-stop check
        current_time = datetime.now(gmt8)
        if current_time >= STOP_DATETIME:
            logger.info(f"Reached stop date ({STOP_DATETIME}). Bot is stopping.")
            break
        
        # Check current time to use correct google sheet tab to upload data
        now = datetime.now()
        print(now.time())
        
        # Run code when is 7am
        if last_run_date != now.date() and now.hour == 7 and 0 <= now.minute < 10:
        
            # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=
            # =========================== GT FTD/STD ==================================
            # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=

            # NAREM 
            safe_call(Fetch.ftd_stdReport, "gcwin99bo.com", "gc99", "THB", "+07:00", "GT_FTD_STD", "1X3_U_OaqDp7IYEVITUh-PaLJtq3VbV6MzI58wRzXcLs", "2ND 3RD", description="IBS GT FTD/STD")

            # ==========================================================================
            # =-=-=-=-==-=-=-=-=-=-=-=-=-= N855T FTD/STD =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-
            # ==========================================================================

            # ELSA
            safe_call(Fetch.ftd_stdReport, "f5x3n8v.com", "n855", "THB", "+07:00", "N855T_FTD_STD", "1PhrRokHUYNLKvI8QWINUD5DtbhtJNkkToDZB0k4frjQ", "2ND 3RD", description="N855T FTD/STD")

            # ==========================================================================
            # =-=-=-=-==-=-=-=-=-=-=-=-=-= N789T FTD/STD =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-
            # ==========================================================================

            # SOFIA
            safe_call(Fetch.ftd_stdReport, "q2n5w3z.com", "n789", "THB", "+07:00", "N789T_FTD_STD", "15jh_5Aulx-PFwA73UhUg32iLon_kiLWw3reik-0t0uU", "2ND 3RD", description="N789T FTD/STD")

            # ==========================================================================
            # =-=-=-=-==-=-=-=-=-=-=-=-=-= N1T FTD/STD =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-
            # ==========================================================================

            # Riley
            safe_call(Fetch.ftd_stdReport, "m8b4x1z6.com", "n191", "THB", "+07:00", "N1T_FTD_STD", "1MN69M3PuBOHBDbLKk9Ycx5Z143PQeITDOE4xoLPHuMI", "2ND 3RD", description="N1T FTD/STD")

            # Prevent rerun today
            last_run_date = now.date()

        # Delay 30 seconds
        time.sleep(30)
        
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
