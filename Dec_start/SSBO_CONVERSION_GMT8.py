import os
import sys
import time
import json
import atexit
import requests
import subprocess
from dotenv import load_dotenv
from pymongo import MongoClient
from colorama import Fore, Style
from urllib.parse import urlencode
from datetime import date, timedelta
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta, timezone
from google_auth_oauthlib.flow import InstalledAppFlow
from Dec_start.runtime import logger, safe_call, MONGODB_URI


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
    TOKEN_PATH = "./api/google2/token.json"
    CREDS_PATH = "./api/google2/credentials.json"

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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO FTD/STD =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # MongoDB Database (Member Info)
    def mongodbAPI_ssbo_FTD_STD(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["UEA8"]
        collection = db[collection]

        # Ensure deposit-specific unique index does not block member inserts
        try:
            collection.drop_index("player_id_1_amount_1_completed_at_1")
        except Exception:
            pass
        # Remove any legacy member indexes before recreating ours
        for idx_name in (
            "login_1_name_1_registerDate_1",
            "ssbo_member_unique",
            "username_1_first_name_1_firstDepositDate_1",
        ):
            try:
                collection.drop_index(idx_name)
            except Exception:
                pass

       # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("username", 1), ("first_name", 1), ("register_info_date", 1)],
            name="ssbo_member_unique",
            unique=True,
            partialFilterExpression={
                "username": {"$type": "string"},
                "first_name": {"$type": "string"},
                "register_info_date": {"$type": "string"},
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
        db = client["UEA8"]

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
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        if row_count > 1:
            cls._sort_range_by_column(
                service,
                SPREADSHEET_ID,
                gs_tab,
                "A",
                "E",
                3,
                3 + row_count - 1,
                sort_column_letter="C",
                descending=False
            )

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n")

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================
    
    # MongoDB Database (Member Info)
    def mongodbAPI_ssbo_MI(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["UEA8"]
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
        RANGE_NAME = cls._build_a1_range(gs_tab, "A", 3, "E")

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

        client = MongoClient(MONGODB_URI)
        db = client["UEA8"]

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
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        if row_count > 1:
            cls._sort_range_by_column(
                service,
                SPREADSHEET_ID,
                gs_tab,
                "A",
                "E",
                3,
                3 + row_count - 1,
                sort_column_letter="C",
                descending=False
            )

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n")

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= SSBO DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_ssbo_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["UEA8"]
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
                    # Convert Z → UTC datetime (default to UTC if no tzinfo)
                    dt = datetime.fromisoformat(str(lastModifiedDate).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)

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
            db = client["UEA8"]
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
        print("Uploaded MongoDB data to Google Sheet.\n")

# Fetch Data
class Fetch(BO_Account, mongodb_2_gs):
    
    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO GET COOKIES =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # Get SSBO Cookies incase Cookies expired
    @classmethod
    def _ssbo_get_cookies(cls, acc_id, acc_pass):

        session = requests.Session()

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
        file_path = "/Users/nera_thomas/Desktop/Telemarketing/get_cookies/demo_cookies.json"

        # data format you want
        data = {"jwt_token": jwt_token}

        # write to json file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print("Saved user cookie to:", file_path)

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO FTD/STD =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # SSBO FTD/STD Report
    @classmethod
    def ssbo_ftd_stdReport(cls, merchants, currency, collection, g_sheet_ID, g_sheet_tab, extra_mongo_collections=None):
        
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
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/demo_cookies.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"jwt_token": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        jwt_token = cookie_data.get("jwt_token", "")

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
                "merchants": merchants,
                "merchantCode": merchants,
                "currencies": currency,
                "start": f"{last_month_date}T17:00:00.000Z",
                "startTime": f"{last_month_date}17:00:00.000Z",
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
                'authorization': f'Bearer {jwt_token}',
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
                # 'Cookie': f"_ga={jwt_token}" if jwt_token"" else 
            }

            response = requests.get(url, headers=headers, params=params)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                Automation.chrome_CDP()
                cls._ssbo_get_cookies()
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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # SSBO Member Info (merchants name = aw8, ip9, uea)
    @classmethod
    def ssbo_member_info(cls, merchants, currency, collection, gs_id, gs_tab, extra_mongo_collections=None):
        
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{collection}] {Fore.YELLOW} MEMBER INFO Data ... {Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get today and yesterday date
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Normalize currency input to list form for API payloads
        if isinstance(currency, (list, tuple, set)):
            currency_list = list(currency)
        else:
            currency_list = [currency]

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/demo_cookies.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"jwt_token": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        jwt_token = cookie_data.get("jwt_token", "")

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
            'authorization': f'Bearer {jwt_token}',
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
            # 'Cookie': f'_ga={jwt_token}'
            }

            # Get Post Response
            response = requests.request("POST", url, headers=headers, data=payload)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                Automation.chrome_CDP()
                cls._ssbo_get_cookies()
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
                'authorization': f'Bearer {jwt_token}',
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
                # 'Cookie': f'_ga={jwt_token}',
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
        
        # upload to google sheet
        cls.upload_to_google_sheet_ssbo_MI(collection, gs_id, gs_tab, extra_mongo_collections=extra_mongo_collections)

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO DEPOSIT LIST =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # SSBO Deposit List (Player ID)
    @classmethod
    def ssbo_deposit_list_PID(cls, merchants, currency, collection, gs_id, gs_tab, start_column, end_column, upload_to_sheet=True):
        
        # Print title
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{collection}] {Fore.YELLOW} DEPOSIT LIST Data ... (PLAYER ID) {Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get today and yesterday date
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/superswan.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"jwt_token": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        jwt_token = cookie_data.get("jwt_token", "")

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
            'authorization': f'Bearer {jwt_token}',
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
            # 'Cookie': f'_ga={jwt_token}',
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
                'authorization': f'Bearer {jwt_token}',
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
                # 'Cookie': f'_ga={jwt_token}',
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

while True:
    try:


        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        # ============================================================== SSBO UM US NEW REGISTER & DEPOSIT LIST PID 【 KAY 】 =============================================================================
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        
        # IBS UM MY
        safe_call(Fetch.member_info, "29018465.asia", "uea8", "MYR", "+08:00", "UM_MI", "1FcuBvYpRyOQOFt1ZafZIXn_wpOYgS1MDbkwnLCfDqWI", "UM", upload_to_sheet=False, description="IBS UM MY MEMBER INFO")
        # SSBO UM MY
        safe_call(Fetch.ssbo_member_info, "uea", "MYR", "SSBO_UM_MI", "1FcuBvYpRyOQOFt1ZafZIXn_wpOYgS1MDbkwnLCfDqWI", "UM", extra_mongo_collections=["UM_MI"], description="SSBO UM MY MEMBER INFO")
        # SSBO US SG
        safe_call(Fetch.ssbo_member_info, "uea", "SGD", "SSBO_US_MI", "1FcuBvYpRyOQOFt1ZafZIXn_wpOYgS1MDbkwnLCfDqWI", "US", description="SSBO US SG MEMBER INFO")

        # SSBO UM MY
        safe_call(Fetch.ssbo_deposit_list_PID, "uea", ["MYR"], "SSBO_UM_DL", "1FcuBvYpRyOQOFt1ZafZIXn_wpOYgS1MDbkwnLCfDqWI", "DEPOSIT LIST", "A", "C", upload_to_sheet=False, description="SSBO UM MY DL PID")
        # IBS UM MY 
        safe_call(Fetch.deposit_list_PID, "29018465.asia", "uea8", "MYR", "+08:00", "UM_DL", "1FcuBvYpRyOQOFt1ZafZIXn_wpOYgS1MDbkwnLCfDqWI", "DEPOSIT LIST", "A", "C", extra_mongo_collections=["SSBO_UM_DL"], description="IBS UM MY DL PID")
        # SSBO US SG
        safe_call(Fetch.ssbo_deposit_list_PID, "uea", ["SGD"], "SSBO_US_DL", "1FcuBvYpRyOQOFt1ZafZIXn_wpOYgS1MDbkwnLCfDqWI", "DEPOSIT LIST", "E", "G", description="SSBO US SG DL PID")


        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        # ============================================================== SSBO UM US FTD/STD REPORT & DEPOSIT LIST PID【 WEINEI 】 ====================================================================================
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        # IBS UM MY
        safe_call(Fetch.ftd_stdReport, "29018465.asia", "uea8", "MYR", "+08:00", "UM_FTD_STD", "1uh3qUqLmVQnr2mBYL2bg8wIReK2mzCS5zpexNYda8cI", "UM", upload_to_sheet=False, description="IBS UM MY FTD/STD")
        # SSBO UM MY
        safe_call(Fetch.ssbo_ftd_stdReport, "uea", "MYR", "SSBO_UM_FTD_STD_2", "1uh3qUqLmVQnr2mBYL2bg8wIReK2mzCS5zpexNYda8cI", "UM", extra_mongo_collections=["UM_FTD_STD"], description="SSBO UM MY FTD/STD")
        # SSBO US SG
        safe_call(Fetch.ssbo_ftd_stdReport, "uea", "SGD", "SSBO_US_FTD_STD", "1uh3qUqLmVQnr2mBYL2bg8wIReK2mzCS5zpexNYda8cI", "US", description="SSBO US SG FTD/STD")

        # SSBO UM MY
        safe_call(Fetch.ssbo_deposit_list_PID, "uea", ["MYR"], "SSBO_UM_DL", "1uh3qUqLmVQnr2mBYL2bg8wIReK2mzCS5zpexNYda8cI", "DEPOSIT LIST", "A", "C", upload_to_sheet=False, description="SSBO UM MY DL PID")
        # IBS UM MY 
        safe_call(Fetch.deposit_list_PID, "29018465.asia", "uea8", "MYR", "+08:00", "UM_DL", "1uh3qUqLmVQnr2mBYL2bg8wIReK2mzCS5zpexNYda8cI", "DEPOSIT LIST", "A", "C", extra_mongo_collections=["SSBO_UM_DL"], description="IBS UM MY DL PID")
        # SSBO US SG
        safe_call(Fetch.ssbo_deposit_list_PID, "uea", ["SGD"], "SSBO_US_DL", "1uh3qUqLmVQnr2mBYL2bg8wIReK2mzCS5zpexNYda8cI", "DEPOSIT LIST", "E", "G", description="SSBO US SG DL PID")



        # delay 10 minutes
        time.sleep(600)

    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
