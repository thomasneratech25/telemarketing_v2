import os
import re
import sys
import time
import pytz
import json
import urllib3
import requests
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from colorama import Fore, Style
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
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
        "dis88": {
            "merchant_code": os.getenv("MERCHANT_CODE_DIS88"),
            "acc_ID": os.getenv("ACC_ID_DIS88"),
            "acc_PASS": os.getenv("ACC_PASS_DIS88")
        },
    }

# MongoDB <-> Google Sheet
class mongodb_2_gs:

    # Handles Google Sheets operations for the Telemarketing project.
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    TOKEN_PATH = "./api/google3/token.json"
    CREDS_PATH = "./api/google3/credentials.json"

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
        db = client["RETENTION"]
        combined = []
        for col_name in collection_names:
            col = db[col_name]
            docs = list(col.find({}, {"_id": 0}))
            combined.extend(cls._normalize_pid_rows(docs))
        return combined

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS AMR =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # MongoDB Database (All Member Report)
    def mongodbAPI_AMR(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["RETENTION"]
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


        client = MongoClient(MONGODB_URI)
        db = client["RETENTION"]

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

    # =========================== ALL MEMBER REPORT ===========================

    # BO All Member Report
    @classmethod
    def allmemberReport(cls, team, bo_link, bo_name, currency, gmt_time, collection, g_sheet_ID, g_sheet_tab):
        
        session = create_session()
        
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
        cookie_file = f"/home/thomas/get_cookies/{bo_link}.json"

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

            # Post Response 
            response = session.post(url, headers=headers, json=payload, timeout=30)

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

                cls._get_cookies(
                    bo_link,
                    cls.accounts[f"{bo_name}"]["merchant_code"],
                    cls.accounts[f"{bo_name}"]["acc_ID"],
                    cls.accounts[f"{bo_name}"]["acc_PASS"],
                    f"/home/thomas/get_cookies/{bo_link}.json"
                )
                # Retry request...
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

while True:
    try:
        
        # Auto-stop check
        current_time = datetime.now(gmt8)
        if current_time >= STOP_DATETIME:
            logger.info(f"Reached stop date ({STOP_DATETIME}). Bot is stopping.")
            break
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS GT ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # GT (ALL MEMBER INFO) KOI
        safe_call(Fetch.allmemberReport, "IBS GT AMR", "gcwin99bo.com", "gc99", "THB", "+08:00", "GT_AMR", "1fOWjL0KQa5Q7x2ApvFz2C3Mg-n1qM9yjeqrVcXn3hME", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS N855T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # N855T (ALL MEMBER INFO) JIRAPORN
        safe_call(Fetch.allmemberReport, "IBS N855T AMR", "f5x3n8v.com", "n855", "THB", "+08:00", "N855T_AMR", "1tJQJuy2-VsZ7u051fOAWjzsjFgP-9H7s25Z4NsVG6tA", "TM - All Member Report")
    
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS N1T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # N1T (ALL MEMBER INFO) SUPATTA
        safe_call(Fetch.allmemberReport, "IBS N1T AMR", "m8b4x1z6.com", "n191", "THB", "+08:00", "N1T_AMR", "19yThSHpDPkXsjI_q-1X1CKhLS_p5BIS9Hk8Qn9fTGfE", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS N789T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # N789T (ALL MEMBER INFO) SUPATTA
        safe_call(Fetch.allmemberReport, "IBS N789T AMR", "q2n5w3z.com", "n789", "THB", "+08:00", "N789T_AMR", "1Aubkpmr1ViquwVgIoA87T--mVjd5bl4zCJLPo03Hxgk", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S8T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # S8T (ALL MEMBER INFO) MIN
        safe_call(Fetch.allmemberReport, "IBS S8T AMR", "siam855bo.com", "s855", "THB", "+08:00", "S8T_AMR", "1qdu-hI9yitWLKuFUVVAK7LLVfW7DbPelGDmk-VuU7s4", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S6T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # S6T (ALL MEMBER INFO) PU
        safe_call(Fetch.allmemberReport, "IBS S6T AMR", "siam66bo.com", "s66", "THB", "+08:00", "S6T_AMR", "18m1h7K_AUy-dm0dhad3J4JiYEgZuzoxEbvc4BQoFcCE", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS 2WT ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # 2WT (ALL MEMBER INFO) SATANG
        safe_call(Fetch.allmemberReport, "IBS 2WT AMR", "w8c4n9be.com", "22w", "THB", "+08:00", "2WT_AMR", "1aShl2QYJbwy1y97nUnKPzjkjlK-JEOHl8KrhzXtbti0", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS 2FT ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # 2FT (ALL MEMBER INFO) NING
        safe_call(Fetch.allmemberReport, "IBS 2FT AMR", "22funbo.com", "22f", "THB", "+08:00", "2FT_AMR", "1YmcYjn2RaQe7b9x7UZof7V9xHwQvSR7qkSLlQPz3hSA", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S2T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # S2T (ALL MEMBER INFO) BOOM
        safe_call(Fetch.allmemberReport, "IBS S2T AMR", "m3v5r6cx.com", "s212", "THB", "+08:00", "S2T_AMR", "1lp2eBaXG_-CfZ52OXUxtLSXoQcPeWJ6IJJ676F95K2c", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS M1T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # M1T (ALL MEMBER INFO) KAEO
        safe_call(Fetch.allmemberReport, "IBS M1T AMR", "zupra7x.com", "mf191", "THB", "+08:00", "M1T_AMR", "1w4Yto0PDjsyYnPsLZnSm72l6bPAiwXuEJasfcld09mI", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J8T (ALL MEMBER INFO) TIP
        safe_call(Fetch.allmemberReport, "IBS J8T AMR", "jw8bo.com", "jw8", "THB", "+08:00", "J8T_AMR", "1dYSGFLO9emW-BGjfxcGVC28esDld1_BYD1npK-NkUpI", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS MST ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # MST (ALL MEMBER INFO) POP
        safe_call(Fetch.allmemberReport, "IBS MST AMR", "bo-msslot.com", "slot", "THB", "+08:00", "MST_AMR", "1k70B4OniQ8_C1MzZNyGoWt0dBGJrC83n0XW0MQEEHCE", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS I8T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # I8T (ALL MEMBER INFO) KUNG
        safe_call(Fetch.allmemberReport, "IBS I8T AMR", "i828.asia", "828", "THB", "+08:00", "I8T_AMR", "12xJ4diNv1rJItUwH9XroDDesqPcffZcJL4Pw2d7rY68", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS G855T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # G855T (ALL MEMBER INFO) VIEW
        safe_call(Fetch.allmemberReport, "IBS G855T AMR", "god855.asia", "g855", "THB", "+08:00", "G855T_AMR", "1Urk-Zo1xVlzeiCJF0HB0F2-n1_ptto5sTzdy4U9g3J0", "TM - All Member Report")

        # # =============================================================================================================================
        # # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S345T ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # # =============================================================================================================================

        # S345T (ALL MEMBER INFO) BUA
        safe_call(Fetch.allmemberReport, "IBS S345T AMR", "57249022.asia", "s345", "THB", "+08:00", "S345T_AMR", "1npR2LLvMZ4hzsdH5bjdI1zabnJy75fq9K4-gWT_xbCo", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS A8N ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # A8N (ALL MEMBER INFO) ANNA
        safe_call(Fetch.allmemberReport, "IBS A8N", "aw8bo.com", "aw8", "NPR", "+08:00", "A8N_AMR", "1tM_8cObTssJKrWY8k1B_w5aWI-IMXMcVVggXDFDXTZ0", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J1B ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J1B (ALL MEMBER INFO) RABBY
        safe_call(Fetch.allmemberReport, "IBS J1B", "batsman88.com", "jaya11", "BDT", "+08:00", "J1B_AMR", "1szQDOMtgWeUT7AWF668PQadx2n0opJUVn9fJFm03N64", "TM - All Member Report")
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS I8N ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # I8N (ALL MEMBER INFO) LOKENDRA
        safe_call(Fetch.allmemberReport, "IBS I8N", "6668889.site", "i88", "NPR", "+08:00", "I8N_AMR", "1Kn6CLHwPX9ugds9Knx0vvup35ClkB1WttZxft96aaIQ", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J1N ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J1N (ALL MEMBER INFO) LAXMI
        safe_call(Fetch.allmemberReport, "IBS J1N", "batsman88.com", "jaya11", "NPR", "+08:00", "J1N_AMR", "1ikAy2DIQzkTEOhibQkNKSPKRwfg0G2J1dG453IDnjBU", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8N ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J8N (ALL MEMBER INFO) SIJAPATI
        safe_call(Fetch.allmemberReport, "IBS J8N", "jw8bo.com", "jw8", "NPR", "+08:00", "J8N_AMR", "1JbqUaaKa1TXnYryZL6nalV1R6Oy1-o8pIrePm9k7NV4", "TM - All Member Report")
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8B ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J8B (ALL MEMBER INFO) ALI
        safe_call(Fetch.allmemberReport, "IBS J8B", "jw8bo.com", "jw8", "BDT", "+08:00", "J8B_AMR", "16y9dNrRShQIjYwBS5T9t-dPo2h8muXxF7qa-wdxmiAg", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS K8N ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # K8N (ALL MEMBER INFO) HIMANI
        safe_call(Fetch.allmemberReport, "IBS K8N", "6668889.store", "k88", "NPR", "+08:00", "K8N_AMR", "14dPUrque2e_1A5TQMnnDgfCvgdQe1ozADt9wpO5wWrM", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS D8M ALL MEMBER REPORT & DEPOSIT LIST -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # D8M (ALL MEMBER INFO) RY
        safe_call(Fetch.allmemberReport, "IBS D8M", "dis88bo.com", "dis88", "MYR", "+08:00", "D8M_AMR", "1lbzsoka2arD6LHDI3krA3DmwsDHcaB7uaBlirfUEugY", "TM - All Member Report")

    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:   
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
