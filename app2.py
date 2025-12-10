import hashlib
import requests
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from collections import defaultdict
from dateutil.parser import parse
from datetime import datetime

# =========================
# HELPERS
# =========================
def parse_sheet_date(value):
    """Parse any date format from sheet including dd-mm, mm/dd, text formats."""
    if not value or not str(value).strip():
        return None
    try:
        return parse(str(value).strip(), dayfirst=True)
    except:
        return None

def to_crm_date(dt):
    """Convert datetime to CRM format YYYY-MM-DD."""
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d")


# =========================
# CRM CLIENT
# =========================
class CRMClient:
    def __init__(self, base_url, username, access_key):
        self.base_url = base_url.rstrip("/") + "/webservice.php"
        self.username = username
        self.access_key = access_key
        self.session_name = None
        self.session_expiry = 0

    def _get_challenge(self):
        url = f"{self.base_url}?operation=getchallenge&username={self.username}"
        response = requests.get(url).json()

        if not response.get("success"):
            raise Exception("Failed to get challenge token")

        return response["result"]

    def _login(self):
        ch = self._get_challenge()
        token = ch["token"]

        # FIX 1 → ignore CRM timestamp completely
        self.session_expiry = time.time() + 3600   # session valid 1 hour

        key_hash = hashlib.md5((token + self.access_key).encode()).hexdigest()
        data = {
            "operation": "login",
            "username": self.username,
            "accessKey": key_hash
        }

        response = requests.post(self.base_url, data=data).json()

        if not response.get("success"):
            raise Exception("CRM login failed: " + str(response))

        self.session_name = response["result"]["sessionName"]
        return self.session_name

    def get_session(self):
        # FIX 2 → refresh session if expired or None
        if not self.session_name or time.time() >= self.session_expiry:
            self._login()
        return self.session_name

    def create_lead(self, lead_data):
        session = self.get_session()
        data = {
            "operation": "create",
            "sessionName": session,
            "elementType": "Leads",
            "element": json.dumps(lead_data)
        }
        response = requests.post(self.base_url, data=data).json()

        # FIX 3 → retry once on invalid session
        if not response.get("success") and "invalid" in str(response).lower():
            session = self._login()
            data["sessionName"] = session
            response = requests.post(self.base_url, data=data).json()

        if not response.get("success"):
            print("🔴 CRM ERROR (create_lead):", response, flush=True)
            raise Exception(f"Failed to create lead: {response}")

        return response["result"]

    def get_lead(self, lead_id):
        session = self.get_session()
        params = {
            "operation": "retrieve",
            "sessionName": session,
            "id": lead_id
        }

        response = requests.get(self.base_url, params=params).json()

        # FIX 4 → retry retrieve if CRM invalidates session
        if not response.get("success"):
            if "invalid" in str(response).lower() or "session" in str(response).lower():
                session = self._login()
                params["sessionName"] = session
                response = requests.get(self.base_url, params=params).json()

        if not response.get("success"):
            print("🔴 CRM ERROR (get_lead):", response, flush=True)
            raise Exception(f"Failed retrieving lead {lead_id}: {response}")

        return response["result"]

    def get_all_comments(self, lead_id):
        session = self.get_session()
        query = (
            f"select commentcontent,createdtime from ModComments "
            f"where related_to='{lead_id}' ORDER BY createdtime ASC;"
        )
        params = {
            "operation": "query",
            "sessionName": session,
            "query": query
        }

        res = requests.get(self.base_url, params=params).json()

        # FIX 5 → retry on invalid session
        if not res.get("success"):
            if "invalid" in str(res).lower():
                session = self._login()
                params["sessionName"] = session
                res = requests.get(self.base_url, params=params).json()

        if not res.get("success"):
            print("🔴 CRM ERROR (get_all_comments):", res, flush=True)
            return ""

        rows = res.get("result", [])
        formatted = []

        for r in rows:
            com = (r.get("commentcontent") or "").strip()
            ts = r.get("createdtime", "").replace("T", " ")
            if com:
                formatted.append(f"{ts} : {com}")

        formatted.reverse()
        return "\n".join(formatted)



# =========================
# CONFIG
# =========================
BASE_URL = "https://b2bgrowthexpo.crm360degree.com"
USERNAME = "admin"
ACCESS_KEY = "VTuOUDEyXX6AfJH3"

SHEET_NAME = "Expo-Sales-Management"
EXHIBITOR_TAB = "exhibitors-1"
SPEAKER_TAB = "speakers-2"

CRM_ID_COL_NAME = "CRM Lead ID"
CRM_UPDATE_COL = "CRM Update"

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
crm = CRMClient(BASE_URL, USERNAME, ACCESS_KEY)

print("📄 Google Sheets authenticated", flush=True)


# =========================
# SHEET HELPERS
# =========================
def col_to_a1(col):
    letters = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def header_to_index(header):
    return {h: i + 1 for i, h in enumerate(header)}

def row_to_dict(header, row):
    return {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}

def ensure_col(ws, hmap, header, col_name):
    if col_name in hmap:
        return hmap[col_name]
    col = len(header) + 1
    ws.update(f"{col_to_a1(col)}1", col_name)
    return col


# =========================
# CENTRAL FIELD MAPPING
# =========================
SHEET_TO_CRM = {
    "First_Name": "firstname",
    "Last Name": "lastname",
    "Company Name": "company",
    "Lead Source": "leadsource",
    "Email": "email",
    "Mobile": "mobile",
    "Show": "cf_1161",
    "Next Followup": "cf_905",
    "Call Attempt": "cf_1047",
    "Linkedin Msg": "cf_1159",
    "Comments": "cf_1155",
    "Pitch Deck URL": "cf_1163",
    "Reply Status": "cf_1173",
    "Follow-Up Count": "cf_1151",
    "LINKEDIN-HEADLINE": "cf_1177",
    "LINKEDIN-REPLY": "cf_1179",
    "LINKEDIN-URL": "cf_939",
    "Stand Size": "cf_1181",
    "Amount": "cf_1183",
    "Company Linkedin Page": "cf_941",
    "Lead Date": "cf_1149",
    "Email-Count":"cf_1207",
    "WhatsApp msg count":"cf_1157",
}


# =========================
# STATIC DEFAULT CRM FIELDS
# =========================
STATIC_CRM_FIELDS = {
    "assigned_user_id": "19x77",
    "leadstatus": "New",
    "memberof": "11x232",
    "cf_1203": "EXPO-SALES-MANAGEMENT"
}


# =========================
# LOAD SHEETS
# =========================
ws_ex = client.open(SHEET_NAME).worksheet(EXHIBITOR_TAB)
ws_sp = client.open(SHEET_NAME).worksheet(SPEAKER_TAB)

ex_vals = ws_ex.get_all_values()
sp_vals = ws_sp.get_all_values()

ex_header = ex_vals[0]
sp_header = sp_vals[0]

ex_hmap = header_to_index(ex_header)
sp_hmap = header_to_index(sp_header)

ex_crm_col = ensure_col(ws_ex, ex_hmap, ex_header, CRM_ID_COL_NAME)
sp_crm_col = ensure_col(ws_sp, sp_hmap, sp_header, CRM_ID_COL_NAME)
ex_update_col = ensure_col(ws_ex, ex_hmap, ex_header, CRM_UPDATE_COL)
sp_update_col = ensure_col(ws_sp, sp_hmap, sp_header, CRM_UPDATE_COL)

# re-fetch after ensure_col to pick up new headers if any
ex_vals = ws_ex.get_all_values()
sp_vals = ws_sp.get_all_values()
ex_header = ex_vals[0]
sp_header = sp_vals[0]
ex_hmap = header_to_index(ex_header)
sp_hmap = header_to_index(sp_header)


# =========================
# CONDITIONAL PAYLOAD BUILDER
# =========================
def build_payload_from_row(row_dict, opp_type, multi, is_exhibitor):
    payload = {}
    payload.update(STATIC_CRM_FIELDS)

    # Normal fields
    for sheet_col, crm_field in SHEET_TO_CRM.items():
        val = row_dict.get(sheet_col, "")
        if isinstance(val, (int, float)):
            val = str(val)
        payload[crm_field] = val or ""
    # ⭐ FIX: CRM mandatory lastname
    
    lname = payload.get("lastname", "").strip()
    fname = payload.get("firstname", "").strip()
    if not lname:
        if fname:
            payload["lastname"] = fname
        else:
            payload["lastname"] = "Unknown"
    
    # phone = mobile
    mob = payload.get("mobile", "")
    if mob and not payload.get("phone"):
        payload["phone"] = mob

    # Conditional mapping for cf_1153
    if is_exhibitor:
        lf_raw = row_dict.get("Last Follow-Up Date", "")
        lf_dt = parse_sheet_date(lf_raw)
        if lf_dt:
            payload["cf_1153"] = to_crm_date(lf_dt)
    else:
        es_raw = row_dict.get("Email Sent-Date", "")
        es_dt = parse_sheet_date(es_raw)
        if es_dt:
            payload["cf_1153"] = to_crm_date(es_dt)

    # Next Followup (same for all)
    nf_raw = row_dict.get("Next Followup", "")
    nf_dt = parse_sheet_date(nf_raw)
    if nf_dt:
        payload["cf_905"] = to_crm_date(nf_dt)

    payload["cf_1049"] = opp_type
    payload["cf_1205"] = multi

    return payload


# =========================
# FLOW 1 – CREATE LEADS
# =========================
def flow1_create_and_sync_duplicates():
    updates = defaultdict(list)
    emap = {}

    # EXHIBITOR
    for i, row in enumerate(ex_vals[1:], start=2):
        d = row_to_dict(ex_header, row)
        email = (d.get("Email") or "").strip().lower()
        if not email:
            continue
        ex_crm_id = row[ex_crm_col - 1] if ex_crm_col - 1 < len(row) else ""
        emap.setdefault(email, {"ex": None, "sp": None})
        emap[email]["ex"] = {"row": i, "data": d, "crm": ex_crm_id.strip()}

    # SPEAKER
    for i, row in enumerate(sp_vals[1:], start=2):
        d = row_to_dict(sp_header, row)
        email = (d.get("Email") or "").strip().lower()
        if not email:
            continue
        sp_crm_id = row[sp_crm_col - 1] if sp_crm_col - 1 < len(row) else ""
        emap.setdefault(email, {"ex": None, "sp": None})
        emap[email]["sp"] = {"row": i, "data": d, "crm": sp_crm_id.strip()}

    # PROCESS
    for email, block in emap.items():
        ex = block.get("ex")
        sp = block.get("sp")

        ex_id = ex["crm"] if ex else ""
        sp_id = sp["crm"] if sp else ""

        # Normalize
        ex_id = ex_id.strip() if ex_id else ""
        sp_id = sp_id.strip() if sp_id else ""

        # Determine primary / secondary
        primary = None
        secondary = None
        primary_ws = None
        secondary_ws = None
        primary_row = None
        secondary_row = None
        primary_is_ex = False

        # Case: exists in both sheets
        if ex and sp:
            # If either side already has CRM id, that side is primary
            if ex_id:
                primary = ex
                primary_id = ex_id
                primary_ws = ws_ex
                primary_row = ex["row"]
                secondary = sp
                secondary_ws = ws_sp
                secondary_row = sp["row"]
                primary_is_ex = True
            elif sp_id:
                primary = sp
                primary_id = sp_id
                primary_ws = ws_sp
                primary_row = sp["row"]
                secondary = ex
                secondary_ws = ws_ex
                secondary_row = ex["row"]
                primary_is_ex = False
            else:
                # Neither has CRM id → exhibitor is primary
                primary = ex
                primary_id = ""
                primary_ws = ws_ex
                primary_row = ex["row"]
                secondary = sp
                secondary_ws = ws_sp
                secondary_row = sp["row"]
                primary_is_ex = True

        # Only exhibitor
        elif ex:
            primary = ex
            primary_id = ex_id
            primary_ws = ws_ex
            primary_row = ex["row"]
            secondary = None
            primary_is_ex = True

        # Only speaker
        else:
            primary = sp
            primary_id = sp_id
            primary_ws = ws_sp
            primary_row = sp["row"]
            secondary = None
            primary_is_ex = False

        # If primary already has CRM id
        if primary_id:
            # Copy to secondary if it exists and doesn't have an id
            if secondary:
                sec_id = secondary["crm"].strip() if secondary.get("crm") else ""
                if not sec_id:
                    updates[secondary_ws].append({
                        "range": f"{col_to_a1(sp_crm_col if secondary_ws==ws_sp else ex_crm_col)}{secondary_row}",
                        "values": [[primary_id]]
                    })
                    updates[secondary_ws].append({
                        "range": f"{col_to_a1(sp_update_col if secondary_ws==ws_sp else ex_update_col)}{secondary_row}",
                        "values": [["DUPLICATE – CRM ID COPIED"]]
                    })
            # Nothing more to do for this email
            continue

        # Primary has no CRM id → create lead
        try:
            print(f"➕ Creating CRM lead for {email}",flush=True)

            pdata = build_payload_from_row(
                primary["data"],
                "Exhibitor/Speaker" if (ex and sp) else (
                    "Exhibitor_opportunity" if ex else "speaker_opportunity"
                ),
                "Exhibitor,Speaker" if (ex and sp) else ("Exhibitor" if ex else "Speaker"),
                True if primary_is_ex else False
            )

            res = crm.create_lead(pdata)
            new_id = res["id"]
            print(f"✅ Created lead {new_id}",flush=True)

            # Update primary row -> ADDED IN CRM
            updates[primary_ws].append({
                "range": f"{col_to_a1(ex_crm_col if primary_ws==ws_ex else sp_crm_col)}{primary_row}",
                "values": [[new_id]]
            })
            updates[primary_ws].append({
                "range": f"{col_to_a1(ex_update_col if primary_ws==ws_ex else sp_update_col)}{primary_row}",
                "values": [["ADDED IN CRM"]]
            })

            # If secondary exists -> copy ID and mark ADDED IN CRM
            if secondary:
                updates[secondary_ws].append({
                    "range": f"{col_to_a1(sp_crm_col if secondary_ws==ws_sp else ex_crm_col)}{secondary_row}",
                    "values": [[new_id]]
                })
                updates[secondary_ws].append({
                    "range": f"{col_to_a1(sp_update_col if secondary_ws==ws_sp else ex_update_col)}{secondary_row}",
                    "values": [["ADDED IN CRM"]]
                })

        except Exception as e:
            err = str(e)

            # ⭐ HANDLE CRM DUPLICATE ERROR
            if "Duplicate(s) detected" in err or "duplicate" in err.lower():
                print(f"⚠️ Duplicate detected in CRM for {email}, marking in sheet...", flush=True)

                # Mark PRIMARY row
                updates[primary_ws].append({
                    "range": f"{col_to_a1(ex_update_col if primary_ws == ws_ex else sp_update_col)}{primary_row}",
                    "values": [["Failed to add in CRM – Duplicate detected"]]
                })

                # Write BLOCKER CRM ID so script skips next time
                updates[primary_ws].append({
                    "range": f"{col_to_a1(ex_crm_col if primary_ws == ws_ex else sp_crm_col)}{primary_row}",
                    "values": [["DUPLICATE"]]
                })

                # If secondary exists → mark that too
                if secondary:
                    updates[secondary_ws].append({
                        "range": f"{col_to_a1(sp_update_col if secondary_ws == ws_sp else ex_update_col)}{secondary_row}",
                        "values": [["Failed to add in CRM – Duplicate detected"]]
                    })
                    updates[secondary_ws].append({
                        "range": f"{col_to_a1(sp_crm_col if secondary_ws == ws_sp else ex_crm_col)}{secondary_row}",
                        "values": [["DUPLICATE"]]
                    })

                continue  # Skip this lead completely

            # OTHER ERRORS
            print(f"❌ Failed to create lead for {email}: {e}", flush=True)


    # APPLY UPDATES
    for ws, batch in updates.items():
        if batch:
            ws.batch_update(batch)

    print("🌱 FLOW 1 COMPLETE",flush=True)


# =========================
# FLOW 2 – CRM → SHEET
# =========================
def flow2_sync_crm_to_sheet():
    updates = defaultdict(list)
    crm_rows = []

    # Build CRM row list
    for i, row in enumerate(ex_vals[1:], start=2):
        crm_id = (row[ex_crm_col - 1] if ex_crm_col - 1 < len(row) else "").strip()
        if crm_id:
            email = row[ex_hmap["Email"] - 1].lower()
            crm_rows.append(("ex", crm_id.strip(), email, i))

    for i, row in enumerate(sp_vals[1:], start=2):
        crm_id = (row[sp_crm_col - 1] if sp_crm_col - 1 < len(row) else "").strip()
        if crm_id:
            email = row[sp_hmap["Email"] - 1].lower()
            crm_rows.append(("sp", crm_id.strip(), email, i))

    for sheet_type, crm_id, email, row_num in crm_rows:
        try:
            # Prepare workspace references
            ws = ws_ex if sheet_type == "ex" else ws_sp
            hmap = ex_hmap if sheet_type == "ex" else sp_hmap
            row_vals = ex_vals[row_num - 1] if sheet_type == "ex" else sp_vals[row_num - 1]
            row_data = row_to_dict(ex_header if sheet_type == "ex" else sp_header, row_vals)

            # Skip syncing for duplicate-marked rows
            crm_update_idx = hmap.get(CRM_UPDATE_COL)
            if crm_update_idx:
                crm_update_val = row_vals[crm_update_idx - 1] if crm_update_idx - 1 < len(row_vals) else ""
                if crm_update_val and "DUPLICATE" in str(crm_update_val).upper():
                    continue

            # Try retrieving CRM record
            crm_data = crm.get_lead(crm_id)
            comments = crm.get_all_comments(crm_id)

            # Correct date column
            sheet_date_col = "Last Follow-Up Date" if sheet_type == "ex" else "Email Sent-Date"
            sheet_raw = row_data.get(sheet_date_col, "")
            crm_raw = crm_data.get("cf_1153", "")

            sdt = parse_sheet_date(sheet_raw)
            cdt = parse_sheet_date(crm_raw)

            # Sheet newer → update CRM
            if sdt and (cdt is None or cdt < sdt):
                session = crm.get_session()
                full = crm_data.copy()
                full["id"] = crm_id
                crm_date = sdt.strftime("%d-%m-%Y")
                full["cf_1153"] = crm_date
                for k in ["createdtime", "modifiedtime"]:
                    full.pop(k, None)
                requests.post(crm.base_url, data={
                    "operation": "update",
                    "sessionName": session,
                    "element": json.dumps(full)
                })

            # CRM newer → update sheet
            elif cdt and (sdt is None or cdt > sdt):
                updates[ws].append({
                    "range": f"{col_to_a1(hmap[sheet_date_col])}{row_num}",
                    "values": [[crm_raw]]
                })

            # Comments → Sheet
            if comments and comments.strip() and "Comments" in hmap:
                updates[ws].append({
                    "range": f"{col_to_a1(hmap['Comments'])}{row_num}",
                    "values": [[comments]]
                })

        except Exception as e:
            err = str(e).lower()
            print(f"❌ Error syncing {email} ({crm_id}): {e}")

            # Detect invalid CRM ID
            is_invalid = (
                "access_denied" in err
                or "permission to perform the operation is denied" in err
                or "does not exist" in err
                or "record you are trying to access" in err
                or "invalid" in err
            )

            if is_invalid:
                print(f"🧹 Removing INVALID CRM ID '{crm_id}' for {email} — Flow-1 will recreate next run")

                crm_id_col = ex_crm_col if sheet_type == "ex" else sp_crm_col
                crm_update_col = ex_update_col if sheet_type == "ex" else sp_update_col

                # Delete CRM Lead ID
                updates[ws].append({
                    "range": f"{col_to_a1(crm_id_col)}{row_num}",
                    "values": [[""]]
                })
                

                # Delete CRM Update
                updates[ws].append({
                    "range": f"{col_to_a1(crm_update_col)}{row_num}",
                    "values": [[""]]
                })

                # Do NOT recreate here → Flow 1 handles this
                continue
            else:
                print(f"⚠️ Unknown CRM error for {email}, skipping...")

    # Apply updates
    for ws, batch in updates.items():
        if batch:
            ws.batch_update(batch)

    print("📝 FLOW 2 COMPLETE", flush=True)

# =========================
# RUN SCRIPT
# =========================
if __name__ == "__main__":
    print("🚀 Starting SYNC...",flush=True)
    flow1_create_and_sync_duplicates()
    # REFRESH SHEET DATA BEFORE FLOW 2
    ex_vals = ws_ex.get_all_values()
    sp_vals = ws_sp.get_all_values()

    ex_header = ex_vals[0]
    sp_header = sp_vals[0]
    
    ex_hmap = header_to_index(ex_header)
    sp_hmap = header_to_index(sp_header)
    
    flow2_sync_crm_to_sheet()
    print("✅ SYNC COMPLETE.",flush=True)
