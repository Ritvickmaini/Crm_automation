import hashlib
import requests
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from collections import defaultdict
import os

# =========================
# CRM CLIENT CLASS
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
            raise Exception(f"Failed to get challenge token: {response}")
        return response["result"]

    def _login(self):
        challenge = self._get_challenge()
        token = challenge["token"]
        expire = challenge["expireTime"]
        self.session_expiry = expire if expire < 1e10 else expire / 1000

        access_key_hash = hashlib.md5((token + self.access_key).encode()).hexdigest()
        data = {
            "operation": "login",
            "username": self.username,
            "accessKey": access_key_hash
        }
        response = requests.post(self.base_url, data=data).json()
        if not response.get("success"):
            raise Exception(f"Login failed: {response}")
        self.session_name = response["result"]["sessionName"]
        return self.session_name

    def get_session(self):
        if not self.session_name or time.time() >= self.session_expiry:
            self._login()
        return self.session_name

    def create_lead(self, lead_data: dict):
        session = self.get_session()
        data = {
            "operation": "create",
            "sessionName": session,
            "elementType": "Leads",
            "element": json.dumps(lead_data)
        }
        response = requests.post(self.base_url, data=data).json()
        if not response.get("success"):
            raise Exception(f"Failed to create lead: {response}")
        return response["result"]

    def get_lead(self, lead_id: str):
        session = self.get_session()
        params = {
            "operation": "retrieve",
            "sessionName": session,
            "id": lead_id
        }
        response = requests.get(self.base_url, params=params).json()
        if not response.get("success"):
            raise Exception(f"Failed to retrieve lead {lead_id}: {response}")
        return response["result"]

    def get_all_comments(self, lead_id):
        session = self.get_session()
        query = (
            f"select commentcontent,createdtime "
            f"from ModComments where related_to='{lead_id}' "
            f"ORDER BY createdtime ASC;"
        )
        params = {
            "operation": "query",
            "sessionName": session,
            "query": query
        }
        response = requests.get(self.base_url, params=params).json()
        if not response.get("success"):
            return ""
        result = response.get("result", [])
        if not result:
            return ""
        formatted = []
        for r in result:
            comment = (r.get("commentcontent") or "").strip()
            ts = r.get("createdtime", "").replace("T", " ")
            if comment:
                formatted.append(f"{ts} : {comment}")
        formatted.reverse()
        return "\n".join(formatted)


# =========================
# CONFIG
# =========================
BASE_URL = "https://b2bgrowthexpo.crm360degree.com"
USERNAME = "ritvick"
ACCESS_KEY = "Sp4y3qQV1ZuanCet"

SHEET_NAME = "Expo-Sales-Management"
EXHIBITOR_TAB = "exhibitors-1"
SPEAKER_TAB = "speakers-2"

CRM_ID_COL_NAME = "CRM Lead ID"
CRM_UPDATE_COL = "CRM Update"

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
crm = CRMClient(BASE_URL, USERNAME, ACCESS_KEY)
print("📄 Google Sheets authenticated", flush=True)


# =========================
# Mapping for CREATE ONLY
# =========================
def map_row_exhibitor(row, opp_type, multi):
    lastname = row.get("Last Name") or row.get("First_Name") or "Unknown"
    return {
        "assigned_user_id": "19x77",
        "leadstatus": "New",
        "memberof": "11x232",
        "cf_1203": "EXPO-SALES-MANAGEMENT",
        "firstname": row.get("First_Name", ""),
        "lastname": lastname,
        "company": row.get("Company Name", ""),
        "leadsource": row.get("Lead Source", ""),
        "email": row.get("Email", ""),
        "mobile": str(row.get("Mobile", "")),
        "phone": str(row.get("Mobile", "")),
        "cf_1161": row.get("Show", ""),
        "cf_905": row.get("Next Followup", ""),
        "cf_1047": row.get("Call Attempt", ""),
        "cf_1171": row.get("Linkedin Msg", ""),
        "cf_1155": row.get("Comments", ""),
        "cf_1163": row.get("Pitch Deck URL", ""),  # <—
        "cf_1049": opp_type,
        "cf_1205": multi,
        "cf_1175": row.get("WhatsApp msg count", ""),
        "cf_1151": row.get("Follow-Up Count", ""),
        "cf_1153": row.get("Last Follow-Up Date", ""),
        "cf_1173": row.get("Reply Status", ""),
        "cf_1177": row.get("LINKEDIN-HEADLINE", ""),
        "cf_1179": row.get("LINKEDIN-REPLY", ""),
        "cf_939": row.get("LINKEDIN-URL", ""),
        "cf_1181": row.get("Stand Size", ""),
        "cf_1183": row.get("Amount", ""),
    }


def map_row_speaker(row, opp_type, multi):
    lastname = row.get("Last Name") or row.get("First_Name") or "Unknown"
    return {
        "assigned_user_id": "19x77",
        "leadstatus": "New",
        "memberof": "11x232",
        "cf_1203": "EXPO-SALES-MANAGEMENT",
        "firstname": row.get("First_Name", ""),
        "lastname": lastname,
        "company": row.get("Company Name", ""),
        "leadsource": row.get("Lead Source", ""),
        "email": row.get("Email", ""),
        "mobile": str(row.get("Mobile", "")),
        "phone": str(row.get("Mobile", "")),
        "cf_1161": row.get("Show", ""),
        "cf_905": row.get("Next Followup", ""),
        "cf_1047": row.get("Call Attempt", ""),
        "cf_1171": row.get("Linkedin Msg Count", ""),
        "cf_1155": row.get("Comments", ""),
        "cf_1163": row.get("Pitch Deck URL", ""),  # <—
        "cf_1049": opp_type,
        "cf_1205": multi,
        "cf_1153": row.get("Email Sent-Date", ""),
        "cf_1173": row.get("Reply Status", ""),
        "cf_1175": row.get("WhatsApp msg count", ""),
        "cf_1177": row.get("Company Linkedin Page", ""),
        "cf_1179": row.get("Personal Linkedin Page", ""),
        "cf_1215": row.get("Lead Date", ""),
    }


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
    return {h: i+1 for i, h in enumerate(header)}

def row_to_dict(header, row):
    return {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}


# =========================
# LOAD SHEETS + ENSURE COLUMNS
# =========================
ws_ex = client.open(SHEET_NAME).worksheet(EXHIBITOR_TAB)
ws_sp = client.open(SHEET_NAME).worksheet(SPEAKER_TAB)

ex_vals = ws_ex.get_all_values()
sp_vals = ws_sp.get_all_values()

ex_header = ex_vals[0]
sp_header = sp_vals[0]

ex_hmap = header_to_index(ex_header)
sp_hmap = header_to_index(sp_header)


def ensure_col(ws, hmap, header, col_name):
    if col_name in hmap:
        return hmap[col_name]
    col = len(header) + 1
    ws.update(f"{col_to_a1(col)}1", col_name)
    return col


ex_crm_col = ensure_col(ws_ex, ex_hmap, ex_header, CRM_ID_COL_NAME)
sp_crm_col = ensure_col(ws_sp, sp_hmap, sp_header, CRM_ID_COL_NAME)

ex_update_col = ensure_col(ws_ex, ex_hmap, ex_header, CRM_UPDATE_COL)
sp_update_col = ensure_col(ws_sp, sp_hmap, sp_header, CRM_UPDATE_COL)


# =========================
# MERGE BY EMAIL
# =========================
merged = {}

for i, row in enumerate(ex_vals[1:], start=2):
    d = row_to_dict(ex_header, row)
    email = (d.get("Email") or "").strip().lower()
    if email:
        crm_id = row[ex_crm_col - 1] if ex_crm_col - 1 < len(row) else ""
        merged[email] = {"status": "exhibitor", "ex": d, "ex_idx": i, "ex_crm": crm_id,
                          "sp": None, "sp_idx": None, "sp_crm": None}

for i, row in enumerate(sp_vals[1:], start=2):
    d = row_to_dict(sp_header, row)
    email = (d.get("Email") or "").strip().lower()
    if email:
        crm_id = row[sp_crm_col - 1] if sp_crm_col - 1 < len(row) else ""
        if email in merged:
            merged[email]["status"] = "both"
            merged[email]["sp"] = d
            merged[email]["sp_idx"] = i
            merged[email]["sp_crm"] = crm_id
        else:
            merged[email] = {"status": "speaker", "ex": None, "ex_idx": None, "ex_crm": None,
                              "sp": d, "sp_idx": i, "sp_crm": crm_id}


# =========================
# BATCH UPDATES
# =========================
updates = defaultdict(list)


def queue(ws, row, col, val):
    updates[ws].append({"range": f"{col_to_a1(col)}{row}", "values": [[val]]})


# =========================
# PROCESS SYNC
# =========================
for email, info in merged.items():
    try:
        status = info["status"]

        # Determine source row for CRM creation
        if status == "exhibitor":
            src = info["ex"]
            ex_idx = info["ex_idx"]
            sp_idx = None
            payload = map_row_exhibitor(src, "Exhibitor_opportunity", "Exhibitor")

        elif status == "speaker":
            src = info["sp"]
            sp_idx = info["sp_idx"]
            ex_idx = None
            payload = map_row_speaker(src, "speaker_opportunity", "Speaker")

        else:
            src = info["ex"]
            ex_idx = info["ex_idx"]
            sp_idx = info["sp_idx"]
            payload = map_row_exhibitor(src, "Exhibitor/Speaker", "Exhibitor,Speaker")

        crm_id = info.get("ex_crm") or info.get("sp_crm")

        # -------------------------------
        # CREATE NEW LEAD IF MISSING
        # -------------------------------
        if not crm_id:
            print(f"➕ Creating new CRM lead for {email}", flush=True)
            created = crm.create_lead(payload)
            crm_id = created["id"]
            print(f"✅ Lead created with ID {crm_id}", flush=True)

            if ex_idx:
                queue(ws_ex, ex_idx, ex_crm_col, crm_id)
                queue(ws_ex, ex_idx, ex_update_col, "ADDED IN CRM")
            if sp_idx:
                queue(ws_sp, sp_idx, sp_crm_col, crm_id)
                queue(ws_sp, sp_idx, sp_update_col, "ADDED IN CRM")

        # -------------------------------
        # FETCH CRM DATA
        # -------------------------------
        crm_data = crm.get_lead(crm_id)
        comments = crm.get_all_comments(crm_id)

        email_count = crm_data.get("cf_1207", "")
        linkedin_val = crm_data.get("cf_1159", "")
        whatsapp_val = crm_data.get("cf_1157", "")
        call_val = crm_data.get("cf_1047", "")

        # ----------------------------------------
        # SYNC REPLY STATUS (Sheet → CRM)
        # ----------------------------------------
        sheet_reply = (src.get("Reply Status") or "").strip()
        crm_reply = (crm_data.get("cf_1173") or "").strip()

        if sheet_reply and sheet_reply != crm_reply:
            try:
                print(f"🔄 Updating Reply Status for {email}: {sheet_reply}", flush=True)
                session = crm.get_session()
                full_payload = crm_data.copy()
                full_payload["id"] = crm_id
                full_payload["cf_1173"] = sheet_reply

                for key in ["modifiedtime", "createdtime"]:
                    full_payload.pop(key, None)

                update_res = requests.post(
                    crm.base_url,
                    data={
                        "operation": "update",
                        "sessionName": session,
                        "element": json.dumps(full_payload)
                    }
                ).json()

                if update_res.get("success"):
                    print(f"✅ Reply Status updated for {email}: {sheet_reply}",flush=True)
                else:
                    print(f"❌ Failed to update Reply Status for {email}: {update_res}",flush=True)

            except Exception as e:
                print(f"❌ Error syncing Reply Status for {email}: {e}",flush=True)

        # ----------------------------------------
        # SYNC PITCH DECK URL (Sheet → CRM)
        # ----------------------------------------
        sheet_pitch = (src.get("Pitch Deck URL") or "").strip()
        crm_pitch = (crm_data.get("cf_1163") or "").strip()

        if sheet_pitch and sheet_pitch != crm_pitch:
            try:
                print(f"🔄 Updating Pitch Deck URL for {email}: {sheet_pitch}", flush=True)
                session = crm.get_session()

                full_payload = crm_data.copy()
                full_payload["id"] = crm_id
                full_payload["cf_1163"] = sheet_pitch

                for key in ["modifiedtime", "createdtime"]:
                    full_payload.pop(key, None)

                update_res = requests.post(
                    crm.base_url,
                    data={
                        "operation": "update",
                        "sessionName": session,
                        "element": json.dumps(full_payload)
                    }
                ).json()

                if update_res.get("success"):
                    print(f"✅ Pitch Deck URL updated for {email}: {sheet_pitch}",flush=True)
                else:
                    print(f"❌ Failed to update Pitch Deck URL for {email}: {update_res}",flush=True)

            except Exception as e:
                print(f"❌ Error syncing Pitch Deck URL for {email}: {e}",flush=True)

        # ----------------------------------------
        # UPDATE EXHIBITOR SHEET
        # ----------------------------------------
        if ex_idx:
            r = ex_idx
            queue(ws_ex, r, ex_hmap["Comments"], comments)
            if "Email-Count" in ex_hmap:
                queue(ws_ex, r, ex_hmap["Email-Count"], email_count)
            if "Linkedin Msg" in ex_hmap:
                queue(ws_ex, r, ex_hmap["Linkedin Msg"], linkedin_val)
            if "WhatsApp msg count" in ex_hmap:
                queue(ws_ex, r, ex_hmap["WhatsApp msg count"], whatsapp_val)
            if "Call Attempt" in ex_hmap:
                queue(ws_ex, r, ex_hmap["Call Attempt"], call_val)
            queue(ws_ex, r, ex_crm_col, crm_id)

        # ----------------------------------------
        # UPDATE SPEAKER SHEET
        # ----------------------------------------
        if sp_idx:
            r = sp_idx
            queue(ws_sp, r, sp_hmap["Comments"], comments)
            if "Email-Count" in sp_hmap:
                queue(ws_sp, r, sp_hmap["Email-Count"], email_count)
            if "Linkedin Msg Count" in sp_hmap:
                queue(ws_sp, r, sp_hmap["Linkedin Msg Count"], linkedin_val)
            if "WhatsApp msg count" in sp_hmap:
                queue(ws_sp, r, sp_hmap["WhatsApp msg count"], whatsapp_val)
            if "Call Attempt" in sp_hmap:
                queue(ws_sp, r, sp_hmap["Call Attempt"], call_val)
            queue(ws_sp, r, sp_crm_col, crm_id)

    except Exception as e:
        print("❌ ERROR for", email, ":", e, flush=True)

# =========================
# EXECUTE BATCH UPDATES
# =========================
for ws, batch in updates.items():
    if batch:
        ws.batch_update(batch)
        print(f"Updated {len(batch)} cells in {ws.title}",flush=True)

print("SYNC COMPLETE....", flush=True)

