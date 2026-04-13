import json
import os
import sqlite3
import subprocess
import sys
from collections import deque
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import psutil
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh

    AUTOREFRESH_AVAILABLE = True
except Exception:
    st_autorefresh = None
    AUTOREFRESH_AVAILABLE = False

from scraper.db import init_db

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "leads.db"
RUNTIME_DIR = APP_DIR / "runtime"
LOGS_DIR = APP_DIR / "logs"
SCRAPER_STATE = RUNTIME_DIR / "scraper_state.json"
MAILER_STATE = RUNTIME_DIR / "mailer_state.json"
SCRAPER_LOG = LOGS_DIR / "scraper.log"
MAILER_LOG = LOGS_DIR / "mailer.log"
CRM_STATUS_OPTIONS = [
    "Pending",
    "Emailed",
    "Replied",
    "Meeting Set",
    "Zoom Scheduled",
    "Closed",
    "Paid",
]
CSV_SEGMENTS = [
    "Filtered View",
    "All",
    "Pending",
    "Emailed",
    "Replied",
    "Meeting Set",
    "Zoom Scheduled",
    "Closed",
    "Paid",
]


def main() -> None:
    ensure_app_dirs()
    ensure_dashboard_columns()

    st.set_page_config(
        page_title="LeadGen Control Center",
        page_icon="L",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_custom_css()

    paid_count = get_paid_count()
    st.sidebar.metric("Total Revenue", f"${paid_count * 1200:,.0f}")
    st.sidebar.caption(f"Paid deals: {paid_count} x $1200")
    st.sidebar.title("LeadGen System")
    page = st.sidebar.radio(
        "Navigation",
        ["Pregled baze", "Zaženi Scraper", "AI Mailer Status"],
        label_visibility="collapsed",
    )

    if page == "Pregled baze":
        render_database_overview()
    elif page == "Zaženi Scraper":
        render_scraper_page()
    else:
        render_mailer_page()


def ensure_app_dirs() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


@st.cache_data(ttl=30)
def load_leads() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                business_name,
                email,
                website_url,
                phone_number,
                rating,
                review_count,
                address,
                search_keyword,
                insecure_site,
                main_shortcoming,
                status,
                scraped_at,
                enriched_at,
                sent_at,
                crm_comment,
                status_updated_at,
                last_sender_email
            FROM leads
            ORDER BY scraped_at DESC, id DESC
            """
        ).fetchall()

    frame = pd.DataFrame([dict(row) for row in rows])
    if frame.empty:
        return frame

    frame["city"] = frame["address"].apply(extract_city)
    frame["status_bucket"] = frame["status"].apply(normalize_status_bucket)
    frame["status_display"] = frame["status"].apply(normalize_display_status)
    frame["insecure_site"] = frame["insecure_site"].fillna(0).astype(int)
    return frame


def ensure_dashboard_columns() -> None:
    init_db(db_path=str(DB_PATH))

    with sqlite3.connect(DB_PATH) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}

        optional_columns = {
            "email": "ALTER TABLE leads ADD COLUMN email TEXT",
            "insecure_site": "ALTER TABLE leads ADD COLUMN insecure_site INTEGER DEFAULT 0",
            "main_shortcoming": "ALTER TABLE leads ADD COLUMN main_shortcoming TEXT",
            "enriched_at": "ALTER TABLE leads ADD COLUMN enriched_at TEXT",
            "status": "ALTER TABLE leads ADD COLUMN status TEXT",
            "sent_at": "ALTER TABLE leads ADD COLUMN sent_at TEXT",
            "crm_comment": "ALTER TABLE leads ADD COLUMN crm_comment TEXT",
            "status_updated_at": "ALTER TABLE leads ADD COLUMN status_updated_at TEXT",
            "last_sender_email": "ALTER TABLE leads ADD COLUMN last_sender_email TEXT",
        }

        for column_name, statement in optional_columns.items():
            if column_name not in columns:
                conn.execute(statement)

        conn.commit()


def render_database_overview() -> None:
    st.title("Pregled baze")
    st.caption("Pregled vseh leadov iz baze z osnovnimi filtri in hitrimi statistikami.")

    leads = load_leads()
    if leads.empty:
        st.info("Baza je prazna ali leads.db še ne obstaja.")
        return

    stat_cols = st.columns(4)
    stat_cols[0].metric("Vsi leadi", len(leads))
    stat_cols[1].metric("Emailed", int((leads["status_bucket"] == "emailed").sum()))
    stat_cols[2].metric("Pending", int((leads["status_bucket"] == "pending").sum()))
    stat_cols[3].metric("Paid", int(leads["status_display"].str.lower().eq("paid").sum()))

    filter_cols = st.columns([1, 1, 2])
    status_choice = filter_cols[0].selectbox("Status", ["all", "pending", "emailed"], index=0)
    city_options = sorted(city for city in leads["city"].dropna().unique().tolist() if city)
    city_choice = filter_cols[1].selectbox("City", ["all", *city_options], index=0)
    search_term = filter_cols[2].text_input("Search", placeholder="Business name, address, e-mail ...")

    filtered = leads.copy()
    if status_choice != "all":
        filtered = filtered[filtered["status_bucket"] == status_choice]
    if city_choice != "all":
        filtered = filtered[filtered["city"] == city_choice]
    if search_term.strip():
        term = search_term.strip().lower()
        search_blob = filtered.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        filtered = filtered[search_blob.str.contains(term, na=False)]

    display_columns = [
        "business_name",
        "city",
        "email",
        "website_url",
        "phone_number",
        "rating",
        "review_count",
        "main_shortcoming",
        "status_display",
        "crm_comment",
        "sent_at",
        "address",
    ]
    renamed = filtered[display_columns].rename(
        columns={
            "business_name": "Business Name",
            "city": "City",
            "email": "Email",
            "website_url": "Website",
            "phone_number": "Phone",
            "rating": "Rating",
            "review_count": "Reviews",
            "main_shortcoming": "Main Shortcoming",
            "status_display": "Status",
            "crm_comment": "CRM Comment",
            "sent_at": "Sent At",
            "address": "Address",
        }
    )

    st.dataframe(renamed, use_container_width=True, hide_index=True)

    st.markdown("### CRM Segment Export")
    export_cols = st.columns([1, 1, 2])
    segment_choice = export_cols[0].selectbox("Segment", CSV_SEGMENTS, index=0)
    export_city_choice = export_cols[1].selectbox("City Filter", ["all", *city_options], index=0)
    file_name = export_cols[2].text_input("File Name", value="crm_segment_export.csv")

    export_frame = build_export_frame(
        leads=leads,
        filtered=filtered,
        segment=segment_choice,
        city_choice=export_city_choice,
    )
    csv_bytes = export_frame.to_csv(index=False).encode("utf-8")
    safe_name = file_name.strip() or "crm_segment_export.csv"
    if not safe_name.lower().endswith(".csv"):
        safe_name = f"{safe_name}.csv"

    st.download_button(
        "Download Segment CSV",
        data=csv_bytes,
        file_name=safe_name,
        mime="text/csv",
        use_container_width=True,
    )

    render_kanban(leads)

    st.markdown("### CRM Quick Edit")
    if filtered.empty:
        st.info("Ni leadov za urejanje glede na aktivne filtre.")
        return

    lead_labels = {}
    for row in filtered.itertuples(index=False):
        label = f"{row.business_name} | {row.city or 'N/A'} | {row.email or 'no-email'} | #{row.id}"
        lead_labels[label] = int(row.id)

    with st.form("crm_quick_edit"):
        selected_label = st.selectbox("Lead", list(lead_labels.keys()))
        selected_id = lead_labels[selected_label]
        current_row = leads.loc[leads["id"] == selected_id].iloc[0]
        current_status = normalize_display_status(current_row.get("status"))
        default_index = CRM_STATUS_OPTIONS.index(current_status) if current_status in CRM_STATUS_OPTIONS else 0

        new_status = st.selectbox("New Status", CRM_STATUS_OPTIONS, index=default_index)
        current_comment = str(current_row.get("crm_comment") or "")
        comment = st.text_area("Comment", value=current_comment, placeholder="Quick note about this lead")
        save = st.form_submit_button("Save CRM Update", use_container_width=True)

    if save:
        update_lead_crm(lead_id=selected_id, status=new_status, comment=comment.strip())
        st.success("CRM status updated.")
        st.cache_data.clear()


def render_scraper_page() -> None:
    st.title("Zaženi Scraper")
    st.caption("Proži Google Maps scraping v ozadju prek obstoječe CLI skripte.")

    state = read_process_state(SCRAPER_STATE, expected_script="main.py")
    status_text, is_running = describe_process_state(state)

    hero_cols = st.columns([2, 1])
    hero_cols[0].markdown(render_status_card("Scraper Status", status_text), unsafe_allow_html=True)
    hero_cols[1].metric("Aktiven proces", "Yes" if is_running else "No")

    with st.form("scraper_form"):
        keyword = st.text_input("Keyword", placeholder="Roofers in Miami")
        country_code = st.text_input("Country Code", value="us", help="Examples: us, si, de")
        results = st.number_input("Number of Results", min_value=1, max_value=500, value=25, step=1)
        user_data_dir = st.text_input("User Data Dir", value="profiles/maps_profile")
        headless = st.checkbox("Run headless", value=False)
        export_targets = st.checkbox("Export target leads after scrape", value=True)
        submitted = st.form_submit_button("Start Scraping", use_container_width=True)

    if submitted:
        if not keyword.strip():
            st.error("Keyword is required.")
        elif not country_code.strip():
            st.error("Country Code is required.")
        elif is_running:
            st.warning("Scraper je že aktiven. Počakaj, da se trenutni proces zaključi.")
        else:
            command = [
                sys.executable,
                str(APP_DIR / "main.py"),
                "--log-file",
                str(SCRAPER_LOG),
                "scrape",
                "--keyword",
                keyword.strip(),
                "--results",
                str(int(results)),
                "--db",
                str(DB_PATH),
                "--country-code",
                country_code.strip().lower(),
                "--user-data-dir",
                user_data_dir.strip() or "profiles/maps_profile",
            ]
            if headless:
                command.append("--headless")
            if export_targets:
                command.extend(["--export-targets", "--output", str(APP_DIR / "target_leads.csv")])

            process = launch_background_process(command, APP_DIR, SCRAPER_LOG)
            write_process_state(
                SCRAPER_STATE,
                {
                    "pid": process.pid,
                    "command": command,
                    "log_path": str(SCRAPER_LOG),
                    "running": True,
                    "started_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
            st.success(f"Scraper started in background. PID: {process.pid}")
            st.cache_data.clear()

    if st.button("Stop Scraper", type="secondary", use_container_width=True):
        stopped, message = stop_process_from_state(SCRAPER_STATE, expected_script="main.py")
        if stopped:
            st.success(message)
            st.cache_data.clear()
        else:
            st.warning(message)

    st.markdown("### Last command")
    st.code(" ".join(state.get("command", [])) if state else "No scraper run recorded yet.")
    render_live_logs("Scraper Logs", SCRAPER_LOG, "scraper")


def render_mailer_page() -> None:
    st.title("AI Mailer Status")
    st.caption("Spremljanje pošiljanja in zagon AI mailer procesa v ozadju.")

    state = read_process_state(MAILER_STATE, expected_script="ai_mailer.py")
    status_text, is_running = describe_process_state(state)
    sent_today = get_sent_today_count()
    pending_count = get_pending_mail_count()

    metric_cols = st.columns(3)
    metric_cols[0].metric("Sent Today", sent_today)
    metric_cols[1].metric("Pending With Email", pending_count)
    metric_cols[2].metric("Mailer Running", "Yes" if is_running else "No")

    st.markdown(render_status_card("Mailer Status", status_text), unsafe_allow_html=True)

    with st.form("mailer_form"):
        limit = st.number_input("Lead Limit", min_value=1, max_value=250, value=10, step=1)
        submitted = st.form_submit_button("Start Sending", use_container_width=True)

    if submitted:
        if is_running:
            st.warning("AI mailer je že aktiven. Počakaj, da se trenutni proces zaključi.")
        else:
            command = [
                sys.executable,
                str(APP_DIR / "ai_mailer.py"),
                "--log-file",
                str(MAILER_LOG),
                "send",
                "--db",
                str(DB_PATH),
                "--config",
                str(APP_DIR / "config.json"),
                "--limit",
                str(int(limit)),
            ]
            process = launch_background_process(command, APP_DIR, MAILER_LOG)
            write_process_state(
                MAILER_STATE,
                {
                    "pid": process.pid,
                    "command": command,
                    "log_path": str(MAILER_LOG),
                    "running": True,
                    "started_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
            st.success(f"AI mailer started in background. PID: {process.pid}")
            st.cache_data.clear()

    if st.button("Stop Mailer", type="secondary", use_container_width=True):
        stopped, message = stop_process_from_state(MAILER_STATE, expected_script="ai_mailer.py")
        if stopped:
            st.success(message)
            st.cache_data.clear()
        else:
            st.warning(message)

    st.markdown("### Last command")
    st.code(" ".join(state.get("command", [])) if state else "No mailer run recorded yet.")
    render_live_logs("Mailer Logs", MAILER_LOG, "mailer")


def get_sent_today_count() -> int:
    if not DB_PATH.exists():
        return 0

    today = date.today().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) = 'emailed' AND DATE(sent_at) = ?",
            (today,),
        ).fetchone()
    return int(row[0] if row else 0)


def get_pending_mail_count() -> int:
    if not DB_PATH.exists():
        return 0

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM leads
            WHERE
                email IS NOT NULL
                AND email != ''
                AND LOWER(COALESCE(status, 'pending')) != 'emailed'
            """
        ).fetchone()
    return int(row[0] if row else 0)


def get_paid_count() -> int:
    if not DB_PATH.exists():
        return 0

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) = 'paid'"
        ).fetchone()
    return int(row[0] if row else 0)


def normalize_status_bucket(value: Optional[str]) -> str:
    if str(value or "").strip().lower() == "emailed":
        return "emailed"
    return "pending"


def normalize_display_status(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return "Pending"
    return text.title()


def build_export_frame(
    leads: pd.DataFrame,
    filtered: pd.DataFrame,
    segment: str,
    city_choice: str,
) -> pd.DataFrame:
    if segment == "Filtered View":
        base = filtered.copy()
    elif segment == "All":
        base = leads.copy()
    else:
        base = leads[leads["status_display"].str.lower() == segment.lower()].copy()

    if city_choice != "all":
        base = base[base["city"] == city_choice]

    columns = [
        "business_name",
        "city",
        "email",
        "status_display",
        "crm_comment",
        "main_shortcoming",
        "website_url",
        "phone_number",
        "rating",
        "review_count",
        "sent_at",
        "address",
    ]
    available = [column for column in columns if column in base.columns]
    result = base[available].rename(
        columns={
            "business_name": "business_name",
            "city": "city",
            "email": "email",
            "status_display": "status",
            "crm_comment": "crm_comment",
            "main_shortcoming": "main_shortcoming",
            "website_url": "website",
            "phone_number": "phone_number",
            "rating": "rating",
            "review_count": "review_count",
            "sent_at": "sent_at",
            "address": "address",
        }
    )
    return result


def render_kanban(leads: pd.DataFrame) -> None:
    st.markdown("### CRM Kanban")

    columns = st.columns(len(CRM_STATUS_OPTIONS))
    for idx, status_name in enumerate(CRM_STATUS_OPTIONS):
        column = columns[idx]
        stage = leads[leads["status_display"].str.lower() == status_name.lower()].copy()

        with column:
            st.markdown(f"**{status_name}**")
            st.metric("Count", len(stage))

            if stage.empty:
                st.caption("No leads")
                continue

            preview = stage.head(8)
            for row in preview.itertuples(index=False):
                business = row.business_name
                city = row.city or "N/A"
                comment = str(row.crm_comment or "")
                short_comment = (comment[:60] + "...") if len(comment) > 60 else comment
                card = f"{business}\n{city}"
                if short_comment:
                    card = f"{card}\n{short_comment}"
                st.code(card, language="text")

            hidden_count = len(stage) - len(preview)
            if hidden_count > 0:
                st.caption(f"+{hidden_count} more")


def extract_city(address: Optional[str]) -> str:
    if not address:
        return ""

    parts = [part.strip() for part in str(address).split(",") if part.strip()]
    if len(parts) >= 3:
        return parts[-3]
    if len(parts) >= 2:
        return parts[-2]
    return parts[0] if parts else ""


def update_lead_crm(lead_id: int, status: str, comment: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE leads
            SET
                status = ?,
                crm_comment = ?,
                status_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, comment or None, lead_id),
        )
        conn.commit()


def launch_background_process(command: list[str], cwd: Path, log_path: Path) -> subprocess.Popen:
    creation_flags = 0
    if os.name == "nt":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    log_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_log = RUNTIME_DIR / f"{Path(command[1]).stem}_stdout.log"
    log_handle = stdout_log.open("a", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        creationflags=creation_flags,
    )
    log_handle.close()
    return process


def read_process_state(path: Path, expected_script: Optional[str] = None) -> dict:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
    except Exception:
        return {}

    pid = int(state.get("pid") or 0)
    state["running"] = is_process_running(pid=pid, expected_script=expected_script)
    return state


def write_process_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def is_process_running(pid: int, expected_script: Optional[str] = None) -> bool:
    if pid <= 0:
        return False

    try:
        process = psutil.Process(pid)
        if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
            return False

        if expected_script:
            cmdline = " ".join(process.cmdline()).lower()
            return expected_script.lower() in cmdline

        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return False


def stop_process_from_state(path: Path, expected_script: str) -> tuple[bool, str]:
    state = read_process_state(path, expected_script=expected_script)
    pid = int(state.get("pid") or 0)
    if pid <= 0:
        return False, "No running process recorded."

    if not is_process_running(pid, expected_script=expected_script):
        state["running"] = False
        state["stopped_at"] = datetime.now().isoformat(timespec="seconds")
        write_process_state(path, state)
        return False, "Process is not running anymore."

    try:
        process = psutil.Process(pid)
        children = process.children(recursive=True)
        for proc in children:
            proc.terminate()
        process.terminate()
        _, alive = psutil.wait_procs([process, *children], timeout=5)
        for proc in alive:
            proc.kill()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as exc:
        return False, f"Could not stop process {pid}: {exc}"

    state["running"] = False
    state["stopped_at"] = datetime.now().isoformat(timespec="seconds")
    write_process_state(path, state)
    return True, f"Stopped process PID {pid}."


def describe_process_state(state: dict) -> tuple[str, bool]:
    if not state:
        return "No run recorded yet.", False

    running = bool(state.get("running"))
    started_at = state.get("started_at", "unknown")
    pid = state.get("pid", "unknown")
    status = "Running" if running else "Idle"
    return f"{status} | PID {pid} | started {started_at}", running


def render_live_logs(title: str, log_path: Path, key_prefix: str) -> None:
    st.markdown(f"### {title}")

    controls = st.columns([1, 1, 1])
    auto_refresh = controls[0].toggle("Auto Refresh", value=True, key=f"{key_prefix}_auto")
    interval = controls[1].selectbox("Interval (sec)", [2, 4, 8], index=1, key=f"{key_prefix}_interval")
    if controls[2].button("Refresh Now", use_container_width=True, key=f"{key_prefix}_refresh"):
        st.cache_data.clear()
        st.rerun()

    if auto_refresh and AUTOREFRESH_AVAILABLE and st_autorefresh:
        st_autorefresh(interval=interval * 1000, key=f"{key_prefix}_autorefresh")

    lines = tail_file(log_path, line_count=10)
    log_container = st.empty()
    log_container.code("\n".join(lines) if lines else "No logs yet.", language="text")


def tail_file(path: Path, line_count: int = 10) -> list[str]:
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            return [line.rstrip("\n") for line in deque(handle, maxlen=line_count)]
    except Exception:
        return []


def render_status_card(title: str, value: str) -> str:
    return f"""
    <div class='status-card'>
        <div class='status-label'>{title}</div>
        <div class='status-value'>{value}</div>
    </div>
    """


def inject_custom_css() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(84, 110, 255, 0.18), transparent 28%),
                radial-gradient(circle at left, rgba(0, 199, 190, 0.14), transparent 24%),
                linear-gradient(180deg, #0c111b 0%, #090d14 100%);
            color: #e8edf7;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f1625 0%, #0a101a 100%);
            border-right: 1px solid rgba(151, 164, 187, 0.12);
        }
        .stTextInput input, .stNumberInput input, .stSelectbox div[data-baseweb="select"] > div {
            background-color: rgba(12, 18, 28, 0.95);
            color: #edf2ff;
            border-radius: 12px;
        }
        .stButton > button, .stFormSubmitButton > button {
            background: linear-gradient(135deg, #34d399 0%, #0ea5e9 100%);
            color: #07111d;
            border: 0;
            border-radius: 12px;
            font-weight: 700;
            padding: 0.7rem 1rem;
        }
        .status-card {
            background: rgba(15, 22, 37, 0.88);
            border: 1px solid rgba(132, 146, 173, 0.18);
            border-radius: 18px;
            padding: 1.1rem 1.2rem;
            box-shadow: 0 24px 60px rgba(0, 0, 0, 0.28);
        }
        .status-label {
            color: #8ea0bc;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.3rem;
        }
        .status-value {
            color: #f7fbff;
            font-size: 1.05rem;
            font-weight: 600;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(132, 146, 173, 0.18);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 24px 60px rgba(0, 0, 0, 0.24);
        }
        [data-testid="stCodeBlock"] {
            border-radius: 16px;
            border: 1px solid rgba(132, 146, 173, 0.15);
            background: rgba(8, 13, 21, 0.9);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
