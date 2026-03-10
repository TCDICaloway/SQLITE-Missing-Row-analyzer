import csv
import os
import re
import shutil
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tkinter import Tk, StringVar, BooleanVar, filedialog, messagebox, Text, END, Listbox
from tkinter import ttk

try:
    from PIL import Image, ImageTk
except ImportError:
    Image = None
    ImageTk = None

APP_TITLE = "SQLITE DB Gap Report"
APP_GEOMETRY = "1540x1020"

TIMEZONE_OPTIONS = [
    "UTC-12:00", "UTC-11:00", "UTC-10:00", "UTC-09:30", "UTC-09:00", "UTC-08:00",
    "UTC-07:00", "UTC-06:00", "UTC-05:00", "UTC-04:00", "UTC-03:30", "UTC-03:00",
    "UTC-02:00", "UTC-01:00", "UTC+00:00", "UTC+01:00", "UTC+02:00", "UTC+03:00",
    "UTC+03:30", "UTC+04:00", "UTC+04:30", "UTC+05:00", "UTC+05:30", "UTC+05:45",
    "UTC+06:00", "UTC+06:30", "UTC+07:00", "UTC+08:00", "UTC+08:45", "UTC+09:00",
    "UTC+09:30", "UTC+10:00", "UTC+10:30", "UTC+11:00", "UTC+12:00", "UTC+12:45",
    "UTC+13:00", "UTC+14:00"
]

COMMON_ID_COLUMNS = ["_id", "rowid", "id", "ROWID"]
COMMON_DATE_COLUMNS = [
    "date", "date_sent", "date_read", "date_delivered", "timestamp", "message_date",
    "created_date", "start_date", "recv_date", "received_date", "sent_time"
]
COMMON_ADDRESS_COLUMNS = [
    "address", "phone_number", "sender", "recipient", "from_address", "to_address",
    "handle_id", "recipient_id"
]
TABLE_HINTS = ["sms", "message", "messages", "pdu", "mms", "mmssms", "chat_message", "imessage"]
LIKELY_DB_EXTENSIONS = {".db", ".sqlite", ".sqlite3", ".storedata", ".sqlitedb", ".db3"}
EXCLUDED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".mp4", ".mov", ".avi",
    ".plist", ".json", ".xml", ".txt", ".csv", ".log", ".pdf", ".doc", ".docx"
}


def parse_utc_offset(offset_text: str) -> timezone:
    match = re.fullmatch(r"UTC([+-])(\d{2}):(\d{2})", offset_text.strip())
    if not match:
        return timezone.utc
    sign, hh, mm = match.groups()
    total_minutes = int(hh) * 60 + int(mm)
    if sign == "-":
        total_minutes *= -1
    return timezone(timedelta(minutes=total_minutes))


def safe_int(value):
    try:
        if value is None or value == "":
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore").strip()
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            if "." in value:
                return int(float(value))
        return int(value)
    except Exception:
        return None


def fmt_dt(dt_obj):
    if not dt_obj:
        return ""
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S %z")


def sql_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def looks_like_db_candidate(member_name: str) -> bool:
    lower_name = member_name.lower()
    suffix = Path(lower_name).suffix
    if suffix in LIKELY_DB_EXTENSIONS:
        return True
    if suffix in EXCLUDED_EXTENSIONS:
        return False
    return any(hint in lower_name for hint in TABLE_HINTS)


def guess_timestamp_system(raw_value):
    val = safe_int(raw_value)
    if val is None:
        return "unknown"
    abs_val = abs(val)

    if 0 <= abs_val <= 2_000_000_000:
        if abs_val < 978_307_200:
            return "mac_absolute_seconds"
        return "unix_seconds"

    if 100_000_000_000 <= abs_val < 100_000_000_000_000:
        if abs_val < 978_307_200_000:
            return "mac_absolute_milliseconds"
        return "unix_milliseconds"

    if 100_000_000_000_000 <= abs_val < 100_000_000_000_000_000:
        if abs_val < 978_307_200_000_000:
            return "mac_absolute_microseconds"
        return "unix_microseconds"

    if 100_000_000_000_000_000 <= abs_val < 100_000_000_000_000_000_000:
        if abs_val < 978_307_200_000_000_000:
            return "mac_absolute_nanoseconds"
        return "unix_nanoseconds"

    return "unknown"


def convert_timestamp(raw_value, tzinfo: timezone, forced_mode=None):
    val = safe_int(raw_value)
    if val is None:
        return None, "unknown"

    mode = forced_mode or guess_timestamp_system(val)
    apple_epoch = datetime(2001, 1, 1, tzinfo=timezone.utc)

    try:
        if mode == "unix_seconds":
            dt_utc = datetime.fromtimestamp(val, tz=timezone.utc)
        elif mode == "unix_milliseconds":
            dt_utc = datetime.fromtimestamp(val / 1000.0, tz=timezone.utc)
        elif mode == "unix_microseconds":
            dt_utc = datetime.fromtimestamp(val / 1_000_000.0, tz=timezone.utc)
        elif mode == "unix_nanoseconds":
            dt_utc = datetime.fromtimestamp(val / 1_000_000_000.0, tz=timezone.utc)
        elif mode == "mac_absolute_seconds":
            dt_utc = apple_epoch + timedelta(seconds=val)
        elif mode == "mac_absolute_milliseconds":
            dt_utc = apple_epoch + timedelta(milliseconds=val)
        elif mode == "mac_absolute_microseconds":
            dt_utc = apple_epoch + timedelta(microseconds=val)
        elif mode == "mac_absolute_nanoseconds":
            dt_utc = apple_epoch + timedelta(seconds=val / 1_000_000_000.0)
        else:
            return None, mode
        return dt_utc.astimezone(tzinfo), mode
    except Exception:
        return None, mode


class ForensicSmsGapAnalyzer:
    def __init__(self, root: Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry(APP_GEOMETRY)

        self.db_path = StringVar()
        self.zip_path = StringVar()
        self.db_search_var = StringVar()
        self.table_var = StringVar()
        self.id_col_var = StringVar()
        self.date_col_var = StringVar()
        self.address_col_var = StringVar()
        self.tz_var = StringVar(value="UTC+00:00")
        self.timestamp_mode_var = StringVar(value="Auto Detect")
        self.use_wal_var = BooleanVar(value=True)
        self.min_gap_var = StringVar(value="1")

        self.export_scope_var = StringVar(value="All displayed rows")
        self.export_format_var = StringVar(value="CSV")

        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.default_gui_logo_path = os.path.join(self.script_dir, "logo.png")
        self.report_logo_path_var = StringVar()

        self.start_year_var = StringVar(value="Any")
        self.start_month_var = StringVar(value="Any")
        self.start_day_var = StringVar(value="Any")
        self.start_hour_var = StringVar(value="00")
        self.start_minute_var = StringVar(value="00")
        self.end_year_var = StringVar(value="Any")
        self.end_month_var = StringVar(value="Any")
        self.end_day_var = StringVar(value="Any")
        self.end_hour_var = StringVar(value="23")
        self.end_minute_var = StringVar(value="59")

        self.post_filter_start_var = StringVar()
        self.post_filter_end_var = StringVar()

        self.tables = []
        self.columns_by_table = {}
        self.analysis_rows = []
        self.filtered_analysis_rows = []
        self.summary = {}
        self.zip_members = []
        self.filtered_zip_members = []
        self.active_db_path = None
        self.active_wal_path = None
        self.temp_work_dir = None

        self.logo_photo = None
        self.logo_label = None

        self._build_ui()
        self.update_logo_preview()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self):
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill="both", expand=True)

        top_header = ttk.LabelFrame(outer, text="Case Branding", padding=10)
        top_header.pack(fill="x", pady=(0, 8))

        logo_left = ttk.Frame(top_header)
        logo_left.pack(side="left", fill="y")

        self.logo_label = ttk.Label(logo_left, text="No GUI Logo Found", anchor="center")
        self.logo_label.pack(side="top", padx=(0, 12))

        logo_controls = ttk.Frame(top_header)
        logo_controls.pack(side="left", fill="x", expand=True)

        ttk.Label(logo_controls, text="Report Logo").grid(row=0, column=0, sticky="w")
        ttk.Entry(
            logo_controls,
            textvariable=self.report_logo_path_var,
            width=60
        ).grid(row=0, column=1, sticky="ew", padx=(6, 6))
        ttk.Button(
            logo_controls,
            text="Browse Report Logo",
            command=self.browse_report_logo
        ).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(
            logo_controls,
            text="Use GUI Logo for Report",
            command=self.copy_gui_logo_to_report
        ).grid(row=0, column=3)

        logo_controls.columnconfigure(1, weight=1)

        file_frame = ttk.LabelFrame(outer, text="Evidence Source", padding=10)
        file_frame.pack(fill="x", pady=(0, 8))

        ttk.Label(file_frame, text="Direct DB").grid(row=0, column=0, sticky="w")
        ttk.Entry(file_frame, textvariable=self.db_path).grid(row=0, column=1, sticky="ew", padx=(5, 8))
        ttk.Button(file_frame, text="Browse DB", command=self.browse_db).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(file_frame, text="Load Direct DB", command=self.load_tables_from_direct_db).grid(row=0, column=3)

        ttk.Label(file_frame, text="ZIP Container").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(file_frame, textvariable=self.zip_path).grid(row=1, column=1, sticky="ew", padx=(5, 8), pady=(8, 0))
        ttk.Button(file_frame, text="Browse ZIP", command=self.browse_zip).grid(row=1, column=2, padx=(0, 8), pady=(8, 0))
        ttk.Button(file_frame, text="Scan ZIP", command=self.scan_zip_for_databases).grid(row=1, column=3, pady=(8, 0))

        ttk.Label(file_frame, text="Search ZIP DBs").grid(row=2, column=0, sticky="w", pady=(8, 0))
        search = ttk.Entry(file_frame, textvariable=self.db_search_var)
        search.grid(row=2, column=1, sticky="ew", padx=(5, 8), pady=(8, 0))
        search.bind("<KeyRelease>", lambda e: self.filter_zip_candidates())
        ttk.Button(file_frame, text="Filter", command=self.filter_zip_candidates).grid(row=2, column=2, padx=(0, 8), pady=(8, 0))
        ttk.Button(file_frame, text="Load Selected From ZIP", command=self.load_selected_zip_db).grid(row=2, column=3, pady=(8, 0))

        ttk.Checkbutton(file_frame, text="Use WAL if found", variable=self.use_wal_var).grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(file_frame, text="Choice is applied when the DB is loaded.").grid(row=3, column=2, columnspan=2, sticky="w", pady=(8, 0))

        file_frame.columnconfigure(1, weight=1)

        zip_list_frame = ttk.LabelFrame(outer, text="ZIP Database Candidates", padding=10)
        zip_list_frame.pack(fill="x", pady=(0, 8))
        self.zip_listbox = Listbox(zip_list_frame, height=5, exportselection=False)
        self.zip_listbox.pack(fill="x", expand=False)
        self.zip_listbox.bind("<Double-Button-1>", lambda e: self.load_selected_zip_db())

        config_frame = ttk.LabelFrame(outer, text="Analysis Configuration", padding=10)
        config_frame.pack(fill="x", pady=(0, 8))

        ttk.Label(config_frame, text="Table").grid(row=0, column=0, sticky="w")
        self.table_combo = ttk.Combobox(config_frame, textvariable=self.table_var, state="readonly", width=28)
        self.table_combo.grid(row=0, column=1, sticky="w", padx=(5, 15))
        self.table_combo.bind("<<ComboboxSelected>>", lambda e: self.on_table_selected())

        ttk.Label(config_frame, text="Row ID Column").grid(row=0, column=2, sticky="w")
        self.id_combo = ttk.Combobox(config_frame, textvariable=self.id_col_var, width=22)
        self.id_combo.grid(row=0, column=3, sticky="w", padx=(5, 15))

        ttk.Label(config_frame, text="Date Column").grid(row=0, column=4, sticky="w")
        self.date_combo = ttk.Combobox(config_frame, textvariable=self.date_col_var, width=22)
        self.date_combo.grid(row=0, column=5, sticky="w", padx=(5, 15))

        ttk.Label(config_frame, text="Address Column").grid(row=0, column=6, sticky="w")
        self.address_combo = ttk.Combobox(config_frame, textvariable=self.address_col_var, width=22)
        self.address_combo.grid(row=0, column=7, sticky="w", padx=(5, 0))

        ttk.Label(config_frame, text="Minimum missing rows").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(config_frame, textvariable=self.min_gap_var, width=8).grid(row=1, column=1, sticky="w", padx=(5, 15), pady=(8, 0))

        filter_frame = ttk.LabelFrame(outer, text="Pre-Analysis Date Filter", padding=10)
        filter_frame.pack(fill="x", pady=(0, 8))
        self._build_date_row(
            filter_frame, 0, "Start",
            self.start_year_var, self.start_month_var, self.start_day_var,
            self.start_hour_var, self.start_minute_var
        )
        self._build_date_row(
            filter_frame, 1, "End",
            self.end_year_var, self.end_month_var, self.end_day_var,
            self.end_hour_var, self.end_minute_var
        )

        time_frame = ttk.LabelFrame(outer, text="Time Conversion", padding=10)
        time_frame.pack(fill="x", pady=(0, 8))
        ttk.Label(time_frame, text="Timezone Offset").grid(row=0, column=0, sticky="w")
        ttk.Combobox(time_frame, textvariable=self.tz_var, values=TIMEZONE_OPTIONS, state="readonly", width=16).grid(row=0, column=1, sticky="w", padx=(5, 18))
        ttk.Label(time_frame, text="Timestamp Mode").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            time_frame,
            textvariable=self.timestamp_mode_var,
            state="readonly",
            width=28,
            values=[
                "Auto Detect", "UNIX Seconds", "UNIX Milliseconds", "UNIX Microseconds", "UNIX Nanoseconds",
                "Mac Absolute Seconds", "Mac Absolute Milliseconds", "Mac Absolute Microseconds", "Mac Absolute Nanoseconds",
            ],
        ).grid(row=0, column=3, sticky="w", padx=(5, 18))
        ttk.Button(time_frame, text="Clear Pre-Filter", command=self.clear_filters).grid(row=0, column=4, padx=(5, 0))
        ttk.Button(time_frame, text="Run Gap Analysis", command=self.run_analysis).grid(row=0, column=5, padx=(12, 0))

        results_frame = ttk.LabelFrame(outer, text="Gap Results", padding=10)
        results_frame.pack(fill="both", expand=True)

        report_bar = ttk.Frame(results_frame)
        report_bar.pack(fill="x", pady=(0, 8))

        ttk.Label(report_bar, text="Export scope").pack(side="left")
        ttk.Combobox(
            report_bar,
            textvariable=self.export_scope_var,
            state="readonly",
            width=22,
            values=["All displayed rows", "Checked rows only"],
        ).pack(side="left", padx=(6, 18))

        ttk.Label(report_bar, text="Export format").pack(side="left")
        ttk.Combobox(
            report_bar,
            textvariable=self.export_format_var,
            state="readonly",
            width=10,
            values=["CSV", "PDF"],
        ).pack(side="left", padx=(6, 18))

        ttk.Button(report_bar, text="Select All", command=self.select_all_rows).pack(side="left")
        ttk.Button(report_bar, text="Clear Selection", command=self.clear_selected_rows).pack(side="left", padx=(6, 18))
        ttk.Button(report_bar, text="Report", command=self.export_from_choice).pack(side="right")

        self.summary_label = ttk.Label(results_frame, text="No analysis run yet.")
        self.summary_label.pack(anchor="w", pady=(0, 4))
        self.wal_status_label = ttk.Label(results_frame, text="WAL Used: No")
        self.wal_status_label.pack(anchor="w", pady=(0, 8))

        post_filter_frame = ttk.Frame(results_frame)
        post_filter_frame.pack(fill="x", pady=(0, 8))
        ttk.Label(post_filter_frame, text="Post-analysis date filter").pack(side="left")
        ttk.Entry(post_filter_frame, textvariable=self.post_filter_start_var, width=20).pack(side="left", padx=(8, 4))
        ttk.Label(post_filter_frame, text="to").pack(side="left")
        ttk.Entry(post_filter_frame, textvariable=self.post_filter_end_var, width=20).pack(side="left", padx=(4, 8))
        ttk.Button(post_filter_frame, text="Apply", command=self.apply_post_analysis_filter).pack(side="left", padx=(0, 6))
        ttk.Button(post_filter_frame, text="Clear", command=self.clear_post_analysis_filter).pack(side="left")

        columns = (
            "selected", "prior_local_time", "number_missing",
            "after_local_time", "prior_row_id", "after_row_id"
        )

        table_wrap = ttk.Frame(results_frame)
        table_wrap.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(table_wrap, columns=columns, show="headings", height=12)
        ysb = ttk.Scrollbar(table_wrap, orient="vertical", command=self.tree.yview)
        xsb = ttk.Scrollbar(table_wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        table_wrap.rowconfigure(0, weight=1)
        table_wrap.columnconfigure(0, weight=1)

        self.tree.bind("<Double-1>", self.toggle_selected_row)

        headings = {
            "selected": "Select",
            "prior_local_time": "Prior Date",
            "number_missing": "Missing Rows",
            "after_local_time": "After Date",
            "prior_row_id": "Prior Row",
            "after_row_id": "After Row",
        }
        widths = {
            "selected": 70,
            "prior_local_time": 190,
            "number_missing": 110,
            "after_local_time": 190,
            "prior_row_id": 110,
            "after_row_id": 110,
        }
        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], anchor="w")

        log_frame = ttk.LabelFrame(outer, text="Log", padding=10)
        log_frame.pack(fill="both", expand=False, pady=(8, 0))
        self.log_text = Text(log_frame, height=7, wrap="word")
        self.log_text.pack(fill="both", expand=True)

    def browse_report_logo(self):
        path = filedialog.askopenfilename(
            title="Select Report Logo",
            filetypes=[("Image Files", "*.png *.jpg *.jpeg"), ("All Files", "*.*")]
        )
        if path:
            self.report_logo_path_var.set(path)

    def copy_gui_logo_to_report(self):
        if os.path.exists(self.default_gui_logo_path):
            self.report_logo_path_var.set(self.default_gui_logo_path)

    def update_logo_preview(self):
        if not self.logo_label:
            return

        path = self.default_gui_logo_path
        if not path or not os.path.exists(path):
            self.logo_label.configure(text="No GUI Logo Found", image="")
            self.logo_photo = None
            return

        if Image is None or ImageTk is None:
            self.logo_label.configure(text=os.path.basename(path), image="")
            self.logo_photo = None
            return

        try:
            img = Image.open(path)
            img.thumbnail((180, 90))
            self.logo_photo = ImageTk.PhotoImage(img)
            self.logo_label.configure(image=self.logo_photo, text="")
        except Exception:
            self.logo_label.configure(text=os.path.basename(path), image="")
            self.logo_photo = None

    def log(self, text: str):
        self.log_text.insert(END, text + "\n")
        self.log_text.see(END)

    def _build_date_row(self, parent, row, label, year_var, month_var, day_var, hour_var, minute_var):
        years = ["Any"] + [str(y) for y in range(2000, 2051)]
        months = ["Any"] + [f"{m:02d}" for m in range(1, 13)]
        days = ["Any"] + [f"{d:02d}" for d in range(1, 32)]
        hours = [f"{h:02d}" for h in range(24)]
        minutes = [f"{m:02d}" for m in range(60)]
        ttk.Label(parent, text=f"{label} Date").grid(row=row, column=0, sticky="w")
        ttk.Combobox(parent, textvariable=year_var, values=years, state="readonly", width=8).grid(row=row, column=1, padx=(5, 5))
        ttk.Combobox(parent, textvariable=month_var, values=months, state="readonly", width=6).grid(row=row, column=2, padx=(0, 5))
        ttk.Combobox(parent, textvariable=day_var, values=days, state="readonly", width=6).grid(row=row, column=3, padx=(0, 12))
        ttk.Label(parent, text="Time").grid(row=row, column=4, sticky="w")
        ttk.Combobox(parent, textvariable=hour_var, values=hours, state="readonly", width=6).grid(row=row, column=5, padx=(5, 5))
        ttk.Label(parent, text=":").grid(row=row, column=6)
        ttk.Combobox(parent, textvariable=minute_var, values=minutes, state="readonly", width=6).grid(row=row, column=7, padx=(5, 0))

    def cleanup_temp_dir(self):
        if self.temp_work_dir and os.path.isdir(self.temp_work_dir):
            shutil.rmtree(self.temp_work_dir, ignore_errors=True)
        self.temp_work_dir = None
        self.active_db_path = None
        self.active_wal_path = None

    def on_close(self):
        self.cleanup_temp_dir()
        self.root.destroy()

    def browse_db(self):
        path = filedialog.askopenfilename(
            title="Select SQLite database",
            filetypes=[("SQLite Databases", "*.db *.sqlite *.sqlite3 *.storedata *.sqlitedb *.db3"), ("All Files", "*.*")]
        )
        if path:
            self.db_path.set(path)

    def browse_zip(self):
        path = filedialog.askopenfilename(
            title="Select ZIP container",
            filetypes=[("ZIP Files", "*.zip"), ("All Files", "*.*")]
        )
        if path:
            self.zip_path.set(path)
            self.scan_zip_for_databases()

    def scan_zip_for_databases(self):
        zip_path = self.zip_path.get().strip()
        if not zip_path or not os.path.exists(zip_path):
            messagebox.showerror(APP_TITLE, "Please select a valid ZIP container.")
            return
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                members = [info.filename for info in zf.infolist() if not info.is_dir() and looks_like_db_candidate(info.filename)]
            self.zip_members = sorted(set(members), key=str.lower)
            self.filtered_zip_members = list(self.zip_members)
            self.refresh_zip_listbox()
            if self.filtered_zip_members:
                self.zip_listbox.selection_clear(0, END)
                self.zip_listbox.selection_set(0)
                self.zip_listbox.activate(0)
            self.log(f"Found {len(self.zip_members)} database candidates in ZIP.")
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Failed to scan ZIP:\n{exc}")

    def refresh_zip_listbox(self):
        self.zip_listbox.delete(0, END)
        for item in self.filtered_zip_members:
            self.zip_listbox.insert(END, item)

    def filter_zip_candidates(self):
        keyword = self.db_search_var.get().strip().lower()
        self.filtered_zip_members = list(self.zip_members) if not keyword else [m for m in self.zip_members if keyword in m.lower()]
        self.refresh_zip_listbox()
        if self.filtered_zip_members:
            self.zip_listbox.selection_clear(0, END)
            self.zip_listbox.selection_set(0)
            self.zip_listbox.activate(0)
        self.log(f"ZIP DB filter applied. {len(self.filtered_zip_members)} candidates shown.")

    def _extract_zip_member_to_temp(self, zf, member_name, dest_dir, new_name=None):
        out_name = new_name or Path(member_name).name
        out_path = os.path.join(dest_dir, out_name)
        with zf.open(member_name) as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return out_path

    def _find_matching_wal_member(self, member_name, zip_names):
        lookup = {n.lower(): n for n in zip_names}
        p = Path(member_name)
        candidates = [
            member_name + "-wal",
            str(p.parent / (p.name + "-wal")),
            str(p.parent / (p.stem + ".db-wal")),
            str(p.parent / (p.name + ".wal")),
            str(p.parent / (p.stem + ".wal")),
        ]
        for candidate in candidates:
            found = lookup.get(candidate.lower())
            if found:
                return found
        return None

    def load_tables_from_direct_db(self):
        self.cleanup_temp_dir()
        db = self.db_path.get().strip()
        if not db or not os.path.exists(db):
            messagebox.showerror(APP_TITLE, "Please select a valid direct database file.")
            return
        self.active_db_path = db
        wal_path = db + "-wal"
        self.active_wal_path = wal_path if self.use_wal_var.get() and os.path.exists(wal_path) else None
        self.load_tables()

    def load_selected_zip_db(self):
        zip_path = self.zip_path.get().strip()
        if not zip_path or not os.path.exists(zip_path):
            messagebox.showerror(APP_TITLE, "Please select a valid ZIP container.")
            return
        selection = self.zip_listbox.curselection()
        if not selection:
            if self.filtered_zip_members:
                self.zip_listbox.selection_set(0)
                selection = (0,)
            else:
                messagebox.showinfo(APP_TITLE, "No ZIP database candidate is selected.")
                return

        member = self.filtered_zip_members[selection[0]]
        self.cleanup_temp_dir()
        self.temp_work_dir = tempfile.mkdtemp(prefix="sqlite_db_gap_report_")

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                self.active_db_path = self._extract_zip_member_to_temp(zf, member, self.temp_work_dir, "active_db.sqlite")
                self.active_wal_path = None
                if self.use_wal_var.get():
                    wal_member = self._find_matching_wal_member(member, names)
                    if wal_member:
                        self.active_wal_path = self._extract_zip_member_to_temp(
                            zf, wal_member, self.temp_work_dir, "active_db.sqlite-wal"
                        )
                        self.log(f"Loaded WAL from ZIP: {wal_member}")
                    else:
                        self.log("No matching WAL file found in ZIP for selected database.")
            self.db_path.set(f"ZIP::{member}")
            self.load_tables()
        except Exception as exc:
            self.cleanup_temp_dir()
            messagebox.showerror(APP_TITLE, f"Failed to load selected DB from ZIP:\n{exc}")

    def _connect_db(self, db_path):
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
        except Exception:
            pass
        return conn

    def load_tables(self):
        db = self.active_db_path
        if not db or not os.path.exists(db):
            messagebox.showerror(APP_TITLE, "No active database is loaded.")
            return
        try:
            with self._connect_db(db) as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                tables = [r[0] for r in cur.fetchall()]
                self.columns_by_table = {}
                for table in tables:
                    cur.execute(f"PRAGMA table_info({sql_ident(table)})")
                    self.columns_by_table[table] = [r[1] for r in cur.fetchall()]
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Failed to load tables:\n{exc}")
            return

        self.tables = sorted(tables, key=lambda t: (0 if any(h in t.lower() for h in TABLE_HINTS) else 1, t.lower()))
        self.table_combo["values"] = self.tables
        if self.tables:
            self.table_var.set(self.tables[0])
            self.on_table_selected()

        wal_text = "Yes" if self.active_wal_path else "No"
        self.log(f"Loaded {len(self.tables)} tables from active database. WAL loaded: {wal_text}")

    def on_table_selected(self):
        table = self.table_var.get().strip()
        cols = self.columns_by_table.get(table, [])
        self.id_combo["values"] = cols
        self.date_combo["values"] = cols
        self.address_combo["values"] = [""] + cols

        lower_map = {c.lower(): c for c in cols}
        id_guess = next((lower_map[c.lower()] for c in COMMON_ID_COLUMNS if c.lower() in lower_map), cols[0] if cols else "")
        date_guess = next((lower_map[c.lower()] for c in COMMON_DATE_COLUMNS if c.lower() in lower_map), "")
        address_guess = next((lower_map[c.lower()] for c in COMMON_ADDRESS_COLUMNS if c.lower() in lower_map), "")

        self.id_col_var.set(id_guess)
        self.date_col_var.set(date_guess)
        self.address_col_var.set(address_guess)
        self.log(f"Selected table: {table}")

    def clear_filters(self):
        self.start_year_var.set("Any")
        self.start_month_var.set("Any")
        self.start_day_var.set("Any")
        self.start_hour_var.set("00")
        self.start_minute_var.set("00")
        self.end_year_var.set("Any")
        self.end_month_var.set("Any")
        self.end_day_var.set("Any")
        self.end_hour_var.set("23")
        self.end_minute_var.set("59")
        self.log("Pre-analysis date filters cleared.")

    def _build_filter_dt(self, year_var, month_var, day_var, hour_var, minute_var, is_end=False):
        y, m, d = year_var.get(), month_var.get(), day_var.get()
        if "Any" in (y, m, d):
            return None
        try:
            return datetime(int(y), int(m), int(d), int(hour_var.get()), int(minute_var.get()), 59 if is_end else 0)
        except Exception:
            return None

    def _parse_post_filter_input(self, text_value):
        value = (text_value or "").strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except Exception:
                pass
        raise ValueError("Invalid date")

    def _forced_mode(self):
        return {
            "Auto Detect": None,
            "UNIX Seconds": "unix_seconds",
            "UNIX Milliseconds": "unix_milliseconds",
            "UNIX Microseconds": "unix_microseconds",
            "UNIX Nanoseconds": "unix_nanoseconds",
            "Mac Absolute Seconds": "mac_absolute_seconds",
            "Mac Absolute Milliseconds": "mac_absolute_milliseconds",
            "Mac Absolute Microseconds": "mac_absolute_microseconds",
            "Mac Absolute Nanoseconds": "mac_absolute_nanoseconds",
        }.get(self.timestamp_mode_var.get())

    def run_analysis(self):
        db = self.active_db_path
        if not db or not os.path.exists(db):
            messagebox.showerror(APP_TITLE, "Load a direct DB or a ZIP-selected DB first.")
            return

        table = self.table_var.get().strip()
        id_col = self.id_col_var.get().strip()
        date_col = self.date_col_var.get().strip()
        if not table or not id_col:
            messagebox.showerror(APP_TITLE, "Please select a table and row ID column.")
            return

        min_gap = safe_int(self.min_gap_var.get())
        if min_gap is None or min_gap < 1:
            min_gap = 1
            self.min_gap_var.set("1")

        tzinfo = parse_utc_offset(self.tz_var.get())
        start_filter = self._build_filter_dt(
            self.start_year_var, self.start_month_var, self.start_day_var,
            self.start_hour_var, self.start_minute_var, False
        )
        end_filter = self._build_filter_dt(
            self.end_year_var, self.end_month_var, self.end_day_var,
            self.end_hour_var, self.end_minute_var, True
        )
        forced_mode = self._forced_mode()

        select_cols = [sql_ident(id_col)]
        select_cols.append(sql_ident(date_col) if date_col else "NULL AS raw_date")
        query = f"SELECT {', '.join(select_cols)} FROM {sql_ident(table)} WHERE {sql_ident(id_col)} IS NOT NULL ORDER BY {sql_ident(id_col)}"

        try:
            with self._connect_db(db) as conn:
                cur = conn.cursor()
                cur.execute(query)
                records = cur.fetchall()
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Failed to query table:\n{exc}")
            return

        normalized = []
        for rec in records:
            row_id = safe_int(rec[0])
            raw_date = rec[1] if len(rec) > 1 else None
            if row_id is None:
                continue
            dt_obj, _ = convert_timestamp(raw_date, tzinfo, forced_mode)
            normalized.append({"row_id": row_id, "dt": dt_obj, "date_text": fmt_dt(dt_obj)})

        if len(normalized) < 2:
            messagebox.showinfo(APP_TITLE, "Not enough rows to analyze gaps.")
            return

        gap_rows = []
        total_missing = 0

        for idx in range(len(normalized) - 1):
            current_row = normalized[idx]
            next_row = normalized[idx + 1]
            gap = next_row["row_id"] - current_row["row_id"]

            if gap <= 1:
                continue

            gap_size = gap - 1
            if gap_size < min_gap:
                continue

            include_gap = True
            if start_filter or end_filter:
                compare_dates = [d for d in (current_row["dt"], next_row["dt"]) if d is not None]
                if compare_dates:
                    if start_filter:
                        include_gap = any(d.replace(tzinfo=None) >= start_filter for d in compare_dates)
                    if include_gap and end_filter:
                        include_gap = any(d.replace(tzinfo=None) <= end_filter for d in compare_dates)
                else:
                    include_gap = False

            if not include_gap:
                continue

            gap_rows.append({
                "selected": False,
                "prior_local_time": current_row["date_text"],
                "number_missing": gap_size,
                "after_local_time": next_row["date_text"],
                "prior_row_id": current_row["row_id"],
                "after_row_id": next_row["row_id"],
            })
            total_missing += gap_size

        self.analysis_rows = gap_rows
        self.filtered_analysis_rows = []
        self.summary = {
            "table": table,
            "analyzed_rows": len(normalized),
            "missing_rows": total_missing,
            "gap_events": len(gap_rows),
            "timezone": self.tz_var.get(),
            "wal_used": bool(self.active_wal_path),
        }
        self.refresh_results()

    def refresh_results(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

        rows = self.filtered_analysis_rows if self.filtered_analysis_rows else self.analysis_rows
        for idx, row in enumerate(rows):
            self.tree.insert(
                "",
                END,
                iid=str(idx),
                values=(
                    "☑" if row.get("selected") else "☐",
                    row["prior_local_time"],
                    row["number_missing"],
                    row["after_local_time"],
                    row["prior_row_id"],
                    row["after_row_id"],
                ),
            )

        if self.summary:
            wal_text = "Yes" if self.summary.get("wal_used") else "No"
            self.summary_label.config(
                text=(
                    f"Table: {self.summary['table']} | Rows analyzed: {self.summary['analyzed_rows']} | "
                    f"Missing rows in gaps: {self.summary['missing_rows']} | Gap events: {self.summary['gap_events']} | "
                    f"TZ: {self.summary['timezone']}"
                )
            )
            self.wal_status_label.config(text=f"WAL Used: {wal_text}")
        else:
            self.summary_label.config(text="No analysis run yet.")
            self.wal_status_label.config(text="WAL Used: No")

    def apply_post_analysis_filter(self):
        if not self.analysis_rows:
            return

        try:
            start_dt = self._parse_post_filter_input(self.post_filter_start_var.get())
            end_dt = self._parse_post_filter_input(self.post_filter_end_var.get())
        except ValueError:
            messagebox.showerror(APP_TITLE, "Use YYYY-MM-DD HH:MM or YYYY-MM-DD for post-analysis date filtering.")
            return

        filtered = []
        for row in self.analysis_rows:
            cmp_dt = None
            for candidate in (row.get("prior_local_time", ""), row.get("after_local_time", "")):
                if candidate:
                    try:
                        cmp_dt = datetime.strptime(candidate[:19], "%Y-%m-%d %H:%M:%S")
                        break
                    except Exception:
                        continue
            if cmp_dt is None:
                continue
            if start_dt and cmp_dt < start_dt:
                continue
            if end_dt and cmp_dt > end_dt:
                continue
            filtered.append(row)

        self.filtered_analysis_rows = filtered
        self.refresh_results()
        self.log(f"Post-analysis filter applied. {len(filtered)} rows shown.")

    def clear_post_analysis_filter(self):
        self.post_filter_start_var.set("")
        self.post_filter_end_var.set("")
        self.filtered_analysis_rows = []
        self.refresh_results()
        self.log("Post-analysis filter cleared.")

    def toggle_selected_row(self, event):
        item = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)
        if not item or col != "#1":
            return

        rows = self.filtered_analysis_rows if self.filtered_analysis_rows else self.analysis_rows
        idx = int(item)
        if 0 <= idx < len(rows):
            rows[idx]["selected"] = not rows[idx].get("selected", False)
            self.refresh_results()

    def select_all_rows(self):
        rows = self.filtered_analysis_rows if self.filtered_analysis_rows else self.analysis_rows
        for row in rows:
            row["selected"] = True
        self.refresh_results()

    def clear_selected_rows(self):
        rows = self.filtered_analysis_rows if self.filtered_analysis_rows else self.analysis_rows
        for row in rows:
            row["selected"] = False
        self.refresh_results()

    def _rows_for_export(self, checked_only=False):
        source_rows = self.filtered_analysis_rows if self.filtered_analysis_rows else self.analysis_rows
        rows = [r for r in source_rows if r.get("selected")] if checked_only else list(source_rows)

        def dt_key(r):
            try:
                return datetime.strptime(r.get("prior_local_time", "")[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return datetime.min

        return sorted(rows, key=dt_key)

    def export_from_choice(self):
        checked_only = self.export_scope_var.get() == "Checked rows only"
        if self.export_format_var.get() == "CSV":
            self.export_csv(checked_only)
        else:
            self.export_pdf(checked_only)

    def export_csv(self, checked_only=False):
        export_rows = self._rows_for_export(checked_only)
        if not export_rows:
            messagebox.showinfo(APP_TITLE, "No analysis results to export for the selected scope.")
            return

        path = filedialog.asksaveasfilename(
            title="Export CSV",
            defaultextension=".csv",
            initialfile="sqlite_db_gap_report.csv",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not path:
            return

        fields = [
            "prior_local_time",
            "number_missing",
            "after_local_time",
            "prior_row_id",
            "after_row_id",
        ]

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for row in export_rows:
                    writer.writerow({k: row.get(k, "") for k in fields})
            self.log(f"CSV exported: {path}")
            messagebox.showinfo(APP_TITLE, f"CSV exported successfully:\n{path}")
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Failed to export CSV:\n{exc}")

    def export_pdf(self, checked_only=False):
        export_rows = self._rows_for_export(checked_only)
        if not export_rows:
            messagebox.showinfo(APP_TITLE, "No analysis results to export for the selected scope.")
            return

        path = filedialog.asksaveasfilename(
            title="Export PDF",
            defaultextension=".pdf",
            initialfile="sqlite_db_gap_report.pdf",
            filetypes=[("PDF Files", "*.pdf")]
        )
        if not path:
            return

        try:
            from reportlab.lib.pagesizes import letter, landscape
            from reportlab.lib.units import inch
            from reportlab.pdfgen import canvas
            from reportlab.lib.utils import ImageReader
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"PDF export requires reportlab. Install it with: pip install reportlab\n\nError:\n{exc}")
            return

        try:
            c = canvas.Canvas(path, pagesize=landscape(letter))
            width, height = landscape(letter)
            left = 0.4 * inch
            y = height - 0.45 * inch
            row_h = 18

            cols = [
                ("Prior Date", 2.35 * inch),
                ("Missing Rows", 1.15 * inch),
                ("After Date", 2.35 * inch),
                ("Prior Row", 1.15 * inch),
                ("After Row", 1.15 * inch),
            ]

            def draw_header():
                nonlocal y
                logo_path = self.report_logo_path_var.get().strip()
                title_x = left

                if logo_path and os.path.exists(logo_path):
                    try:
                        img = ImageReader(logo_path)
                        c.drawImage(img, left, y - 40, width=90, height=40, preserveAspectRatio=True, mask="auto")
                        title_x = left + 100
                    except Exception:
                        title_x = left

                c.setFont("Helvetica-Bold", 12)
                c.drawString(title_x, y, APP_TITLE)
                y -= 18
                c.setFont("Helvetica", 9)
                c.drawString(title_x, y, self.summary_label.cget("text"))
                y -= 14
                c.drawString(title_x, y, self.wal_status_label.cget("text"))
                y -= 22

                x = left
                c.setFont("Helvetica-Bold", 9)
                for title, w in cols:
                    c.rect(x, y - row_h + 4, w, row_h, stroke=1, fill=0)
                    c.drawString(x + 4, y - 9, title)
                    x += w
                y -= row_h

            def new_page():
                nonlocal y
                c.showPage()
                y = height - 0.45 * inch
                draw_header()
                c.setFont("Helvetica", 8)

            draw_header()
            c.setFont("Helvetica", 8)

            for row in export_rows:
                if y < 40:
                    new_page()

                values = [
                    str(row.get("prior_local_time", "")),
                    str(row.get("number_missing", "")),
                    str(row.get("after_local_time", "")),
                    str(row.get("prior_row_id", "")),
                    str(row.get("after_row_id", "")),
                ]

                x = left
                for (title, w), value in zip(cols, values):
                    c.rect(x, y - row_h + 4, w, row_h, stroke=1, fill=0)
                    c.drawString(x + 4, y - 9, value[:40])
                    x += w
                y -= row_h

            c.save()
            self.log(f"PDF exported: {path}")
            messagebox.showinfo(APP_TITLE, f"PDF exported successfully:\n{path}")
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Failed to export PDF:\n{exc}")


def main():
    root = Tk()
    style = ttk.Style()
    try:
        style.theme_use("vista")
    except Exception:
        pass
    ForensicSmsGapAnalyzer(root)
    root.mainloop()


if __name__ == "__main__":
    main()
    