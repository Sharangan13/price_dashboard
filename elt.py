import pdfplumber
import requests
import psycopg2
import re, os, time
from datetime import datetime, timedelta

# ── CONFIG ──────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",   "db.dszaprwbwjcveutcveqm.supabase.co"),
    "port":     int(os.environ.get("DB_PORT", 6543)),
    "dbname":   os.environ.get("DB_NAME",   "postgres"),
    "user":     os.environ.get("DB_USER",   "postgres"),
    "password": os.environ.get("DB_PASS",   "Sharangan1998@")
}

PDF_FOLDER = "downloaded_pdfs"
os.makedirs(PDF_FOLDER, exist_ok=True)

# ── Test Supabase Connection ─────────────────────────────
def test_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close(); conn.close()
        print("✅ Supabase connected!")
        print(f"   PostgreSQL version: {version.split(',')[0]}")
        return True
    except psycopg2.OperationalError as e:
        print("❌ Supabase connection failed!")
        print(f"   Reason: {e}")
        return False
    except Exception as e:
        print("❌ Unexpected error:", e)
        return False

# ── Get already-processed dates from DB ─────────────────
def get_processed_dates():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT report_date::text FROM prices")
    dates = {row[0] for row in cur.fetchall()}
    cur.close(); conn.close()
    return dates

# ── Get skipped dates (holidays / no PDF) ───────────────
def get_skipped_dates():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT report_date::text FROM skipped_dates")
    dates = {row[0] for row in cur.fetchall()}
    cur.close(); conn.close()
    return dates

# ── Mark a date as skipped ───────────────────────────────
def mark_skipped(date_str, reason="no PDF"):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO skipped_dates (report_date, reason)
        VALUES (%s, %s)
        ON CONFLICT (report_date) DO NOTHING
    ''', (date_str, reason))
    conn.commit()
    cur.close(); conn.close()

# ── URL Builder ──────────────────────────────────────────
def get_pdf_urls(date):
    d = date.strftime("%Y%m%d")
    base = "https://www.cbsl.gov.lk/sites/default/files/cbslweb_documents/statistics/pricerpt"
    return [
        f"{base}/price_report_{d}_e.pdf",
        f"{base}/price_report_{d}e.pdf",
        f"{base}/price_report_{d}.pdf",
    ]

# ── Download PDF ─────────────────────────────────────────
def download_pdf(date):
    fname = os.path.join(PDF_FOLDER, f"report_{date.strftime('%Y%m%d')}.pdf")
    if os.path.exists(fname):
        return fname

    headers = {"User-Agent": "Mozilla/5.0"}
    for url in get_pdf_urls(date):
        for attempt in range(3):
            try:
                r = requests.get(url, headers=headers, timeout=20)
                if r.status_code == 200 and (
                    "application/pdf" in r.headers.get("Content-Type", "") or
                    b"%PDF" in r.content[:10]
                ):
                    print(f"   ✔ Downloaded: {url}")
                    with open(fname, 'wb') as f:
                        f.write(r.content)
                    return fname
            except Exception:
                time.sleep(2)
    return None

# ── Parse Values ─────────────────────────────────────────
def parse_line_values(tokens):
    values = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        tc = t.replace(',', '')

        if t == 'n.a.':
            values.append(None)
            i += 1
        elif re.match(r'^\d+$', tc):
            if i + 1 < len(tokens) and re.match(r'^\d+\.\d+$', tokens[i+1].replace(',', '')):
                combined = tc + tokens[i+1].replace(',', '')
                values.append(float(combined))
                i += 2
            else:
                values.append(float(tc))
                i += 1
        elif re.match(r'^\d[\d,]*\.\d+$', tc):
            values.append(float(tc))
            i += 1
        else:
            i += 1
    return values

# ── ITEMS ────────────────────────────────────────────────
ITEMS = [
    ("Beans","Rs./kg","Vegetables",10),
    ("Carrot","Rs./kg","Vegetables",10),
    ("Cabbage","Rs./kg","Vegetables",10),
    ("Tomato","Rs./kg","Vegetables",10),
    ("Brinjal","Rs./kg","Vegetables",10),
    ("Pumpkin","Rs./kg","Vegetables",10),
    ("Snake gourd","Rs./kg","Vegetables",10),
    ("Green Chilli","Rs./kg","Vegetables",10),
    ("Lime","Rs./kg","Vegetables",10),
    ("Red Onion (Local)","Rs./kg","Other",10),
    ("Red Onion (lmp)","Rs./kg","Other",10),
    ("Big Onion (Local)","Rs./kg","Other",10),
    ("Big Onion (Imp)","Rs./kg","Other",10),
    ("Potato (Local)","Rs./kg","Other",10),
    ("Potato (Imp)","Rs./kg","Other",10),
    ("Dried Chilli (Imp)","Rs./kg","Other",10),
    ("Coconut (Avg.)","Rs./Nut","Other",10),
    ("Coconut oil","Rs./Ltr","Other",6),
    ("Red Dhal","Rs./kg","Other",6),
    ("Sugar (White)","Rs./kg","Other",6),
    ("Egg (White)","Rs./Each","Other",6),
    ("Katta (Imp)","Rs./kg","Other",6),
    ("Sprat (Imp)","Rs./kg","Other",6),
    ("Banana (Sour)","Rs./kg","Fruits",10),
    ("Papaw","Rs./kg","Fruits",10),
    ("Pineapple","Rs./kg","Fruits",10),
    ("Apple (Imp)","Rs./Each","Fruits",4),
    ("Orange (Imp)","Rs./Each","Fruits",4),
]

ITEM_DB_NAME = {"Red Onion (lmp)": "Red Onion (Imp)"}

def get_db_name(name):
    return ITEM_DB_NAME.get(name, name)

# ── Mapping ──────────────────────────────────────────────
def map_values(vals, cols):
    def g(i): return vals[i] if i < len(vals) else None

    if cols == 10:
        return g(1), g(3), g(5), g(7), g(9)
    elif cols == 6:
        return g(1), None, g(3), None, g(5)
    elif cols == 4:
        return None, None, g(1), None, g(3)
    return None, None, None, None, None

# ── Extract ──────────────────────────────────────────────
def extract_prices(fp, report_date):
    records = []
    try:
        with pdfplumber.open(fp) as pdf:
            if len(pdf.pages) < 2:
                return records
            text = pdf.pages[1].extract_text()
    except Exception as e:
        print(f"   ⚠ Corrupt PDF, skipping: {e}")
        os.remove(fp)
        return records

    if not text:
        return records

    for line in text.split('\n'):
        line = line.strip()
        for name, unit, cat, cols in ITEMS:
            prefix = f"{name} {unit}"
            if prefix in line:
                tokens = line.split()[len(prefix.split()):]
                vals = parse_line_values(tokens)
                if not any(v is not None for v in vals):
                    continue
                ws_p, ws_d, rt_p, rt_d, rt_n = map_values(vals, cols)
                records.append((
                    report_date, cat, get_db_name(name), unit,
                    ws_p, ws_d, rt_p, rt_d, rt_n
                ))
                break
    return records

# ── Save to DB ───────────────────────────────────────────
def save_to_db(records):
    if not records:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.executemany('''
            INSERT INTO prices
            (report_date, category, item, unit,
             pettah_ws, dambulla_ws,
             pettah_rt, dambulla_rt, narahenpita_rt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (report_date, item) DO NOTHING
        ''', records)
        conn.commit()
        print(f"   💾 Saved {len(records)} rows")
    except Exception as e:
        conn.rollback()
        print("DB Error:", e)
    finally:
        cur.close(); conn.close()

# ── MAIN ETL ─────────────────────────────────────────────
def run_etl(start_date=None, end_date=None, days_back=7):

    if not test_db_connection():
        return

    if not end_date:
        end_date = datetime.today()
    if not start_date:
        start_date = end_date - timedelta(days=days_back)

    processed_dates = get_processed_dates()
    skipped_dates   = get_skipped_dates()
    all_done        = processed_dates | skipped_dates

    print(f"✅ Already in DB  : {len(processed_dates)} dates")
    print(f"⏭  Known no-PDF   : {len(skipped_dates)} dates")
    print(f"📅 Checking range : {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")

    current = start_date
    total_ok, total_skip = 0, 0

    while current <= end_date:

        # Skip weekends
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue

        date_str = current.strftime('%Y-%m-%d')

        if date_str in all_done:
            print(f"⏭  {date_str} already done, skipping")
            current += timedelta(days=1)
            continue

        print(f"\n📅 {date_str}")
        fp = download_pdf(current)

        if not fp:
            print("   ⚠ No PDF — saving to skipped_dates")
            mark_skipped(date_str)
            total_skip += 1
            current += timedelta(days=1)
            continue

        recs = extract_prices(fp, date_str)
        if not recs:
            print("   ⚠ No data extracted — saving to skipped_dates")
            mark_skipped(date_str, reason="no data")
            total_skip += 1
        else:
            save_to_db(recs)
            total_ok += 1

        current += timedelta(days=1)

    print(f"\n🎯 DONE  ✅ Saved: {total_ok}  ⏭ Skipped: {total_skip}")

# ── RUN ──────────────────────────────────────────────────
run_etl(start_date=datetime(2026, 1, 1))