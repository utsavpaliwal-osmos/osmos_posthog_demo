import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ===== CONFIG =====
API_KEY = os.getenv("POSTHOG_API_KEY")
PROJECT_ID = os.getenv("POSTHOG_PROJECT_ID")
HOST = os.getenv("POSTHOG_HOST")
DATE_RANGE = "-30d"
BASE_URL = f"https://us.posthog.com/api/projects/{PROJECT_ID}/query/"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

HOST_FILTER = [
    {"key": "$host", "value": [HOST], "operator": "exact", "type": "event"}
]


def query(payload):
    resp = requests.post(BASE_URL, headers=HEADERS, json={"query": payload})
    resp.raise_for_status()
    return resp.json()


def print_table(headers, rows):
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
    fmt = " | ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("-+-".join("-" * w for w in col_widths))
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))
    print()


def fetch_overview():
    print("=" * 60)
    print(f" WEB ANALYTICS REPORT — {HOST}")
    print(f" Date range: last 30 days")
    print(f" Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    data = query({
        "kind": "WebOverviewQuery",
        "dateRange": {"date_from": DATE_RANGE},
        "properties": HOST_FILTER,
    })

    print("\n📊 OVERVIEW")
    for r in data["results"]:
        val = r["value"]
        if val is None:
            val = "N/A"
        elif r["kind"] == "duration_s":
            mins, secs = divmod(int(val), 60)
            val = f"{mins}m {secs}s"
        elif r["kind"] == "percentage":
            val = f"{val:.1f}%"
        else:
            val = f"{int(val):,}"
        print(f"  {r['key']:>20s}: {val}")
    print()


def fetch_top_pages():
    data = query({
        "kind": "WebStatsTableQuery",
        "dateRange": {"date_from": DATE_RANGE},
        "properties": HOST_FILTER,
        "breakdownBy": "Page",
        "limit": 10,
    })
    print("📄 TOP PAGES")
    rows = []
    for r in data["results"]:
        page = r[0]
        visitors = int(r[1][0]) if r[1][0] else 0
        views = int(r[2][0]) if r[2][0] else 0
        rows.append([page, visitors, views])
    print_table(["Page", "Visitors", "Views"], rows)


def fetch_traffic_sources():
    data = query({
        "kind": "WebStatsTableQuery",
        "dateRange": {"date_from": DATE_RANGE},
        "properties": HOST_FILTER,
        "breakdownBy": "InitialChannelType",
        "limit": 10,
    })
    print("🔗 TRAFFIC SOURCES")
    rows = []
    for r in data["results"]:
        channel = r[0]
        visitors = int(r[1][0]) if r[1][0] else 0
        views = int(r[2][0]) if r[2][0] else 0
        rows.append([channel, visitors, views])
    print_table(["Channel", "Visitors", "Views"], rows)


def fetch_devices():
    data = query({
        "kind": "WebStatsTableQuery",
        "dateRange": {"date_from": DATE_RANGE},
        "properties": HOST_FILTER,
        "breakdownBy": "DeviceType",
        "limit": 10,
    })
    print("💻 DEVICES")
    rows = []
    for r in data["results"]:
        rows.append([r[0], int(r[1][0]) if r[1][0] else 0, int(r[2][0]) if r[2][0] else 0])
    print_table(["Device", "Visitors", "Views"], rows)


def fetch_browsers():
    data = query({
        "kind": "WebStatsTableQuery",
        "dateRange": {"date_from": DATE_RANGE},
        "properties": HOST_FILTER,
        "breakdownBy": "Browser",
        "limit": 10,
    })
    print("🌐 BROWSERS")
    rows = []
    for r in data["results"]:
        rows.append([r[0], int(r[1][0]) if r[1][0] else 0, int(r[2][0]) if r[2][0] else 0])
    print_table(["Browser", "Visitors", "Views"], rows)


def fetch_countries():
    data = query({
        "kind": "WebStatsTableQuery",
        "dateRange": {"date_from": DATE_RANGE},
        "properties": HOST_FILTER,
        "breakdownBy": "Country",
        "limit": 10,
    })
    print("🌍 COUNTRIES")
    rows = []
    for r in data["results"]:
        rows.append([r[0], int(r[1][0]) if r[1][0] else 0, int(r[2][0]) if r[2][0] else 0])
    print_table(["Country", "Visitors", "Views"], rows)


def fetch_persons():
    data = query({
        "kind": "HogQLQuery",
        "query": f"""
            SELECT
                person.properties.name as name,
                person.properties.email as email,
                person.properties.company_name as company,
                person.properties.$device_type as device,
                person.properties.$browser as browser,
                person.properties.$geoip_country_name as country,
                min(timestamp) as first_visit,
                max(timestamp) as last_visit,
                count() as pageviews
            FROM events
            WHERE properties.$host = '{HOST}'
              AND event = '$pageview'
            GROUP BY name, email, company, device, browser, country
            ORDER BY last_visit DESC
        """,
    })
    print("👤 PERSONS")
    rows = []
    for r in data["results"]:
        name = r[0] or "Anonymous"
        email = r[1] or "-"
        company = r[2] or "-"
        device = r[3] or "-"
        browser = r[4] or "-"
        country = r[5] or "-"
        first_visit = r[6][:16].replace("T", " ") if r[6] else "-"
        last_visit = r[7][:16].replace("T", " ") if r[7] else "-"
        pageviews = r[8]
        rows.append([name, email, company, device, browser, country, pageviews, first_visit, last_visit])
    print_table(
        ["Name", "Email", "Company", "Device", "Browser", "Country", "Views", "First Visit", "Last Visit"],
        rows,
    )


def fetch_custom_events():
    data = query({
        "kind": "HogQLQuery",
        "query": f"""
            SELECT
                event,
                count() as count,
                count(DISTINCT distinct_id) as unique_users
            FROM events
            WHERE properties.$host = '{HOST}'
              AND event NOT LIKE '$%'
            GROUP BY event
            ORDER BY count DESC
        """,
    })
    print("⚡ CUSTOM EVENTS")
    rows = []
    for r in data["results"]:
        rows.append([r[0], int(r[1]), int(r[2])])
    print_table(["Event", "Total Count", "Unique Users"], rows)


if __name__ == "__main__":
    fetch_overview()
    fetch_top_pages()
    fetch_traffic_sources()
    fetch_devices()
    fetch_browsers()
    fetch_countries()
    fetch_persons()
    fetch_custom_events()
