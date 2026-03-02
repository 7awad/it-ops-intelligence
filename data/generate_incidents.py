"""
generate_incidents.py
Generates 1,000 realistic mock IT incident tickets and saves to CSV.
"""

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

# --- Config ---
NUM_RECORDS = 1000
OUTPUT_FILE = "data/incidents.csv"

# --- Lookup tables ---
CATEGORIES = ["Network", "Hardware", "Software", "Security", "Access & Identity"]

SEVERITY_LEVELS = ["Low", "Medium", "High", "Critical"]
SEVERITY_WEIGHTS = [0.35, 0.35, 0.20, 0.10]

STATUS_OPTIONS = ["Resolved", "In Progress", "Open", "Closed"]
STATUS_WEIGHTS = [0.55, 0.20, 0.15, 0.10]

AFFECTED_SYSTEMS = [
    "Email Server", "VPN Gateway", "Active Directory", "ERP System",
    "File Server", "Database Cluster", "CI/CD Pipeline", "Firewall",
    "Load Balancer", "Backup System", "HR Portal", "Finance Platform",
    "Wi-Fi Infrastructure", "Endpoint Security", "Cloud Storage"
]

INCIDENT_TEMPLATES = {
    "Network": [
        "Users reporting slow internet connectivity across floor {floor}.",
        "VPN tunnel dropping intermittently for remote employees.",
        "Switch port failure detected on floor {floor}, affecting {n} workstations.",
        "DNS resolution failures reported by {n} users.",
        "Network latency spike observed on core router — avg {ms}ms.",
    ],
    "Hardware": [
        "Workstation fan failure reported — device overheating in room {floor}.",
        "Server disk {disk} showing SMART errors — potential failure imminent.",
        "Monitor flickering reported on {n} machines in department {dept}.",
        "UPS battery failure in server room — risk of unplanned shutdown.",
        "Printer offline — {n} users unable to print on floor {floor}.",
    ],
    "Software": [
        "Application crash on ERP module — {n} users affected.",
        "OS update causing compatibility issues with {system}.",
        "Memory leak detected in {system} — service restarted {n} times today.",
        "SSL certificate expired on internal portal — browser warnings reported.",
        "Scheduled batch job failed — {n} records unprocessed.",
    ],
    "Security": [
        "Suspicious login attempt detected from IP {ip} — account locked.",
        "Phishing email reported by {n} employees — malicious link identified.",
        "Unauthorized access attempt on {system} at {time}.",
        "Malware detected on endpoint in {dept} — device quarantined.",
        "Privileged account used outside business hours — flagged for review.",
    ],
    "Access & Identity": [
        "User locked out of Active Directory after {n} failed attempts.",
        "New employee onboarding — {n} accounts need provisioning.",
        "Password reset requests exceeding normal volume — {n} tickets today.",
        "MFA enrollment failing for {n} users post-migration.",
        "Access revocation pending for {n} departed employees.",
    ]
}

RESOLUTION_TEMPLATES = {
    "Network": "Network team investigated and resolved connectivity issue. Root cause: {cause}.",
    "Hardware": "Hardware replaced/repaired by on-site technician. Device back online.",
    "Software": "Software team applied patch/restart. Issue resolved after {n} minutes.",
    "Security": "Security team investigated, contained threat, and updated policies.",
    "Access & Identity": "IT admin provisioned/reset access. Users notified via email."
}

CAUSES = [
    "misconfigured routing table", "failed switch port", "ISP outage",
    "firmware bug", "capacity overload", "hardware fault", "expired certificate"
]


def random_incident_description(category):
    template = random.choice(INCIDENT_TEMPLATES[category])
    return template.format(
        floor=random.randint(1, 10),
        n=random.randint(2, 50),
        ms=random.randint(200, 2000),
        disk=f"DISK-{random.randint(1,4)}",
        dept=fake.bs().split()[0].capitalize(),
        system=random.choice(AFFECTED_SYSTEMS),
        ip=fake.ipv4(),
        time=fake.time(),
        cause=random.choice(CAUSES)
    )


def random_resolution(category, status):
    if status in ["Open", "In Progress"]:
        return "Investigation ongoing. No resolution yet."
    template = RESOLUTION_TEMPLATES[category]
    return template.format(n=random.randint(5, 120), cause=random.choice(CAUSES))


def generate_incidents(n=NUM_RECORDS):
    records = []
    base_date = datetime.now() - timedelta(days=180)

    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        severity = random.choices(SEVERITY_LEVELS, weights=SEVERITY_WEIGHTS)[0]
        status = random.choices(STATUS_OPTIONS, weights=STATUS_WEIGHTS)[0]
        created_at = base_date + timedelta(
            days=random.randint(0, 179),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        resolution_time_hours = (
            None if status in ["Open", "In Progress"]
            else round(random.uniform(0.5, 72.0), 2)
        )
        resolved_at = (
            created_at + timedelta(hours=resolution_time_hours)
            if resolution_time_hours else None
        )

        records.append({
            "incident_id": f"INC-{i:04d}",
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "resolved_at": resolved_at.strftime("%Y-%m-%d %H:%M:%S") if resolved_at else None,
            "category": category,
            "severity": severity,
            "status": status,
            "affected_system": random.choice(AFFECTED_SYSTEMS),
            "assigned_team": f"Team-{random.choice(['Alpha','Beta','Gamma','Delta'])}",
            "reporter": fake.name(),
            "description": random_incident_description(category),
            "resolution_notes": random_resolution(category, status),
            "resolution_time_hours": resolution_time_hours,
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    df = generate_incidents()
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Generated {len(df)} incident records → {OUTPUT_FILE}")
    print(df.head(3).to_string())