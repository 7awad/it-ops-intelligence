"""
gemini_enrich.py
Uses Google Gemini API to enrich incident records with:
- AI severity validation
- Plain-English summary
- Resolution suggestion
Processes in batches and updates BigQuery.
"""

import os
import time
import pandas as pd
import google.generativeai as genai
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET", "it_ops")
TABLE_ID = os.getenv("BQ_TABLE", "incidents")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

BATCH_SIZE = 15      # Stay within 20 RPD daily limit
SLEEP_SECS = 13      # 5 RPM = 1 request per 12s — 13s to stay safe


def fetch_unenriched_incidents(client, limit=15):
    """Fetch incidents that haven't been AI-enriched yet."""
    query = f"""
        SELECT incident_id, category, severity, affected_system, description, resolution_notes
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE ai_enriched = FALSE OR ai_enriched IS NULL
        LIMIT {limit}
    """
    df = client.query(query).to_dataframe()
    print(f"✅ Fetched {len(df)} unenriched incidents.")
    return df


def build_prompt(row):
    return f"""You are an IT operations analyst. Analyze this incident and respond in EXACTLY this format with no extra text:

SEVERITY: [Low/Medium/High/Critical]
SUMMARY: [1 sentence plain-English summary of what happened]
SUGGESTION: [1 sentence recommended resolution action]

Incident Details:
- Category: {row['category']}
- Recorded Severity: {row['severity']}
- Affected System: {row['affected_system']}
- Description: {row['description']}
- Resolution Notes: {row['resolution_notes']}
"""


def parse_gemini_response(text):
    lines = text.strip().split("\n")
    result = {"ai_severity_label": None, "ai_summary": None, "ai_resolution_suggestion": None}
    for line in lines:
        if line.startswith("SEVERITY:"):
            result["ai_severity_label"] = line.replace("SEVERITY:", "").strip()
        elif line.startswith("SUMMARY:"):
            result["ai_summary"] = line.replace("SUMMARY:", "").strip()
        elif line.startswith("SUGGESTION:"):
            result["ai_resolution_suggestion"] = line.replace("SUGGESTION:", "").strip()
    return result


def enrich_batch(df_batch):
    results = []
    total = len(df_batch)
    for i, (_, row) in enumerate(df_batch.iterrows()):
        try:
            prompt = build_prompt(row)
            response = model.generate_content(prompt)
            parsed = parse_gemini_response(response.text)
            parsed["incident_id"] = row["incident_id"]
            parsed["ai_enriched"] = True
            results.append(parsed)
            print(f"  ✅ {i+1}/{total} — {row['incident_id']} enriched")
            if i < total - 1:  # No sleep after last record
                time.sleep(SLEEP_SECS)
        except Exception as e:
            print(f"  ⚠️  {row['incident_id']} failed: {e}")
            results.append({
                "incident_id": row["incident_id"],
                "ai_severity_label": "Unknown",
                "ai_summary": "Enrichment failed.",
                "ai_resolution_suggestion": "Manual review required.",
                "ai_enriched": False
            })
    return pd.DataFrame(results)


def update_bigquery(client, enriched_df):
    """Update BigQuery rows with AI enrichment results using MERGE."""
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.incidents_temp_enrichment"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(enriched_df, temp_table, job_config=job_config)
    job.result()

    merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
        USING `{temp_table}` S
        ON T.incident_id = S.incident_id
        WHEN MATCHED THEN UPDATE SET
            T.ai_severity_label = S.ai_severity_label,
            T.ai_summary = S.ai_summary,
            T.ai_resolution_suggestion = S.ai_resolution_suggestion,
            T.ai_enriched = S.ai_enriched
    """
    client.query(merge_query).result()
    client.delete_table(temp_table)
    print(f"✅ Updated {len(enriched_df)} rows in BigQuery.")


def run_enrichment():
    bq_client = bigquery.Client(project=PROJECT_ID)
    df = fetch_unenriched_incidents(bq_client, limit=BATCH_SIZE)

    if df.empty:
        print("✅ All incidents already enriched.")
        return

    print(f"🔄 Enriching {len(df)} incidents (~{len(df) * SLEEP_SECS // 60} min)...")
    enriched = enrich_batch(df)
    update_bigquery(bq_client, enriched)
    print("✅ Enrichment complete. Run again tomorrow for more.")


if __name__ == "__main__":
    run_enrichment()