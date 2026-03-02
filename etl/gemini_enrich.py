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
model = genai.GenerativeModel("gemini-2.0-flash")  # Free tier model

BATCH_SIZE = 50  # Process 50 at a time to respect free tier rate limits


def fetch_unenriched_incidents(client, limit=200):
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
    for _, row in df_batch.iterrows():
        try:
            prompt = build_prompt(row)
            response = model.generate_content(prompt)
            parsed = parse_gemini_response(response.text)
            parsed["incident_id"] = row["incident_id"]
            parsed["ai_enriched"] = True
            results.append(parsed)
            time.sleep(1.2)  # ~50 RPM free tier limit 
        except Exception as e:
            print(f"⚠️  Error on {row['incident_id']}: {e}")
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

    # Load enriched results to a temp table
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(enriched_df, temp_table, job_config=job_config)
    job.result()

    # Merge temp into main table
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
    df = fetch_unenriched_incidents(bq_client, limit=200)

    if df.empty:
        print("✅ All incidents already enriched.")
        return

    total_batches = (len(df) // BATCH_SIZE) + 1
    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} records)...")
        enriched = enrich_batch(batch)
        update_bigquery(bq_client, enriched)

    print("✅ Enrichment complete.")


if __name__ == "__main__":
    run_enrichment()
