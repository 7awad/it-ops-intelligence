# 🖥️ IT Operations Intelligence Platform

An end-to-end IT incident analytics platform built with **GCP Cloud Storage**, **BigQuery**, **Google Gemini API**, and **Streamlit**. Ingests mock IT operational incident data, enriches it with AI-powered classification and summarization, and surfaces trends through an interactive analytics dashboard.

**Live Demo:** [your-streamlit-url]  
**Built by:** Jawad Almatar

---

## 🏗️ Architecture

```
Mock Incident Data (Python/Faker)
        ↓
GCP Cloud Storage (raw CSV)
        ↓
Python ETL → BigQuery (structured incidents table)
        ↓
Gemini API (AI enrichment: severity validation, summary, resolution suggestion)
        ↓
Streamlit App (Dashboard + AI Insights + Natural Language Q&A)
```

---

## ✨ Features

### 📊 Overview Dashboard
- KPI metrics: total incidents, critical count, open/resolved counts, avg resolution time
- Incident volume trend over time (area chart)
- Severity distribution (donut chart)
- Category breakdown and top affected systems
- Avg resolution time by category
- Filterable incident log table

### 🤖 AI Incident Insights
- Gemini-powered severity re-classification per incident
- AI-generated plain-English summaries
- AI-suggested resolution actions
- AI vs original severity comparison (sunburst chart)
- Enriched incident explorer with expandable cards

### 💬 Natural Language Q&A
- Ask plain-English questions about the incident dataset
- Gemini answers using live dataset statistics and sample data
- Example questions included for quick exploration

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Generation | Python, Faker |
| Cloud Storage | GCP Cloud Storage |
| Data Warehouse | Google BigQuery |
| AI Enrichment | Google Gemini API (gemini-1.5-flash) |
| ETL | Python (pandas, google-cloud) |
| Dashboard | Streamlit + Plotly |
| Deployment | Streamlit Community Cloud |

---

## 🚀 Local Setup

### 1. Clone & Install
```bash
git clone https://github.com/yourusername/it-ops-intelligence
cd it-ops-intelligence
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Configure Environment
Create a `.env` file:
```
GOOGLE_APPLICATION_CREDENTIALS=gcp-key.json
GEMINI_API_KEY=your_gemini_api_key
GCP_PROJECT_ID=your_project_id
GCP_BUCKET_NAME=it-ops-raw-data
BQ_DATASET=it_ops
BQ_TABLE=incidents
```

### 3. Run the Pipeline
```bash
# Step 1: Generate mock data
python data/generate_incidents.py

# Step 2: Upload to GCS
python etl/upload_to_gcs.py

# Step 3: Load into BigQuery
python etl/load_to_bigquery.py

# Step 4: Enrich with Gemini AI
python etl/gemini_enrich.py

# Step 5: Launch dashboard
streamlit run app/main.py
```

---

## ☁️ Deployment (Streamlit Community Cloud)

1. Push to GitHub (make sure `gcp-key.json` is in `.gitignore`)
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app
3. Select your repo and `app/main.py`
4. Add secrets in the Streamlit Cloud dashboard (from `.streamlit/secrets.toml`)

---

## 📁 Project Structure

```
it-ops-intelligence/
├── data/
│   ├── generate_incidents.py    # Mock data generator (1,000 incidents)
│   └── incidents.csv            # Generated dataset (gitignored)
├── etl/
│   ├── upload_to_gcs.py         # Upload raw CSV to Cloud Storage
│   ├── load_to_bigquery.py      # ETL: GCS → BigQuery
│   └── gemini_enrich.py         # AI enrichment pipeline
├── app/
│   └── main.py                  # Streamlit dashboard (3 tabs)
├── .streamlit/
│   └── secrets.toml             # Deployment secrets (gitignored)
├── .env                         # Local env variables (gitignored)
├── gcp-key.json                 # GCP service account key (gitignored)
├── requirements.txt
└── README.md
```

---

## 📊 Dataset

1,000 synthetically generated IT incident tickets spanning:
- **5 categories:** Network, Hardware, Software, Security, Access & Identity
- **4 severity levels:** Critical, High, Medium, Low
- **4 statuses:** Resolved, In Progress, Open, Closed
- **15 affected systems:** Email Server, VPN, Active Directory, ERP, etc.
- **180 days** of incident history

---

## 🔑 GCP Free Tier Usage

| Service | Free Tier Limit | This Project Uses |
|---------|----------------|-------------------|
| Cloud Storage | 5GB / month | ~1MB |
| BigQuery Storage | 10GB | ~5MB |
| BigQuery Queries | 1TB / month | <1MB |
| Gemini API | 15 RPM, 1M TPM | ~200 calls |
