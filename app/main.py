"""
app/main.py
IT Operations Intelligence Platform — Streamlit Dashboard
Tabs: Overview Dashboard | AI Incident Insights | Natural Language Q&A
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

# --- Config: works both locally (.env) and on Streamlit Cloud (st.secrets) ---
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)

PROJECT_ID = get_secret("GCP_PROJECT_ID")
DATASET_ID = get_secret("BQ_DATASET", "it_ops")
TABLE_ID = get_secret("BQ_TABLE", "incidents")
GEMINI_API_KEY = get_secret("GEMINI_API_KEY")

# --- BigQuery client: uses service account from st.secrets on Cloud, ADC locally ---
def get_bq_client():
    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    except Exception:
        return bigquery.Client(project=PROJECT_ID)

genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-2.5-flash")

# --- Page Config ---
st.set_page_config(
    page_title="IT Ops Intelligence Platform",
    page_icon="🖥️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS ---
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e2e;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #7c3aed;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #1e1e2e;
        border-radius: 6px;
        padding: 8px 20px;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


# --- Data Loading ---
@st.cache_data(ttl=300)
def load_data():
    client = get_bq_client()
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        ORDER BY created_at DESC
    """
    df = client.query(query).to_dataframe()
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce")
    df["week"] = df["created_at"].dt.to_period("W").astype(str)
    df["month"] = df["created_at"].dt.to_period("M").astype(str)
    return df


# --- Sidebar ---
st.sidebar.image("https://img.icons8.com/fluency/96/server.png", width=60)
st.sidebar.title("IT Ops Intelligence")
st.sidebar.markdown("---")

try:
    df = load_data()
    data_loaded = True
except Exception as e:
    st.error(f"⚠️ Could not connect to BigQuery: {e}")
    st.info("Make sure your GCP credentials are configured correctly.")
    data_loaded = False
    df = pd.DataFrame()

if data_loaded:
    st.sidebar.subheader("Filters")
    selected_category = st.sidebar.multiselect(
        "Category", df["category"].unique().tolist(),
        default=df["category"].unique().tolist()
    )
    selected_severity = st.sidebar.multiselect(
        "Severity", ["Critical", "High", "Medium", "Low"],
        default=["Critical", "High", "Medium", "Low"]
    )
    selected_status = st.sidebar.multiselect(
        "Status", df["status"].unique().tolist(),
        default=df["status"].unique().tolist()
    )

    filtered_df = df[
        df["category"].isin(selected_category) &
        df["severity"].isin(selected_severity) &
        df["status"].isin(selected_status)
    ]

    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Showing:** {len(filtered_df):,} of {len(df):,} incidents")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["📊 Overview Dashboard", "🤖 AI Incident Insights", "💬 Natural Language Q&A"])


# ============================================================
# TAB 1 — OVERVIEW DASHBOARD
# ============================================================
with tab1:
    st.title("🖥️ IT Operations Intelligence Platform")
    st.caption("Real-time incident analytics across all IT systems")

    if not data_loaded or filtered_df.empty:
        st.warning("No data available. Check your BigQuery connection.")
    else:
        col1, col2, col3, col4, col5 = st.columns(5)
        total = len(filtered_df)
        critical = len(filtered_df[filtered_df["severity"] == "Critical"])
        open_inc = len(filtered_df[filtered_df["status"].isin(["Open", "In Progress"])])
        resolved = len(filtered_df[filtered_df["status"] == "Resolved"])
        avg_res = filtered_df["resolution_time_hours"].dropna().mean()

        col1.metric("Total Incidents", f"{total:,}")
        col2.metric("🔴 Critical", f"{critical:,}")
        col3.metric("🟡 Open / In Progress", f"{open_inc:,}")
        col4.metric("✅ Resolved", f"{resolved:,}")
        col5.metric("Avg Resolution Time", f"{avg_res:.1f}h" if not pd.isna(avg_res) else "N/A")

        st.markdown("---")

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Incidents by Category")
            cat_counts = filtered_df["category"].value_counts().reset_index()
            cat_counts.columns = ["Category", "Count"]
            fig = px.bar(cat_counts, x="Category", y="Count", color="Category",
                         color_discrete_sequence=px.colors.qualitative.Bold)
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            st.subheader("Severity Distribution")
            sev_counts = filtered_df["severity"].value_counts().reset_index()
            sev_counts.columns = ["Severity", "Count"]
            color_map = {"Critical": "#ef4444", "High": "#f97316", "Medium": "#eab308", "Low": "#22c55e"}
            fig = px.pie(sev_counts, values="Count", names="Severity",
                         color="Severity", color_discrete_map=color_map, hole=0.4)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        col_c, col_d = st.columns(2)
        with col_c:
            st.subheader("Incident Volume Over Time")
            weekly = filtered_df.groupby("week").size().reset_index(name="count")
            fig = px.area(weekly, x="week", y="count", color_discrete_sequence=["#7c3aed"])
            fig.update_layout(height=350, xaxis_title="Week", yaxis_title="Incidents")
            st.plotly_chart(fig, use_container_width=True)

        with col_d:
            st.subheader("Avg Resolution Time by Category (Hours)")
            res_time = (
                filtered_df.dropna(subset=["resolution_time_hours"])
                .groupby("category")["resolution_time_hours"]
                .mean()
                .reset_index()
            )
            res_time.columns = ["Category", "Avg Hours"]
            res_time = res_time.sort_values("Avg Hours", ascending=True)
            fig = px.bar(res_time, x="Avg Hours", y="Category", orientation="h",
                         color="Avg Hours", color_continuous_scale="RdYlGn_r")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        col_e, col_f = st.columns(2)
        with col_e:
            st.subheader("Status Breakdown")
            status_counts = filtered_df["status"].value_counts().reset_index()
            status_counts.columns = ["Status", "Count"]
            fig = px.bar(status_counts, x="Status", y="Count",
                         color="Status", color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)

        with col_f:
            st.subheader("Top 10 Affected Systems")
            sys_counts = filtered_df["affected_system"].value_counts().head(10).reset_index()
            sys_counts.columns = ["System", "Count"]
            fig = px.bar(sys_counts, x="Count", y="System", orientation="h",
                         color_discrete_sequence=["#06b6d4"])
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Incident Log")
        display_cols = ["incident_id", "created_at", "category", "severity", "status",
                        "affected_system", "description", "resolution_time_hours"]
        st.dataframe(filtered_df[display_cols].head(100), use_container_width=True, hide_index=True)


# ============================================================
# TAB 2 — AI INCIDENT INSIGHTS
# ============================================================
with tab2:
    st.title("🤖 AI-Powered Incident Insights")
    st.caption("Incidents enriched by Google Gemini — auto-classified, summarized, and triaged")

    if not data_loaded:
        st.warning("No data available.")
    else:
        enriched_df = df[df["ai_enriched"] == True].copy()
        not_enriched = len(df) - len(enriched_df)

        col1, col2, col3 = st.columns(3)
        col1.metric("AI-Enriched Incidents", f"{len(enriched_df):,}")
        col2.metric("Pending Enrichment", f"{not_enriched:,}")
        enrichment_rate = (len(enriched_df) / len(df) * 100) if len(df) > 0 else 0
        col3.metric("Enrichment Coverage", f"{enrichment_rate:.0f}%")

        st.markdown("---")

        if enriched_df.empty:
            st.info("No AI-enriched incidents yet. Run `python etl/gemini_enrich.py` to enrich incidents.")
        else:
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("AI vs Original Severity Labels")
                comparison = enriched_df.groupby(["severity", "ai_severity_label"]).size().reset_index(name="count")
                fig = px.sunburst(comparison, path=["severity", "ai_severity_label"],
                                  values="count", color_discrete_sequence=px.colors.qualitative.Bold)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col_b:
                st.subheader("AI Severity Distribution")
                ai_sev = enriched_df["ai_severity_label"].value_counts().reset_index()
                ai_sev.columns = ["Severity", "Count"]
                color_map = {"Critical": "#ef4444", "High": "#f97316", "Medium": "#eab308", "Low": "#22c55e"}
                fig = px.pie(ai_sev, values="Count", names="Severity",
                             color="Severity", color_discrete_map=color_map, hole=0.4)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("Sample AI-Enriched Incidents")
            sample_filter_cat = st.selectbox("Filter by category", ["All"] + enriched_df["category"].unique().tolist())
            display_df = enriched_df if sample_filter_cat == "All" else enriched_df[enriched_df["category"] == sample_filter_cat]

            for _, row in display_df.head(8).iterrows():
                sev_color = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}.get(row.get("ai_severity_label", ""), "⚪")
                with st.expander(f"{sev_color} {row['incident_id']} — {row['affected_system']} ({row['category']})"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Original Severity:** {row['severity']}")
                        st.markdown(f"**AI Severity:** {row.get('ai_severity_label', 'N/A')}")
                        st.markdown(f"**Status:** {row['status']}")
                    with col2:
                        st.markdown(f"**Assigned Team:** {row['assigned_team']}")
                        st.markdown(f"**Resolution Time:** {row['resolution_time_hours']} hrs" if pd.notna(row['resolution_time_hours']) else "**Resolution Time:** Pending")
                    st.markdown("**📝 Original Description:**")
                    st.info(row["description"])
                    st.markdown("**🤖 AI Summary:**")
                    st.success(row.get("ai_summary", "Not available"))
                    st.markdown("**💡 AI Resolution Suggestion:**")
                    st.warning(row.get("ai_resolution_suggestion", "Not available"))


# ============================================================
# TAB 3 — NATURAL LANGUAGE Q&A
# ============================================================
with tab3:
    st.title("💬 Ask Questions About Your IT Incidents")
    st.caption("Powered by Google Gemini — ask anything about the incident dataset")

    if not data_loaded:
        st.warning("No data available.")
    else:
        @st.cache_data(ttl=300)
        def build_data_context(_df):
            summary = {
                "total_incidents": len(_df),
                "categories": _df["category"].value_counts().to_dict(),
                "severities": _df["severity"].value_counts().to_dict(),
                "statuses": _df["status"].value_counts().to_dict(),
                "top_affected_systems": _df["affected_system"].value_counts().head(5).to_dict(),
                "avg_resolution_hours": round(_df["resolution_time_hours"].dropna().mean(), 2),
                "date_range": f"{_df['created_at'].min().date()} to {_df['created_at'].max().date()}",
                "open_critical": len(_df[(_df["severity"] == "Critical") & (_df["status"].isin(["Open", "In Progress"]))]),
            }
            sample = _df.head(20)[
                ["incident_id", "category", "severity", "status", "affected_system", "description", "resolution_time_hours"]
            ].to_dict(orient="records")
            return summary, sample

        data_summary, sample_incidents = build_data_context(df)

        # --- Caching wrapper for Gemini calls ---
        @st.cache_data(ttl=3600)
        def ask_gemini_cached(question, summary_str, sample_str):
            prompt = f"""You are an expert IT operations analyst.

You have access to an IT incident dataset with the following statistics:
{summary_str}

Sample of recent incidents:
{sample_str}

Answer this question accurately based on the data provided:
"{question}"

Be concise, specific, and reference actual numbers from the dataset where possible.
If the question cannot be answered from the available data, say so clearly."""
            response = gemini_model.generate_content(prompt)
            return response.text

        st.subheader("Example Questions")
        example_cols = st.columns(3)
        examples = [
            "What are the most common incident categories?",
            "Which systems have the highest number of critical incidents?",
            "What is the average resolution time for security incidents?",
            "Are there any patterns in how incidents are resolved?",
            "What percentage of incidents are still open?",
            "Which team handles the most incidents?",
        ]
        for i, ex in enumerate(examples):
            if example_cols[i % 3].button(ex, key=f"ex_{i}"):
                st.session_state["qa_input"] = ex

        st.markdown("---")

        st.caption("💡 Powered by Gemini free tier · identical questions are cached for 1 hour to preserve quota")

        user_question = st.text_input(
            "Ask a question about your IT incident data:",
            value=st.session_state.get("qa_input", ""),
            placeholder="e.g. What were the most critical incidents last week?",
            key="qa_input_box"
        )

        if st.button("🔍 Ask Gemini", type="primary") and user_question:
            with st.spinner("Analyzing your incident data..."):
                try:
                    answer = ask_gemini_cached(
                        user_question,
                        str(data_summary),
                        str(sample_incidents[:10])
                    )
                    st.subheader("📊 Answer")
                    st.markdown(answer)
                except Exception as e:
                    st.error(f"Gemini API error: {e}")

        st.markdown("---")
        st.subheader("💡 Quick Stats")
        quick_col1, quick_col2, quick_col3 = st.columns(3)
        quick_col1.info(f"**Most common category:** {df['category'].mode()[0]}")
        quick_col2.info(f"**Most affected system:** {df['affected_system'].mode()[0]}")
        open_critical = len(df[(df["severity"] == "Critical") & (df["status"].isin(["Open", "In Progress"]))])
        quick_col3.warning(f"**Open critical incidents:** {open_critical}")