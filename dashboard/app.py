import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Reddit Sentiment Schism Engine",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap');
    html, body, [class*="css"] { font-family: 'Syne', sans-serif; background-color: #0a0a0f; color: #e8e8f0; }
    .main { background-color: #0a0a0f; }
    .block-container { padding: 2rem 3rem; }
    h1, h2, h3 { font-family: 'Syne', sans-serif; font-weight: 800; }
    .header-tag { font-family: 'Space Mono', monospace; font-size: .7rem; color: #ff6633; letter-spacing: .2em; text-transform: uppercase; }
    .schism-alert { background: linear-gradient(135deg,#1a0a0a,#2a0f0f); border: 1px solid #ff3333; border-left: 4px solid #ff3333; border-radius: 8px; padding: 1rem 1.5rem; margin: .5rem 0; font-family: 'Space Mono',monospace; }
    .no-schism { background: #0f1a0f; border: 1px solid #1a4a1a; border-left: 4px solid #33ff66; border-radius: 8px; padding: 1rem 1.5rem; margin: .5rem 0; font-family: 'Space Mono',monospace; }
    div[data-testid="stMetric"] { background: #12121a; border: 1px solid #2a2a3a; border-radius: 12px; padding: 1rem; }
</style>
""", unsafe_allow_html=True)

SCHISM_THRESHOLD = 0.1
COLORS = {"enthusiasts": "#ff6633", "critics": "#3399ff", "public": "#33ff99"}
SCORED_PATH = os.getenv("SCORED_PATH", "../output/sentiment_scored")


@st.cache_data
def load_scored(path):
    columns = [
        "analysis_topic", "matched_keywords", "ecosystem", "sentiment",
        "created_utc", "subreddit", "title", "text", "post_id",
    ]
    df = pd.read_parquet(path, columns=columns)
    df["date"] = pd.to_datetime(df["created_utc"], unit="s", errors="coerce")
    df = df.dropna(subset=["analysis_topic", "date", "sentiment"])
    return df


st.markdown('<p class="header-tag">⚡ Topic-aware sentiment intelligence</p>', unsafe_allow_html=True)
st.title("Reddit Sentiment Schism Engine")
st.markdown("Compare sentiment across Enthusiasts, Critics and Public **for the same selected subject**.")
st.divider()

try:
    all_scored = load_scored(SCORED_PATH)
except Exception as exc:
    st.error(f"Could not load topic-aware scored data from {SCORED_PATH}: {exc}")
    st.info("Run the updated producer and PySpark batch job first. Older records without analysis_topic are intentionally excluded.")
    st.stop()

if all_scored.empty:
    st.warning("No topic-aware records found. Run the updated ingestion and processing pipeline.")
    st.stop()

topics = sorted(all_scored["analysis_topic"].dropna().unique().tolist())
selected_topic = st.selectbox("Analysis topic", topics)
scored_df = all_scored[all_scored["analysis_topic"] == selected_topic].copy()

min_date = scored_df["date"].min().date()
max_date = scored_df["date"].max().date()
col_a, col_b = st.columns(2)
with col_a:
    start_date = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date)
with col_b:
    end_date = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

if start_date > end_date:
    st.error("The start date must be on or before the end date.")
    st.stop()

scored_df = scored_df[
    scored_df["date"].dt.date.between(start_date, end_date)
].copy()
if scored_df.empty:
    st.warning("No posts are available for this topic and date range.")
    st.stop()

summary_df = (
    scored_df.groupby("ecosystem")
    .agg(
        avg_sentiment=("sentiment", "mean"),
        std_sentiment=("sentiment", "std"),
        post_count=("post_id", "count"),
    )
    .reset_index()
)
score_map = summary_df.set_index("ecosystem")["avg_sentiment"].to_dict()
ecosystems = sorted(score_map)
schism_pairs = []
for i in range(len(ecosystems)):
    for j in range(i + 1, len(ecosystems)):
        e1, e2 = ecosystems[i], ecosystems[j]
        diff = abs(score_map[e1] - score_map[e2])
        schism_pairs.append((e1, e2, diff))

st.caption(f"Selected subject: **{selected_topic}** | Date window: {start_date} to {end_date} | Schism threshold: {SCHISM_THRESHOLD:.2f}")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Posts analyzed", f"{len(scored_df):,}")
c2.metric("Ecosystems represented", scored_df["ecosystem"].nunique())
c3.metric("Subreddits", scored_df["subreddit"].nunique())
c4.metric("Schism pairs", sum(diff >= SCHISM_THRESHOLD for _, _, diff in schism_pairs))
st.divider()

left, right = st.columns([1.2, 1], gap="large")
with left:
    st.subheader("Ecosystem sentiment")
    fig_bar = go.Figure()
    for _, row in summary_df.iterrows():
        eco = row["ecosystem"]
        std = row["std_sentiment"]
        fig_bar.add_trace(go.Bar(
            x=[eco], y=[row["avg_sentiment"]], name=eco,
            marker_color=COLORS.get(eco, "#888888"),
            error_y=dict(type="data", array=[0 if pd.isna(std) else std], visible=True),
            customdata=[[row["post_count"]]],
            hovertemplate="%{x}<br>Mean sentiment: %{y:.3f}<br>Posts: %{customdata[0]}<extra></extra>",
        ))
    fig_bar.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font=dict(family="Space Mono", color="#e8e8f0"), showlegend=False,
        height=320, margin=dict(l=10, r=10, t=20, b=10),
        yaxis=dict(gridcolor="#1a1a2a", zeroline=True, zerolinecolor="#555", range=[-1, 1]),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Weekly sentiment trend")
    weekly = (
        scored_df.set_index("date")
        .groupby([pd.Grouper(freq="W"), "ecosystem"])["sentiment"]
        .mean().reset_index()
    )
    fig_line = px.line(weekly, x="date", y="sentiment", color="ecosystem", color_discrete_map=COLORS)
    fig_line.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font=dict(family="Space Mono", color="#e8e8f0"), height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(gridcolor="#1a1a2a", zeroline=True, zerolinecolor="#555", range=[-1, 1]),
        legend=dict(bgcolor="#12121a", bordercolor="#2a2a3a"),
    )
    st.plotly_chart(fig_line, use_container_width=True)

with right:
    st.subheader("Topic-specific schism report")
    if not schism_pairs:
        st.info("At least two ecosystems need posts in this topic to calculate divergence.")
    for e1, e2, diff in schism_pairs:
        if diff >= SCHISM_THRESHOLD:
            st.markdown(f'<div class="schism-alert">⚡ <b>SCHISM DETECTED</b><br>{e1} vs {e2}<br>Divergence: <b>{diff:.4f}</b></div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="no-schism">✓ No schism: {e1} vs {e2}<br>Divergence: <b>{diff:.4f}</b></div>', unsafe_allow_html=True)

    st.subheader("Ecosystem statistics")
    display_summary = summary_df.copy()
    display_summary["avg_sentiment"] = display_summary["avg_sentiment"].map(lambda x: f"{x:+.4f}")
    display_summary["std_sentiment"] = display_summary["std_sentiment"].map(lambda x: "—" if pd.isna(x) else f"{x:.4f}")
    st.dataframe(display_summary, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Sample posts")
selected_eco = st.selectbox("Ecosystem", ecosystems)
sample = (
    scored_df[scored_df["ecosystem"] == selected_eco]
    .nlargest(10, "sentiment")[["title", "sentiment", "subreddit", "matched_keywords"]]
)
st.dataframe(sample.style.format({"sentiment": "{:+.3f}"}), use_container_width=True, hide_index=True)
