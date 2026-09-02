import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import os

st.set_page_config(
    page_title="Reddit Sentiment Schism Engine",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Syne', sans-serif;
        background-color: #0a0a0f;
        color: #e8e8f0;
    }
    .main { background-color: #0a0a0f; }
    .block-container { padding: 2rem 3rem; }

    h1, h2, h3 { font-family: 'Syne', sans-serif; font-weight: 800; }

    .metric-card {
        background: #12121a;
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
    }
    .metric-value {
        font-family: 'Space Mono', monospace;
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.75rem;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: #888;
    }
    .schism-alert {
        background: linear-gradient(135deg, #1a0a0a, #2a0f0f);
        border: 1px solid #ff3333;
        border-left: 4px solid #ff3333;
        border-radius: 8px;
        padding: 1rem 1.5rem;
        margin: 0.5rem 0;
        font-family: 'Space Mono', monospace;
        font-size: 0.9rem;
    }
    .no-schism {
        background: #0f1a0f;
        border: 1px solid #1a4a1a;
        border-left: 4px solid #33ff66;
        border-radius: 8px;
        padding: 1rem 1.5rem;
        margin: 0.5rem 0;
        font-family: 'Space Mono', monospace;
        font-size: 0.9rem;
    }
    .header-tag {
        font-family: 'Space Mono', monospace;
        font-size: 0.7rem;
        color: #ff6633;
        letter-spacing: 0.2em;
        text-transform: uppercase;
    }
    div[data-testid="stMetric"] {
        background: #12121a;
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)

def hex_to_rgba(hex_color, alpha=0.2):
    hex_color = hex_color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

# ── Load data ─────────────────────────────────────────────────────────────────
SUMMARY_PATH = "../output/sentiment_summary"
SCORED_PATH  = "../output/sentiment_scored"

@st.cache_data
def load_summary():
    return pd.read_parquet(SUMMARY_PATH)

@st.cache_data
def load_scored():
    df = pd.read_parquet(SCORED_PATH, columns=["ecosystem", "sentiment", "created_utc", "subreddit", "title"])
    df["date"] = pd.to_datetime(df["created_utc"], unit="s")
    return df

summary_df = load_summary()
scored_df  = load_scored()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<p class="header-tag">⚡ Live Intelligence System</p>', unsafe_allow_html=True)
st.title("Reddit Sentiment Schism Engine")
st.markdown("Distributed sentiment analysis across tech ecosystems via Kafka + Spark")
st.divider()
# ── Time filter ───────────────────────────────────────────────────────────────
st.subheader("Time Range")
min_date = scored_df["date"].min().date()
max_date = scored_df["date"].max().date()

col_a, col_b = st.columns(2)
with col_a:
    start_date = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date)
with col_b:
    end_date = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

st.divider()

# filter scored_df by date range
scored_df = scored_df[
    (scored_df["date"].dt.date >= start_date) &
    (scored_df["date"].dt.date <= end_date)
]

# recompute summary from filtered data
summary_df = (
    scored_df
    .groupby("ecosystem")
    .agg(
        avg_sentiment=("sentiment", "mean"),
        std_sentiment=("sentiment", "std"),
    )
    .reset_index()
)

# ── Top metrics ───────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Posts Analyzed", f"{len(scored_df):,}")
with col2:
    st.metric("Ecosystems Monitored", scored_df["ecosystem"].nunique())
with col3:
    st.metric("Subreddits", scored_df["subreddit"].nunique())
with col4:
    # count schisms
    scores = summary_df.set_index("ecosystem")["avg_sentiment"].to_dict()
    ecosystems = list(scores.keys())
    schisms = sum(
        1 for i in range(len(ecosystems))
        for j in range(i+1, len(ecosystems))
        if abs(scores[ecosystems[i]] - scores[ecosystems[j]]) >= 0.1
    )
    st.metric("Schisms Detected", schisms)

st.divider()

# ── Main layout ───────────────────────────────────────────────────────────────
left, right = st.columns([1.2, 1], gap="large")

with left:
    st.subheader("Ecosystem Sentiment Scores")

    # bar chart
    colors = {"enthusiasts": "#ff6633", "critics": "#3399ff", "public": "#33ff99"}
    fig_bar = go.Figure()
    for _, row in summary_df.iterrows():
        eco = row["ecosystem"]
        fig_bar.add_trace(go.Bar(
            x=[eco],
            y=[row["avg_sentiment"]],
            name=eco,
            marker_color=colors.get(eco, "#888"),
            error_y=dict(type="data", array=[row["std_sentiment"] or 0], visible=True),
            width=0.4,
        ))
    fig_bar.update_layout(
        paper_bgcolor="#0a0a0f",
        plot_bgcolor="#0a0a0f",
        font=dict(family="Space Mono", color="#e8e8f0"),
        showlegend=False,
        height=320,
        margin=dict(l=10, r=10, t=20, b=10),
        yaxis=dict(gridcolor="#1a1a2a", zeroline=True, zerolinecolor="#333"),
        xaxis=dict(gridcolor="#1a1a2a"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # sentiment over time
    st.subheader("Sentiment Over Time")
    daily = (
        scored_df
        .set_index("date")
        .groupby([pd.Grouper(freq="W"), "ecosystem"])["sentiment"]
        .mean()
        .reset_index()
    )
    fig_line = px.line(
        daily, x="date", y="sentiment", color="ecosystem",
        color_discrete_map=colors,
    )
    fig_line.update_layout(
        paper_bgcolor="#0a0a0f",
        plot_bgcolor="#0a0a0f",
        font=dict(family="Space Mono", color="#e8e8f0"),
        height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(gridcolor="#1a1a2a", zeroline=True, zerolinecolor="#333"),
        xaxis=dict(gridcolor="#1a1a2a"),
        legend=dict(bgcolor="#12121a", bordercolor="#2a2a3a"),
    )
    st.plotly_chart(fig_line, use_container_width=True)

with right:
    st.subheader("Schism Report")
    for i in range(len(ecosystems)):
        for j in range(i+1, len(ecosystems)):
            e1, e2 = ecosystems[i], ecosystems[j]
            diff = abs(scores[e1] - scores[e2])
            if diff >= 0.1:
                st.markdown(f"""
                <div class="schism-alert">
                    ⚡ <b>SCHISM DETECTED</b><br>
                    {e1} vs {e2}<br>
                    Divergence: <b>{diff:.4f}</b>
                </div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="no-schism">
                    ✓ Stable: {e1} vs {e2}<br>
                    Divergence: {diff:.4f}
                </div>""", unsafe_allow_html=True)

    # radar chart
    st.subheader("Ecosystem Fingerprint")
    categories = ["avg_sentiment", "std_sentiment"]
    fig_radar = go.Figure()
    for _, row in summary_df.iterrows():
        eco = row["ecosystem"]
        fig_radar.add_trace(go.Scatterpolar(
            r=[abs(row["avg_sentiment"]), row["std_sentiment"] or 0],
            theta=["Sentiment", "Volatility"],
            fill="toself",
            name=eco,
            line_color=colors.get(eco, "#888"),
            fillcolor=hex_to_rgba(colors.get(eco, "#888888")),
        ))
    fig_radar.update_layout(
        polar=dict(
            bgcolor="#12121a",
            radialaxis=dict(visible=True, gridcolor="#2a2a3a", color="#888"),
            angularaxis=dict(gridcolor="#2a2a3a", color="#888"),
        ),
        paper_bgcolor="#0a0a0f",
        font=dict(family="Space Mono", color="#e8e8f0"),
        height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(bgcolor="#12121a", bordercolor="#2a2a3a"),
        showlegend=True,
    )
    st.plotly_chart(fig_radar, use_container_width=True)

    # sample posts
    st.subheader("Sample Posts by Ecosystem")
    selected_eco = st.selectbox("Ecosystem", scored_df["ecosystem"].unique())
    sample = (
        scored_df[scored_df["ecosystem"] == selected_eco]
        .nlargest(5, "sentiment")[["title", "sentiment", "subreddit"]]
    )
    st.dataframe(
        sample.style.format({"sentiment": "{:+.3f}"}),
        use_container_width=True,
        hide_index=True,
    )
