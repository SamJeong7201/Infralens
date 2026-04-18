import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from analyzer import load_data, detect_idle, detect_peak_jobs, detect_overprovision

st.set_page_config(page_title="InfraLens", page_icon="⚡", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: #0a0a0f;
}

section[data-testid="stSidebar"] {
    background: #0f0f1a;
    border-right: 1px solid #1e1e2e;
}

.hero-container {
    text-align: center;
    padding: 60px 40px 40px;
}

.hero-logo {
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 4px;
    color: #6366f1;
    text-transform: uppercase;
    margin-bottom: 16px;
}

.hero-title {
    font-size: 56px;
    font-weight: 700;
    background: linear-gradient(135deg, #ffffff 0%, #a5b4fc 50%, #6366f1 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1.1;
    margin-bottom: 16px;
}

.hero-sub {
    font-size: 18px;
    color: #6b7280;
    font-weight: 400;
    margin-bottom: 48px;
}

.upload-zone {
    border: 1.5px dashed #2d2d44;
    border-radius: 16px;
    padding: 48px 32px;
    text-align: center;
    background: #111120;
    transition: all 0.2s;
    margin: 0 auto;
    max-width: 560px;
}

.upload-icon {
    font-size: 36px;
    margin-bottom: 12px;
}

.upload-title {
    font-size: 16px;
    font-weight: 600;
    color: #e5e7eb;
    margin-bottom: 6px;
}

.upload-sub {
    font-size: 13px;
    color: #4b5563;
}

.metric-card {
    background: #111120;
    border: 1px solid #1e1e2e;
    border-radius: 14px;
    padding: 24px;
    text-align: center;
}

.metric-label {
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: #6b7280;
    margin-bottom: 8px;
}

.metric-value {
    font-size: 32px;
    font-weight: 700;
    color: #ffffff;
    line-height: 1;
}

.metric-sub {
    font-size: 12px;
    color: #4b5563;
    margin-top: 6px;
}

.metric-value.green { color: #34d399; }
.metric-value.red   { color: #f87171; }
.metric-value.amber { color: #fbbf24; }
.metric-value.blue  { color: #818cf8; }

.section-title {
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #4b5563;
    margin-bottom: 20px;
}

.action-card {
    background: #111120;
    border: 1px solid #1e1e2e;
    border-radius: 14px;
    padding: 24px 28px;
    margin-bottom: 12px;
}

.action-number {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 2px;
    color: #6366f1;
    text-transform: uppercase;
    margin-bottom: 8px;
}

.action-title {
    font-size: 17px;
    font-weight: 600;
    color: #f9fafb;
    margin-bottom: 6px;
}

.action-desc {
    font-size: 14px;
    color: #6b7280;
    margin-bottom: 16px;
    line-height: 1.6;
}

.action-saving {
    font-size: 22px;
    font-weight: 700;
    color: #34d399;
}

.sidebar-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #4b5563;
    margin-bottom: 12px;
    margin-top: 24px;
}

div[data-testid="stFileUploader"] {
    background: #111120;
    border: 1.5px dashed #2d2d44;
    border-radius: 12px;
    padding: 12px;
}

div[data-testid="stButton"] > button {
    background: #6366f1;
    color: white;
    border: none;
    border-radius: 10px;
    padding: 10px 20px;
    font-weight: 600;
    font-size: 14px;
    width: 100%;
    transition: all 0.2s;
}

div[data-testid="stButton"] > button:hover {
    background: #4f46e5;
    transform: translateY(-1px);
}

.divider {
    height: 1px;
    background: #1e1e2e;
    margin: 32px 0;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──
with st.sidebar:
    st.markdown('<div class="sidebar-label">Data Source</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader("Upload CSV", type="csv", label_visibility="collapsed")
    st.markdown('<div class="sidebar-label">Or use sample</div>', unsafe_allow_html=True)
    use_sample = st.button("Run with sample data")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-label">About</div>', unsafe_allow_html=True)
    st.markdown('<span style="font-size:13px;color:#4b5563;line-height:1.7">Upload your GPU metrics CSV to detect waste and get exact dollar savings with time-specific action plans.</span>', unsafe_allow_html=True)

# ── Data load ──
df = None
if uploaded:
    df = pd.read_csv(uploaded)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour
    df['date'] = df['timestamp'].dt.date
elif use_sample:
    df = load_data('gpu_metrics_30d.csv')

# ── Hero (no data) ──
if df is None:
    st.markdown("""
    <div class="hero-container">
        <div class="hero-logo">AI Infrastructure Intelligence</div>
        <div class="hero-title">InfraLens</div>
        <div class="hero-sub">Upload your GPU metrics — get exact dollar savings<br>with time-specific action plans. Automatically.</div>
        <div class="upload-zone">
            <div class="upload-icon">⬆</div>
            <div class="upload-title">Drop your CSV here</div>
            <div class="upload-sub">or use the sample data on the left to see a live demo</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Analysis ──
idle = detect_idle(df, {})
peak = detect_peak_jobs(df, {})
over = detect_overprovision(df, {})
total = idle['monthly_savings'].sum() + peak['monthly_savings'] + over['monthly_savings']

# ── Metrics ──
st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Savings Overview</div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Total Savings</div>
        <div class="metric-value green">${total:,.0f}</div>
        <div class="metric-sub">per month</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">GPU Idle Waste</div>
        <div class="metric-value red">${idle['monthly_savings'].sum():,.0f}</div>
        <div class="metric-sub">per month</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Peak Shifting</div>
        <div class="metric-value amber">${peak['monthly_savings']:,.0f}</div>
        <div class="metric-sub">per month</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Overprovisioning</div>
        <div class="metric-value blue">${over['monthly_savings']:,.0f}</div>
        <div class="metric-sub">per month</div>
    </div>""", unsafe_allow_html=True)

st.markdown('<div style="height:40px"></div>', unsafe_allow_html=True)

# ── Charts ──
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<div class="section-title">GPU Utilization — 24h Pattern</div>', unsafe_allow_html=True)
    hourly = df.groupby('hour')['gpu_util_pct'].mean().reset_index()
    hourly['status'] = hourly['gpu_util_pct'].apply(
        lambda x: 'Idle Waste' if x < 15 else ('Peak' if x > 70 else 'Normal')
    )
    color_map = {'Idle Waste': '#f87171', 'Peak': '#818cf8', 'Normal': '#34d399'}
    fig = px.bar(hourly, x='hour', y='gpu_util_pct', color='status',
                 color_discrete_map=color_map,
                 labels={'hour': 'Hour', 'gpu_util_pct': 'Avg GPU Util (%)'},
                 template='plotly_dark')
    fig.update_layout(
        height=280,
        margin=dict(t=8, b=8, l=0, r=0),
        paper_bgcolor='#111120',
        plot_bgcolor='#111120',
        legend_title_text='',
        font=dict(family='Inter', color='#6b7280'),
        xaxis=dict(gridcolor='#1e1e2e'),
        yaxis=dict(gridcolor='#1e1e2e'),
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">Savings Breakdown</div>', unsafe_allow_html=True)
    fig2 = go.Figure(go.Pie(
        labels=['GPU Idle', 'Peak Shifting', 'Overprovisioning'],
        values=[idle['monthly_savings'].sum(), peak['monthly_savings'], over['monthly_savings']],
        hole=0.6,
        marker_colors=['#f87171', '#fbbf24', '#818cf8'],
        textfont=dict(family='Inter', size=12),
    ))
    fig2.update_layout(
        height=280,
        margin=dict(t=8, b=8, l=0, r=0),
        paper_bgcolor='#111120',
        font=dict(family='Inter', color='#9ca3af'),
        legend=dict(font=dict(size=12)),
        showlegend=True,
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)

# ── Action Plans ──
st.markdown('<div class="section-title">Action Plans</div>', unsafe_allow_html=True)

st.markdown(f"""
<div class="action-card">
    <div class="action-number">Finding 01 · GPU Idle</div>
    <div class="action-title">GPUs running idle 01:00 – 05:00 daily</div>
    <div class="action-desc">
        {idle['gpu_id'].count()} GPUs detected with &lt;15% utilization during off-hours 
        while still drawing full power. Enable power-saving mode or suspend idle instances.
    </div>
    <div class="action-saving">Save ${idle['monthly_savings'].sum():,.0f} / month</div>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="action-card">
    <div class="action-number">Finding 02 · Peak Rate Training</div>
    <div class="action-title">Training jobs scheduled during peak pricing (14:00 – 18:00)</div>
    <div class="action-desc">
        {peak['peak_hours_count']} training jobs detected during peak tariff window at $4.10/hr.
        Rescheduling to 02:00 – 05:00 reduces cost to $2.10/hr with zero performance impact.
    </div>
    <div class="action-saving">Save ${peak['monthly_savings']:,.0f} / month</div>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="action-card">
    <div class="action-number">Finding 03 · Overprovisioning</div>
    <div class="action-title">{over['reducible_gpus']} GPUs can be safely reduced overnight</div>
    <div class="action-desc">
        {over['total_gpus']} GPUs active 24/7 but average night utilization is only 
        {over['avg_night_active']} active units. Safe to scale down {over['reducible_gpus']} GPUs 
        from 22:00 – 06:00 without impacting workloads.
    </div>
    <div class="action-saving">Save ${over['monthly_savings']:,.0f} / month</div>
</div>
""", unsafe_allow_html=True)

st.markdown('<div style="height:48px"></div>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;font-size:12px;color:#2d2d44;letter-spacing:2px">INFRALENS · AI INFRASTRUCTURE COST OPTIMIZATION</div>', unsafe_allow_html=True)
