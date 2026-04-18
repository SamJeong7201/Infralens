import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from analyzer import load_data, detect_idle, detect_peak_jobs, detect_overprovision, detect_thermal

st.set_page_config(page_title="InfraLens", page_icon="⚡", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #0a0a0f; }
section[data-testid="stSidebar"] { background: #0f0f1a; border-right: 1px solid #1e1e2e; }
.metric-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 24px; text-align: center; }
.metric-label { font-size: 12px; font-weight: 500; letter-spacing: 1px; text-transform: uppercase; color: #6b7280; margin-bottom: 8px; }
.metric-value { font-size: 32px; font-weight: 700; color: #ffffff; line-height: 1; }
.metric-sub { font-size: 12px; color: #4b5563; margin-top: 6px; }
.metric-value.green { color: #34d399; }
.metric-value.red   { color: #f87171; }
.metric-value.amber { color: #fbbf24; }
.metric-value.blue  { color: #818cf8; }
.action-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 24px 28px; margin-bottom: 12px; }
.action-number { font-size: 11px; font-weight: 700; letter-spacing: 2px; color: #6366f1; text-transform: uppercase; margin-bottom: 8px; }
.action-title { font-size: 17px; font-weight: 600; color: #f9fafb; margin-bottom: 6px; }
.action-desc { font-size: 14px; color: #6b7280; margin-bottom: 16px; line-height: 1.6; }
.action-saving { font-size: 22px; font-weight: 700; color: #34d399; }
.section-title { font-size: 13px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; color: #4b5563; margin-bottom: 20px; }
.tier-badge { display: inline-block; font-size: 11px; padding: 3px 10px; border-radius: 20px; font-weight: 600; }
.tier-pro { background: #312e81; color: #a5b4fc; }
.tier-standard { background: #1e3a5f; color: #93c5fd; }
.tier-basic { background: #1f2937; color: #9ca3af; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚡ InfraLens")
    st.caption("AI Infrastructure Cost Optimization")
    st.divider()
    st.markdown("**Upload CSV**")
    uploaded = st.file_uploader("Upload your GPU/server metrics CSV", type="csv", label_visibility="collapsed")
    st.markdown("**Or use sample data**")
    use_sample = st.button("Run with sample data", use_container_width=True)
    st.divider()
    st.markdown('<span style="font-size:12px;color:#4b5563">Supports any CSV format — AI auto-detects columns</span>', unsafe_allow_html=True)

df = None
col_map = {}

if uploaded:
    with st.spinner("AI analyzing your data..."):
        df, col_map = load_data(uploaded)
    st.sidebar.success(f"{len(df):,} rows loaded")
elif use_sample:
    with st.spinner("Loading sample data..."):
        df, col_map = load_data('gpu_metrics_30d.csv')
    st.sidebar.success("Sample data loaded")

if df is None:
    st.markdown("""
    <div style="text-align:center;padding:80px 40px">
        <div style="font-size:13px;font-weight:600;letter-spacing:4px;color:#6366f1;margin-bottom:16px">AI INFRASTRUCTURE INTELLIGENCE</div>
        <div style="font-size:52px;font-weight:700;background:linear-gradient(135deg,#fff 0%,#a5b4fc 50%,#6366f1 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:16px">InfraLens</div>
        <div style="font-size:18px;color:#6b7280;margin-bottom:48px">Upload any GPU or server metrics CSV<br>AI auto-detects columns and finds exact dollar savings.</div>
        <div style="background:#111120;border:1.5px dashed #2d2d44;border-radius:16px;padding:48px 32px;max-width:480px;margin:0 auto">
            <div style="font-size:32px;margin-bottom:12px">⬆</div>
            <div style="font-size:16px;font-weight:600;color:#e5e7eb;margin-bottom:6px">Drop your CSV here</div>
            <div style="font-size:13px;color:#4b5563">Any format — GPU util, power, cost, temperature</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# 분석
idle    = detect_idle(df, col_map)
peak    = detect_peak_jobs(df, col_map)
over    = detect_overprovision(df, col_map)
thermal = detect_thermal(df, col_map)

idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0
total = idle_total + peak['monthly_savings'] + over['monthly_savings']

# 티어 표시
tier = 'Pro' if len(col_map) >= 7 else ('Standard' if len(col_map) >= 4 else 'Basic')
tier_class = f"tier-{tier.lower()}"

st.markdown(f'<div style="height:16px"></div>', unsafe_allow_html=True)
c_title, c_tier = st.columns([4, 1])
with c_title:
    st.markdown('<div class="section-title">Savings Overview</div>', unsafe_allow_html=True)
with c_tier:
    st.markdown(f'<div style="text-align:right;padding-top:4px"><span class="tier-badge {tier_class}">{tier} Analysis</span></div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Total Savings</div><div class="metric-value green">${total:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric-card"><div class="metric-label">GPU Idle Waste</div><div class="metric-value red">${idle_total:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Peak Shifting</div><div class="metric-value amber">${peak["monthly_savings"]:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Overprovisioning</div><div class="metric-value blue">${over["monthly_savings"]:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)

st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)

# AI 매핑 결과 표시
with st.expander(f"AI Column Mapping — {len(col_map)} columns detected", expanded=False):
    cols = st.columns(3)
    for i, (standard, original) in enumerate(col_map.items()):
        with cols[i % 3]:
            st.markdown(f'<div style="font-size:12px;color:#6b7280">{standard}</div><div style="font-size:13px;color:#e5e7eb;font-family:monospace">{original}</div>', unsafe_allow_html=True)

st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

col1, col2 = st.columns([3, 2])
with col1:
    st.markdown('<div class="section-title">Utilization — 24h Pattern</div>', unsafe_allow_html=True)
    if 'gpu_util' in df.columns and 'hour' in df.columns:
        hourly = df.groupby('hour')['gpu_util'].mean().reset_index()
        hourly['status'] = hourly['gpu_util'].apply(lambda x: 'Idle Waste' if x < 20 else ('Peak' if x > 70 else 'Normal'))
        fig = px.bar(hourly, x='hour', y='gpu_util', color='status',
                     color_discrete_map={'Idle Waste': '#f87171', 'Peak': '#818cf8', 'Normal': '#34d399'},
                     labels={'hour': 'Hour', 'gpu_util': 'Avg Utilization (%)'},
                     template='plotly_dark')
        fig.update_layout(height=280, margin=dict(t=8,b=8,l=0,r=0), paper_bgcolor='#111120', plot_bgcolor='#111120', legend_title_text='')
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">Savings Breakdown</div>', unsafe_allow_html=True)
    if total > 0:
        fig2 = go.Figure(go.Pie(
            labels=['GPU Idle', 'Peak Shifting', 'Overprovisioning'],
            values=[idle_total, peak['monthly_savings'], over['monthly_savings']],
            hole=0.6, marker_colors=['#f87171', '#fbbf24', '#818cf8']
        ))
        fig2.update_layout(height=280, margin=dict(t=8,b=8,l=0,r=0), paper_bgcolor='#111120', font=dict(color='#9ca3af'))
        st.plotly_chart(fig2, use_container_width=True)

st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Action Plans</div>', unsafe_allow_html=True)

if len(idle) > 0 and idle_total > 0:
    st.markdown(f"""
    <div class="action-card">
        <div class="action-number">Finding 01 · Idle Waste</div>
        <div class="action-title">{len(idle)} device(s) running idle during off-hours</div>
        <div class="action-desc">Low utilization detected while still drawing significant power. Enable power-saving mode or suspend idle instances during off-peak windows to eliminate waste.</div>
        <div class="action-saving">Save ${idle_total:,.0f} / month</div>
    </div>""", unsafe_allow_html=True)

if peak['peak_hours_count'] > 0:
    st.markdown(f"""
    <div class="action-card">
        <div class="action-number">Finding 02 · Peak-Rate Scheduling</div>
        <div class="action-title">{peak['peak_hours_count']} sessions running during peak pricing</div>
        <div class="action-desc">High-cost workloads detected during peak tariff periods. Rescheduling to off-peak window (${peak['offpeak_rate']}/hr) saves significantly with zero performance impact.</div>
        <div class="action-saving">Save ${peak['monthly_savings']:,.0f} / month</div>
    </div>""", unsafe_allow_html=True)

if over['monthly_savings'] > 0:
    st.markdown(f"""
    <div class="action-card">
        <div class="action-number">Finding 03 · Overprovisioning</div>
        <div class="action-title">Fleet can be safely reduced during low-demand hours</div>
        <div class="action-desc">{over['total_gpus']} devices active 24/7. Analysis shows safe reduction opportunity with 25% buffer maintained — no workload impact.</div>
        <div class="action-saving">Save ${over['monthly_savings']:,.0f} / month</div>
    </div>""", unsafe_allow_html=True)

if thermal is not None and len(thermal) > 0:
    high_risk = thermal[thermal['throttle_risk_pct'] > 5]
    if len(high_risk) > 0:
        st.markdown(f"""
        <div class="action-card">
            <div class="action-number">Finding 04 · Thermal Risk</div>
            <div class="action-title">{len(high_risk)} device(s) at thermal throttle risk</div>
            <div class="action-desc">Temperatures exceeding 85°C detected. Thermal throttling reduces performance while maintaining power draw — worst of both worlds. Improve cooling or reduce workload density.</div>
            <div class="action-saving" style="color:#f87171">Performance at risk</div>
        </div>""", unsafe_allow_html=True)

if total == 0:
    st.info("No significant optimization opportunities detected in this dataset. Try uploading a dataset with GPU utilization, power consumption, or cost data.")

st.markdown('<div style="height:48px"></div>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;font-size:12px;color:#2d2d44;letter-spacing:2px">INFRALENS · AI INFRASTRUCTURE COST OPTIMIZATION</div>', unsafe_allow_html=True)
