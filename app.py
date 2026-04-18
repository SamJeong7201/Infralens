import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from analyzer import load_data, detect_idle, detect_peak_jobs, detect_overprovision

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
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚡ InfraLens")
    st.caption("AI Infrastructure Cost Optimization")
    st.divider()
    st.markdown("**Upload CSV**")
    uploaded = st.file_uploader("Upload your GPU metrics CSV", type="csv", label_visibility="collapsed")
    st.markdown("**Or use sample data**")
    use_sample = st.button("Run with sample data", use_container_width=True)

df = None
col_map = {}

if uploaded:
    df, col_map = load_data(uploaded)
    st.sidebar.success(f"{len(df):,} rows loaded")
elif use_sample:
    df, col_map = load_data('gpu_metrics_30d.csv')
    st.sidebar.success("Sample data loaded")

if df is None:
    st.markdown("""
    <div style="text-align:center;padding:80px 40px">
        <div style="font-size:13px;font-weight:600;letter-spacing:4px;color:#6366f1;margin-bottom:16px">AI INFRASTRUCTURE INTELLIGENCE</div>
        <div style="font-size:52px;font-weight:700;background:linear-gradient(135deg,#fff 0%,#a5b4fc 50%,#6366f1 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:16px">InfraLens</div>
        <div style="font-size:18px;color:#6b7280;margin-bottom:48px">Upload your GPU metrics — get exact dollar savings<br>with time-specific action plans. Automatically.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

idle = detect_idle(df, col_map)
peak = detect_peak_jobs(df, col_map)
over = detect_overprovision(df, col_map)

idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0
total = idle_total + peak['monthly_savings'] + over['monthly_savings']

st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Savings Overview</div>', unsafe_allow_html=True)

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

col1, col2 = st.columns([3, 2])
with col1:
    st.markdown('<div class="section-title">GPU Utilization — 24h Pattern</div>', unsafe_allow_html=True)
    hourly = df.groupby('hour')['gpu_util'].mean().reset_index()
    hourly['status'] = hourly['gpu_util'].apply(lambda x: 'Idle Waste' if x < 15 else ('Peak' if x > 70 else 'Normal'))
    fig = px.bar(hourly, x='hour', y='gpu_util', color='status',
                 color_discrete_map={'Idle Waste': '#f87171', 'Peak': '#818cf8', 'Normal': '#34d399'},
                 labels={'hour': 'Hour', 'gpu_util': 'Avg GPU Util (%)'},
                 template='plotly_dark')
    fig.update_layout(height=280, margin=dict(t=8,b=8,l=0,r=0), paper_bgcolor='#111120', plot_bgcolor='#111120', legend_title_text='')
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">Savings Breakdown</div>', unsafe_allow_html=True)
    fig2 = go.Figure(go.Pie(
        labels=['GPU Idle', 'Peak Shifting', 'Overprovisioning'],
        values=[idle_total, peak['monthly_savings'], over['monthly_savings']],
        hole=0.6, marker_colors=['#f87171', '#fbbf24', '#818cf8']
    ))
    fig2.update_layout(height=280, margin=dict(t=8,b=8,l=0,r=0), paper_bgcolor='#111120', font=dict(color='#9ca3af'))
    st.plotly_chart(fig2, use_container_width=True)

st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Action Plans</div>', unsafe_allow_html=True)

if len(idle) > 0:
    st.markdown(f"""
    <div class="action-card">
        <div class="action-number">Finding 01 · GPU Idle Waste</div>
        <div class="action-title">{len(idle)} GPUs running idle during off-hours</div>
        <div class="action-desc">GPUs detected with low utilization while still drawing full power. Enable power-saving mode or suspend idle instances during off-peak windows.</div>
        <div class="action-saving">Save ${idle_total:,.0f} / month</div>
    </div>""", unsafe_allow_html=True)

if peak['peak_hours_count'] > 0:
    st.markdown(f"""
    <div class="action-card">
        <div class="action-number">Finding 02 · Peak-Rate Training</div>
        <div class="action-title">{peak['peak_hours_count']} training sessions during peak pricing</div>
        <div class="action-desc">Training jobs scheduled during peak tariff (${peak.get('peak_rate','?')}/hr). Rescheduling to off-peak window (${peak['offpeak_rate']}/hr) saves significantly with zero performance impact.</div>
        <div class="action-saving">Save ${peak['monthly_savings']:,.0f} / month</div>
    </div>""", unsafe_allow_html=True)

if over['monthly_savings'] > 0:
    st.markdown(f"""
    <div class="action-card">
        <div class="action-number">Finding 03 · Overprovisioning</div>
        <div class="action-title">GPU fleet can be safely reduced overnight</div>
        <div class="action-desc">{over['total_gpus']} GPUs active 24/7. Analysis shows safe reduction opportunity during low-demand hours without impacting workloads (25% buffer maintained).</div>
        <div class="action-saving">Save ${over['monthly_savings']:,.0f} / month</div>
    </div>""", unsafe_allow_html=True)

st.markdown('<div style="height:48px"></div>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;font-size:12px;color:#2d2d44;letter-spacing:2px">INFRALENS · AI INFRASTRUCTURE COST OPTIMIZATION</div>', unsafe_allow_html=True)
