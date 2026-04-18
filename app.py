import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
from data_loader import load_and_prepare
from cost_model import simulate_before_after
from analyzer import (detect_idle_advanced, detect_peak_waste_advanced,
                      detect_overprovision_advanced, compute_efficiency_scores)
from recommender import generate_recommendations, format_report

st.set_page_config(page_title="InfraLens", page_icon="⚡", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #0a0a0f; }
section[data-testid="stSidebar"] { background: #0f0f1a; border-right: 1px solid #1e1e2e; }
.metric-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 20px; text-align: center; }
.metric-label { font-size: 11px; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; color: #6b7280; margin-bottom: 8px; }
.metric-value { font-size: 28px; font-weight: 700; line-height: 1; }
.metric-sub { font-size: 11px; color: #4b5563; margin-top: 6px; }
.green { color: #34d399; } .red { color: #f87171; }
.amber { color: #fbbf24; } .blue { color: #818cf8; } .white { color: #ffffff; }
.rec-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 22px 26px; margin-bottom: 10px; }
.rec-priority { font-size: 11px; font-weight: 700; letter-spacing: 2px; color: #6366f1; text-transform: uppercase; margin-bottom: 6px; }
.rec-title { font-size: 16px; font-weight: 600; color: #f9fafb; margin-bottom: 8px; }
.rec-detail { font-size: 13px; color: #6b7280; margin-bottom: 10px; line-height: 1.6; }
.rec-action { font-size: 13px; color: #a5b4fc; margin-bottom: 12px; line-height: 1.6; padding: 8px 12px; background: #1a1a2e; border-radius: 8px; border-left: 3px solid #6366f1; }
.rec-saving { font-size: 20px; font-weight: 700; color: #34d399; }
.rec-meta { font-size: 11px; color: #4b5563; margin-top: 6px; }
.section-title { font-size: 12px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; color: #4b5563; margin-bottom: 16px; }
.grade-badge { display: inline-block; font-size: 12px; font-weight: 700; padding: 2px 10px; border-radius: 20px; }
.grade-a { background: #064e3b; color: #34d399; }
.grade-b { background: #1e3a5f; color: #60a5fa; }
.grade-c { background: #422006; color: #fbbf24; }
.grade-d { background: #450a0a; color: #f87171; }
.exec-box { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 24px 28px; margin-bottom: 20px; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──
with st.sidebar:
    st.markdown("### ⚡ InfraLens")
    st.caption("AI Infrastructure Cost Optimization")
    st.divider()
    st.markdown("**Upload CSV**")
    uploaded = st.file_uploader("GPU / server metrics CSV", type="csv", label_visibility="collapsed")
    st.markdown("**Or use sample**")
    use_sample = st.button("Run with sample data", use_container_width=True)
    st.divider()
    st.markdown("**Settings**")
    schedule = st.selectbox("Cloud pricing", ['aws_us_east', 'gcp_us_central', 'kepco_korea'], label_visibility="collapsed")
    dc_type  = st.selectbox("DC type", ['average', 'modern', 'hyperscale', 'old'], label_visibility="collapsed")
    st.caption("Supports any CSV — AI auto-detects columns")

df = None
col_map = {}
quality = {}

if uploaded:
    with st.spinner("AI analyzing columns..."):
        df, col_map, quality = load_and_prepare(uploaded)
    st.sidebar.success(f"{len(df):,} rows loaded")
elif use_sample:
    with st.spinner("Loading sample data..."):
        df, col_map, quality = load_and_prepare('gpu_metrics_30d.csv')
    st.sidebar.success("Sample data loaded")

if df is None:
    st.markdown("""
    <div style="text-align:center;padding:80px 40px">
        <div style="font-size:12px;font-weight:600;letter-spacing:4px;color:#6366f1;margin-bottom:16px">AI INFRASTRUCTURE INTELLIGENCE</div>
        <div style="font-size:50px;font-weight:700;background:linear-gradient(135deg,#fff 0%,#a5b4fc 50%,#6366f1 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:16px">InfraLens</div>
        <div style="font-size:17px;color:#6b7280;margin-bottom:48px">Upload any GPU or server metrics CSV<br>Get exact dollar savings with time-specific action plans.</div>
        <div style="background:#111120;border:1.5px dashed #2d2d44;border-radius:16px;padding:48px 32px;max-width:480px;margin:0 auto">
            <div style="font-size:28px;margin-bottom:12px">⬆</div>
            <div style="font-size:15px;font-weight:600;color:#e5e7eb;margin-bottom:6px">Drop your CSV here</div>
            <div style="font-size:13px;color:#4b5563">Any format — AI auto-detects columns automatically</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Run Analysis ──
with st.spinner("Running advanced analysis..."):
    idle   = detect_idle_advanced(df)
    peak   = detect_peak_waste_advanced(df, schedule)
    over   = detect_overprovision_advanced(df)
    scores = compute_efficiency_scores(df)
    sim    = simulate_before_after(df, schedule=schedule, dc_type=dc_type)
    recs   = generate_recommendations(idle, peak, over, sim, scores)

idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0

# ── Executive Summary ──
st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Executive Summary</div>', unsafe_allow_html=True)

st.markdown(f"""
<div class="exec-box">
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:24px;text-align:center">
        <div>
            <div class="metric-label">Current Cost</div>
            <div class="metric-value white">${sim['before_monthly']:,.0f}</div>
            <div class="metric-sub">per month</div>
        </div>
        <div>
            <div class="metric-label">After Optimization</div>
            <div class="metric-value white">${sim['after_monthly']:,.0f}</div>
            <div class="metric-sub">per month</div>
        </div>
        <div>
            <div class="metric-label">Monthly Savings</div>
            <div class="metric-value green">${sim['savings_monthly']:,.0f}</div>
            <div class="metric-sub">{sim['savings_pct']}% reduction</div>
        </div>
        <div>
            <div class="metric-label">Annual Savings</div>
            <div class="metric-value green">${sim['savings_annual']:,.0f}</div>
            <div class="metric-sub">per year</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Findings ──
st.markdown('<div class="section-title">Savings Breakdown</div>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Idle Waste</div><div class="metric-value red">${idle_total:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Peak Shifting</div><div class="metric-value amber">${peak["monthly_savings"]:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Overprovisioning</div><div class="metric-value blue">${over["monthly_savings"]:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)

st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)

# ── Charts ──
col1, col2 = st.columns([3, 2])
with col1:
    st.markdown('<div class="section-title">Utilization — 24h Pattern</div>', unsafe_allow_html=True)
    if 'gpu_util' in df.columns and 'hour' in df.columns:
        hourly = df.groupby('hour')['gpu_util'].mean().reset_index()
        hourly['status'] = hourly['gpu_util'].apply(
            lambda x: 'Idle Waste' if x < 20 else ('Peak' if x > 70 else 'Normal'))
        fig = px.bar(hourly, x='hour', y='gpu_util', color='status',
                     color_discrete_map={'Idle Waste':'#f87171','Peak':'#818cf8','Normal':'#34d399'},
                     labels={'hour':'Hour','gpu_util':'Avg Util (%)'},
                     template='plotly_dark')
        fig.update_layout(height=260, margin=dict(t=8,b=8,l=0,r=0),
                          paper_bgcolor='#111120', plot_bgcolor='#111120', legend_title_text='')
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">Before vs After</div>', unsafe_allow_html=True)
    fig2 = go.Figure()
    fig2.add_bar(name='Before', x=['Monthly Cost'], y=[sim['before_monthly']], marker_color='#f87171')
    fig2.add_bar(name='After',  x=['Monthly Cost'], y=[sim['after_monthly']],  marker_color='#34d399')
    fig2.update_layout(height=260, margin=dict(t=8,b=8,l=0,r=0),
                       paper_bgcolor='#111120', plot_bgcolor='#111120',
                       barmode='group', template='plotly_dark', legend_title_text='')
    st.plotly_chart(fig2, use_container_width=True)

st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

# ── Efficiency Scores ──
if len(scores) > 0:
    st.markdown('<div class="section-title">GPU Efficiency Scores</div>', unsafe_allow_html=True)
    cols = st.columns(min(len(scores), 4))
    for i, (_, row) in enumerate(scores.iterrows()):
        if i >= 4: break
        grade_class = f"grade-{row['grade'].lower()}"
        with cols[i % 4]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">{row['gpu_id']}</div>
                <div class="metric-value white">{row['efficiency']:.0f}</div>
                <div style="margin:6px 0"><span class="grade-badge {grade_class}">Grade {row['grade']}</span></div>
                <div class="metric-sub">util {row['avg_util']}% · waste {row['waste_pct']}%</div>
            </div>""", unsafe_allow_html=True)

st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)

# ── Action Plans ──
st.markdown('<div class="section-title">Action Plans — Sorted by Impact</div>', unsafe_allow_html=True)

for rec in recs:
    effort_color = '#34d399' if rec.effort == 'Low' else '#fbbf24' if rec.effort == 'Medium' else '#f87171'
    saving_text = f"Save ${rec.monthly_savings:,.0f} / month" if rec.monthly_savings > 0 else "Performance improvement"
    st.markdown(f"""
    <div class="rec-card">
        <div class="rec-priority">#{rec.priority} · {rec.category}</div>
        <div class="rec-title">{rec.title}</div>
        <div class="rec-detail">{rec.detail}</div>
        <div class="rec-action">Action: {rec.action}</div>
        <div class="rec-saving">{saving_text}</div>
        <div class="rec-meta">
            Effort: <span style="color:{effort_color}">{rec.effort}</span> &nbsp;·&nbsp;
            Timeframe: {rec.timeframe} &nbsp;·&nbsp;
            Confidence: {rec.confidence:.0f}%
        </div>
    </div>""", unsafe_allow_html=True)

# ── AI Column Mapping ──
with st.expander(f"AI Column Mapping — {len(col_map)} columns detected ({quality.get('tier','?')} tier)"):
    cols = st.columns(3)
    for i, (standard, original) in enumerate(col_map.items()):
        with cols[i % 3]:
            st.markdown(f'<div style="font-size:11px;color:#6b7280">{standard}</div>'
                        f'<div style="font-size:13px;color:#e5e7eb;font-family:monospace">{original}</div>',
                        unsafe_allow_html=True)

# ── Download Report ──
st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)
report_text = format_report(recs, sim, quality)
st.download_button(
    label="Download Full Report (.txt)",
    data=report_text,
    file_name="infralens_report.txt",
    mime="text/plain",
    use_container_width=True
)

st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;font-size:11px;color:#1e1e2e;letter-spacing:2px">INFRALENS · AI INFRASTRUCTURE COST OPTIMIZATION</div>', unsafe_allow_html=True)
