import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import html as html_lib
import warnings
warnings.filterwarnings('ignore')

from data_loader import load_and_prepare
from data_profiler import profile_dataset, analyze_billing
from cost_model import simulate_before_after
from analyzer import (detect_idle_maximum, detect_peak_waste_advanced,
                      detect_thermal_throttling, detect_memory_bandwidth_bottleneck,
                      detect_inter_gpu_waste, detect_workload_gap,
                      compute_advanced_efficiency_score,
                      detect_overprovision_advanced, engineer_features)
from recommender import generate_recommendations

st.set_page_config(page_title="InfraLens", page_icon="⚡", layout="wide")

# session_state 초기화
for key in ['df','col_map','quality','file_id','analysis','pdf_ts','pdf_billing',
            'pdf_lab','lab_analysis','lab_recs','version']:
    if key not in st.session_state:
        st.session_state[key] = None

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
.rec-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 22px 26px; margin-bottom: 4px; }
.rec-priority { font-size: 11px; font-weight: 700; letter-spacing: 2px; color: #6366f1; text-transform: uppercase; margin-bottom: 6px; }
.rec-title { font-size: 16px; font-weight: 600; color: #f9fafb; margin-bottom: 8px; }
.rec-detail { font-size: 13px; color: #6b7280; margin-bottom: 10px; line-height: 1.6; }
.rec-saving { font-size: 20px; font-weight: 700; color: #34d399; }
.rec-meta { font-size: 11px; color: #4b5563; margin-top: 6px; }
.section-title { font-size: 12px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; color: #4b5563; margin-bottom: 16px; }
.grade-badge { display: inline-block; font-size: 12px; font-weight: 700; padding: 2px 10px; border-radius: 20px; }
.grade-a { background: #064e3b; color: #34d399; }
.grade-b { background: #1e3a5f; color: #60a5fa; }
.grade-c { background: #422006; color: #fbbf24; }
.grade-d { background: #450a0a; color: #f87171; }
.exec-box { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 24px 28px; margin-bottom: 20px; }
.type-badge { display: inline-block; font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 20px; margin-left: 8px; }
.type-timeseries { background: #1e3a5f; color: #60a5fa; }
.type-billing { background: #2d1b4e; color: #a78bfa; }
</style>
""", unsafe_allow_html=True)

# ── session_state 초기화 ──
for key in ['df','col_map','quality','file_id','analysis','pdf_ts','pdf_billing']:
    if key not in st.session_state:
        st.session_state[key] = None

# ── 사이드바 ──
with st.sidebar:
    st.markdown("### ⚡ InfraLens")
    st.caption("AI Infrastructure Cost Optimization")
    st.divider()

    # 버전 선택
    st.markdown("**Select Mode**")
    version = st.radio(
        "mode",
        ["🏢  Business", "🔬  Research Lab"],
        label_visibility="collapsed",
        key="version_radio"
    )
    st.session_state.version = "business" if "Business" in version else "lab"
    st.divider()

    st.markdown("**Upload CSV**")
    if st.session_state.version == "lab":
        st.caption("GPU metrics CSV + optional Slurm jobs CSV")
        uploaded = st.file_uploader("GPU metrics CSV", type="csv", label_visibility="collapsed", key="upload_main")
        uploaded_jobs = st.file_uploader("Slurm jobs CSV (optional)", type="csv", label_visibility="collapsed", key="upload_jobs")
    else:
        uploaded = st.file_uploader("GPU / server / billing CSV", type="csv", label_visibility="collapsed", key="upload_main")
        uploaded_jobs = None

    st.markdown("**Or use sample**")
    use_sample = st.button("Run with sample data", use_container_width=True)
    st.divider()

    if st.session_state.version == "business":
        st.markdown("**Settings**")
        schedule = st.selectbox("Cloud pricing", ['aws_us_east', 'gcp_us_central', 'kepco_korea'], label_visibility="collapsed")
        dc_type  = st.selectbox("DC type", ['average', 'modern', 'hyperscale', 'old'], label_visibility="collapsed")
    else:
        schedule = 'aws_us_east'
        dc_type  = 'average'
        st.markdown("**Lab Settings**")
        lab_name = st.text_input("Lab name", value="Your Lab", label_visibility="collapsed")

    st.caption("Supports any CSV — AI auto-detects columns & data type")

# ── 데이터 로딩 (파일 바뀔 때만) ──
if uploaded is not None:
    file_id = uploaded.name + str(uploaded.size)
    if st.session_state.file_id != file_id:
        with st.spinner("AI analyzing your data..."):
            st.session_state.df, st.session_state.col_map, st.session_state.quality = load_and_prepare(uploaded)
        st.session_state.file_id = file_id
        st.session_state.analysis = None
        st.session_state.pdf_ts = None
        st.session_state.pdf_billing = None
        st.session_state.lab_analysis = None
        st.session_state.lab_recs = None
        st.session_state.pdf_lab = None
    st.sidebar.success(f"{len(st.session_state.df):,} rows loaded")

    # Slurm jobs CSV (Lab 모드)
    if uploaded_jobs is not None:
        jobs_id = uploaded_jobs.name + str(uploaded_jobs.size)
        if st.session_state.get('jobs_file_id') != jobs_id:
            st.session_state.jobs_df = pd.read_csv(uploaded_jobs)
            st.session_state.jobs_file_id = jobs_id
            st.session_state.lab_analysis = None
    elif 'jobs_df' not in st.session_state:
        st.session_state.jobs_df = None

elif use_sample:
    sample_file = 'lab_gpu_metrics.csv' if st.session_state.version == 'lab' else 'gpu_metrics_30d.csv'
    sample_id   = f'sample_{st.session_state.version}'
    if st.session_state.file_id != sample_id:
        with st.spinner("Loading sample data..."):
            if st.session_state.version == 'lab':
                st.session_state.df = pd.read_csv('lab_gpu_metrics.csv')
                st.session_state.col_map = {}
                st.session_state.quality = {
                    'clean_rows': len(st.session_state.df),
                    'devices': st.session_state.df['gpu_id'].nunique() if 'gpu_id' in st.session_state.df.columns else '?',
                    'date_range': 'Sample data',
                    'tier': 'A'
                }
                st.session_state.jobs_df = pd.read_csv('lab_slurm_jobs.csv')
            else:
                st.session_state.df, st.session_state.col_map, st.session_state.quality = load_and_prepare('gpu_metrics_30d.csv')
                st.session_state.jobs_df = None
        st.session_state.file_id = sample_id
        st.session_state.analysis = None
        st.session_state.lab_analysis = None
        st.session_state.lab_recs = None
        st.session_state.pdf_ts = None
        st.session_state.pdf_billing = None
        st.session_state.pdf_lab = None
    st.sidebar.success("Sample data loaded")

df      = st.session_state.df
col_map = st.session_state.col_map or {}
quality = st.session_state.quality or {}

# ── 데이터 없으면 랜딩 ──
if df is None:
    st.markdown("""
    <div style="text-align:center;padding:80px 40px">
        <div style="font-size:12px;font-weight:600;letter-spacing:4px;color:#6366f1;margin-bottom:16px">AI INFRASTRUCTURE INTELLIGENCE</div>
        <div style="font-size:50px;font-weight:700;background:linear-gradient(135deg,#fff 0%,#a5b4fc 50%,#6366f1 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:16px">InfraLens</div>
        <div style="font-size:17px;color:#6b7280;margin-bottom:48px">Upload any GPU, server, or cloud billing CSV<br>AI auto-detects data type and finds exact dollar savings.</div>
        <div style="background:#111120;border:1.5px dashed #2d2d44;border-radius:16px;padding:48px 32px;max-width:520px;margin:0 auto">
            <div style="font-size:28px;margin-bottom:12px">⬆</div>
            <div style="font-size:15px;font-weight:600;color:#e5e7eb;margin-bottom:8px">Drop your CSV here</div>
            <div style="font-size:13px;color:#4b5563;line-height:1.8">
                GPU monitoring data · Cloud billing records<br>
                nvidia-smi CSV · Any infrastructure CSV
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ══════════════════════════════════════════
# LAB 버전 분기
# ══════════════════════════════════════════
if st.session_state.version == 'lab':
    from lab_analyzer import run_lab_analysis
    from lab_recommender import generate_lab_recommendations
    from lab_report_pdf import generate_lab_pdf

    jobs_df = st.session_state.get('jobs_df', None)
    _lab_name = st.session_state.get('lab_name', 'Your Lab') if 'lab_name' in dir() else 'Your Lab'

    # 분석 캐싱
    if st.session_state.lab_analysis is None:
        with st.spinner("Running lab analysis..."):
            st.session_state.lab_analysis = run_lab_analysis(df, jobs_df)
            st.session_state.lab_recs = generate_lab_recommendations(st.session_state.lab_analysis)

    analysis = st.session_state.lab_analysis
    recs     = st.session_state.lab_recs
    cu = analysis.get('cluster_util', {})
    pt = analysis.get('power_thermal', {})
    uf = analysis.get('user_fairness', {})
    qb = analysis.get('queue_bottleneck', {})

    # ── Lab UI ──
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
        <div style="font-size:11px;font-weight:600;letter-spacing:3px;color:#2563eb">
            RESEARCH LAB EDITION
        </div>
        <div style="background:#1e3a5f;color:#60a5fa;font-size:11px;font-weight:700;
                    padding:2px 10px;border-radius:20px">
            🔬 Lab Mode
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Executive metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Cluster Utilization</div><div class="metric-value {"green" if cu.get("overall_util",0)>60 else "amber" if cu.get("overall_util",0)>40 else "red"}">{cu.get("overall_util",0):.0f}%</div><div class="metric-sub">overall average</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Idle GPU Time</div><div class="metric-value red">{cu.get("idle_util_pct",0):.0f}%</div><div class="metric-sub">recoverable capacity</div></div>', unsafe_allow_html=True)
    with c3:
        avg_wait = qb.get("avg_wait", 0)
        st.markdown(f'<div class="metric-card"><div class="metric-label">Avg Queue Wait</div><div class="metric-value {"red" if avg_wait>60 else "amber"}">{avg_wait:.0f} min</div><div class="metric-sub">job wait time</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Monthly Elec. Cost</div><div class="metric-value white">${pt.get("monthly_elec_cost",0):,.0f}</div><div class="metric-sub">estimated</div></div>', unsafe_allow_html=True)

    st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)

    # 시각화
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown('<div class="section-title">Utilization — 24h Pattern</div>', unsafe_allow_html=True)
        if 'hour' in df.columns and 'gpu_util' in df.columns:
            hourly = df.groupby('hour')['gpu_util'].mean().reset_index()
            hourly['status'] = hourly['gpu_util'].apply(
                lambda x: 'Idle (<15%)' if x < 15 else ('Peak (>70%)' if x > 70 else 'Normal'))
            fig = px.bar(hourly, x='hour', y='gpu_util', color='status',
                        color_discrete_map={'Idle (<15%)':'#f87171','Peak (>70%)':'#818cf8','Normal':'#34d399'},
                        labels={'hour':'Hour','gpu_util':'Avg Util (%)'},
                        template='plotly_dark')
            fig.add_hline(y=15, line_dash="dash", line_color="red", opacity=0.5,
                         annotation_text="idle threshold")
            fig.update_layout(height=260, margin=dict(t=8,b=8,l=0,r=0),
                             paper_bgcolor='#111120', plot_bgcolor='#111120', legend_title_text='')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-title">Weekday vs Weekend</div>', unsafe_allow_html=True)
        fig2 = go.Figure()
        fig2.add_bar(name='Weekday', x=['Avg Utilization'],
                    y=[cu.get('weekday_util', 0)], marker_color='#34d399')
        fig2.add_bar(name='Weekend', x=['Avg Utilization'],
                    y=[cu.get('weekend_util', 0)], marker_color='#f87171')
        fig2.update_layout(height=260, margin=dict(t=8,b=8,l=0,r=0),
                          paper_bgcolor='#111120', plot_bgcolor='#111120',
                          barmode='group', template='plotly_dark')
        st.plotly_chart(fig2, use_container_width=True)

    # 사용자별 사용률
    if uf.get('user_gpu_pct'):
        st.markdown('<div class="section-title">GPU Usage by User</div>', unsafe_allow_html=True)
        user_data = sorted(uf['user_gpu_pct'].items(), key=lambda x: -x[1])[:10]
        users = [u for u, _ in user_data]
        pcts  = [p for _, p in user_data]
        fig3 = px.bar(x=pcts, y=users, orientation='h',
                     labels={'x':'GPU Time %','y':'User'},
                     template='plotly_dark',
                     color=pcts,
                     color_continuous_scale='Blues')
        fig3.update_layout(height=280, margin=dict(t=8,b=8,l=0,r=0),
                          paper_bgcolor='#111120', plot_bgcolor='#111120',
                          showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    # Recommendations
    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Optimization Recommendations</div>', unsafe_allow_html=True)

    impact_colors = {
        'throughput': '#6366f1',
        'fairness':   '#9333ea',
        'efficiency': '#0d9488',
        'power':      '#059669',
    }

    for rec in recs:
        color = impact_colors.get(rec.impact, '#6366f1')
        effort_color = '#34d399' if rec.effort=='Low' else '#fbbf24' if rec.effort=='Medium' else '#f87171'
        st.markdown(f"""
        <div class="rec-card">
            <div class="rec-priority" style="color:{color}">#{rec.priority} · {rec.category}</div>
            <div class="rec-title">{rec.title}</div>
            <div class="rec-detail">{rec.detail}</div>
            <div style="display:flex;gap:16px;margin-bottom:8px">
                <span style="font-size:11px;color:#6b7280">
                    Now: <b style="color:#f87171">{rec.metric_before}</b>
                </span>
                <span style="font-size:11px;color:#6b7280">→</span>
                <span style="font-size:11px;color:#6b7280">
                    Target: <b style="color:#34d399">{rec.metric_after}</b>
                </span>
            </div>
            <div class="rec-meta">
                Effort: <span style="color:{effort_color}">{rec.effort}</span> &nbsp;·&nbsp;
                Owner: {rec.owner} &nbsp;·&nbsp;
                {rec.timeframe}
            </div>
        </div>""", unsafe_allow_html=True)
        action_html = html_lib.escape(rec.action).replace('\n', '<br>').replace('  ', '&nbsp;&nbsp;')
        st.markdown(f'<div style="background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:12px;margin-top:-8px;margin-bottom:16px;font-family:monospace;font-size:12px;color:#a5b4fc;line-height:1.8;">{action_html}</div>', unsafe_allow_html=True)

    # PDF
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    lab_name_input = st.text_input('Lab name (for report)', value='Your Lab', key='lab_name_input')
    if st.button('Generate Lab PDF Report', use_container_width=True, key='lab_pdf_btn'):
        with st.spinner('Generating lab report...'):
            st.session_state.pdf_lab = generate_lab_pdf(
                recs, analysis,
                metrics_df=df,
                jobs_df=jobs_df,
                lab_name=lab_name_input
            )
    if st.session_state.get('pdf_lab') is not None:
        st.download_button(
            label='⬇ Download Lab PDF Report',
            data=st.session_state.pdf_lab,
            file_name='infralens_lab_report.pdf',
            mime='application/pdf',
            use_container_width=True,
            key='lab_pdf_dl'
        )

    st.stop()

# ══════════════════════════════════════════
# BUSINESS 버전 (기존 코드)
# ══════════════════════════════════════════

# ── 데이터 타입 감지 ──
profile   = profile_dataset(df)
data_type = profile['data_type']
type_label = 'Time-series' if data_type == 'timeseries' else \
             'Billing Records' if data_type == 'billing' else 'Mixed'
type_class = 'type-timeseries' if data_type == 'timeseries' else 'type-billing'

st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════
# BILLING 분기
# ══════════════════════════════════════════
if data_type == 'billing':
    with st.spinner("Analyzing billing records..."):
        billing = analyze_billing(df, col_map)

    total_savings = billing['monthly_savings']
    monthly_cost  = billing.get('monthly_cost', 0)
    after_cost    = monthly_cost - total_savings

    c_title, c_type = st.columns([4, 1])
    with c_title:
        st.markdown('<div class="section-title">Executive Summary</div>', unsafe_allow_html=True)
    with c_type:
        st.markdown(f'<div style="text-align:right;padding-top:4px"><span class="type-badge {type_class}">{type_label}</span></div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="exec-box">
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:24px;text-align:center">
            <div><div class="metric-label">Current Cost</div><div class="metric-value white">${monthly_cost:,.0f}</div><div class="metric-sub">per month</div></div>
            <div><div class="metric-label">After Optimization</div><div class="metric-value white">${after_cost:,.0f}</div><div class="metric-sub">per month</div></div>
            <div><div class="metric-label">Monthly Savings</div><div class="metric-value green">${total_savings:,.0f}</div><div class="metric-sub">{round(total_savings/max(monthly_cost,1)*100,1)}% reduction</div></div>
            <div><div class="metric-label">Annual Savings</div><div class="metric-value green">${total_savings*12:,.0f}</div><div class="metric-sub">per year</div></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if len(billing['service_breakdown']) > 0:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-title">Cost by Service</div>', unsafe_allow_html=True)
            sb = billing['service_breakdown'].head(10)
            fig = px.bar(sb, x='cost', y='service', orientation='h',
                        color='pct', color_continuous_scale='Reds',
                        labels={'cost':'Cost ($)','service':'Service','pct':'% of Total'},
                        template='plotly_dark')
            fig.update_layout(height=300, margin=dict(t=8,b=8,l=0,r=0),
                             paper_bgcolor='#111120', plot_bgcolor='#111120')
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown('<div class="section-title">Top Cost Resources</div>', unsafe_allow_html=True)
            if len(billing['top_cost_resources']) > 0:
                st.dataframe(billing['top_cost_resources'].head(8).rename(columns={
                    'resource_id':'Resource','total_cost':'Cost ($)'}),
                    hide_index=True, use_container_width=True)

    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Action Plans — Sorted by Impact</div>', unsafe_allow_html=True)
    for i, finding in enumerate(billing['findings']):
        effort_color = '#34d399' if finding['effort']=='Low' else '#fbbf24' if finding['effort']=='Medium' else '#f87171'
        st.markdown(f"""
        <div class="rec-card">
            <div class="rec-priority">#{i+1} · {finding['type']}</div>
            <div class="rec-title">{finding['title']}</div>
            <div class="rec-detail">{finding['detail']}</div>
            <div class="rec-action">Action: {finding['action']}</div>
            <div class="rec-saving">Save ${finding['monthly_savings']:,.0f} / month</div>
            <div class="rec-meta">Effort: <span style="color:{effort_color}">{finding['effort']}</span> &nbsp;·&nbsp; Timeframe: {finding['timeframe']} &nbsp;·&nbsp; Confidence: {finding['confidence']:.0f}%</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    company_name_b = st.text_input('Company name (for report)', value='Your Company', key='billing_company')
    if st.button('Generate PDF Report', use_container_width=True, key='billing_pdf_btn'):
        with st.spinner('Generating PDF...'):
            from report_pdf import generate_billing_pdf
            st.session_state.pdf_billing = generate_billing_pdf(billing, quality, company_name=company_name_b)
    if st.session_state.pdf_billing is not None:
        st.download_button(
            label='⬇ Download PDF Report',
            data=st.session_state.pdf_billing,
            file_name='infralens_billing_report.pdf',
            mime='application/pdf',
            use_container_width=True,
            key='billing_dl'
        )

# ══════════════════════════════════════════
# TIMESERIES 분기
# ══════════════════════════════════════════
else:
    # 분석 캐싱 — 같은 파일이면 재실행 안 함
    if st.session_state.analysis is None:
        with st.spinner("Running advanced GPU analysis..."):
            df_eng = engineer_features(df)
            idle    = detect_idle_maximum(df_eng)
            peak    = detect_peak_waste_advanced(df_eng, schedule)
            over    = detect_overprovision_advanced(df_eng)
            sim     = simulate_before_after(df_eng, schedule=schedule, dc_type=dc_type)
            scores  = compute_advanced_efficiency_score(df_eng)
            thermal = detect_thermal_throttling(df_eng)
            mem_b   = detect_memory_bandwidth_bottleneck(df_eng)
            inter   = detect_inter_gpu_waste(df_eng)
            gap     = detect_workload_gap(df_eng)
            recs    = generate_recommendations(
                idle, peak, over, sim, scores, df=df_eng,
                thermal=thermal, mem_bottleneck=mem_b,
                inter_gpu=inter, workload_gap=gap
            )
            st.session_state.analysis = {
                'idle': idle, 'peak': peak, 'over': over,
                'sim': sim, 'scores': scores, 'recs': recs, 'df_eng': df_eng
            }

    # 캐시에서 꺼내기
    A       = st.session_state.analysis
    idle    = A['idle']
    peak    = A['peak']
    over    = A['over']
    sim     = A['sim']
    scores  = A['scores']
    recs    = A['recs']
    df_eng  = A['df_eng']

    idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0

    c_title, c_type = st.columns([4, 1])
    with c_title:
        st.markdown('<div class="section-title">Executive Summary</div>', unsafe_allow_html=True)
    with c_type:
        st.markdown(f'<div style="text-align:right;padding-top:4px"><span class="type-badge {type_class}">{type_label}</span></div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="exec-box">
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:24px;text-align:center">
            <div><div class="metric-label">Current Cost</div><div class="metric-value white">${sim['before_monthly']:,.0f}</div><div class="metric-sub">per month</div></div>
            <div><div class="metric-label">After Optimization</div><div class="metric-value white">${sim['after_monthly']:,.0f}</div><div class="metric-sub">per month</div></div>
            <div><div class="metric-label">Monthly Savings</div><div class="metric-value green">${sim['savings_monthly']:,.0f}</div><div class="metric-sub">{sim['savings_pct']}% reduction</div></div>
            <div><div class="metric-label">Annual Savings</div><div class="metric-value green">${sim['savings_annual']:,.0f}</div><div class="metric-sub">per year</div></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Idle Waste</div><div class="metric-value red">${idle_total:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Peak Shifting</div><div class="metric-value amber">${peak["monthly_savings"]:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><div class="metric-label">Overprovisioning</div><div class="metric-value blue">${over["monthly_savings"]:,.0f}</div><div class="metric-sub">per month</div></div>', unsafe_allow_html=True)

    st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown('<div class="section-title">Utilization — 24h Pattern</div>', unsafe_allow_html=True)
        if 'gpu_util' in df_eng.columns and 'hour' in df_eng.columns:
            hourly = df_eng.groupby('hour')['gpu_util'].mean().reset_index()
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

    if len(scores) > 0:
        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        st.markdown('<div class="section-title">GPU Efficiency Scores</div>', unsafe_allow_html=True)
        score_col = 'total_score' if 'total_score' in scores.columns else 'efficiency'
        grade_col = 'grade' if 'grade' in scores.columns else 'grade'
        cols = st.columns(min(len(scores), 4))
        for i, (_, row) in enumerate(scores.iterrows()):
            if i >= 4: break
            grade = row.get('grade','C')
            grade_class = f"grade-{grade.lower()}"
            with cols[i % 4]:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">{row['gpu_id'].upper()}</div>
                    <div class="metric-value white">{row[score_col]:.0f}</div>
                    <div style="margin:6px 0"><span class="grade-badge {grade_class}">Grade {grade}</span></div>
                    <div class="metric-sub">util {row['avg_util']}% · waste {row['waste_pct']}%</div>
                </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:24px"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Action Plans — Sorted by Impact</div>', unsafe_allow_html=True)

    for rec in recs:
        effort_color = '#34d399' if rec.effort=='Low' else '#fbbf24' if rec.effort=='Medium' else '#f87171'
        saving_text = f"Save ${rec.monthly_savings:,.0f} / month" if rec.monthly_savings > 0 else "Performance improvement"
        st.markdown(f"""
        <div class="rec-card">
            <div class="rec-priority">#{rec.priority} · {rec.category}</div>
            <div class="rec-title">{rec.title}</div>
            <div class="rec-detail">{rec.detail}</div>
            <div class="rec-saving">{saving_text}</div>
            <div class="rec-meta">
                Effort: <span style="color:{effort_color}">{rec.effort}</span> &nbsp;·&nbsp;
                Timeframe: {rec.timeframe} &nbsp;·&nbsp;
                Confidence: {rec.confidence:.0f}%
            </div>
        </div>""", unsafe_allow_html=True)
        action_html = html_lib.escape(rec.action).replace('\n', '<br>').replace('  ', '&nbsp;&nbsp;')
        st.markdown(f'<div style="background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:12px;margin-top:-8px;margin-bottom:16px;font-family:monospace;font-size:12px;color:#a5b4fc;line-height:1.8;">{action_html}</div>', unsafe_allow_html=True)

    # PDF
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    company_name = st.text_input('Company name (for report)', value='Your Company', key='ts_company')
    if st.button('Generate PDF Report', use_container_width=True, key='ts_pdf_btn'):
        with st.spinner('Generating PDF report...'):
            from report_pdf import generate_pdf
            st.session_state.pdf_ts = generate_pdf(
                recs, sim, quality, scores, df=df_eng, company_name=company_name
            )
    if st.session_state.pdf_ts is not None:
        st.download_button(
            label='⬇ Download PDF Report',
            data=st.session_state.pdf_ts,
            file_name='infralens_report.pdf',
            mime='application/pdf',
            use_container_width=True,
            key='ts_pdf_dl'
        )

# ── AI 매핑 공통 ──
with st.expander(f"AI Column Mapping — {len(col_map)} columns detected ({quality.get('tier','?')} tier)"):
    cols = st.columns(3)
    for i, (standard, original) in enumerate(col_map.items()):
        with cols[i % 3]:
            st.markdown(f'<div style="font-size:11px;color:#6b7280">{standard}</div>'
                       f'<div style="font-size:13px;color:#e5e7eb;font-family:monospace">{original}</div>',
                       unsafe_allow_html=True)

st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;font-size:11px;color:#1e1e2e;letter-spacing:2px">INFRALENS · AI INFRASTRUCTURE COST OPTIMIZATION</div>', unsafe_allow_html=True)
