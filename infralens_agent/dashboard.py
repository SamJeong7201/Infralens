"""
dashboard.py
────────────
InfraLens Agent 로컬 대시보드
streamlit run dashboard.py
→ localhost:8501

데이터 밖으로 안 나감
모든 것이 로컬에서만 작동
"""
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import subprocess
import yaml
import sys
from pathlib import Path
from datetime import datetime, timedelta

# ── 설정 ──
CONFIG_PATH = Path(__file__).parent / 'config.yaml'

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

def load_db(hours=24):
    config  = load_config()
    db_path = Path(__file__).parent / config['storage']['db_path']
    if not db_path.exists():
        return pd.DataFrame(), pd.DataFrame()
    conn    = sqlite3.connect(str(db_path))
    cutoff  = (datetime.now() - timedelta(hours=hours)).isoformat()
    metrics = pd.read_sql_query(
        f"SELECT * FROM gpu_metrics WHERE timestamp > '{cutoff}' ORDER BY timestamp",
        conn
    )
    try:
        actions = pd.read_sql_query(
            "SELECT * FROM actions_log ORDER BY timestamp DESC LIMIT 50", conn
        )
    except:
        actions = pd.DataFrame()
    conn.close()
    return metrics, actions

# ── 페이지 설정 ──
st.set_page_config(
    page_title="InfraLens Agent",
    page_icon="⚡",
    layout="wide"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #0a0a0f; }
.metric-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 20px; text-align: center; }
.metric-label { font-size: 11px; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; color: #6b7280; margin-bottom: 8px; }
.metric-value { font-size: 28px; font-weight: 700; line-height: 1; }
.rec-card { background: #111120; border: 1px solid #1e1e2e; border-radius: 14px; padding: 20px; margin-bottom: 10px; }
.alert-card { background: #1a0a0a; border: 1px solid #7f1d1d; border-radius: 10px; padding: 14px; margin-bottom: 8px; }
section[data-testid="stSidebar"] { background: #0f0f1a; border-right: 1px solid #1e1e2e; }
</style>
""", unsafe_allow_html=True)

config   = load_config()
lab_name = config['lab']['name']

# ── 사이드바 ──
with st.sidebar:
    st.markdown("### ⚡ InfraLens Agent")
    st.caption(f"**{lab_name}**")
    st.divider()

    hours = st.selectbox("Time range", [1, 6, 24, 48, 168],
                         format_func=lambda x: f'Last {x}h' if x < 168 else 'Last 7d',
                         index=2)

    st.divider()

    # 수동 수집 버튼
    if st.button("🔄 Collect Now", use_container_width=True):
        with st.spinner("Collecting..."):
            result = subprocess.run(
                [sys.executable, 'collect.py'],
                capture_output=True, text=True, cwd=str(Path(__file__).parent)
            )
            if result.returncode == 0:
                st.success("Collected!")
            else:
                st.error(result.stderr[:200])
        st.rerun()

    st.divider()
    st.caption("📍 Data stays on this server")
    st.caption("No raw data leaves your environment")

# ── 데이터 로드 ──
metrics, actions = load_db(hours)

# ── 데이터 없으면 ──
if metrics.empty:
    st.markdown("""
    <div style="text-align:center;padding:80px 40px">
        <div style="font-size:50px;font-weight:700;background:linear-gradient(135deg,#fff 0%,#a5b4fc 50%,#6366f1 100%);
                    -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:16px">
            InfraLens Agent
        </div>
        <div style="font-size:17px;color:#6b7280;margin-bottom:32px">
            No data yet. Run collection first.
        </div>
        <div style="background:#111120;border:1.5px dashed #2d2d44;border-radius:16px;
                    padding:32px;max-width:520px;margin:0 auto;text-align:left">
            <div style="font-size:13px;color:#6b7280;font-family:monospace">
                # Run once:<br>
                python run.py<br><br>
                # Or auto every hour:<br>
                python run.py --loop
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── 분석 ──
latest_ts  = metrics['timestamp'].max()
latest     = metrics[metrics['timestamp'] == latest_ts]
overall    = round(metrics['gpu_util'].mean(), 1)
n_gpus     = metrics['gpu_index'].nunique()
n_idle     = len(latest[latest['gpu_util'] < 15])
total_pw   = round(latest['power_draw'].sum(), 0)
idle_pw    = round(latest[latest['gpu_util'] < 15]['power_draw'].sum(), 0)
idle_pct   = round((metrics['gpu_util'] < 15).sum() / len(metrics) * 100, 1)

# ── 헤더 ──
st.markdown(f"""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
    <div style="font-size:22px;font-weight:700;color:white">⚡ {lab_name}</div>
    <div style="background:#1e3a5f;color:#60a5fa;font-size:11px;font-weight:700;
                padding:2px 10px;border-radius:20px">LIVE</div>
    <div style="font-size:12px;color:#4b5563;margin-left:auto">
        Last update: {latest_ts[:16]}
    </div>
</div>
""", unsafe_allow_html=True)

# ── 메트릭 카드 ──
c1, c2, c3, c4, c5 = st.columns(5)
cards = [
    (c1, "Cluster Util",  f"{overall}%",
     "green" if overall > 60 else "amber" if overall > 30 else "red"),
    (c2, "Idle GPUs",     f"{n_idle}/{n_gpus}",
     "red" if n_idle > 0 else "green"),
    (c3, "Idle Time",     f"{idle_pct}%",
     "red" if idle_pct > 50 else "amber"),
    (c4, "Total Power",   f"{total_pw}W", "white"),
    (c5, "Idle Power",    f"{idle_pw}W",
     "red" if idle_pw > 500 else "amber"),
]
for col, label, value, color in cards:
    with col:
        st.markdown(f'''<div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value {color}">{value}</div>
        </div>''', unsafe_allow_html=True)

st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)

# ── 차트 ──
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<div style="font-size:12px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#4b5563;margin-bottom:12px">Utilization History</div>', unsafe_allow_html=True)
    if 'timestamp' in metrics.columns:
        metrics['ts'] = pd.to_datetime(metrics['timestamp'])
        hourly = metrics.groupby(metrics['ts'].dt.floor('h'))['gpu_util'].mean().reset_index()
        hourly.columns = ['time', 'util']
        fig = px.area(hourly, x='time', y='util',
                     labels={'time':'', 'util':'GPU Util (%)'},
                     template='plotly_dark',
                     color_discrete_sequence=['#6366f1'])
        fig.add_hline(y=15, line_dash="dash", line_color="#f87171",
                     annotation_text="idle threshold")
        fig.update_layout(height=220, margin=dict(t=8,b=8,l=0,r=0),
                         paper_bgcolor='#111120', plot_bgcolor='#111120')
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<div style="font-size:12px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#4b5563;margin-bottom:12px">Current GPU Status</div>', unsafe_allow_html=True)
    if not latest.empty:
        for _, row in latest.iterrows():
            util  = row['gpu_util']
            color = '#f87171' if util < 15 else '#34d399' if util > 60 else '#fbbf24'
            bar   = int(util / 100 * 12)
            bar_s = '█' * bar + '░' * (12 - bar)
            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;
                        background:#111120;padding:8px 12px;border-radius:8px">
                <div style="font-size:11px;color:#6b7280;width:80px">GPU-{row['gpu_index']}</div>
                <div style="font-size:11px;color:{color};font-family:monospace">{bar_s}</div>
                <div style="font-size:11px;color:{color};width:40px">{util:.0f}%</div>
                <div style="font-size:11px;color:#4b5563">{row['power_draw']:.0f}W</div>
            </div>
            """, unsafe_allow_html=True)

st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)

# ── 알림 + 권장사항 ──
from analyze import analyze_current_state, detect_anomalies, generate_recommendations
state  = analyze_current_state(metrics)
alerts = detect_anomalies(metrics)
recs   = generate_recommendations(state, config['lab']['mode'])

col3, col4 = st.columns([1, 2])

with col3:
    st.markdown('<div style="font-size:12px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#4b5563;margin-bottom:12px">Alerts</div>', unsafe_allow_html=True)
    if not alerts:
        st.markdown('<div style="color:#34d399;font-size:13px;padding:12px;background:#111120;border-radius:8px">✅ All systems normal</div>', unsafe_allow_html=True)
    else:
        for a in alerts:
            sev_color = '#f87171' if a['severity'] == 'critical' else '#fbbf24'
            st.markdown(f"""
            <div class="alert-card">
                <div style="font-size:11px;font-weight:700;color:{sev_color};margin-bottom:4px">
                    {'🚨' if a['severity']=='critical' else '⚠️'} {a['severity'].upper()}
                </div>
                <div style="font-size:12px;color:#e5e7eb">{a['message']}</div>
            </div>
            """, unsafe_allow_html=True)

with col4:
    st.markdown('<div style="font-size:12px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#4b5563;margin-bottom:12px">Recommendations</div>', unsafe_allow_html=True)
    if not recs:
        st.markdown('<div style="color:#34d399;font-size:13px;padding:12px;background:#111120;border-radius:8px">✅ No actions needed</div>', unsafe_allow_html=True)
    else:
        for rec in recs:
            with st.container():
                st.markdown(f"""
                <div class="rec-card">
                    <div style="font-size:11px;font-weight:700;color:#6366f1;margin-bottom:4px">
                        #{rec['priority']} · {rec['category']}
                    </div>
                    <div style="font-size:13px;font-weight:600;color:#f9fafb;margin-bottom:8px">
                        {rec['title']}
                    </div>
                    <div style="font-size:11px;color:#6b7280">
                        Risk: {rec['risk']} &nbsp;·&nbsp;
                        {'Auto-safe' if rec.get('auto_safe') else 'Requires approval'}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # 명령어 보기 + 실행
                with st.expander("View commands"):
                    st.code(rec['command'], language='bash')

                    col_dry, col_run, col_rb = st.columns(3)
                    with col_dry:
                        if st.button(f"🔍 Dry Run", key=f"dry_{rec['priority']}"):
                            st.info(f"Would execute:\n{rec['command']}")

                    with col_run:
                        if not config['execution']['enabled']:
                            st.warning("Execution disabled in config.yaml")
                        else:
                            if st.button(f"▶️ Execute", key=f"exec_{rec['priority']}",
                                        type="primary"):
                                with st.spinner("Executing..."):
                                    result = subprocess.run(
                                        rec['command'].split('\n')[0].strip(),
                                        shell=True, capture_output=True, text=True
                                    )
                                    if result.returncode == 0:
                                        st.success(f"✅ Done!\n{result.stdout}")
                                    else:
                                        st.error(f"❌ Failed\n{result.stderr}")

                    with col_rb:
                        if st.button(f"↩️ Rollback", key=f"rb_{rec['priority']}"):
                            st.code(rec['rollback'], language='bash')

# ── PDF 리포트 ──
st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)
st.divider()

col_pdf1, col_pdf2, col_pdf3 = st.columns([2, 1, 1])
with col_pdf1:
    company_name = st.text_input("Company / Lab name for report",
                                 value=lab_name, key='pdf_name')
with col_pdf2:
    report_type = st.selectbox("Report type",
                               ["Business Edition", "Lab Edition"],
                               key='report_type')
with col_pdf3:
    st.markdown('<div style="height:28px"></div>', unsafe_allow_html=True)
    gen_btn = st.button("📄 Generate PDF", use_container_width=True, key='gen_pdf')

if gen_btn:
    with st.spinner("Generating PDF report..."):
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))

            if report_type == "Business Edition":
                from report_pdf import generate_pdf
                from recommender import generate_recommendations as gen_biz_recs
                from data_loader import load_and_prepare
                from analyzer import engineer_features
                from cost_model import simulate_before_after

                # metrics를 business 형태로 변환
                df = metrics.rename(columns={
                    'gpu_util': 'gpu_util',
                    'power_draw': 'power_draw',
                    'temperature': 'temperature',
                })
                if 'electricity_rate' not in df.columns:
                    df['electricity_rate'] = 4.10

                df = engineer_features(df)
                from analyzer import (detect_idle_maximum, detect_peak_waste_advanced,
                                      detect_overprovision_advanced,
                                      compute_advanced_efficiency_score,
                                      detect_thermal_throttling,
                                      detect_memory_bandwidth_bottleneck,
                                      detect_inter_gpu_waste, detect_workload_gap)
                idle   = detect_idle_maximum(df)
                peak   = detect_peak_waste_advanced(df)
                over   = detect_overprovision_advanced(df)
                sim    = simulate_before_after(df)
                scores = compute_advanced_efficiency_score(df)
                thermal = detect_thermal_throttling(df)
                mem_b   = detect_memory_bandwidth_bottleneck(df)
                inter   = detect_inter_gpu_waste(df)
                gap     = detect_workload_gap(df)
                biz_recs = gen_biz_recs(idle, peak, over, sim, scores, df=df,
                    thermal=thermal, mem_bottleneck=mem_b,
                    inter_gpu=inter, workload_gap=gap)
                quality = {'clean_rows': len(df), 'devices': n_gpus,
                          'date_range': f'Last {hours}h', 'tier': 'A'}
                pdf_bytes = generate_pdf(biz_recs, sim, quality, scores,
                                        df=df, company_name=company_name)
                filename = 'infralens_business_report.pdf'

            else:
                from lab_report_pdf import generate_lab_pdf
                from lab_analyzer import run_lab_analysis
                from lab_recommender import generate_lab_recommendations
                lab_analysis = run_lab_analysis(metrics)
                lab_recs     = generate_lab_recommendations(lab_analysis)
                pdf_bytes    = generate_lab_pdf(lab_recs, lab_analysis,
                                               metrics_df=metrics,
                                               lab_name=company_name)
                filename = 'infralens_lab_report.pdf'

            st.session_state['pdf_bytes'] = pdf_bytes
            st.session_state['pdf_name']  = filename
            st.success("PDF ready!")

        except Exception as e:
            st.error(f"PDF generation failed: {e}")

if st.session_state.get('pdf_bytes'):
    st.download_button(
        label=f"⬇ Download {st.session_state.get('pdf_name','report.pdf')}",
        data=st.session_state['pdf_bytes'],
        file_name=st.session_state.get('pdf_name', 'report.pdf'),
        mime='application/pdf',
        use_container_width=True,
        key='dl_pdf_agent'
    )

# ── 실행 히스토리 ──
if not actions.empty:
    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:12px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#4b5563;margin-bottom:12px">Execution History</div>', unsafe_allow_html=True)
    st.dataframe(
        actions[['timestamp','action_type','command','status']].head(10),
        hide_index=True,
        use_container_width=True
    )

st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;font-size:11px;color:#1e1e2e;letter-spacing:2px">INFRALENS AGENT · LOCAL-FIRST GPU OPTIMIZATION</div>', unsafe_allow_html=True)
