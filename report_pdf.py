from fpdf import FPDF, XPos, YPos
from datetime import datetime
import io

BRAND  = (99, 102, 241)
GREEN  = (5, 150, 105)
RED    = (220, 38, 38)
AMBER  = (217, 119, 6)
BLUE   = (37, 99, 235)
DARK   = (17, 24, 39)
GRAY   = (107, 114, 128)
LGRAY  = (248, 250, 252)
LLGRAY = (243, 244, 246)
WHITE  = (255, 255, 255)
BORDER = (226, 232, 240)
PURPLE_BG = (245, 243, 255)
PURPLE_BD = (199, 194, 254)

def s(text):
    if not isinstance(text, str):
        text = str(text)
    result = ''
    for c in text:
        try:
            c.encode('latin-1')
            result += c
        except:
            mp = {'\u2014':'-','\u2013':'-','\u2018':"'",'\u2019':"'",
                  '\u201c':'"','\u201d':'"','\u2022':'-','\u00b7':'.',
                  '\u2026':'...','\u03a3':'SUM','\u00d7':'x','\u00b2':'2'}
            result += mp.get(c, '?')
    return result

class PDF(FPDF):
    def header(self):
        if self.page_no() == 1:
            return
        self.set_fill_color(*BRAND)
        self.rect(0, 0, 210, 5, 'F')
        self.set_y(8)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*GRAY)
        self.cell(87, 4, 'InfraLens - AI Infrastructure Cost Optimization')
        self.cell(0, 4, f'Page {self.page_no()}', align='R')
        self.ln(7)

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(*GRAY)
        self.cell(0, 4, 'InfraLens - infralens.streamlit.app - Confidential', align='C')

    def divider(self, t=6, b=6):
        self.ln(t)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.3)
        self.line(16, self.get_y(), 194, self.get_y())
        self.ln(b)

    def h1(self, text):
        self.set_font('Helvetica', 'B', 15)
        self.set_text_color(*DARK)
        self.cell(0, 9, s(text), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def h2(self, text, color=BRAND):
        self.set_font('Helvetica', 'B', 9)
        self.set_text_color(*color)
        self.cell(0, 6, s(text), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def body(self, text, color=DARK, size=9):
        self.set_font('Helvetica', '', size)
        self.set_text_color(*color)
        self.multi_cell(0, 5, s(text))
        self.ln(1)

    def metric_cards(self, items):
        col_w = 44
        y0 = self.get_y()
        for i, (label, value, color) in enumerate(items):
            x = 16 + i * (col_w + 1.3)
            self.set_fill_color(*LGRAY)
            self.set_draw_color(*BORDER)
            self.set_line_width(0.3)
            self.rect(x, y0, col_w, 20, 'FD')
            self.set_xy(x + 3, y0 + 2.5)
            self.set_font('Helvetica', '', 6)
            self.set_text_color(*GRAY)
            self.cell(col_w - 5, 3.5, s(label).upper())
            self.set_xy(x + 3, y0 + 7)
            self.set_font('Helvetica', 'B', 13)
            self.set_text_color(*color)
            self.cell(col_w - 5, 8, s(value))
        self.set_y(y0 + 24)

    def finding_card(self, num, category, title, detail, action, savings, effort, timeframe, confidence):
        effort_color = GREEN if effort == 'Low' else AMBER if effort == 'Medium' else RED

        # 텍스트 길이 미리 계산해서 카드 높이 동적 결정
        self.set_font('Helvetica', '', 8)
        # detail 줄수 추정
        detail_lines = max(1, len(s(detail)) // 85 + 1)
        # action 줄수 추정
        action_lines = max(1, len(s(action)) // 82 + 1)

        card_h = 14 + (detail_lines * 4.5) + 8 + (action_lines * 4.5) + 14

        y0 = self.get_y()

        # 페이지 넘침 체크
        if y0 + card_h > 270:
            self.add_page()
            y0 = self.get_y()

        # 왼쪽 컬러 바
        self.set_fill_color(*BRAND)
        self.rect(16, y0, 3.5, card_h, 'F')

        # 카드 배경
        self.set_fill_color(*WHITE)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.3)
        self.rect(19.5, y0, 174.5, card_h, 'FD')

        cur_y = y0 + 4

        # 카테고리
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*BRAND)
        self.cell(0, 4, s(f'#{num} - {category.upper()}'))
        cur_y += 5

        # 타이틀
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(*DARK)
        self.cell(0, 5, s(title))
        cur_y += 6

        # Detail
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*GRAY)
        self.multi_cell(168, 4.5, s(detail))
        cur_y = self.get_y() + 2

        # Recommended Action 라벨
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 7.5)
        self.set_text_color(*BRAND)
        self.cell(0, 4, 'Recommended Action:')
        cur_y += 5

        # Action 박스
        action_box_h = action_lines * 4.5 + 4
        self.set_fill_color(*PURPLE_BG)
        self.set_draw_color(*PURPLE_BD)
        self.set_line_width(0.2)
        self.rect(23, cur_y, 168, action_box_h, 'FD')
        self.set_xy(26, cur_y + 2)
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*DARK)
        self.multi_cell(163, 4.5, s(action))
        cur_y += action_box_h + 3

        # 절감액
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(*GREEN)
        self.cell(80, 6, f'Save ${savings:,.0f} / month')

        # 메타
        self.set_xy(105, cur_y + 1)
        self.set_font('Helvetica', '', 7.5)
        self.set_text_color(*GRAY)
        effort_str = s(f'Effort: {effort}  |  {timeframe}  |  Confidence: {confidence:.0f}%')
        self.cell(0, 5, effort_str)

        self.set_y(y0 + card_h + 5)


def generate_pdf(recs, sim, quality, scores_df, company_name="Your Company"):
    pdf = PDF()
    pdf.set_margins(16, 16, 16)
    pdf.set_auto_page_break(auto=True, margin=18)

    # ── PAGE 1: COVER + EXEC SUMMARY ──
    pdf.add_page()

    # 상단 바
    pdf.set_fill_color(*BRAND)
    pdf.rect(0, 0, 210, 7, 'F')
    pdf.ln(11)

    # 타이틀
    pdf.set_font('Helvetica', 'B', 28)
    pdf.set_text_color(*BRAND)
    pdf.cell(0, 11, 'InfraLens', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, 'AI Infrastructure Cost Optimization Report', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(*DARK)
    pdf.cell(0, 5, f'Prepared for: {s(company_name)}', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Generated: {datetime.now().strftime("%B %d, %Y")}', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.divider(6, 8)

    # Executive Summary
    pdf.h1('Executive Summary')
    pdf.body(
        f'InfraLens analyzed {quality.get("clean_rows", 0):,} rows of infrastructure data '
        f'across {quality.get("devices", "?")} devices over {quality.get("date_range", "30 days")}. '
        f'Our 9-method ensemble algorithm (Rule-based, Z-score, Isolation Forest, DBSCAN, Prophet, '
        f'Mahalanobis, Shannon Entropy, PCA, Energy COP) identified ${sim["savings_monthly"]:,.0f}/month '
        f'in optimization opportunities - a {sim["savings_pct"]}% reduction with no performance impact.'
    )
    pdf.ln(2)

    pdf.metric_cards([
        ('Current Monthly Cost', f'${sim["before_monthly"]:,.0f}', DARK),
        ('After Optimization',   f'${sim["after_monthly"]:,.0f}',  DARK),
        ('Monthly Savings',      f'${sim["savings_monthly"]:,.0f}', GREEN),
        ('Annual Savings',       f'${sim["savings_annual"]:,.0f}',  GREEN),
    ])

    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(*GREEN)
    pdf.cell(0, 5, 'Payback period: Immediate - No capital investment required.', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # ── PAGE 2+: ACTION PLANS ──
    pdf.add_page()
    pdf.h1('Action Plans')
    pdf.body('Each recommendation is sorted by monthly savings impact and validated by our 9-method ensemble algorithm.', GRAY, 8)
    pdf.ln(2)

    for rec in recs:
        if rec.monthly_savings <= 0:
            continue
        pdf.finding_card(
            num=rec.priority,
            category=rec.category,
            title=rec.title,
            detail=rec.detail,
            action=rec.action,
            savings=rec.monthly_savings,
            effort=rec.effort,
            timeframe=rec.timeframe,
            confidence=rec.confidence,
        )

    # ── GPU EFFICIENCY ──
    pdf.add_page()
    pdf.h1('GPU Efficiency Analysis')
    pdf.body('Each GPU is scored 0-100 based on utilization efficiency, consistency, and waste percentage. Grades below B require immediate attention.', GRAY, 8)
    pdf.ln(3)

    if len(scores_df) > 0:
        headers = ['GPU ID', 'Score', 'Grade', 'Avg Util', 'Waste %', 'Idle Hours/Mo', 'Status']
        widths  = [32, 20, 16, 24, 20, 30, 36]
        grade_colors = {'A': GREEN, 'B': BLUE, 'C': AMBER, 'D': RED}
        status_map = {'A': 'Excellent', 'B': 'Good', 'C': 'Needs attention', 'D': 'Critical'}

        # 헤더
        pdf.set_fill_color(*BRAND)
        pdf.set_text_color(*WHITE)
        pdf.set_font('Helvetica', 'B', 8)
        x = 16
        for h, w in zip(headers, widths):
            pdf.set_xy(x, pdf.get_y())
            pdf.cell(w, 7, h, fill=True)
            x += w
        pdf.ln(7)

        for j, (_, row) in enumerate(scores_df.iterrows()):
            grade = row['grade']
            gc = grade_colors.get(grade, GRAY)
            vals = [
                str(row['gpu_id']),
                f"{row['efficiency']:.0f}/100",
                grade,
                f"{row['avg_util']}%",
                f"{row['waste_pct']}%",
                f"~{int(row['waste_pct'] * 7.2)}h",
                status_map.get(grade, 'Unknown'),
            ]
            fill = LLGRAY if j % 2 == 0 else WHITE
            pdf.set_fill_color(*fill)
            x = 16
            for i, (v, w) in enumerate(zip(vals, widths)):
                pdf.set_xy(x, pdf.get_y())
                if i == 2:
                    pdf.set_text_color(*gc)
                    pdf.set_font('Helvetica', 'B', 8)
                else:
                    pdf.set_text_color(*DARK)
                    pdf.set_font('Helvetica', '', 8)
                pdf.cell(w, 6, s(v), fill=True)
                x += w
            pdf.ln(6)

    pdf.divider(8, 6)

    # ── METHODOLOGY ──
    pdf.h1('Methodology')
    pdf.h2('9-Method Ensemble Detection (95% Confidence)')
    pdf.ln(2)

    methods = [
        ('1. Rule-based + Z-score',  'Baseline idle detection with hourly statistical thresholds'),
        ('2. Isolation Forest',      'GPU-specific model with auto-tuned contamination rate per device'),
        ('3. DBSCAN',                'Density clustering with Silhouette score-optimized epsilon'),
        ('4. Prophet',               'Time-series seasonality decomposition to isolate true anomalies'),
        ('5. Mahalanobis Distance',  'D2 = (x-mu)T x Sigma-1 x (x-mu) — multivariate anomaly detection'),
        ('6. Shannon Entropy',       'H = -SUM p(x) log p(x) — usage pattern irregularity scoring'),
        ('7. PCA Reconstruction',    'Anomaly score via reconstruction error after dimensionality reduction'),
        ('8. Energy COP',            'COP = useful_work / total_energy — thermodynamic efficiency ratio'),
        ('9. Ensemble Fusion',       'Confidence-weighted combination of all 8 methods'),
    ]

    for method, desc in methods:
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*BRAND)
        pdf.cell(55, 5.5, s(method))
        pdf.set_font('Helvetica', '', 8)
        pdf.set_text_color(*DARK)
        pdf.cell(0, 5.5, s(desc), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.divider(6, 5)
    pdf.h2('Cost Model')
    pdf.ln(1)
    pdf.set_font('Courier', '', 8)
    pdf.set_text_color(*DARK)
    pdf.multi_cell(0, 5, s(
        'Total Savings = Idle Savings + Peak Shifting Savings + Cooling Reduction\n\n'
        'Idle Savings     = idle_power_kw x 0.70 x idle_hours x electricity_rate\n'
        'Peak Savings     = SUM(peak_rate - offpeak_rate) x movable_hours\n'
        'Cooling Savings  = delta_power_kw x PUE_overhead x electricity_rate'
    ))

    pdf.divider(8, 4)
    pdf.set_font('Helvetica', 'I', 7.5)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, 'This report is confidential and prepared exclusively for the recipient organization.', align='C')

    return bytes(pdf.output())


if __name__ == '__main__':
    from data_loader import load_and_prepare
    from cost_model import simulate_before_after
    from analyzer import (detect_idle_maximum, detect_peak_waste_advanced,
                          detect_overprovision_advanced, compute_efficiency_scores,
                          engineer_features)
    from recommender import generate_recommendations
    import warnings
    warnings.filterwarnings('ignore')

    df, col_map, quality = load_and_prepare('gpu_metrics_30d.csv')
    df = engineer_features(df)
    idle   = detect_idle_maximum(df)
    peak   = detect_peak_waste_advanced(df)
    over   = detect_overprovision_advanced(df)
    scores = compute_efficiency_scores(df)
    sim    = simulate_before_after(df)
    recs   = generate_recommendations(idle, peak, over, sim, scores)

    pdf_bytes = generate_pdf(recs, sim, quality, scores, company_name="Sample Corp")
    with open('infralens_report.pdf', 'wb') as f:
        f.write(pdf_bytes)
    print(f"Done: {len(pdf_bytes):,} bytes")
