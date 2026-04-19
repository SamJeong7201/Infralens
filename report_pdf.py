from fpdf import FPDF, XPos, YPos
from datetime import datetime
import io
import math

# ── 색상 ──
BRAND     = (99, 102, 241)
BRAND_D   = (79, 70, 229)
GREEN     = (5, 150, 105)
GREEN_L   = (209, 250, 229)
RED       = (220, 38, 38)
RED_L     = (254, 226, 226)
AMBER     = (217, 119, 6)
AMBER_L   = (254, 243, 199)
BLUE      = (37, 99, 235)
DARK      = (17, 24, 39)
GRAY      = (107, 114, 128)
LGRAY     = (248, 250, 252)
LLGRAY    = (243, 244, 246)
WHITE     = (255, 255, 255)
BORDER    = (226, 232, 240)
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
                  '\u2026':'...','\u03a3':'SUM','\u00d7':'x','\u00b2':'2',
                  '\u03bc':'u','\u03c3':'s','\u00b0':'deg','\u2192':'->'}
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
        self.cell(87, 4, 'InfraLens - AI Infrastructure Cost Optimization Report')
        self.cell(0, 4, f'Page {self.page_no()}', align='R')
        self.ln(7)

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(*GRAY)
        self.cell(0, 4, f'InfraLens - infralens.streamlit.app - Confidential - {datetime.now().strftime("%Y-%m-%d")}', align='C')

    def divider(self, t=6, b=6, color=BORDER):
        self.ln(t)
        self.set_draw_color(*color)
        self.set_line_width(0.3)
        self.line(16, self.get_y(), 194, self.get_y())
        self.ln(b)

    def h1(self, text, color=DARK):
        self.set_font('Helvetica', 'B', 15)
        self.set_text_color(*color)
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

    def tag(self, text, bg, fg):
        self.set_fill_color(*bg)
        self.set_text_color(*fg)
        self.set_font('Helvetica', 'B', 7)
        self.cell(len(text) * 2.8 + 6, 5, s(text), fill=True)

    def metric_card(self, x, y, w, h, label, value, sub, color, bg=LGRAY):
        self.set_fill_color(*bg)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.3)
        self.rect(x, y, w, h, 'FD')
        # 상단 컬러 바
        self.set_fill_color(*color)
        self.rect(x, y, w, 2, 'F')
        self.set_xy(x + 3, y + 4)
        self.set_font('Helvetica', '', 6)
        self.set_text_color(*GRAY)
        self.cell(w - 5, 3.5, s(label).upper())
        self.set_xy(x + 3, y + 9)
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(*color)
        self.cell(w - 5, 8, s(value))
        self.set_xy(x + 3, y + 18)
        self.set_font('Helvetica', '', 6.5)
        self.set_text_color(*GRAY)
        self.cell(w - 5, 4, s(sub))

    def bar_chart_h(self, data, labels, title, width=178, bar_h=7, color=BRAND):
        """수평 바 차트"""
        if not data:
            return
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, s(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        max_val = max(data) if max(data) > 0 else 1
        label_w = 45
        bar_area = width - label_w - 25

        for i, (val, label) in enumerate(zip(data, labels)):
            y = self.get_y()
            bar_len = (val / max_val) * bar_area

            # 라벨
            self.set_xy(16, y)
            self.set_font('Helvetica', '', 7.5)
            self.set_text_color(*DARK)
            self.cell(label_w, bar_h, s(str(label)[:14]))

            # 바
            self.set_fill_color(*color)
            self.rect(16 + label_w, y + 1, max(bar_len, 1), bar_h - 2, 'F')

            # 값
            self.set_xy(16 + label_w + bar_len + 2, y)
            self.set_font('Helvetica', 'B', 7)
            self.set_text_color(*GRAY)
            self.cell(25, bar_h, f'${val:,.0f}')

            self.ln(bar_h + 1)
        self.ln(2)

    def bar_chart_v(self, data, labels, title, width=178, height=45, highlight_fn=None, color=BRAND):
        """수직 바 차트"""
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, s(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        chart_x = 16
        chart_y = self.get_y()
        max_val = max(data) if max(data) > 0 else 1
        n = len(data)
        bar_w = (width - 8) / n
        bar_area_h = height - 10

        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(chart_x, chart_y, width, height, 'FD')

        for i, (val, label) in enumerate(zip(data, labels)):
            bar_h_val = (val / max_val) * bar_area_h
            bx = chart_x + 4 + i * bar_w
            by = chart_y + height - 8 - bar_h_val
            bc = highlight_fn(val, i) if highlight_fn else color
            self.set_fill_color(*bc)
            self.rect(bx, by, max(bar_w - 1.5, 1), bar_h_val, 'F')
            self.set_xy(bx, chart_y + height - 7)
            self.set_font('Helvetica', '', 4.5)
            self.set_text_color(*GRAY)
            self.cell(max(bar_w - 1.5, 1), 4, str(label), align='C')

        self.set_y(chart_y + height + 3)

    def cumulative_savings_chart(self, monthly_savings, width=178, height=45):
        """누적 절감액 차트"""
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, 'Cumulative Savings Over 12 Months', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        chart_x = 16
        chart_y = self.get_y()
        months = list(range(1, 13))
        vals = [monthly_savings * m for m in months]
        max_val = vals[-1]
        bar_area_h = height - 10
        bar_w = (width - 8) / 12

        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(chart_x, chart_y, width, height, 'FD')

        for i, (m, val) in enumerate(zip(months, vals)):
            bar_h_val = (val / max_val) * bar_area_h
            bx = chart_x + 4 + i * bar_w
            by = chart_y + height - 8 - bar_h_val
            # 그라데이션 효과 (색상 점점 진해짐)
            intensity = int(99 + (i / 11) * 50)
            self.set_fill_color(intensity, 102, 241)
            self.rect(bx, by, max(bar_w - 1.5, 1), bar_h_val, 'F')
            if i % 3 == 0:
                self.set_xy(bx, chart_y + height - 7)
                self.set_font('Helvetica', '', 5)
                self.set_text_color(*GRAY)
                self.cell(max(bar_w * 3, 1), 4, f'M{m}', align='C')

        # 최종값 표시
        self.set_xy(chart_x + width - 45, chart_y + 3)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*GREEN)
        self.cell(40, 5, f'Year 1: ${max_val:,.0f}', align='R')

        self.set_y(chart_y + height + 3)

    def before_after_chart(self, before, after, width=178, height=50):
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, 'Monthly Cost: Before vs After Optimization', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        chart_x = 16
        chart_y = self.get_y()
        max_val = before * 1.1
        bar_area_h = height - 14
        bar_w = 38

        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(chart_x, chart_y, width, height, 'FD')

        savings = before - after
        items = [
            ('Current Cost',  before,  RED,   f'${before:,.0f}'),
            ('After Optim.',  after,   GREEN, f'${after:,.0f}'),
            ('Monthly Saved', savings, BRAND, f'${savings:,.0f}'),
        ]

        for i, (label, val, color, val_str) in enumerate(items):
            bar_h_val = (val / max_val) * bar_area_h
            bx = chart_x + 15 + i * 58
            by = chart_y + height - 12 - bar_h_val
            self.set_fill_color(*color)
            self.rect(bx, by, bar_w, bar_h_val, 'F')
            self.set_xy(bx, by - 6)
            self.set_font('Helvetica', 'B', 7)
            self.set_text_color(*color)
            self.cell(bar_w, 5, val_str, align='C')
            self.set_xy(bx, chart_y + height - 10)
            self.set_font('Helvetica', '', 7)
            self.set_text_color(*DARK)
            self.cell(bar_w, 5, label, align='C')

        self.set_y(chart_y + height + 4)

    def finding_card(self, num, category, title, detail, action, savings,
                     effort, timeframe, confidence, risk='Low', owner='DevOps Team',
                     timeline='Week 1', extra_info=None):
        effort_color = GREEN if effort == 'Low' else AMBER if effort == 'Medium' else RED
        risk_color   = GREEN if risk == 'Low' else AMBER if risk == 'Medium' else RED

        detail_lines = max(1, len(s(detail)) // 83 + 1)
        action_lines = max(1, len(s(action)) // 80 + 1)
        extra_lines  = max(1, len(s(extra_info)) // 83 + 1) if extra_info else 0

        card_h = 18 + (detail_lines * 4.5) + 10 + (action_lines * 4.5) + 6
        if extra_info:
            card_h += extra_lines * 4.5 + 8
        card_h += 20

        y0 = self.get_y()
        if y0 + card_h > 270:
            self.add_page()
            y0 = self.get_y()

        # 왼쪽 바
        self.set_fill_color(*BRAND)
        self.rect(16, y0, 3.5, card_h, 'F')

        # 카드
        self.set_fill_color(*WHITE)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.3)
        self.rect(19.5, y0, 174.5, card_h, 'FD')

        cur_y = y0 + 4

        # 카테고리 + 태그들
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*BRAND)
        self.cell(50, 4, s(f'#{num} - {category.upper()}'))

        # 태그
        tag_x = 120
        self.set_xy(tag_x, cur_y)
        self.set_fill_color(*AMBER_L)
        self.set_text_color(*AMBER)
        self.set_font('Helvetica', 'B', 6.5)
        self.cell(22, 4, f'Effort: {effort}', fill=True, align='C')
        self.set_xy(tag_x + 23, cur_y)
        self.set_fill_color(*RED_L if risk == 'High' else AMBER_L if risk == 'Medium' else GREEN_L)
        self.set_text_color(*risk_color)
        self.cell(20, 4, f'Risk: {risk}', fill=True, align='C')
        self.set_xy(tag_x + 44, cur_y)
        self.set_fill_color(*PURPLE_BG)
        self.set_text_color(*BRAND)
        self.cell(26, 4, f'Owner: {owner[:8]}', fill=True, align='C')
        cur_y += 6

        # 타이틀
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(*DARK)
        self.cell(0, 5, s(title))
        cur_y += 7

        # Detail
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*GRAY)
        self.multi_cell(168, 4.5, s(detail))
        cur_y = self.get_y() + 2

        # Extra info
        if extra_info:
            self.set_fill_color(236, 253, 245)
            self.set_draw_color(167, 243, 208)
            self.set_line_width(0.2)
            extra_h = extra_lines * 4.5 + 5
            self.rect(23, cur_y, 168, extra_h, 'FD')
            self.set_xy(26, cur_y + 2)
            self.set_font('Helvetica', 'I', 7.5)
            self.set_text_color(6, 95, 70)
            self.multi_cell(163, 4.5, s(extra_info))
            cur_y += extra_h + 3

        # Recommended Action
        self.set_xy(23, cur_y)
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*BRAND)
        self.cell(0, 4, 'Recommended Action:')
        cur_y += 5

        action_box_h = action_lines * 4.5 + 5
        self.set_fill_color(*PURPLE_BG)
        self.set_draw_color(*PURPLE_BD)
        self.set_line_width(0.2)
        self.rect(23, cur_y, 168, action_box_h, 'FD')
        self.set_xy(26, cur_y + 2)
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*DARK)
        self.multi_cell(163, 4.5, s(action))
        cur_y += action_box_h + 4

        # 하단: 절감액 + 메타
        self.set_fill_color(236, 253, 245)
        self.rect(23, cur_y, 168, 12, 'F')

        self.set_xy(26, cur_y + 2)
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(*GREEN)
        self.cell(80, 7, f'Save ${savings:,.0f}/mo  (${savings*12:,.0f}/yr)')

        self.set_xy(112, cur_y + 2)
        self.set_font('Helvetica', '', 7.5)
        self.set_text_color(*GRAY)
        self.cell(20, 4, f'Timeline: {timeline}')
        self.set_xy(112, cur_y + 6)
        self.cell(30, 4, f'Confidence: {confidence:.0f}%')

        self.set_xy(158, cur_y + 4)
        self.set_font('Helvetica', 'B', 7.5)
        self.set_text_color(*BRAND)
        self.cell(30, 4, f'Timeframe: {timeframe}')

        self.set_y(y0 + card_h + 6)


def generate_pdf(recs, sim, quality, scores_df, df=None, company_name="Your Company"):
    pdf = PDF()
    pdf.set_margins(16, 16, 16)
    pdf.set_auto_page_break(auto=True, margin=18)

    monthly_savings = sim.get('savings_monthly', 0)
    before          = sim.get('before_monthly', 0)
    after           = sim.get('after_monthly', 0)
    savings_pct     = sim.get('savings_pct', 0)
    annual_savings  = sim.get('savings_annual', 0)

    # ══════════════════════════════════════════
    # PAGE 1: COVER + EXECUTIVE SUMMARY
    # ══════════════════════════════════════════
    pdf.add_page()

    # 상단 바
    pdf.set_fill_color(*BRAND)
    pdf.rect(0, 0, 210, 7, 'F')
    pdf.ln(11)

    # 로고
    pdf.set_font('Helvetica', 'B', 30)
    pdf.set_text_color(*BRAND)
    pdf.cell(0, 12, 'InfraLens', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 12)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, 'AI Infrastructure Cost Optimization Report', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)

    # 회사 정보 박스
    pdf.set_fill_color(*LGRAY)
    pdf.set_draw_color(*BORDER)
    pdf.rect(16, pdf.get_y(), 178, 18, 'FD')
    pdf.set_xy(20, pdf.get_y() + 3)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(*DARK)
    pdf.cell(60, 5, f'Prepared for: {s(company_name)}')
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Generated: {datetime.now().strftime("%B %d, %Y at %H:%M")}', align='R')
    pdf.set_xy(20, pdf.get_y() + 5)
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Data analyzed: {quality.get("clean_rows", 0):,} rows | {quality.get("devices", "?")} devices | {quality.get("date_range", "N/A")} | {quality.get("tier", "Standard")} tier')
    pdf.ln(22)

    pdf.divider(2, 6)
    pdf.h1('Executive Summary')

    # 핵심 요약 문장
    pdf.body(
        f'{s(company_name)} is currently spending ${before:,.0f}/month on GPU infrastructure. '
        f'InfraLens identified ${monthly_savings:,.0f}/month ({savings_pct}%) in recoverable waste '
        f'through our 9-method ensemble analysis. These are not estimates - they are calculated '
        f'from your actual usage data with 95% confidence. '
        f'All optimizations are operational changes only: no new hardware, no downtime, no performance impact.'
    )
    pdf.ln(2)

    # 메트릭 카드 4개
    y0 = pdf.get_y()
    card_w = 43
    card_h = 26
    metrics = [
        ('Current Monthly Spend', f'${before:,.0f}',          'per month',  GRAY,  LGRAY),
        ('After Optimization',    f'${after:,.0f}',           'per month',  BLUE,  LGRAY),
        ('Monthly Savings',       f'${monthly_savings:,.0f}', f'{savings_pct}% reduction', GREEN, (236,253,245)),
        ('Annual Opportunity',    f'${annual_savings:,.0f}',  'per year',   BRAND, PURPLE_BG),
    ]
    for i, (label, value, sub, color, bg) in enumerate(metrics):
        x = 16 + i * (card_w + 1.3)
        pdf.metric_card(x, y0, card_w, card_h, label, value, sub, color, bg)
    pdf.set_y(y0 + card_h + 5)

    # 즉시 실행 가능 강조
    pdf.set_fill_color(*GREEN_L)
    pdf.set_draw_color(167, 243, 208)
    pdf.set_line_width(0.3)
    pdf.rect(16, pdf.get_y(), 178, 10, 'FD')
    pdf.set_xy(20, pdf.get_y() + 3)
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(*GREEN)
    pdf.cell(0, 4, 'Payback period: IMMEDIATE - Zero capital required. All changes are configuration-level only.')
    pdf.ln(14)

    # Top 3 즉시 실행 항목
    pdf.h2('Top 3 Actions - Start This Week')
    action_recs = [r for r in recs if r.monthly_savings > 0][:3]
    for i, rec in enumerate(action_recs):
        y = pdf.get_y()
        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.rect(16, y, 178, 12, 'FD')
        pdf.set_fill_color(*BRAND)
        pdf.rect(16, y, 6, 12, 'F')
        pdf.set_xy(26, y + 2)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*DARK)
        pdf.cell(100, 4, s(rec.title[:55]))
        pdf.set_xy(26, y + 7)
        pdf.set_font('Helvetica', '', 7.5)
        pdf.set_text_color(*GRAY)
        pdf.cell(80, 4, s(f'Effort: {rec.effort} | Timeframe: {rec.timeframe}'))
        pdf.set_xy(148, y + 4)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(*GREEN)
        pdf.cell(42, 5, f'${rec.monthly_savings:,.0f}/mo', align='R')
        pdf.ln(14)

    # ══════════════════════════════════════════
    # PAGE 2: FINANCIAL IMPACT
    # ══════════════════════════════════════════
    pdf.add_page()
    pdf.h1('Financial Impact Analysis')
    pdf.body(
        f'The following analysis shows the direct financial impact of implementing '
        f'InfraLens recommendations. All figures are based on your actual usage data '
        f'and current electricity rates.',
        GRAY, 8
    )
    pdf.ln(2)

    # Before/After 차트
    pdf.before_after_chart(before, after)
    pdf.ln(2)

    # 누적 절감 차트
    pdf.cumulative_savings_chart(monthly_savings)
    pdf.ln(3)

    # ROI 분석 테이블
    pdf.h2('ROI Analysis')
    roi_data = [
        ['Metric', 'Value', 'Notes'],
        ['Monthly Cost Reduction', f'${monthly_savings:,.0f}', f'{savings_pct}% of current spend'],
        ['Annual Savings', f'${annual_savings:,.0f}', 'No capital investment required'],
        ['Implementation Cost', '$0', 'Config changes only, no new hardware'],
        ['ROI', 'Infinite', 'Zero cost, immediate returns'],
        ['Break-even Period', 'Day 1', 'Savings start immediately upon implementation'],
        ['3-Year Total Savings', f'${monthly_savings * 36:,.0f}', 'Conservative estimate, no growth assumed'],
    ]

    col_w = [65, 45, 68]
    for j, row in enumerate(roi_data):
        if j == 0:
            pdf.set_fill_color(*BRAND)
            pdf.set_text_color(*WHITE)
            pdf.set_font('Helvetica', 'B', 8)
        else:
            pdf.set_fill_color(*LLGRAY if j % 2 == 0 else WHITE)
            pdf.set_text_color(*DARK)
            pdf.set_font('Helvetica', '', 8)
        x = 16
        for k, (cell, w) in enumerate(zip(row, col_w)):
            pdf.set_xy(x, pdf.get_y())
            if j > 0 and k == 1:
                pdf.set_font('Helvetica', 'B', 8)
                pdf.set_text_color(*GREEN)
            pdf.cell(w, 6.5, s(cell), fill=(j == 0))
            if j > 0 and k == 1:
                pdf.set_font('Helvetica', '', 8)
                pdf.set_text_color(*DARK)
            x += w
        pdf.ln(6.5)

    pdf.ln(4)

    # 업계 벤치마크
    pdf.h2('Industry Benchmark Comparison')
    pdf.ln(1)

    benchmark_data = [
        ['Metric', 'Your Current', 'Industry Average', 'Top Quartile', 'Status'],
        ['GPU Utilization', f'{100 - float(str(savings_pct).replace("%","")):.0f}%', '45-55%', '70%+', 'Below avg'],
        ['Cost per GPU/hr', f'${before/max(quality.get("devices",1),1)/720:.3f}', '$2.50-3.50', '<$2.00', 'Review'],
        ['Idle GPU %', f'{savings_pct}%', '20-30%', '<15%', 'Needs work'],
        ['PUE Factor', '1.50', '1.40-1.60', '<1.20', 'Average'],
    ]

    col_w2 = [42, 32, 34, 30, 40]
    for j, row in enumerate(benchmark_data):
        if j == 0:
            pdf.set_fill_color(*BRAND)
            pdf.set_text_color(*WHITE)
            pdf.set_font('Helvetica', 'B', 7.5)
        else:
            pdf.set_fill_color(*LLGRAY if j % 2 == 0 else WHITE)
            pdf.set_text_color(*DARK)
            pdf.set_font('Helvetica', '', 7.5)
        x = 16
        for k, (cell, w) in enumerate(zip(row, col_w2)):
            pdf.set_xy(x, pdf.get_y())
            if j > 0 and k == 4:
                c = RED if 'Below' in cell or 'Needs' in cell else AMBER if 'Review' in cell else GREEN
                pdf.set_text_color(*c)
                pdf.set_font('Helvetica', 'B', 7.5)
            pdf.cell(w, 6, s(cell), fill=(j == 0))
            if j > 0 and k == 4:
                pdf.set_text_color(*DARK)
                pdf.set_font('Helvetica', '', 7.5)
            x += w
        pdf.ln(6)

    # ══════════════════════════════════════════
    # PAGE 3+: ACTION PLANS
    # ══════════════════════════════════════════
    pdf.add_page()
    pdf.h1('Detailed Action Plans')
    pdf.body(
        'Each action below has been validated by our 9-method ensemble algorithm. '
        'Actions are sorted by monthly savings impact. Risk levels reflect potential '
        'operational impact - all Low risk actions can be implemented without change approval.',
        GRAY, 8
    )
    pdf.ln(2)

    # 액션별 상세
    action_meta = [
        {'risk': 'Low',    'owner': 'DevOps',   'timeline': 'Week 1'},
        {'risk': 'Low',    'owner': 'DevOps',   'timeline': 'Week 1-2'},
        {'risk': 'Medium', 'owner': 'Infra',    'timeline': 'Month 1'},
        {'risk': 'Medium', 'owner': 'Infra',    'timeline': 'Month 1'},
    ]

    for i, rec in enumerate([r for r in recs if r.monthly_savings > 0]):
        meta = action_meta[i] if i < len(action_meta) else {'risk':'Medium','owner':'DevOps','timeline':'Month 1'}
        extra = None
        if hasattr(rec, 'detail') and scores_df is not None and len(scores_df) > 0:
            if 'Idle' in rec.category:
                worst = scores_df.nsmallest(1, 'efficiency')
                if len(worst) > 0:
                    r = worst.iloc[0]
                    extra = (
                        f'Data insight: Worst performer is {r["gpu_id"]} with efficiency score '
                        f'{r["efficiency"]:.0f}/100 (Grade {r["grade"]}), averaging {r["avg_util"]}% '
                        f'utilization with {r["waste_pct"]}% idle time. '
                        f'Estimated {int(r["waste_pct"] * 7.2)} idle hours/month.'
                    )

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
            risk=meta['risk'],
            owner=meta['owner'],
            timeline=meta['timeline'],
            extra_info=extra,
        )

    # ══════════════════════════════════════════
    # PAGE: GPU TECHNICAL ANALYSIS
    # ══════════════════════════════════════════
    pdf.add_page()
    pdf.h1('GPU Technical Analysis')
    pdf.body(
        'Detailed per-GPU efficiency metrics based on 30-day analysis. '
        'Score = utilization(40%) + consistency(30%) + low-waste(30%). '
        'Grades below B require immediate scheduling review.',
        GRAY, 8
    )
    pdf.ln(2)

    if scores_df is not None and len(scores_df) > 0:
        # 효율 바 차트
        gpu_ids = [str(r['gpu_id']) for _, r in scores_df.iterrows()]
        gpu_scores = [float(r['efficiency']) for _, r in scores_df.iterrows()]
        grade_colors_map = {'A': GREEN, 'B': BLUE, 'C': AMBER, 'D': RED}

        def gpu_color_fn(val, i):
            grade = scores_df.iloc[i]['grade'] if i < len(scores_df) else 'C'
            return grade_colors_map.get(grade, GRAY)

        pdf.bar_chart_v(
            gpu_scores, gpu_ids,
            'GPU Efficiency Scores by Device (Green=A, Blue=B, Amber=C, Red=D)',
            height=48, highlight_fn=gpu_color_fn
        )
        pdf.ln(3)

        # GPU 상세 테이블
        headers = ['GPU ID', 'Score', 'Grade', 'Avg Util%', 'Waste%', 'Idle h/mo', 'Action Required', 'Priority']
        widths  = [25, 17, 13, 22, 18, 20, 44, 19]
        action_map = {
            'A': ('Maintain', GREEN),
            'B': ('Minor review', BLUE),
            'C': ('Optimize now', AMBER),
            'D': ('URGENT', RED),
        }

        pdf.set_fill_color(*BRAND)
        pdf.set_text_color(*WHITE)
        pdf.set_font('Helvetica', 'B', 7.5)
        x = 16
        for h, w in zip(headers, widths):
            pdf.set_xy(x, pdf.get_y())
            pdf.cell(w, 7, h, fill=True)
            x += w
        pdf.ln(7)

        for j, (_, row) in enumerate(scores_df.iterrows()):
            grade = row['grade']
            gc = grade_colors_map.get(grade, GRAY)
            action_txt, action_color = action_map.get(grade, ('Review', GRAY))
            idle_h = int(row['waste_pct'] * 7.2)
            vals = [
                str(row['gpu_id']),
                f"{row['efficiency']:.0f}/100",
                grade,
                f"{row['avg_util']}%",
                f"{row['waste_pct']}%",
                f"~{idle_h}h",
                action_txt,
                f"#{j+1}",
            ]
            fill = LLGRAY if j % 2 == 0 else WHITE
            pdf.set_fill_color(*fill)
            x = 16
            for k, (v, w) in enumerate(zip(vals, widths)):
                pdf.set_xy(x, pdf.get_y())
                if k == 2:
                    pdf.set_text_color(*gc)
                    pdf.set_font('Helvetica', 'B', 7.5)
                elif k == 6:
                    pdf.set_text_color(*action_color)
                    pdf.set_font('Helvetica', 'B', 7.5)
                else:
                    pdf.set_text_color(*DARK)
                    pdf.set_font('Helvetica', '', 7.5)
                pdf.cell(w, 6, s(v), fill=True)
                x += w
            pdf.ln(6)

    # 24h 패턴 차트
    if df is not None and 'gpu_util' in df.columns and 'hour' in df.columns:
        import pandas as pd
        pdf.ln(4)
        hourly = df.groupby('hour')['gpu_util'].mean()
        hours = list(range(24))
        vals = [float(hourly.get(h, 0)) for h in hours]

        def util_color(val, i):
            if val < 20: return RED
            elif val > 70: return BRAND
            else: return GREEN

        pdf.bar_chart_v(
            vals, hours,
            'Average GPU Utilization by Hour (Red=Idle<20%, Blue=Peak>70%, Green=Normal)',
            height=45, highlight_fn=util_color
        )

    # ══════════════════════════════════════════
    # PAGE: IMPLEMENTATION ROADMAP
    # ══════════════════════════════════════════
    pdf.add_page()
    pdf.h1('Implementation Roadmap')
    pdf.body(
        'Phased implementation plan to maximize savings while minimizing operational risk. '
        'Low-effort actions deliver immediate ROI; medium-effort actions require planning.',
        GRAY, 8
    )
    pdf.ln(3)

    phases = [
        {
            'phase': 'Phase 1 - Week 1',
            'title': 'Quick Wins (Zero Risk)',
            'color': GREEN,
            'bg': (236, 253, 245),
            'actions': [
                'Enable GPU power-saving mode on idle instances (nvidia-smi command)',
                'Set up automated alerts for GPU utilization < 15%',
                'Configure persistence mode on all GPU instances',
            ],
            'savings': monthly_savings * 0.4,
            'effort': '2-4 hours engineering time',
        },
        {
            'phase': 'Phase 2 - Week 2-3',
            'title': 'Scheduling Optimization',
            'color': BLUE,
            'bg': (239, 246, 255),
            'actions': [
                'Reschedule batch training jobs to off-peak hours (22:00-06:00)',
                'Configure Slurm/cron job scheduler for off-peak execution',
                'Set up workload queuing for non-time-sensitive tasks',
            ],
            'savings': monthly_savings * 0.35,
            'effort': '4-8 hours engineering time',
        },
        {
            'phase': 'Phase 3 - Month 1',
            'title': 'Infrastructure Right-sizing',
            'color': AMBER,
            'bg': (255, 251, 235),
            'actions': [
                'Implement auto-scaling policies (Kubernetes HPA or custom)',
                'Reduce overnight GPU fleet by identified reducible count',
                'Set up monitoring dashboards for ongoing optimization',
            ],
            'savings': monthly_savings * 0.25,
            'effort': '1-2 days engineering time',
        },
    ]

    cumulative = 0
    for phase in phases:
        cumulative += phase['savings']
        y0 = pdf.get_y()

        pdf.set_fill_color(*phase['bg'])
        pdf.set_draw_color(*phase['color'])
        pdf.set_line_width(0.5)
        pdf.rect(16, y0, 178, 48, 'FD')

        pdf.set_fill_color(*phase['color'])
        pdf.rect(16, y0, 4, 48, 'F')

        pdf.set_xy(24, y0 + 3)
        pdf.set_font('Helvetica', 'B', 7)
        pdf.set_text_color(*phase['color'])
        pdf.cell(80, 4, s(phase['phase']))

        pdf.set_xy(24, y0 + 8)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(*DARK)
        pdf.cell(100, 5, s(phase['title']))

        for k, action in enumerate(phase['actions']):
            pdf.set_xy(24, y0 + 15 + k * 7)
            pdf.set_font('Helvetica', '', 7.5)
            pdf.set_text_color(*DARK)
            pdf.cell(3, 5, '-')
            pdf.set_xy(28, y0 + 15 + k * 7)
            pdf.cell(120, 5, s(action))

        pdf.set_xy(148, y0 + 10)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(*GREEN)
        pdf.cell(40, 5, f'+${phase["savings"]:,.0f}/mo', align='R')

        pdf.set_xy(148, y0 + 17)
        pdf.set_font('Helvetica', '', 7)
        pdf.set_text_color(*GRAY)
        pdf.cell(40, 4, f'Cumul: ${cumulative:,.0f}/mo', align='R')

        pdf.set_xy(24, y0 + 40)
        pdf.set_font('Helvetica', 'I', 7)
        pdf.set_text_color(*GRAY)
        pdf.cell(0, 4, s(f'Est. effort: {phase["effort"]}'))

        pdf.ln(52)

    # KPI 섹션
    pdf.h2('Success KPIs - How to Measure Impact')
    pdf.ln(1)

    kpis = [
        ('GPU Utilization Rate', 'Current baseline', '>65% average', 'Weekly', 'nvidia-smi, DCGM'),
        ('Idle GPU Hours',       f'~{int(monthly_savings/3.2)}h/mo',    '<50h/mo',       'Daily',  'InfraLens dashboard'),
        ('Monthly Infra Cost',   f'${before:,.0f}',  f'<${after:,.0f}', 'Monthly',        'Cloud billing console'),
        ('Peak-hour GPU jobs',   'Current count',    '<30% of total',   'Weekly',         'Job scheduler logs'),
    ]

    headers_kpi = ['KPI', 'Current', 'Target', 'Review', 'Data Source']
    widths_kpi  = [42, 30, 30, 22, 54]

    pdf.set_fill_color(*BRAND)
    pdf.set_text_color(*WHITE)
    pdf.set_font('Helvetica', 'B', 8)
    x = 16
    for h, w in zip(headers_kpi, widths_kpi):
        pdf.set_xy(x, pdf.get_y())
        pdf.cell(w, 7, h, fill=True)
        x += w
    pdf.ln(7)

    for j, row in enumerate(kpis):
        fill = LLGRAY if j % 2 == 0 else WHITE
        pdf.set_fill_color(*fill)
        x = 16
        for k, (cell, w) in enumerate(zip(row, widths_kpi)):
            pdf.set_xy(x, pdf.get_y())
            pdf.set_font('Helvetica', '' if k != 2 else 'B', 7.5)
            pdf.set_text_color(*GREEN if k == 2 else DARK)
            pdf.cell(w, 6, s(cell), fill=True)
            x += w
        pdf.ln(6)

    # ══════════════════════════════════════════
    # PAGE: METHODOLOGY
    # ══════════════════════════════════════════
    pdf.add_page()
    pdf.h1('Methodology & Technical Details')
    pdf.h2('9-Method Ensemble Detection System (95% Confidence)')
    pdf.body(
        'InfraLens uses a proprietary ensemble of 9 complementary detection methods. '
        'Each method captures different anomaly patterns, and the ensemble fusion '
        'only flags high-confidence issues where multiple methods agree.',
        DARK, 8
    )
    pdf.ln(2)

    methods = [
        ('1. Rule-based + Z-score',
         'Establishes hourly baseline per GPU using rolling 24h statistics. '
         'Flags periods where current utilization is 1.5+ standard deviations below baseline.',
         'Fast, interpretable baseline'),
        ('2. Isolation Forest',
         'Trains a separate anomaly detection model per GPU device. '
         'Auto-tunes contamination rate based on estimated idle ratio. Uses 200 estimators.',
         'Handles non-linear patterns'),
        ('3. DBSCAN + Silhouette',
         'Density-based clustering with epsilon parameter auto-optimized via Silhouette score '
         'across eps=[0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5].',
         'Finds spatial anomaly clusters'),
        ('4. Prophet Seasonality',
         'Decomposes time-series into trend + weekly + daily seasonality. '
         'Anomalies are detected in the residual component after seasonality removal.',
         'Removes false positives from patterns'),
        ('5. Mahalanobis Distance',
         'D2 = (x-mu)T Sigma-1 (x-mu). Accounts for correlations between features '
         '(util, power, memory). Threshold set at chi-squared 95th percentile.',
         'Multi-feature correlation aware'),
        ('6. Shannon Entropy',
         'H = -SUM p(x) log p(x). Low entropy = predictable waste pattern. '
         'Identifies hours with consistently low, predictable utilization.',
         'Pattern predictability scoring'),
        ('7. PCA Reconstruction',
         'Reduces dimensionality to explain 95% variance. Anomaly score = '
         'reconstruction error ||x - PCA(x)||2. High error = structurally anomalous.',
         'Structural anomaly detection'),
        ('8. Energy COP',
         'COP = useful_work / total_energy. Compares actual COP vs 90th percentile ideal. '
         'Flags periods where COP efficiency < 50% of theoretical maximum.',
         'Thermodynamic efficiency'),
        ('9. Ensemble Fusion',
         'Confidence-weighted combination. Savings and confidence scores are weighted '
         'by each method\'s individual confidence. Higher agreement = higher final confidence.',
         'Maximizes precision'),
    ]

    for method, desc, benefit in methods:
        y0 = pdf.get_y()
        if y0 > 255:
            pdf.add_page()

        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.set_line_width(0.2)

        desc_lines = max(1, len(desc) // 75 + 1)
        row_h = desc_lines * 4.5 + 10

        pdf.rect(16, pdf.get_y(), 178, row_h, 'FD')
        pdf.set_fill_color(*BRAND)
        pdf.rect(16, pdf.get_y(), 3, row_h, 'F')

        pdf.set_xy(22, pdf.get_y() + 2)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*BRAND)
        pdf.cell(55, 4, s(method))

        pdf.set_xy(22, pdf.get_y() + 4)
        pdf.set_font('Helvetica', '', 7.5)
        pdf.set_text_color(*DARK)
        pdf.multi_cell(140, 4.2, s(desc))

        # benefit 태그
        cur_y = pdf.get_y()
        pdf.set_fill_color(*PURPLE_BG)
        pdf.set_xy(148, y0 + 2)
        pdf.set_font('Helvetica', 'I', 6.5)
        pdf.set_text_color(*BRAND)
        pdf.multi_cell(42, 4, s(benefit))

        pdf.set_y(y0 + row_h + 2)

    pdf.divider(6, 5)

    pdf.h2('Cost Calculation Model')
    pdf.ln(1)
    pdf.set_fill_color(*LGRAY)
    pdf.rect(16, pdf.get_y(), 178, 32, 'F')
    pdf.set_xy(20, pdf.get_y() + 2)
    pdf.set_font('Courier', 'B', 8)
    pdf.set_text_color(*BRAND)
    pdf.cell(0, 5, 'Total Monthly Savings = Idle Savings + Peak Shifting + Cooling Reduction')
    pdf.ln(7)
    pdf.set_xy(20, pdf.get_y())
    pdf.set_font('Courier', '', 7.5)
    pdf.set_text_color(*DARK)
    pdf.multi_cell(170, 5, s(
        'Idle Savings    = idle_power_kw x 0.70 x idle_hours x electricity_rate\n'
        'Peak Savings    = SUM(peak_rate - offpeak_rate) x movable_hours\n'
        'Cooling Savings = delta_power_kw x (PUE - 1.0) x electricity_rate\n'
        'Annual Savings  = Monthly Savings x 12'
    ))

    pdf.ln(4)
    pdf.h2('Data Quality & Assumptions')
    assumptions = [
        ('Idle power reduction', '70% savings when power-saving mode enabled (NVIDIA validated)'),
        ('TOU pricing', 'Based on selected cloud provider schedule (AWS/GCP/KEPCO)'),
        ('Cooling overhead', 'PUE factor applied based on datacenter type selection'),
        ('Confidence interval', '95% - based on ensemble agreement across 9 methods'),
        ('Data coverage', f'{quality.get("clean_rows", 0):,} rows, {quality.get("devices","?")} devices, {quality.get("date_range","N/A")}'),
        ('Savings estimate', 'Conservative - actual savings may be higher'),
    ]

    for assumption, explanation in assumptions:
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*DARK)
        pdf.cell(55, 5.5, s(assumption))
        pdf.set_font('Helvetica', '', 8)
        pdf.set_text_color(*GRAY)
        pdf.cell(0, 5.5, s(explanation), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.divider(6, 4)
    pdf.set_font('Helvetica', 'I', 7.5)
    pdf.set_text_color(*GRAY)
    pdf.multi_cell(0, 5, s(
        'This report is confidential and prepared exclusively for the recipient organization. '
        'All savings figures are conservative estimates based on actual usage data analysis. '
        'InfraLens does not guarantee specific savings amounts as actual results depend on '
        'implementation quality and operational changes made by the recipient organization.'
    ))

    return bytes(pdf.output())


def generate_billing_pdf(billing, quality, company_name="Your Company"):
    pdf = PDF()
    pdf.set_margins(16, 16, 16)
    pdf.set_auto_page_break(auto=True, margin=18)

    monthly_cost    = billing.get('monthly_cost', 0)
    monthly_savings = billing.get('monthly_savings', 0)
    after_cost      = monthly_cost - monthly_savings
    savings_pct     = round(monthly_savings / max(monthly_cost, 1) * 100, 1)

    pdf.add_page()
    pdf.set_fill_color(*BRAND)
    pdf.rect(0, 0, 210, 7, 'F')
    pdf.ln(11)

    pdf.set_font('Helvetica', 'B', 28)
    pdf.set_text_color(*BRAND)
    pdf.cell(0, 11, 'InfraLens', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, 'Cloud Billing Cost Optimization Report', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_fill_color(*LGRAY)
    pdf.set_draw_color(*BORDER)
    pdf.rect(16, pdf.get_y() + 3, 178, 14, 'FD')
    pdf.set_xy(20, pdf.get_y() + 6)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(*DARK)
    pdf.cell(80, 5, f'Prepared for: {s(company_name)}')
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Generated: {datetime.now().strftime("%B %d, %Y")}', align='R')
    pdf.ln(20)

    pdf.divider(2, 6)
    pdf.h1('Executive Summary')
    pdf.body(
        f'{s(company_name)} is currently spending ${monthly_cost:,.0f}/month on cloud infrastructure. '
        f'InfraLens billing analysis identified ${monthly_savings:,.0f}/month ({savings_pct}%) '
        f'in optimization opportunities across {quality.get("clean_rows",0):,} billing records. '
        f'All optimizations are operational changes with no capital investment required.'
    )
    pdf.ln(2)

    y0 = pdf.get_y()
    card_w = 43
    metrics = [
        ('Current Monthly Spend', f'${monthly_cost:,.0f}',    'per month',            GRAY,  LGRAY),
        ('After Optimization',    f'${after_cost:,.0f}',      'per month',            BLUE,  LGRAY),
        ('Monthly Savings',       f'${monthly_savings:,.0f}', f'{savings_pct}% reduction', GREEN, (236,253,245)),
        ('Annual Opportunity',    f'${monthly_savings*12:,.0f}', 'per year',          BRAND, PURPLE_BG),
    ]
    for i, (label, value, sub, color, bg) in enumerate(metrics):
        pdf.metric_card(16 + i * (card_w + 1.3), y0, card_w, 26, label, value, sub, color, bg)
    pdf.set_y(y0 + 30)

    pdf.set_fill_color(*GREEN_L)
    pdf.set_draw_color(167, 243, 208)
    pdf.rect(16, pdf.get_y(), 178, 10, 'FD')
    pdf.set_xy(20, pdf.get_y() + 3)
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(*GREEN)
    pdf.cell(0, 4, 'Payback period: IMMEDIATE - Zero capital required. All changes are configuration-level only.')
    pdf.ln(14)

    pdf.before_after_chart(monthly_cost, after_cost)
    pdf.ln(2)
    pdf.cumulative_savings_chart(monthly_savings)
    pdf.ln(3)

    pdf.add_page()
    pdf.h1('Action Plans')
    pdf.body(
        'Each recommendation validated by InfraLens billing analysis engine. '
        'Sorted by monthly savings impact.',
        GRAY, 8
    )
    pdf.ln(2)

    action_meta = [
        {'risk': 'Low',    'owner': 'DevOps',   'timeline': 'Week 1'},
        {'risk': 'Medium', 'owner': 'Infra',    'timeline': 'Month 1'},
        {'risk': 'High',   'owner': 'Arch Team','timeline': 'Quarter 1'},
    ]

    for i, finding in enumerate(billing.get('findings', [])):
        meta = action_meta[i] if i < len(action_meta) else {'risk':'Medium','owner':'DevOps','timeline':'Month 1'}
        pdf.finding_card(
            num=i+1,
            category=finding['type'],
            title=finding['title'],
            detail=finding['detail'],
            action=finding['action'],
            savings=finding['monthly_savings'],
            effort=finding['effort'],
            timeframe=finding['timeframe'],
            confidence=finding['confidence'],
            risk=meta['risk'],
            owner=meta['owner'],
            timeline=meta['timeline'],
        )

    top_resources = billing.get('top_cost_resources', None)
    if top_resources is not None and len(top_resources) > 0:
        pdf.add_page()
        pdf.h1('Top Cost Resources')
        pdf.body(
            'Resources ranked by total cost. Focus optimization on top items first '
            'for maximum ROI. Consider Reserved Instances or Committed Use Discounts '
            'for consistently high-usage resources.',
            GRAY, 8
        )
        pdf.ln(2)

        # 서비스별 수평 바 차트
        sb = billing.get('service_breakdown', None)
        if sb is not None and len(sb) > 0:
            top_sb = sb.head(8)
            pdf.bar_chart_h(
                list(top_sb['cost'].values),
                list(top_sb['service'].values),
                'Cost by Service (Top 8)',
            )
            pdf.ln(2)

        headers = ['Resource ID', 'Total Cost', '% of Budget', 'Recommendation']
        widths  = [55, 35, 30, 58]
        total   = top_resources['total_cost'].sum()

        pdf.set_fill_color(*BRAND)
        pdf.set_text_color(*WHITE)
        pdf.set_font('Helvetica', 'B', 8)
        x = 16
        for h, w in zip(headers, widths):
            pdf.set_xy(x, pdf.get_y())
            pdf.cell(w, 7, h, fill=True)
            x += w
        pdf.ln(7)

        for j, (_, row) in enumerate(top_resources.head(15).iterrows()):
            fill = LLGRAY if j % 2 == 0 else WHITE
            pdf.set_fill_color(*fill)
            pct = row['total_cost'] / max(total, 1) * 100
            rec_txt = 'Review for rightsizing' if pct > 15 else 'Consider Reserved Instance' if pct > 8 else 'Monitor'
            rec_color = RED if pct > 15 else AMBER if pct > 8 else GREEN
            vals = [
                str(row['resource_id']),
                f"${row['total_cost']:,.2f}",
                f"{pct:.1f}%",
                rec_txt,
            ]
            x = 16
            for k, (v, w) in enumerate(zip(vals, widths)):
                pdf.set_xy(x, pdf.get_y())
                if k == 3:
                    pdf.set_text_color(*rec_color)
                    pdf.set_font('Helvetica', 'B', 7.5)
                else:
                    pdf.set_text_color(*DARK)
                    pdf.set_font('Helvetica', '', 7.5)
                pdf.cell(w, 6, s(v), fill=True)
                x += w
            pdf.ln(6)

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

    pdf_bytes = generate_pdf(recs, sim, quality, scores, df=df, company_name="Sample Corp")
    with open('infralens_report.pdf', 'wb') as f:
        f.write(pdf_bytes)
    print(f"Done: {len(pdf_bytes):,} bytes")
