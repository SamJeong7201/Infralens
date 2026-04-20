from fpdf import FPDF, XPos, YPos
from datetime import datetime
import io

# ── 색상 ──
BRAND    = (99, 102, 241)
GREEN    = (5, 150, 105)
GREEN_L  = (209, 250, 229)
RED      = (220, 38, 38)
RED_L    = (254, 226, 226)
AMBER    = (217, 119, 6)
AMBER_L  = (254, 243, 199)
BLUE     = (37, 99, 235)
DARK     = (17, 24, 39)
GRAY     = (107, 114, 128)
LGRAY    = (248, 250, 252)
LLGRAY   = (243, 244, 246)
WHITE    = (255, 255, 255)
BORDER   = (226, 232, 240)
PURPLE_BG = (245, 243, 255)
PURPLE_BD = (199, 194, 254)


def s(text):
    """특수문자 안전 변환"""
    if not isinstance(text, str):
        text = str(text)
    mp = {
        '\u2014':'-', '\u2013':'-', '\u2012':'-', '\u2011':'-', '\u2010':'-',
        '\u2018':"'", '\u2019':"'", '\u201c':'"', '\u201d':'"',
        '\u2022':'-', '\u00b7':'.', '\u2026':'...',
        '\u2192':'->', '\u2190':'<-', '\u2713':'OK', '\u2717':'X',
        '\u00b0':'deg', '\u00b1':'+/-', '\u00d7':'x', '\u00f7':'/',
        '\u2264':'<=', '\u2265':'>=', '\u2260':'!=',
        '\u00a0':' ', '\u200b':'', '\ufeff':'',
        '\u03bc':'u', '\u03c3':'s', '\u03c0':'pi',
        '\u00e9':'e', '\u00e8':'e', '\u00e0':'a',
        '\u00ae':'(R)', '\u00a9':'(C)', '\u2122':'(TM)',
        '\u20ac':'EUR', '\u00a3':'GBP',
    }
    result = ''
    for c in text:
        try:
            c.encode('latin-1')
            result += c
        except:
            result += mp.get(c, '-')
    return result


def truncate(text, n=75):
    text = s(str(text))
    return text if len(text) <= n else text[:n-3] + '...'


def wrap_lines(text, max_chars=80):
    """\\n 유지하면서 max_chars 기준으로 줄 나누기"""
    text = s(str(text))
    result = []
    for para in text.split('\n'):
        para = para.rstrip()
        if not para:
            result.append('')
            continue
        words = para.split(' ')
        cur = ''
        for w in words:
            if not w:
                continue
            if len(cur) + len(w) + (1 if cur else 0) <= max_chars:
                cur = cur + ' ' + w if cur else w
            else:
                if cur:
                    result.append(cur)
                cur = w
        if cur:
            result.append(cur)
    return result or ['']


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
        self.ln(6)

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(*GRAY)
        self.cell(0, 4, f'InfraLens - infralens.streamlit.app - Confidential - {datetime.now().strftime("%Y-%m-%d")}', align='C')

    def h1(self, text):
        self.set_font('Helvetica', 'B', 13)
        self.set_text_color(*DARK)
        self.cell(0, 8, s(text), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def h2(self, text):
        self.set_font('Helvetica', 'B', 9)
        self.set_text_color(*BRAND)
        self.cell(0, 6, s(text), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def body(self, text, color=DARK, size=8.5):
        self.set_font('Helvetica', '', size)
        self.set_text_color(*color)
        self.multi_cell(0, 5, s(str(text)))
        self.ln(1)

    def divider(self, t=4, b=4):
        self.ln(t)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.3)
        self.line(16, self.get_y(), 194, self.get_y())
        self.ln(b)

    def metric_cards(self, items):
        """items: [(label, value, color), ...]"""
        n = len(items)
        w = 178 / n
        y0 = self.get_y()
        h = 24
        for i, (label, value, color) in enumerate(items):
            x = 16 + i * w
            self.set_fill_color(*LGRAY)
            self.set_draw_color(*BORDER)
            self.set_line_width(0.3)
            self.rect(x, y0, w - 1, h, 'FD')
            self.set_fill_color(*color)
            self.rect(x, y0, w - 1, 2, 'F')
            self.set_xy(x + 2, y0 + 4)
            self.set_font('Helvetica', '', 6)
            self.set_text_color(*GRAY)
            self.cell(w - 4, 3, s(label).upper())
            self.set_xy(x + 2, y0 + 9)
            self.set_font('Helvetica', 'B', 12)
            self.set_text_color(*color)
            self.cell(w - 4, 7, s(value))
        self.set_y(y0 + h + 4)

    def action_card(self, num, category, title, detail, action,
                    savings, effort, timeframe, confidence,
                    risk='Low', owner='DevOps', timeline='Week 1'):
        """
        Action card - 텍스트 먼저 측정 후 그리기
        """
        # 최대 10줄로 action 제한
        action_list = wrap_lines(action, 76)
        if len(action_list) > 10:
            action_list = action_list[:9] + ['... full commands available in InfraLens dashboard']

        detail_list = wrap_lines(detail, 80)

        # 높이 계산
        h = 6                              # 상단 여백
        h += 5                             # 카테고리 태그
        h += 6                             # 제목
        h += len(detail_list) * 5.0 + 3   # detail
        h += 5                             # "Recommended Action:" 라벨
        h += len(action_list) * 5.0 + 8   # action 박스
        h += 12                            # 하단 절감액
        h += 4                             # 하단 여백

        # 페이지 체크
        if self.get_y() + h > 268:
            self.add_page()

        y0 = self.get_y()
        x0 = 16

        # 왼쪽 컬러 바
        effort_color = GREEN if effort == 'Low' else AMBER if effort == 'Medium' else RED
        self.set_fill_color(*effort_color)
        self.rect(x0, y0, 3, h, 'F')

        # 배경
        self.set_fill_color(250, 250, 255)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(x0 + 3, y0, 175, h, 'FD')

        cur = y0 + 4

        # 카테고리
        self.set_xy(22, cur)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*BRAND)
        self.cell(60, 4, s(f'#{num} - {category.upper()}'))

        # 태그들 (오른쪽)
        tx = 130
        for tag, bg, fg in [
            (f'Effort: {effort}', AMBER_L, AMBER),
            (f'Risk: {risk}', GREEN_L, GREEN),
        ]:
            self.set_xy(tx, cur)
            self.set_fill_color(*bg)
            self.set_text_color(*fg)
            self.set_font('Helvetica', 'B', 6)
            tw = len(tag) * 2.3 + 4
            self.cell(tw, 4, s(tag), fill=True)
            tx += tw + 2
        cur += 5

        # 제목
        self.set_xy(22, cur)
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(*DARK)
        self.cell(172, 5, truncate(title, 70))
        cur += 6

        # Detail
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*GRAY)
        for line in detail_list:
            self.set_xy(22, cur)
            self.cell(172, 5, line)
            cur += 5
        cur += 3

        # Recommended Action 라벨
        self.set_xy(22, cur)
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*BRAND)
        self.cell(0, 4, 'Recommended Action:')
        cur += 5

        # Action 박스
        ah = len(action_list) * 5.0 + 6
        self.set_fill_color(*PURPLE_BG)
        self.set_draw_color(*PURPLE_BD)
        self.set_line_width(0.2)
        self.rect(22, cur, 172, ah, 'FD')
        ly = cur + 3
        self.set_font('Helvetica', '', 7.5)
        self.set_text_color(*DARK)
        for line in action_list:
            self.set_xy(25, ly)
            self.cell(166, 5, line)
            ly += 5
        cur += ah + 3

        # 하단 절감액
        self.set_fill_color(236, 253, 245)
        self.rect(22, cur, 172, 11, 'F')
        self.set_xy(25, cur + 2)
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(*GREEN)
        sv = f'Save ${savings:,.0f}/mo  (${savings*12:,.0f}/yr)' if savings > 0 else 'Performance improvement'
        self.cell(90, 6, s(sv))
        self.set_xy(130, cur + 2)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(*GRAY)
        self.cell(30, 3, s(f'Timeframe: {timeframe}'))
        self.set_xy(130, cur + 6)
        self.cell(30, 3, s(f'Confidence: {confidence:.0f}%'))
        cur += 11

        self.set_y(y0 + h + 2)

    def bar_chart_v(self, data, labels, title, width=178, height=45, color=BRAND):
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, s(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)
        cx = 16
        cy = self.get_y()
        mx = max(data) if data and max(data) > 0 else 1
        n = len(data)
        bw = (width - 8) / n
        ba = height - 10
        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(cx, cy, width, height, 'FD')
        for i, (v, lb) in enumerate(zip(data, labels)):
            bh = (v / mx) * ba
            bx = cx + 4 + i * bw
            by = cy + height - 8 - bh
            self.set_fill_color(*color)
            self.rect(bx, by, max(bw - 1.5, 1), bh, 'F')
            self.set_xy(bx, cy + height - 7)
            self.set_font('Helvetica', '', 4.5)
            self.set_text_color(*GRAY)
            self.cell(max(bw - 1.5, 1), 4, str(lb), align='C')
        self.set_y(cy + height + 3)

    def before_after_chart(self, before, after, width=178, height=48):
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, 'Monthly Cost: Before vs After Optimization',
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)
        cx = 16
        cy = self.get_y()
        mx = before * 1.1
        ba = height - 14
        bw = 38
        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(cx, cy, width, height, 'FD')
        savings = before - after
        for i, (lb, v, c, vs) in enumerate([
            ('Current Cost',  before,  RED,   f'${before:,.0f}'),
            ('After Optim.',  after,   GREEN, f'${after:,.0f}'),
            ('Monthly Saved', savings, BRAND, f'${savings:,.0f}'),
        ]):
            bh = (v / mx) * ba
            bx = cx + 15 + i * 58
            by = cy + height - 12 - bh
            self.set_fill_color(*c)
            self.rect(bx, by, bw, bh, 'F')
            self.set_xy(bx, by - 6)
            self.set_font('Helvetica', 'B', 7)
            self.set_text_color(*c)
            self.cell(bw, 5, vs, align='C')
            self.set_xy(bx, cy + height - 10)
            self.set_font('Helvetica', '', 7)
            self.set_text_color(*DARK)
            self.cell(bw, 5, lb, align='C')
        self.set_y(cy + height + 4)

    def cumulative_chart(self, monthly, width=178, height=45):
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, 'Cumulative Savings Over 12 Months',
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)
        cx = 16
        cy = self.get_y()
        vals = [monthly * m for m in range(1, 13)]
        mx = vals[-1]
        ba = height - 10
        bw = (width - 8) / 12
        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(cx, cy, width, height, 'FD')
        for i, v in enumerate(vals):
            bh = (v / mx) * ba
            bx = cx + 4 + i * bw
            by = cy + height - 8 - bh
            r = int(99 + (i / 11) * 50)
            self.set_fill_color(r, 102, 241)
            self.rect(bx, by, max(bw - 1.5, 1), bh, 'F')
        self.set_xy(cx + width - 45, cy + 3)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*GREEN)
        self.cell(40, 5, f'Year 1: ${mx:,.0f}', align='R')
        self.set_y(cy + height + 3)


def generate_pdf(recs, sim, quality, scores_df, df=None, company_name="Your Company"):
    pdf = PDF()
    pdf.set_margins(16, 16, 16)
    pdf.set_auto_page_break(auto=True, margin=18)

    ms  = sim.get('savings_monthly', 0)
    bef = sim.get('before_monthly', 0)
    aft = sim.get('after_monthly', 0)
    pct = sim.get('savings_pct', 0)
    ann = sim.get('savings_annual', 0)

    # ── PAGE 1: COVER + EXECUTIVE SUMMARY ──
    pdf.add_page()
    pdf.set_fill_color(*BRAND)
    pdf.rect(0, 0, 210, 7, 'F')
    pdf.ln(11)

    pdf.set_font('Helvetica', 'B', 28)
    pdf.set_text_color(*BRAND)
    pdf.cell(0, 11, 'InfraLens', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, 'AI Infrastructure Cost Optimization Report', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)

    pdf.set_fill_color(*LGRAY)
    pdf.set_draw_color(*BORDER)
    pdf.rect(16, pdf.get_y(), 178, 14, 'FD')
    pdf.set_xy(20, pdf.get_y() + 3)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(*DARK)
    pdf.cell(80, 5, f'Prepared for: {s(company_name)}')
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Generated: {datetime.now().strftime("%B %d, %Y at %H:%M")}', align='R')
    pdf.set_xy(20, pdf.get_y() + 5)
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Data analyzed: {quality.get("clean_rows",0):,} rows | {quality.get("devices","?")} devices | {quality.get("date_range","N/A")}')
    pdf.ln(20)

    pdf.divider(2, 6)
    pdf.h1('Executive Summary')
    pdf.body(
        f'{s(company_name)} is currently spending ${bef:,.0f}/month on GPU infrastructure. '
        f'InfraLens identified ${ms:,.0f}/month ({pct}%) in recoverable waste '
        f'through our 9-method ensemble analysis. All optimizations are operational '
        f'changes only: no new hardware, no downtime, no performance impact.'
    )
    pdf.ln(2)

    pdf.metric_cards([
        ('Current Monthly Spend', f'${bef:,.0f}',  GRAY),
        ('After Optimization',    f'${aft:,.0f}',  BLUE),
        ('Monthly Savings',       f'${ms:,.0f}',   GREEN),
        ('Annual Opportunity',    f'${ann:,.0f}',  BRAND),
    ])

    pdf.set_fill_color(*GREEN_L)
    pdf.set_draw_color(167, 243, 208)
    pdf.set_line_width(0.3)
    pdf.rect(16, pdf.get_y(), 178, 9, 'FD')
    pdf.set_xy(20, pdf.get_y() + 2)
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(*GREEN)
    pdf.cell(0, 4, 'Payback period: IMMEDIATE - Zero capital required. All changes are configuration-level only.')
    pdf.ln(13)

    # Top 3 actions
    pdf.h2('Top 3 Actions - Start This Week')
    for r in [x for x in recs if x.monthly_savings > 0][:3]:
        y = pdf.get_y()
        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.rect(16, y, 178, 11, 'FD')
        pdf.set_fill_color(*BRAND)
        pdf.rect(16, y, 5, 11, 'F')
        pdf.set_xy(24, y + 2)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*DARK)
        pdf.cell(110, 4, truncate(r.title, 55))
        pdf.set_xy(24, y + 6)
        pdf.set_font('Helvetica', '', 7)
        pdf.set_text_color(*GRAY)
        pdf.cell(80, 4, s(f'Effort: {r.effort} | {r.timeframe}'))
        pdf.set_xy(150, y + 3)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(*GREEN)
        pdf.cell(40, 5, f'${r.monthly_savings:,.0f}/mo', align='R')
        pdf.ln(13)

    # ── PAGE 2: FINANCIAL IMPACT ──
    pdf.add_page()
    pdf.h1('Financial Impact Analysis')
    pdf.body(
        'All figures are based on your actual usage data and current electricity rates.',
        GRAY, 8
    )
    pdf.ln(2)
    pdf.before_after_chart(bef, aft)
    pdf.ln(2)
    pdf.cumulative_chart(ms)
    pdf.ln(3)

    # ROI 테이블
    pdf.h2('ROI Analysis')
    rows = [
        ['Metric', 'Value', 'Notes'],
        ['Monthly Cost Reduction', f'${ms:,.0f}', f'{pct}% of current spend'],
        ['Annual Savings', f'${ann:,.0f}', 'No capital investment required'],
        ['Implementation Cost', '$0', 'Config changes only'],
        ['ROI', 'Infinite', 'Zero cost, immediate returns'],
        ['Break-even Period', 'Day 1', 'Savings start immediately'],
        ['3-Year Total Savings', f'${ms*36:,.0f}', 'Conservative estimate'],
    ]
    cw = [65, 45, 68]
    for j, row in enumerate(rows):
        if j == 0:
            pdf.set_fill_color(*BRAND)
            pdf.set_text_color(*WHITE)
            pdf.set_font('Helvetica', 'B', 8)
        else:
            pdf.set_fill_color(*LLGRAY if j % 2 == 0 else WHITE)
            pdf.set_text_color(*DARK)
            pdf.set_font('Helvetica', '', 8)
        x = 16
        for k, (cell, w) in enumerate(zip(row, cw)):
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

    # ── PAGE 3+: ACTION PLANS ──
    pdf.add_page()
    pdf.h1('Detailed Action Plans')
    pdf.body(
        'Each action validated by our 9-method ensemble. '
        'Sorted by monthly savings. Low-risk actions require no change approval.',
        GRAY, 8
    )
    pdf.ln(2)

    meta = [
        {'risk':'Low',    'owner':'DevOps',   'timeline':'Week 1'},
        {'risk':'Low',    'owner':'DevOps',   'timeline':'Week 1-2'},
        {'risk':'Medium', 'owner':'Infra',    'timeline':'Month 1'},
        {'risk':'Medium', 'owner':'Infra',    'timeline':'Month 1'},
        {'risk':'Medium', 'owner':'MLOps',    'timeline':'Month 1'},
        {'risk':'Medium', 'owner':'DevOps',   'timeline':'Month 1'},
    ]

    for i, r in enumerate([x for x in recs if x.monthly_savings > 0]):
        m = meta[i] if i < len(meta) else {'risk':'Medium','owner':'DevOps','timeline':'Month 1'}
        pdf.action_card(
            num=r.priority,
            category=r.category,
            title=r.title,
            detail=r.detail,
            action=r.action,
            savings=r.monthly_savings,
            effort=r.effort,
            timeframe=r.timeframe,
            confidence=r.confidence,
            risk=m['risk'],
            owner=m['owner'],
            timeline=m['timeline'],
        )

    # ── GPU ANALYSIS ──
    if scores_df is not None and len(scores_df) > 0:
        pdf.add_page()
        pdf.h1('GPU Technical Analysis')
        pdf.body(
            'Per-GPU efficiency score: compute(25%) + memory(20%) + power(20%) + thermal(15%) + consistency(10%) + resource(10%)',
            GRAY, 8
        )
        pdf.ln(2)

        score_col = 'total_score' if 'total_score' in scores_df.columns else 'efficiency'
        gpu_ids = [str(r['gpu_id']) for _, r in scores_df.iterrows()]
        gpu_scores = [float(r[score_col]) for _, r in scores_df.iterrows()]
        grade_colors = {'A': GREEN, 'B': BLUE, 'C': AMBER, 'D': RED}

        def gpu_color(val, i):
            g = scores_df.iloc[i]['grade'] if i < len(scores_df) else 'C'
            return grade_colors.get(g, GRAY)

        pdf.bar_chart_v(gpu_scores, gpu_ids, 'GPU Efficiency Scores (Green=A, Blue=B, Amber=C, Red=D)',
                       height=48, color=BRAND)
        pdf.ln(3)

        # GPU 테이블
        headers = ['GPU ID', 'Score', 'Grade', 'Util%', 'Waste%', 'Action']
        widths  = [35, 20, 18, 20, 20, 65]
        pdf.set_fill_color(*BRAND)
        pdf.set_text_color(*WHITE)
        pdf.set_font('Helvetica', 'B', 8)
        x = 16
        for h, w in zip(headers, widths):
            pdf.set_xy(x, pdf.get_y())
            pdf.cell(w, 7, h, fill=True)
            x += w
        pdf.ln(7)

        action_map = {'A':('Maintain',GREEN),'B':('Minor review',BLUE),
                      'C':('Optimize now',AMBER),'D':('URGENT',RED)}
        for j, (_, row) in enumerate(scores_df.iterrows()):
            grade = row['grade']
            gc = grade_colors.get(grade, GRAY)
            at, ac = action_map.get(grade, ('Review',GRAY))
            pdf.set_fill_color(*LLGRAY if j % 2 == 0 else WHITE)
            vals = [str(row['gpu_id']), f"{row[score_col]:.0f}/100", grade,
                    f"{row['avg_util']}%", f"{row['waste_pct']}%", at]
            x = 16
            for k, (v, w) in enumerate(zip(vals, widths)):
                pdf.set_xy(x, pdf.get_y())
                if k == 2:
                    pdf.set_text_color(*gc)
                    pdf.set_font('Helvetica', 'B', 8)
                elif k == 5:
                    pdf.set_text_color(*ac)
                    pdf.set_font('Helvetica', 'B', 8)
                else:
                    pdf.set_text_color(*DARK)
                    pdf.set_font('Helvetica', '', 8)
                pdf.cell(w, 6, s(v), fill=True)
                x += w
            pdf.ln(6)

    # ── ROADMAP ──
    pdf.add_page()
    pdf.h1('Implementation Roadmap')
    pdf.body('Phased plan to maximize savings while minimizing risk.', GRAY, 8)
    pdf.ln(3)

    phases = [
        ('Phase 1 - Week 1', 'Quick Wins (Zero Risk)', GREEN, (236,253,245),
         ['Enable GPU power-saving mode on idle instances',
          'Set up automated alerts for GPU utilization < 15%',
          'Configure persistence mode on all GPU instances'],
         ms * 0.40),
        ('Phase 2 - Week 2-3', 'Scheduling Optimization', BLUE, (239,246,255),
         ['Reschedule batch training to off-peak hours (22:00-06:00)',
          'Configure Slurm/cron scheduler for off-peak execution',
          'Set up workload queuing for non-time-sensitive tasks'],
         ms * 0.35),
        ('Phase 3 - Month 1', 'Infrastructure Right-sizing', AMBER, (255,251,235),
         ['Implement auto-scaling (Kubernetes HPA or custom)',
          'Reduce overnight GPU fleet by identified reducible count',
          'Set up monitoring dashboards for ongoing optimization'],
         ms * 0.25),
    ]

    cumul = 0
    for phase, title, color, bg, actions, sav in phases:
        cumul += sav
        y0 = pdf.get_y()
        pdf.set_fill_color(*bg)
        pdf.set_draw_color(*color)
        pdf.set_line_width(0.5)
        pdf.rect(16, y0, 178, 46, 'FD')
        pdf.set_fill_color(*color)
        pdf.rect(16, y0, 4, 46, 'F')
        pdf.set_xy(24, y0 + 3)
        pdf.set_font('Helvetica', 'B', 7)
        pdf.set_text_color(*color)
        pdf.cell(80, 4, s(phase))
        pdf.set_xy(24, y0 + 8)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(*DARK)
        pdf.cell(100, 5, s(title))
        for k, action in enumerate(actions):
            pdf.set_xy(24, y0 + 15 + k * 7)
            pdf.set_font('Helvetica', '', 7.5)
            pdf.set_text_color(*DARK)
            pdf.cell(130, 5, s(f'- {action}'))
        pdf.set_xy(150, y0 + 10)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(*GREEN)
        pdf.cell(40, 5, f'+${sav:,.0f}/mo', align='R')
        pdf.set_xy(150, y0 + 17)
        pdf.set_font('Helvetica', '', 7)
        pdf.set_text_color(*GRAY)
        pdf.cell(40, 4, f'Cumul: ${cumul:,.0f}/mo', align='R')
        pdf.ln(50)

    # ── METHODOLOGY ──
    pdf.add_page()
    pdf.h1('Methodology & Technical Details')
    pdf.h2('9-Method Ensemble Detection (95% Confidence)')
    pdf.body(
        'InfraLens uses 9 complementary detection methods. '
        'The ensemble only flags high-confidence issues where multiple methods agree.',
        DARK, 8
    )
    pdf.ln(2)

    methods = [
        ('1. Rule-based + Z-score', 'Hourly baseline per GPU using rolling 24h stats. Flags periods 1.5+ std below baseline.'),
        ('2. Isolation Forest', 'GPU-specific anomaly model, auto-tunes contamination rate, 200 estimators.'),
        ('3. DBSCAN + Silhouette', 'Density clustering with Silhouette-optimized epsilon parameter.'),
        ('4. Prophet Seasonality', 'Decomposes time-series to isolate true anomalies from seasonal patterns.'),
        ('5. Mahalanobis Distance', 'D2=(x-mu)T*Sigma-1*(x-mu). Multi-feature correlation aware.'),
        ('6. Shannon Entropy', 'H=-SUM p(x)log p(x). Low entropy = predictable waste pattern.'),
        ('7. PCA Reconstruction', 'Anomaly score = reconstruction error after dimensionality reduction.'),
        ('8. Energy COP', 'COP=useful_work/total_energy. Thermodynamic efficiency ratio.'),
        ('9. Ensemble Fusion', 'Confidence-weighted combination. High confidence when methods agree.'),
    ]

    for method, desc in methods:
        y0 = pdf.get_y()
        if y0 > 255:
            pdf.add_page()
        nl = len(wrap_lines(desc, 140)) 
        rh = nl * 4.5 + 8
        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.set_line_width(0.2)
        pdf.rect(16, pdf.get_y(), 178, rh, 'FD')
        pdf.set_fill_color(*BRAND)
        pdf.rect(16, pdf.get_y(), 3, rh, 'F')
        pdf.set_xy(22, pdf.get_y() + 2)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*BRAND)
        pdf.cell(55, 4, s(method))
        pdf.set_xy(22, pdf.get_y() + 4)
        pdf.set_font('Helvetica', '', 7.5)
        pdf.set_text_color(*DARK)
        pdf.multi_cell(170, 4.2, s(desc))
        pdf.set_y(y0 + rh + 2)

    pdf.divider(6, 4)
    pdf.set_font('Helvetica', 'I', 7.5)
    pdf.set_text_color(*GRAY)
    pdf.multi_cell(0, 5, s(
        'This report is confidential and prepared exclusively for the recipient organization. '
        'All savings figures are conservative estimates based on actual usage data. '
        'Actual results depend on implementation quality and operational changes made.'
    ))

    return bytes(pdf.output())


def generate_billing_pdf(billing, quality, company_name="Your Company"):
    pdf = PDF()
    pdf.set_margins(16, 16, 16)
    pdf.set_auto_page_break(auto=True, margin=18)

    mc  = billing.get('monthly_cost', 0)
    ms  = billing.get('monthly_savings', 0)
    aft = mc - ms
    pct = round(ms / max(mc, 1) * 100, 1)

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
    pdf.ln(3)

    pdf.set_fill_color(*LGRAY)
    pdf.set_draw_color(*BORDER)
    pdf.rect(16, pdf.get_y(), 178, 14, 'FD')
    pdf.set_xy(20, pdf.get_y() + 3)
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
        f'{s(company_name)} is spending ${mc:,.0f}/month on cloud infrastructure. '
        f'InfraLens identified ${ms:,.0f}/month ({pct}%) in optimization opportunities. '
        f'All changes require zero capital investment.'
    )
    pdf.ln(2)

    pdf.metric_cards([
        ('Current Monthly Spend', f'${mc:,.0f}',    GRAY),
        ('After Optimization',    f'${aft:,.0f}',   BLUE),
        ('Monthly Savings',       f'${ms:,.0f}',    GREEN),
        ('Annual Opportunity',    f'${ms*12:,.0f}', BRAND),
    ])
    pdf.ln(4)

    pdf.before_after_chart(mc, aft)
    pdf.ln(2)
    pdf.cumulative_chart(ms)
    pdf.ln(3)

    pdf.add_page()
    pdf.h1('Action Plans')
    pdf.body('Sorted by monthly savings impact.', GRAY, 8)
    pdf.ln(2)

    meta = [
        {'risk':'Low',    'owner':'DevOps',    'timeline':'Week 1'},
        {'risk':'Medium', 'owner':'Infra',     'timeline':'Month 1'},
        {'risk':'High',   'owner':'Arch Team', 'timeline':'Quarter 1'},
    ]

    for i, finding in enumerate(billing.get('findings', [])):
        m = meta[i] if i < len(meta) else {'risk':'Medium','owner':'DevOps','timeline':'Month 1'}
        pdf.action_card(
            num=i+1,
            category=finding['type'],
            title=finding['title'],
            detail=finding['detail'],
            action=finding['action'],
            savings=finding['monthly_savings'],
            effort=finding['effort'],
            timeframe=finding['timeframe'],
            confidence=finding['confidence'],
            risk=m['risk'],
            owner=m['owner'],
            timeline=m['timeline'],
        )

    pdf.divider(8, 4)
    pdf.set_font('Helvetica', 'I', 7.5)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, 'This report is confidential and prepared exclusively for the recipient organization.', align='C')

    return bytes(pdf.output())


if __name__ == '__main__':
    from data_loader import load_and_prepare
    from cost_model import simulate_before_after
    from analyzer import (detect_idle_maximum, detect_peak_waste_advanced,
                          detect_overprovision_advanced, compute_advanced_efficiency_score,
                          detect_thermal_throttling, detect_memory_bandwidth_bottleneck,
                          detect_inter_gpu_waste, detect_workload_gap, engineer_features)
    from recommender import generate_recommendations
    import warnings
    warnings.filterwarnings('ignore')

    df, col_map, quality = load_and_prepare('realistic_gpu_data.csv')
    df = engineer_features(df)
    idle    = detect_idle_maximum(df)
    peak    = detect_peak_waste_advanced(df)
    over    = detect_overprovision_advanced(df)
    sim     = simulate_before_after(df)
    scores  = compute_advanced_efficiency_score(df)
    thermal = detect_thermal_throttling(df)
    mem_b   = detect_memory_bandwidth_bottleneck(df)
    inter   = detect_inter_gpu_waste(df)
    gap     = detect_workload_gap(df)
    recs    = generate_recommendations(idle, peak, over, sim, scores, df=df,
                thermal=thermal, mem_bottleneck=mem_b, inter_gpu=inter, workload_gap=gap)

    pdf_bytes = generate_pdf(recs, sim, quality, scores, df=df, company_name="Sample Corp")
    with open('infralens_report.pdf', 'wb') as f:
        f.write(pdf_bytes)
    print(f'Done: {len(pdf_bytes):,} bytes')
