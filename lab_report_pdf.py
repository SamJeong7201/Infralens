"""
lab_report_pdf.py
─────────────────
연구실 GPU 최적화 리포트 PDF
비즈니스 버전과 완전히 분리된 파일
"""
from fpdf import FPDF, XPos, YPos
from datetime import datetime
import pandas as pd

# ── 색상 (연구실 = 파란/초록 계열) ──
BRAND    = (37,  99,  235)   # 파란색
GREEN    = (5,   150, 105)
RED      = (220, 38,  38)
AMBER    = (217, 119, 6)
DARK     = (17,  24,  39)
GRAY     = (107, 114, 128)
LGRAY    = (248, 250, 252)
LLGRAY   = (243, 244, 246)
WHITE    = (255, 255, 255)
BORDER   = (226, 232, 240)
TEAL     = (13,  148, 136)
PURPLE   = (109, 40,  217)
GREEN_L  = (209, 250, 229)
AMBER_L  = (254, 243, 199)
RED_L    = (254, 226, 226)
BLUE_L   = (219, 234, 254)
TEAL_BG  = (240, 253, 250)
TEAL_BD  = (153, 246, 228)


def s(text):
    if not isinstance(text, str):
        text = str(text)
    mp = {
        '\u2014':'-','\u2013':'-','\u2018':"'",'\u2019':"'",
        '\u201c':'"','\u201d':'"','\u2022':'-','\u00b7':'.',
        '\u2026':'...','\u2192':'->','\u2190':'<-',
        '\u2264':'<=','\u2265':'>=','\u00b0':'deg',
        '\u00a0':' ','\u200b':'','\ufeff':'',
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


def compress_action(action_text, max_lines=18):
    """PDF용 action 압축 - SITUATION + Step 1만"""
    lines = action_text.split('\n')
    result = []
    section = None
    step_count = 0
    sit_count = 0

    for line in lines:
        ls = line.strip()
        if ls.startswith('SITUATION'):
            section = 'situation'
            result.append('SITUATION')
        elif ls.startswith('WHAT TO DO'):
            section = 'what'
            result.append('')
            result.append(line)
        elif ls.startswith('Step '):
            step_count += 1
            section = 'step'
            if step_count <= 1:
                result.append('')
                result.append(line)
        elif ls.startswith('HOW TO VERIFY'):
            section = 'verify'
            result.append('')
            result.append('HOW TO VERIFY')
        elif ls.startswith('RISK') or ls.startswith('ROLLBACK') or ls.startswith('ENVIRONMENT') or ls.startswith('EXPECTED'):
            break
        else:
            if section == 'situation' and sit_count < 3 and ls:
                result.append(line)
                sit_count += 1
            elif section == 'what' and ls:
                result.append(line)
            elif section == 'step' and step_count <= 1:
                result.append(line)
            elif section == 'verify' and ls:
                result.append(line)
                break

    if len(result) > max_lines:
        result = result[:max_lines-1] + ['  ... see full plan in InfraLens dashboard']
    return '\n'.join(result)


class LabPDF(FPDF):

    def header(self):
        if self.page_no() == 1:
            return
        self.set_fill_color(*BRAND)
        self.rect(0, 0, 210, 5, 'F')
        self.set_y(8)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*GRAY)
        self.cell(87, 4, 'InfraLens Lab Edition - GPU Cluster Optimization Report')
        self.cell(0, 4, f'Page {self.page_no()}', align='R')
        self.ln(6)

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(*GRAY)
        self.cell(0, 4, f'InfraLens Lab Edition - Confidential - {datetime.now().strftime("%Y-%m-%d")}', align='C')

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
        n  = len(items)
        w  = 178 / n
        y0 = self.get_y()
        h  = 24
        for i, (label, value, color) in enumerate(items):
            x = 16 + i * w
            self.set_fill_color(*LGRAY)
            self.set_draw_color(*BORDER)
            self.set_line_width(0.3)
            self.rect(x, y0, w-1, h, 'FD')
            self.set_fill_color(*color)
            self.rect(x, y0, w-1, 2, 'F')
            self.set_xy(x+2, y0+4)
            self.set_font('Helvetica', '', 6)
            self.set_text_color(*GRAY)
            self.cell(w-4, 3, s(label).upper())
            self.set_xy(x+2, y0+9)
            self.set_font('Helvetica', 'B', 11)
            self.set_text_color(*color)
            self.cell(w-4, 7, s(value))
        self.set_y(y0 + h + 4)

    def utilization_heatmap(self, metrics_df):
        """GPU x 시간대 히트맵"""
        if 'gpu_util' not in metrics_df.columns or 'hour' not in metrics_df.columns:
            return
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, s('GPU Utilization Heatmap - Hourly Average'),
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        gpu_ids = sorted(metrics_df['gpu_id'].unique()) if 'gpu_id' in metrics_df.columns else []
        hours   = list(range(24))
        label_w = 28
        cell_w  = (178 - label_w) / 24
        cell_h  = max(5.0, min(7.0, 180 / max(len(gpu_ids), 1)))

        # 시간 헤더
        cx = 16 + label_w
        cy = self.get_y()
        self.set_font('Helvetica', '', 5)
        self.set_text_color(*GRAY)
        for h in hours:
            if h % 3 == 0:
                self.set_xy(cx + h * cell_w, cy)
                self.cell(cell_w*3, 4, f'{h:02d}:00', align='C')
        self.ln(4)

        for gpu in gpu_ids:
            if self.get_y() + cell_h > 268:
                self.add_page()
            cy = self.get_y()
            self.set_xy(16, cy)
            self.set_font('Helvetica', '', 5.5)
            self.set_text_color(*DARK)
            self.cell(label_w, cell_h, s(str(gpu)[-10:]), align='R')
            gdf    = metrics_df[metrics_df['gpu_id'] == gpu]
            hourly = gdf.groupby('hour')['gpu_util'].mean()
            for h in hours:
                util = hourly.get(h, 0)
                bx   = 16 + label_w + h * cell_w
                if util < 15:   r,g,b = 180,40,40
                elif util < 30: r,g,b = 220,100,50
                elif util < 50: r,g,b = 240,180,50
                elif util < 70: r,g,b = 100,180,80
                else:           r,g,b = 30,140,60
                self.set_fill_color(r,g,b)
                self.set_draw_color(255,255,255)
                self.set_line_width(0.1)
                self.rect(bx, cy, cell_w, cell_h, 'FD')
                if cell_w > 6:
                    self.set_xy(bx, cy)
                    self.set_font('Helvetica', '', 4)
                    self.set_text_color(255,255,255)
                    self.cell(cell_w, cell_h, f'{util:.0f}', align='C')
            self.ln(cell_h)

        # 범례
        self.ln(2)
        lx = 16
        for (r,g,b), label in [
            ((180,40,40),'Idle <15%'),((220,100,50),'Low 15-30%'),
            ((240,180,50),'Normal 30-50%'),((100,180,80),'Active 50-70%'),
            ((30,140,60),'Peak >70%'),
        ]:
            self.set_fill_color(r,g,b)
            self.rect(lx, self.get_y(), 8, 4, 'F')
            self.set_xy(lx+9, self.get_y())
            self.set_font('Helvetica', '', 6)
            self.set_text_color(*GRAY)
            self.cell(28, 4, label)
            lx += 38
        self.ln(8)

    def user_bar_chart(self, user_pct: dict):
        """사용자별 GPU 사용률 바 차트"""
        if not user_pct:
            return
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, 'GPU Usage by User (%)', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        sorted_users = sorted(user_pct.items(), key=lambda x: -x[1])[:10]
        max_val = max(v for _, v in sorted_users) if sorted_users else 1
        bar_w   = 120
        row_h   = 7

        for user, pct in sorted_users:
            if self.get_y() + row_h > 268:
                break
            y = self.get_y()
            # 라벨
            self.set_xy(16, y)
            self.set_font('Helvetica', '', 7.5)
            self.set_text_color(*DARK)
            self.cell(35, row_h, s(user), align='R')
            # 바
            filled = (pct / max_val) * bar_w
            self.set_fill_color(*BLUE_L)
            self.rect(53, y+1, bar_w, row_h-2, 'F')
            self.set_fill_color(*BRAND)
            self.rect(53, y+1, filled, row_h-2, 'F')
            # 수치
            self.set_xy(53 + bar_w + 3, y)
            self.set_font('Helvetica', 'B', 7)
            self.set_text_color(*BRAND)
            self.cell(20, row_h, f'{pct:.1f}%')
            self.ln(row_h)
        self.ln(4)

    def weekday_weekend_chart(self, cu: dict):
        """평일 vs 주말 사용률 차트"""
        if 'hourly_util' not in cu:
            return
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*DARK)
        self.cell(0, 5, 'Hourly Utilization Pattern',
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

        hourly = cu['hourly_util']
        hours  = list(range(24))
        vals   = [hourly.get(h, 0) for h in hours]
        max_v  = max(max(vals), 1)

        cx     = 16
        cy     = self.get_y()
        width  = 178
        height = 45
        bar_w  = (width - 4) / 24
        bar_a  = height - 10

        self.set_fill_color(*LGRAY)
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.rect(cx, cy, width, height, 'FD')

        # threshold 15%
        ty = cy + height - 8 - (15/max_v)*bar_a
        self.set_draw_color(*RED)
        self.set_line_width(0.3)
        self.line(cx+2, ty, cx+width-2, ty)
        self.set_xy(cx+width-32, ty-4)
        self.set_font('Helvetica', 'I', 5.5)
        self.set_text_color(*RED)
        self.cell(30, 4, 'idle threshold', align='R')

        for i, (h, v) in enumerate(zip(hours, vals)):
            bh = (v/max_v)*bar_a
            bx = cx + 2 + i*bar_w
            by = cy + height - 8 - bh
            if v < 15:   fc = RED
            elif v < 50: fc = AMBER
            else:        fc = GREEN
            self.set_fill_color(*fc)
            self.rect(bx, by, max(bar_w-1,1), bh, 'F')

        for h in [0,3,6,9,12,15,18,21,23]:
            bx = cx + 2 + h*bar_w
            self.set_xy(bx, cy+height-7)
            self.set_font('Helvetica', '', 5)
            self.set_text_color(*GRAY)
            self.cell(bar_w*3, 4, f'{h:02d}:00', align='C')

        self.set_y(cy + height + 4)

    def rec_card(self, rec):
        """연구실 권장사항 카드"""
        impact_colors = {
            'throughput': BRAND,
            'fairness':   PURPLE,
            'efficiency': TEAL,
            'power':      GREEN,
        }
        color = impact_colors.get(rec.impact, BRAND)
        effort_color = GREEN if rec.effort=='Low' else AMBER if rec.effort=='Medium' else RED

        # RISK/ROLLBACK 파싱
        r_lines, rb_lines = [], []
        mode = None
        for ln in rec.action.split('\n'):
            ls = ln.strip()
            if ls.startswith('RISK'):
                mode = 'risk'
                val = ls.replace('RISK','').strip().lstrip('-').strip()
                if val: r_lines.append(val)
            elif ls.startswith('ROLLBACK'):
                mode = 'rollback'
            elif ls.startswith('ENVIRONMENT'):
                break
            elif mode == 'risk' and ls:
                r_lines.append(ls)
            elif mode == 'rollback' and ls:
                rb_lines.append(ls)
        r_lines  = [l for l in r_lines  if l][:3]
        rb_lines = [l for l in rb_lines if l][:3]

        # action 정리
        action_clean = '\n'.join([
            ln for ln in rec.action.split('\n')
            if not ln.strip().startswith(('RISK','ROLLBACK','ENVIRONMENT'))
        ])
        action_compressed = compress_action(action_clean, max_lines=18)
        action_list = wrap_lines(action_compressed, 76)
        detail_list = wrap_lines(rec.detail, 80)

        if self.get_y() > 210:
            self.add_page()

        y0 = self.get_y()

        # 왼쪽 바
        self.set_fill_color(*color)
        self.rect(16, y0, 3, 4, 'F')
        self.ln(4)

        # 카테고리 + 태그
        self.set_x(23)
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(*color)
        self.cell(70, 4, s(f'#{rec.priority} - {rec.category.upper()}'))
        ty = self.get_y()
        tx = 130
        for tag, bg, fg in [
            (f'Effort: {rec.effort}', AMBER_L, AMBER),
            (f'Owner: {rec.owner}',   BLUE_L,  BRAND),
        ]:
            self.set_xy(tx, ty)
            self.set_fill_color(*bg)
            self.set_text_color(*fg)
            self.set_font('Helvetica', 'B', 6)
            tw = len(tag)*2.3+4
            self.cell(tw, 4, s(tag), fill=True)
            tx += tw+2
        self.ln(6)

        # 제목
        self.set_x(23)
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(*DARK)
        self.cell(170, 5, truncate(rec.title, 70))
        self.ln(6)

        # Before → After
        self.set_x(23)
        self.set_fill_color(*LGRAY)
        self.set_font('Helvetica', '', 7.5)
        self.set_text_color(*GRAY)
        self.cell(80, 5, s(f'Now: {rec.metric_before}'), fill=True)
        self.set_font('Helvetica', 'B', 7.5)
        self.set_text_color(*GREEN)
        self.cell(5, 5, '->', fill=False)
        self.set_fill_color(*GREEN_L)
        self.cell(80, 5, s(f'Target: {rec.metric_after}'), fill=True)
        self.ln(7)

        # Detail
        self.set_font('Helvetica', '', 8)
        self.set_text_color(*GRAY)
        for line in detail_list:
            self.set_x(23)
            self.cell(170, 5, line)
            self.ln(5)
        self.ln(3)

        # Recommended Action
        self.set_x(23)
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*BRAND)
        self.cell(0, 4, 'Recommended Action:')
        self.ln(6)

        for line in action_list:
            self.set_x(23)
            self.set_fill_color(245, 243, 255)
            self.set_text_color(*DARK)
            self.set_font('Helvetica', '', 7.5)
            self.cell(170, 5, line, fill=True)
            self.ln(5)
        self.ln(3)

        # RISK & ROLLBACK
        if r_lines or rb_lines:
            self.set_x(23)
            self.set_fill_color(255, 251, 235)
            self.set_font('Helvetica', 'B', 8)
            self.set_text_color(*AMBER)
            self.cell(170, 6, 'RISK & ROLLBACK', fill=True)
            self.ln(6)
            if r_lines:
                self.set_x(23)
                self.set_fill_color(255, 251, 235)
                self.set_font('Helvetica', 'B', 7)
                self.set_text_color(*DARK)
                self.cell(170, 5, 'Risk:', fill=True)
                self.ln(5)
                self.set_font('Helvetica', '', 7)
                for line in r_lines:
                    self.set_x(25)
                    self.set_fill_color(255, 251, 235)
                    self.cell(168, 4.5, s('- '+line), fill=True)
                    self.ln(4.5)
            if rb_lines:
                self.ln(2)
                self.set_x(23)
                self.set_fill_color(255, 251, 235)
                self.set_font('Helvetica', 'B', 7)
                self.set_text_color(*DARK)
                self.cell(170, 5, 'Rollback:', fill=True)
                self.ln(5)
                self.set_font('Helvetica', '', 7)
                for line in rb_lines:
                    self.set_x(25)
                    self.set_fill_color(255, 251, 235)
                    self.cell(168, 4.5, s('- '+line), fill=True)
                    self.ln(4.5)
            self.ln(3)

        # 구분선
        self.set_draw_color(*BORDER)
        self.set_line_width(0.3)
        self.line(16, self.get_y(), 194, self.get_y())
        self.ln(6)


def generate_lab_pdf(recs, analysis, metrics_df=None,
                     jobs_df=None, lab_name='Your Lab') -> bytes:

    pdf = LabPDF()
    pdf.set_margins(16, 16, 16)
    pdf.set_auto_page_break(auto=True, margin=18)

    cu = analysis.get('cluster_util', {})
    pt = analysis.get('power_thermal', {})
    uf = analysis.get('user_fairness', {})
    je = analysis.get('job_efficiency', {})
    qb = analysis.get('queue_bottleneck', {})

    n_gpus   = analysis.get('n_gpus', 0)
    n_users  = analysis.get('n_users', 0)
    overall  = cu.get('overall_util', 0)
    idle_pct = cu.get('idle_util_pct', 0)
    wasted_h = cu.get('wasted_gpu_hours', 0)

    # ── PAGE 1: COVER + OVERVIEW ──
    pdf.add_page()
    pdf.set_fill_color(*BRAND)
    pdf.rect(0, 0, 210, 7, 'F')
    pdf.ln(11)

    pdf.set_font('Helvetica', 'B', 28)
    pdf.set_text_color(*BRAND)
    pdf.cell(0, 11, 'InfraLens', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, s('GPU Cluster Optimization Report - Lab Edition'),
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)

    # 헤더 박스
    pdf.set_fill_color(*LGRAY)
    pdf.set_draw_color(*BORDER)
    pdf.rect(16, pdf.get_y(), 178, 14, 'FD')
    pdf.set_xy(20, pdf.get_y()+3)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(*DARK)
    pdf.cell(80, 5, s(f'Lab: {lab_name}'))
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, f'Generated: {datetime.now().strftime("%B %d, %Y")}', align='R')
    pdf.set_xy(20, pdf.get_y()+5)
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5, s(f'{n_gpus} GPUs  |  {n_users} users  |  {analysis.get("date_range","N/A")}'))
    pdf.ln(18)

    pdf.divider(2, 6)
    pdf.h1('Cluster Overview')
    pdf.body(
        f'{lab_name} operates {n_gpus} GPUs shared by {n_users} users. '
        f'Current average utilization is {overall}%, with {idle_pct:.0f}% of GPU time sitting idle. '
        f'That represents {wasted_h:.0f} GPU-hours per month that could be used for queued research jobs.',
        DARK, 8.5
    )
    pdf.ln(2)

    pdf.metric_cards([
        ('Overall Utilization', f'{overall}%',
         GREEN if overall > 60 else AMBER if overall > 40 else RED),
        ('Idle GPU Time',       f'{idle_pct:.0f}%',
         RED if idle_pct > 50 else AMBER),
        ('Avg Queue Wait',      f'{qb.get("avg_wait",0):.0f} min',
         RED if qb.get("avg_wait",0) > 60 else AMBER),
        ('Monthly Elec. Cost',  f'${pt.get("monthly_elec_cost",0):,.0f}',
         GRAY),
    ])

    # Top 3 recommendations
    pdf.h2('Top Recommendations')
    for r in recs[:3]:
        impact_colors = {
            'throughput': BRAND, 'fairness': PURPLE,
            'efficiency': TEAL,  'power': GREEN,
        }
        color = impact_colors.get(r.impact, BRAND)
        y = pdf.get_y()
        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.rect(16, y, 178, 11, 'FD')
        pdf.set_fill_color(*color)
        pdf.rect(16, y, 4, 11, 'F')
        pdf.set_xy(23, y+2)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*DARK)
        pdf.cell(120, 4, truncate(r.title, 60))
        pdf.set_xy(23, y+6)
        pdf.set_font('Helvetica', '', 7)
        pdf.set_text_color(*GRAY)
        pdf.cell(80, 4, s(f'{r.impact.upper()} | Effort: {r.effort} | {r.timeframe}'))
        pdf.set_xy(155, y+3)
        pdf.set_font('Helvetica', 'B', 7)
        color2 = impact_colors.get(r.impact, BRAND)
        pdf.set_text_color(*color2)
        pdf.cell(35, 4, s(r.metric_after), align='R')
        pdf.ln(13)

    # ── PAGE 2: DATA EVIDENCE ──
    pdf.add_page()
    pdf.h1('Your Actual Usage Data')
    pdf.body(
        'Every recommendation below is based on observed patterns in your cluster data.',
        GRAY, 8
    )
    pdf.ln(2)

    if metrics_df is not None and 'gpu_util' in metrics_df.columns:
        pdf.utilization_heatmap(metrics_df)
        pdf.ln(2)

    if metrics_df is not None and 'hour' in metrics_df.columns:
        pdf.weekday_weekend_chart(cu)
        pdf.ln(2)

    # ── PAGE 3: USER ANALYSIS ──
    pdf.add_page()
    pdf.h1('User & Fairness Analysis')

    # 사용자 바 차트
    if uf.get('user_gpu_pct'):
        pdf.user_bar_chart(uf['user_gpu_pct'])
        pdf.ln(2)

    # 역할별 테이블
    if uf.get('role_summary'):
        pdf.h2('Usage by Role')
        headers = ['Role', 'GPU Time %', 'Fair Share %', 'Status']
        widths  = [40, 35, 35, 68]
        n_users_total = n_users if n_users > 0 else 1
        # 최대 사용자 기준으로 상대적 불균형 계산
        max_role_pct = max(uf['role_summary'].values()) if uf['role_summary'] else 1

        pdf.set_fill_color(*BRAND)
        pdf.set_text_color(*WHITE)
        pdf.set_font('Helvetica', 'B', 8)
        x = 16
        for h, w in zip(['Role', 'GPU Time %', 'vs Median', 'Status'], widths):
            pdf.set_xy(x, pdf.get_y())
            pdf.cell(w, 7, h, fill=True)
            x += w
        pdf.ln(7)

        pct_values = sorted(uf['role_summary'].values())
        median_pct = pct_values[len(pct_values)//2] if pct_values else 1

        for j, (role, pct) in enumerate(
            sorted(uf['role_summary'].items(), key=lambda x: -x[1])
        ):
            ratio = pct / max(median_pct, 0.1)
            status = 'Imbalance detected' if ratio > 2.5 else \
                     'Below median' if ratio < 0.5 else 'Normal'
            sc = RED if 'Imbalance' in status else AMBER if 'Below' in status else GREEN
            pdf.set_fill_color(*LLGRAY if j%2==0 else WHITE)
            vs_median = f'{ratio:.1f}x median'
            vals = [role, f'{pct:.1f}%', vs_median, status]
            x = 16
            for k, (v, w) in enumerate(zip(vals, widths)):
                pdf.set_xy(x, pdf.get_y())
                pdf.set_text_color(sc if k==3 else DARK)
                pdf.set_font('Helvetica', 'B' if k==3 else '', 8)
                pdf.cell(w, 6, s(v), fill=True)
                x += w
            pdf.ln(6)
        pdf.ln(4)

    # Queue 분석
    if qb:
        pdf.h2('Queue Analysis')
        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.rect(16, pdf.get_y(), 178, 22, 'FD')
        y0 = pdf.get_y()
        for i, (label, val, color) in enumerate([
            ('Avg Wait', f'{qb.get("avg_wait",0):.0f} min', AMBER),
            ('P90 Wait', f'{qb.get("p90_wait",0):.0f} min', RED),
            ('Long Waits (>2hr)', str(qb.get("long_wait_jobs",0)), RED),
            ('Avg Job Length', f'{qb.get("avg_run_hours",0):.1f} hr', GRAY),
        ]):
            x = 16 + i*44.5
            pdf.set_xy(x+2, y0+3)
            pdf.set_font('Helvetica', '', 6)
            pdf.set_text_color(*GRAY)
            pdf.cell(40, 3, s(label).upper())
            pdf.set_xy(x+2, y0+8)
            pdf.set_font('Helvetica', 'B', 12)
            pdf.set_text_color(*color)
            pdf.cell(40, 7, s(val))
        pdf.set_y(y0+24)

    # ── PAGE 4+: RECOMMENDATIONS ──
    pdf.add_page()
    pdf.h1('Optimization Recommendations')
    pdf.body(
        f'Based on analysis of {analysis.get("n_rows",0):,} data points from your cluster. '
        f'All recommendations use standard Slurm commands — no new software required.',
        GRAY, 8
    )
    pdf.ln(2)

    for rec in recs:
        pdf.rec_card(rec)

    # ── METHODOLOGY ──
    pdf.add_page()
    pdf.h1('Methodology')
    pdf.body(
        'InfraLens analyzes your GPU metrics and Slurm job logs to identify '
        'utilization patterns, fairness issues, and efficiency opportunities. '
        'Findings are based primarily on observed data, with impact estimates derived from cluster benchmarks.',
        DARK, 8
    )
    pdf.ln(3)

    methods = [
        ('Cluster Utilization', 'Per-GPU, per-hour utilization from nvidia-smi / DCGM metrics. Idle defined as <15% sustained utilization.'),
        ('User Fairness',       'Fairness imbalance computed against median user GPU-hours — no equal-split assumption. Flags users exceeding 2.5x median.'),
        ('Queue Analysis',      'Slurm job logs — submit time, start time, wait time, GPU count. Pending reason distribution derived from observed Slurm reason codes (Resources, Priority, Fairshare, QOS).'),
        ('Job Efficiency',      'Average GPU utilization per job type from DCGM metrics. Flags multi-GPU jobs under 30% utilization as over-allocated.'),
        ('Power Analysis',      'Power draw (W) x GPU count x hours x $0.12/kWh. Idle power computed separately to isolate recoverable electricity cost.'),
        ('Impact Simulation',   'Queue improvement estimates based on backfill scheduling studies on similarly-sized Slurm clusters (8-16 GPU nodes, mixed workloads).'),
    ]
    for method, desc in methods:
        y0 = pdf.get_y()
        if y0 > 255:
            pdf.add_page()
        rh = 14
        pdf.set_fill_color(*LGRAY)
        pdf.set_draw_color(*BORDER)
        pdf.set_line_width(0.2)
        pdf.rect(16, y0, 178, rh, 'FD')
        pdf.set_fill_color(*BRAND)
        pdf.rect(16, y0, 3, rh, 'F')
        pdf.set_xy(22, y0+2)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_text_color(*BRAND)
        pdf.cell(55, 4, s(method))
        pdf.set_xy(22, y0+7)
        pdf.set_font('Helvetica', '', 7.5)
        pdf.set_text_color(*DARK)
        pdf.cell(170, 4, s(desc))
        pdf.set_y(y0+rh+2)

    pdf.ln(4)
    pdf.set_font('Helvetica', 'I', 7.5)
    pdf.set_text_color(*GRAY)
    pdf.multi_cell(0, 5, s(
        'This report is confidential and prepared for the recipient lab only. '
        'Figures are based on actual cluster data provided.'
    ))

    return bytes(pdf.output())


if __name__ == '__main__':
    from lab_analyzer import run_lab_analysis
    from lab_recommender import generate_lab_recommendations
    import warnings
    warnings.filterwarnings('ignore')

    metrics  = pd.read_csv('lab_gpu_metrics.csv')
    jobs     = pd.read_csv('lab_slurm_jobs.csv')
    analysis = run_lab_analysis(metrics, jobs)
    recs     = generate_lab_recommendations(analysis)

    pdf_bytes = generate_lab_pdf(
        recs, analysis,
        metrics_df=metrics,
        jobs_df=jobs,
        lab_name='AI Research Lab'
    )
    with open('infralens_lab_report.pdf', 'wb') as f:
        f.write(pdf_bytes)
    print(f'Done: {len(pdf_bytes):,} bytes')
