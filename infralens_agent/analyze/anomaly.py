"""
anomaly.py
──────────
역할: 통계적 이상감지 기반 함수
      다른 모든 감지 모듈이 이걸 사용

알고리즘:
  - Z-score: 평균에서 얼마나 멀리 떨어졌는지
  - IQR: 사분위수 기반 이상치 감지
  - EMA: 지수이동평균 (노이즈 제거)
"""

import numpy as np
import pandas as pd


def z_score(series: pd.Series) -> pd.Series:
    """
    Z-score 계산
    값이 평균에서 표준편차 몇 배 떨어졌는지
    |z| > 2 → 이상, |z| > 3 → 심각한 이상
    """
    mean = series.mean()
    std  = series.std()
    if std == 0:
        return pd.Series(0.0, index=series.index)
    return (series - mean) / std


def iqr_bounds(series: pd.Series, factor: float = 1.5):
    """
    IQR 기반 정상 범위 계산
    Q1 - factor*IQR ~ Q3 + factor*IQR 범위 밖이면 이상
    factor=1.5 → 일반 이상치
    factor=3.0 → 극단 이상치
    """
    q1  = series.quantile(0.25)
    q3  = series.quantile(0.75)
    iqr = q3 - q1
    return q1 - factor * iqr, q3 + factor * iqr


def ema(series: pd.Series, span: int = 6) -> pd.Series:
    """
    지수이동평균 (Exponential Moving Average)
    최근 값에 더 높은 가중치
    span=6 → 최근 6개 포인트 기준
    """
    return series.ewm(span=span, adjust=False).mean()


def linear_slope(series: pd.Series) -> float:
    """
    선형회귀 기울기
    양수 → 증가 추세, 음수 → 감소 추세
    메모리 누수 감지에 사용
    """
    if len(series) < 2:
        return 0.0
    x = np.arange(len(series), dtype=float)
    y = series.values.astype(float)
    # 최소제곱법
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


def gini_coefficient(values: list) -> float:
    """
    지니계수 계산 (0 = 완전 균등, 1 = 완전 불균등)
    GPU 간 사용률 불균형 측정에 사용
    """
    arr = np.array(values, dtype=float)
    if arr.sum() == 0:
        return 0.0
    arr = np.sort(arr)
    n   = len(arr)
    idx = np.arange(1, n + 1)
    return float((2 * (idx * arr).sum()) / (n * arr.sum()) - (n + 1) / n)
