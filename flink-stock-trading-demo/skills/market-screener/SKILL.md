---
name: market-screener
description: Screen stocks using technical indicators. Use this skill when evaluating whether a stock meets specific technical conditions.
---

# Market Screener Skill

Screen and evaluate stocks based on technical analysis indicators.

## Use Cases

- Determine if a stock is overbought or oversold
- Assess whether trading volume is abnormally high
- Identify price breakout signals

## Screening Rules

### 1. RSI Screening
- RSI < 30: **Oversold** — potential bounce opportunity, mark as buy candidate
- RSI > 70: **Overbought** — potential pullback risk, mark as sell candidate
- 30 ≤ RSI ≤ 70: Neutral, no signal triggered

### 2. MACD Screening
- MACD line crosses above signal line (golden cross): Buy signal
- MACD line crosses below signal line (death cross): Sell signal
- Histogram turning from negative to positive: Trend improving

### 3. Volume Screening
- Current volume > Average volume × 2: Volume surge, trend may accelerate
- Current volume < Average volume × 0.5: Volume contraction, trend may weaken

## Composite Score

Combine the three dimensions above into a composite score of 0-3:
- 3 points: Strong signal, recommend immediate action
- 2 points: Moderate signal, consider taking action
- 1 point: Weak signal, recommend waiting
- 0 points: No signal

## Usage

1. Fetch current market data for the stock
2. Call the `calculate_rsi` tool to compute RSI
3. Call the `calculate_macd` tool to compute MACD
4. Compare current volume against historical average
5. Score according to the rules above and provide a recommendation