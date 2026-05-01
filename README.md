# Polymarket Temperature Strategy

A Polymarket high-temperature market scanner and trading bot.

This repository is sanitized for public GitHub use. It does not include private keys, wallet addresses, CSV logs, order state, or live trading history.

## What It Scans

The bot scans:

```text
https://polymarket.com/weather/high-temperature
```

It focuses on markets shaped like:

```text
Highest temperature in <city> on <date>?
```

## Strategy Summary

The strategy is built around asymmetric payoff:

- YES positions target low-price, high-upside opportunities.
- NO positions are allowed at higher prices when the model finds stronger confirmation.
- Historical weather, forecast distance, market price, edge, EV, liquidity, spread, and risk limits are all used before an order is allowed.

Current public template settings are designed for:

- Small fixed downside per order.
- Higher expected payoff on low-price YES tickets.
- Stricter YES filters using history and forecast distance.
- Mid-price YES requires intraday confirmation.
- NO has separate max price and stricter EV/score thresholds.
- Same city and same target date can be capped to avoid correlated overexposure.

## Important Safety Notes

This is not financial advice. Prediction markets are risky. The bot can lose money.

Before using live mode:

- Read the code.
- Start with `POLY_AUTO_ORDER=false`.
- Use a fresh wallet with small funds.
- Never commit `.env`.
- Never share your private key.
- Check whether Polymarket is legal and available in your jurisdiction.

## Files

```text
polymarket_temperature_quant.py      Main strategy and order logic
run_temperature_paper_24h.ps1        24-hour runner for Windows PowerShell
run_temperature_paper_24h.bat        Double-click launcher
run_temperature_paper_24h_background.bat  Background launcher
.env.example                         Safe public config template
requirements.txt                     Python dependencies
.gitignore                           Excludes secrets, logs, CSVs, state
```

Ignored local-only files include:

```text
.env
csv/
*.csv
*.log
*order_state*.json
```

## Setup

Install dependencies:

```powershell
pip install -r requirements.txt
```

Create local config:

```powershell
copy .env.example .env
```

Edit `.env` locally. Do not commit it.

For paper/dry-run mode:

```env
POLY_AUTO_ORDER=false
```

For live mode:

```env
POLY_AUTO_ORDER=true
POLY_PRIVATE_KEY=your_private_key_here
POLY_FUNDER=your_polymarket_funder_or_proxy_wallet_here
```

## Run

From the repository directory:

```powershell
.\run_temperature_paper_24h.bat
```

Or:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_temperature_paper_24h.ps1
```

The runner writes local logs and CSVs into ignored files.

## Key Config

### Mode

```env
POLY_AUTO_ORDER=false
POLY_ONLY_TODAY=false
```

`POLY_AUTO_ORDER=false` means signals are recorded but orders are not sent.

`POLY_ONLY_TODAY=false` allows future target dates. Set `true` to only trade same-day markets.

### YES Low-Price Mode

```env
POLY_YES_EARLY_MAX_PRICE=0.08
POLY_YES_EARLY_SIZE_MULTIPLIER=0.45
POLY_YES_MAX_PRICE=0.18
```

YES tickets at or below the early max price can be entered without intraday confirmation, but with smaller sizing.

### YES Intraday Confirmation

```env
POLY_YES_INTRADAY_ENABLED=true
POLY_YES_INTRADAY_CONFIRM_ABOVE_PRICE=0.10
POLY_YES_INTRADAY_CONFIRM_DISTANCE=0.80
```

YES above the confirmation price requires current/hourly temperature context to be close enough to the threshold.

### NO Confirmation Mode

```env
POLY_NO_MAX_PRICE=0.45
POLY_NO_MIN_EDGE=0.10
POLY_NO_MIN_EV=0.16
POLY_NO_MIN_SCORE=0.12
```

NO is allowed to pay more than YES, but it must pass stricter EV and score thresholds.

### Exposure Limits

```env
POLY_LIVE_MAX_ORDERS_PER_SCAN=5
POLY_LIVE_MAX_DOLLARS_PER_SCAN=5.00
POLY_MAX_ORDERS_PER_CITY_DATE=2
```

These limits help avoid overexposure during one scan and prevent too many correlated orders on the same city/date.

## GitHub Publishing Checklist

Before pushing:

```powershell
git status --short
git ls-files
```

Confirm that these are not tracked:

```text
.env
csv/
*.csv
*.log
*order_state*.json
```

## License

Add a license before publishing if you want others to reuse the code.
