$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

function Import-DotEnv {
    param([string]$Path = (Join-Path $ScriptDir ".env"))
    if (-not (Test-Path -LiteralPath $Path)) { return }
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) { return }
        $parts = $line.Split("=", 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($value.Length -ge 2 -and (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'")))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        if ($key) {
            [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
}

function Set-EnvDefault {
    param([string]$Name, [string]$Value)
    if (-not [System.Environment]::GetEnvironmentVariable($Name, "Process")) {
        [System.Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

Import-DotEnv

Set-EnvDefault "PYTHONIOENCODING" "utf-8"
Set-EnvDefault "PYTHONUNBUFFERED" "1"
Set-EnvDefault "POLY_RUN_ONCE" "true"
Set-EnvDefault "POLY_SCAN_LIMIT" "200"
Set-EnvDefault "POLY_MAX_PAGES" "60"
Set-EnvDefault "POLY_SLEEP_SECONDS" "300"
Set-EnvDefault "POLY_AUTO_ORDER" "false"
Set-EnvDefault "POLY_VERBOSE" "false"
Set-EnvDefault "POLY_TEMP_BAND_C" "2"
Set-EnvDefault "POLY_ALLOW_SIDE" "AUTO"
Set-EnvDefault "POLY_MIN_PRICE" "0.04"
Set-EnvDefault "POLY_MAX_PRICE" "0.18"
Set-EnvDefault "POLY_YES_MAX_PRICE" "0.18"
Set-EnvDefault "POLY_NO_MAX_PRICE" "0.45"
Set-EnvDefault "POLY_MIN_EDGE" "0.10"
Set-EnvDefault "POLY_MIN_EV" "0.08"
Set-EnvDefault "POLY_MIN_SCORE" "0.08"
Set-EnvDefault "POLY_NO_MIN_EDGE" "0.10"
Set-EnvDefault "POLY_NO_MIN_EV" "0.16"
Set-EnvDefault "POLY_NO_MIN_SCORE" "0.12"
Set-EnvDefault "POLY_YES_MIN_EDGE" "0.22"
Set-EnvDefault "POLY_YES_MIN_EV" "0.18"
Set-EnvDefault "POLY_YES_MIN_SCORE" "0.22"
Set-EnvDefault "POLY_YES_EARLY_MAX_PRICE" "0.08"
Set-EnvDefault "POLY_YES_EARLY_SIZE_MULTIPLIER" "0.45"
Set-EnvDefault "POLY_YES_INTRADAY_ENABLED" "true"
Set-EnvDefault "POLY_YES_INTRADAY_CONFIRM_ABOVE_PRICE" "0.10"
Set-EnvDefault "POLY_YES_INTRADAY_CONFIRM_DISTANCE" "0.80"
Set-EnvDefault "POLY_YES_INTRADAY_MAX_DAYS_AHEAD" "1"
Set-EnvDefault "POLY_YES_EXACT_EXTRA_EDGE" "0.16"
Set-EnvDefault "POLY_YES_EXACT_EXTRA_EV" "0.16"
Set-EnvDefault "POLY_YES_EXACT_MIN_CONFIDENCE" "0.72"
Set-EnvDefault "POLY_YES_EXACT_MIN_PRICE" "0.04"
Set-EnvDefault "POLY_YES_EXACT_MAX_DAYS_AHEAD" "1"
Set-EnvDefault "POLY_YES_EXACT_MAX_FORECAST_DISTANCE" "0.60"
Set-EnvDefault "POLY_YES_EXACT_MAX_HISTORY_GAP" "0.12"
Set-EnvDefault "POLY_YES_EXACT_MIN_HISTORY_PROB" "0.18"
Set-EnvDefault "POLY_YES_EXACT_MAX_HISTORY_MEAN_DISTANCE" "0.90"
Set-EnvDefault "POLY_YES_ABOVE_MIN_HISTORY_PROB" "0.30"
Set-EnvDefault "POLY_YES_EXACT_SIGNAL_MULTIPLIER" "0.45"
Set-EnvDefault "POLY_EXACT_EXTRA_EDGE" "0.06"
Set-EnvDefault "POLY_PROBABILITY_SHRINK" "0.70"
Set-EnvDefault "POLY_HISTORY_ENABLED" "true"
Set-EnvDefault "POLY_HISTORY_WEIGHT" "0.30"
Set-EnvDefault "POLY_HISTORY_LOOKBACK_YEARS" "5"
Set-EnvDefault "POLY_HISTORY_WINDOW_DAYS" "15"
Set-EnvDefault "POLY_KELLY_FRACTION" "0.25"
Set-EnvDefault "POLY_MAX_TRADE_PCT" "0.03"
Set-EnvDefault "POLY_NET_EV_SPREAD_WEIGHT" "0.50"
Set-EnvDefault "POLY_NET_EV_LOW_PRICE_CUTOFF" "0.08"
Set-EnvDefault "POLY_NET_EV_LOW_PRICE_PENALTY" "0.01"
Set-EnvDefault "POLY_EXACT_COST_PENALTY" "0.03"
Set-EnvDefault "POLY_HISTORY_GAP_REDUCE" "0.18"
Set-EnvDefault "POLY_HISTORY_GAP_HARD_CAP" "0.25"
Set-EnvDefault "POLY_STRONG_SIGNAL_EDGE" "0.24"
Set-EnvDefault "POLY_STRONG_SIGNAL_EV" "0.35"
Set-EnvDefault "POLY_STRONG_SIGNAL_CONFIDENCE" "0.72"
Set-EnvDefault "POLY_MAX_SIGNAL_MULTIPLIER" "1.80"
Set-EnvDefault "POLY_WEAK_SIGNAL_MULTIPLIER" "0.50"
Set-EnvDefault "POLY_EXACT_SIGNAL_MULTIPLIER" "0.80"
Set-EnvDefault "POLY_MIN_VOLUME" "5000"
Set-EnvDefault "POLY_BANKROLL" "40"
Set-EnvDefault "POLY_LIVE_MIN_ORDER_SIZE" "1.00"
Set-EnvDefault "POLY_LIVE_MAX_ORDERS_PER_SCAN" "5"
Set-EnvDefault "POLY_LIVE_MAX_DOLLARS_PER_SCAN" "5.00"
Set-EnvDefault "POLY_MAX_ORDERS_PER_CITY_DATE" "2"
Set-EnvDefault "POLY_DAILY_TAKE_PROFIT_PCT" "0.80"
Set-EnvDefault "POLY_DAILY_STOP_LOSS_PCT" "0.50"
Set-EnvDefault "POLY_LOG_CSV" (Join-Path $ScriptDir "csv\polymarket_temperature_signals.csv")
Set-EnvDefault "POLY_ORDER_STATE_JSON" (Join-Path $ScriptDir "csv\polymarket_temperature_order_state.json")

$durationSeconds = 24 * 60 * 60
$intervalSeconds = [int]$env:POLY_SLEEP_SECONDS
$endTime = (Get-Date).AddSeconds($durationSeconds)
$logFile = Join-Path $ScriptDir "temperature_paper_24h_runner.log"
$scanNo = 0

"[$(Get-Date -Format s)] temperature paper trading started. auto_order=$env:POLY_AUTO_ORDER bankroll=$env:POLY_BANKROLL stop_loss_pct=$env:POLY_DAILY_STOP_LOSS_PCT take_profit_pct=$env:POLY_DAILY_TAKE_PROFIT_PCT end_time=$($endTime.ToString('s'))" | Tee-Object -FilePath $logFile -Append

while ((Get-Date) -lt $endTime) {
    $scanNo += 1
    "[$(Get-Date -Format s)] temperature scan #$scanNo started" | Tee-Object -FilePath $logFile -Append

    python -u (Join-Path $ScriptDir "polymarket_temperature_quant.py") 2>&1 | Tee-Object -FilePath $logFile -Append
    $exitCode = $LASTEXITCODE

    "[$(Get-Date -Format s)] temperature scan #$scanNo python_exit_code=$exitCode" | Tee-Object -FilePath $logFile -Append
    "[$(Get-Date -Format s)] temperature scan #$scanNo finished. sleeping ${intervalSeconds}s" | Tee-Object -FilePath $logFile -Append

    $slept = 0
    while ($slept -lt $intervalSeconds -and (Get-Date) -lt $endTime) {
        $remaining = $intervalSeconds - $slept
        "[$(Get-Date -Format s)] runner heartbeat. next_scan_in=${remaining}s" | Tee-Object -FilePath $logFile -Append
        $chunk = [Math]::Min(60, $remaining)
        Start-Sleep -Seconds $chunk
        $slept += $chunk
    }
}

"[$(Get-Date -Format s)] 24h temperature paper trading finished" | Tee-Object -FilePath $logFile -Append
Write-Host ""
Write-Host "24h temperature paper trading complete."
Write-Host "Signals: $env:POLY_LOG_CSV"
Write-Host "Runner log: $logFile"
Read-Host "Press Enter to exit"
