@echo off
start "Polymarket Temperature Paper 24h" /min powershell -ExecutionPolicy Bypass -File "%~dp0run_temperature_paper_24h.ps1"
exit
