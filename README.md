# Polymarket 高温预测量化策略

这是一个面向 Polymarket 高温预测市场的扫描与交易脚本。

本仓库已经做过公开脱敏处理，不包含私钥、钱包地址、CSV 交易记录、订单状态文件或实盘日志。

## 策略做什么

脚本主要扫描 Polymarket 的高温市场：

```text
https://polymarket.com/weather/high-temperature
```

重点处理这类市场：

```text
Highest temperature in <city> on <date>?
```

也就是某个城市在某一天最高温是否达到某个阈值的预测市场。

## 核心思路

策略目标不是追求高胜率，而是追求“小亏多赚”的赔率结构。

主要逻辑：

- YES 侧偏向低价高赔率机会。
- NO 侧改为保守确认型，只做更低价格、更近日期、更强历史支持的机会。
- 结合天气预报、历史温度、盘中实况、市场价格、盘口流动性、edge、EV 和仓位风控。
- 控制同城市同日期的重复下注，减少相关性风险。
- 支持 DRY_RUN 纸面测试和 LIVE 自动下单。

## 风险提醒

这不是投资建议。预测市场有风险，脚本可能亏钱。

实盘前请务必：

- 先阅读代码。
- 先用 `POLY_AUTO_ORDER=false` 测试。
- 使用小资金、新钱包测试。
- 不要把 `.env` 上传到 GitHub。
- 不要泄露私钥。
- 确认 Polymarket 在你所在地区是否可用、是否合法。

## 文件说明

```text
polymarket_temperature_quant.py             主策略脚本
run_temperature_paper_24h.ps1               Windows PowerShell 24小时循环运行器
run_temperature_paper_24h.bat               双击启动入口
run_temperature_paper_24h_background.bat    后台启动入口
.env.example                                公开安全配置模板
requirements.txt                            Python 依赖
.gitignore                                  排除私钥、CSV、日志、订单状态等本地文件
```

以下文件不会上传：

```text
.env
csv/
*.csv
*.log
*order_state*.json
```

## 安装依赖

在项目目录执行：

```powershell
pip install -r requirements.txt
```

## 配置方法

复制配置模板：

```powershell
copy .env.example .env
```

然后只在本地编辑 `.env`。

纸面测试模式：

```env
POLY_AUTO_ORDER=false
```

实盘模式：

```env
POLY_AUTO_ORDER=true
POLY_PRIVATE_KEY=你的私钥
POLY_FUNDER=你的 Polymarket funder/proxy 钱包地址
```

注意：`.env` 已被 `.gitignore` 排除，不要手动强制上传。

## 运行方法

双击：

```text
run_temperature_paper_24h.bat
```

或者在 PowerShell 中执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\run_temperature_paper_24h.ps1
```

运行后会在本地生成 CSV、日志和订单状态文件，这些文件默认不会进入 Git。

## 关键参数说明

### 运行模式

```env
POLY_AUTO_ORDER=false
POLY_ONLY_TODAY=false
```

`POLY_AUTO_ORDER=false`：只记录信号，不真实下单。

`POLY_AUTO_ORDER=true`：满足条件时自动下单。

`POLY_ONLY_TODAY=true`：只做当天市场。

`POLY_ONLY_TODAY=false`：允许做未来日期市场。

## YES 策略

YES 主要走低价高赔率路线。

```env
POLY_YES_EARLY_MAX_PRICE=0.08
POLY_YES_EARLY_SIZE_MULTIPLIER=0.45
POLY_YES_MAX_PRICE=0.18
```

含义：

- YES 价格小于等于 `0.08` 时，允许提前埋伏。
- 提前埋伏会自动小仓，仓位乘数为 `0.45`。
- YES 最高买入价格为 `0.18`。

## YES 盘中确认

```env
POLY_YES_INTRADAY_ENABLED=true
POLY_YES_INTRADAY_CONFIRM_ABOVE_PRICE=0.10
POLY_YES_INTRADAY_CONFIRM_DISTANCE=0.80
POLY_METAR_ENABLED=true
```

含义：

- YES 价格高于 `0.10` 时，需要盘中实况或小时级预报确认。
- 当前温度或小时级峰值需要距离阈值不超过 `0.80°C`。
- 如果市场能识别出机场站点，脚本会优先尝试 AviationWeather METAR 实况温度。
- 这样可以保留低价提前埋伏机会，同时减少中价 YES 的错误入场。

## NO 策略

NO 主要走确认型逻辑。

```env
POLY_NO_MAX_PRICE=0.20
POLY_NO_MIN_EDGE=0.16
POLY_NO_MIN_EV=0.25
POLY_NO_MIN_SCORE=0.18
POLY_NO_MAX_DAYS_AHEAD=0
POLY_NO_SIZE_MULTIPLIER=0.55
POLY_NO_EXACT_MIN_FORECAST_DISTANCE=1.20
POLY_NO_EXACT_MIN_HISTORY_NO_PROB=0.82
POLY_NO_METAR_BLOCK_DISTANCE=1.00
POLY_NO_METAR_RISK_DISTANCE=1.50
```

含义：

- NO 最高买入价格收紧到 `0.20`，避免中高价 NO 错一次亏太多。
- NO 默认只做当天市场，`POLY_NO_MAX_DAYS_AHEAD=0`。
- NO 仓位乘数为 `0.55`，即使信号很强也会比普通信号更小仓。
- exact 类型 NO 要求预报温度距离阈值至少 `1.20°C`，避免贴边误判。
- exact 类型 NO 要求过去 5 年同期历史上“不等于该阈值”的概率至少 `82%`。
- 如果 METAR 实况温度已经距离阈值过近，NO 会被过滤，避免盘中临近击穿还继续买 NO。

## 历史温度

```env
POLY_HISTORY_ENABLED=true
POLY_HISTORY_LOOKBACK_YEARS=5
POLY_HISTORY_WINDOW_DAYS=15
```

脚本会参考过去多年同日期附近的温度数据，用来判断当前阈值是否有历史支持。

## 结算站点对齐

Polymarket 天气市场经常按 Weather Underground 指定站点结算，例如：

```text
https://www.wunderground.com/history/daily/us/ca/san-francisco/KSFO
```

其中 `KSFO` 就是旧金山机场气象站代码。

脚本会优先从 `resolutionSource` 提取这类站点代码，并使用对应机场/气象站坐标查询天气数据。这样可以减少城市中心温度和实际结算站点温度不一致造成的误判。

如果市场没有提供可识别站点代码，脚本才会回退到城市 geocoding。

## 站点级自校准

```env
POLY_STATION_CALIBRATION_ENABLED=true
POLY_STATION_CALIBRATION_JSON=csv\station_calibration.json
POLY_STATION_CALIBRATION_MIN_SAMPLES=8
POLY_STATION_CALIBRATION_MAX_ABS_SHIFT=0.06
```

脚本支持按 `station_code` 读取校准参数，例如 `KSFO`、`ZSPD`、`RKSI`。当某个站点历史样本足够多时，可以对模型概率做轻微修正，避免所有城市/机场使用同一套置信度。

示例：

```json
{
  "KSFO": {
    "samples": 20,
    "yes_bias": -0.03,
    "probability_shrink": 0.85
  },
  "RKSI:exact": {
    "samples": 12,
    "yes_bias": 0.02
  }
}
```

含义：

- `yes_bias=-0.03`：该站点 YES 概率整体下调 3%。
- `probability_shrink=0.85`：该站点概率向 50% 收缩，降低过度自信。
- `站点:市场类型` 可以只针对 exact/above/below 生效。

## 入场快照

```env
POLY_SNAPSHOT_ENABLED=true
POLY_SNAPSHOT_DIR=csv\snapshots
```

每次真正记录或发送订单时，脚本会保存一份 JSON 快照，包含当时的价格、预测、历史概率、METAR 温度、仓位、EV、过滤理由等信息。这个文件用于后续复盘，不包含私钥。

## 仓位与风控

```env
POLY_BANKROLL=40
POLY_LIVE_MIN_ORDER_SIZE=1.00
POLY_LIVE_MAX_ORDERS_PER_SCAN=5
POLY_LIVE_MAX_DOLLARS_PER_SCAN=5.00
POLY_MAX_ORDERS_PER_CITY_DATE=2
```

含义：

- 单笔最小下单金额为 `1U`。
- 每轮最多下 `5` 单。
- 每轮最多投入 `5U`。
- 同城市同日期最多持有 `2` 单，避免相关性过高。

## 分档仓位逻辑

脚本不是固定每个信号都下同样金额，而是会根据机会质量自动调整仓位乘数。

强信号会提高仓位：

```env
POLY_STRONG_SIGNAL_EDGE=0.24
POLY_STRONG_SIGNAL_EV=0.35
POLY_STRONG_SIGNAL_CONFIDENCE=0.72
POLY_MAX_SIGNAL_MULTIPLIER=1.80
```

含义：

- 当 edge、EV、模型确定性都达到强信号门槛时，仓位乘数最高可以放大到 `1.80`。
- 这用于捕捉收益率更高的机会。

弱信号会降低仓位：

```env
POLY_WEAK_SIGNAL_MULTIPLIER=0.50
```

含义：

- 如果信号刚刚过线但不够强，仓位会自动减半。

exact 市场会额外降仓：

```env
POLY_EXACT_SIGNAL_MULTIPLIER=0.80
POLY_YES_EXACT_SIGNAL_MULTIPLIER=0.45
```

含义：

- exact 温度市场更难命中，所以默认会降低仓位。
- YES exact 风险更高，因此仓位折扣更大。

历史与预报分歧过大时会减仓或过滤：

```env
POLY_HISTORY_GAP_REDUCE=0.18
POLY_HISTORY_GAP_HARD_CAP=0.25
```

含义：

- 如果历史温度概率和当前预报概率差距较大，仓位会降低。
- 如果分歧超过硬上限，信号会被过滤。

注意：这里的“分档仓位”是指下单前根据机会质量决定轻仓或重仓，不是持仓后持续补仓的马丁格尔式加仓。

## 止盈与止损

```env
POLY_DAILY_TAKE_PROFIT_PCT=0.80
POLY_TAKE_PROFIT_CLOSE_ALL_ENABLED=true
POLY_DAILY_STOP_LOSS_PCT=0.50
```

含义：

- 当日浮盈达到 bankroll 的 `80%` 时，触发止盈逻辑。
- 可以配置是否自动清仓。
- 当日亏损达到设定比例时停止继续扩大风险。

## 推送 GitHub 前检查

提交前建议检查：

```powershell
git status --short
git ls-files
```

确认以下文件没有被追踪：

```text
.env
csv/
*.csv
*.log
*order_state*.json
```

## 免责声明

本项目仅用于研究和学习量化策略、预测市场定价与自动化交易流程。

任何实盘交易风险由使用者自行承担。
