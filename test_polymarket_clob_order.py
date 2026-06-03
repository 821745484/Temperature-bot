import argparse
import json
import os
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Optional

try:
    from eth_account import Account
except ImportError:
    Account = None

from py_clob_client_v2 import ClobClient, OrderArgs, OrderType, PartialCreateOrderOptions, Side
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderPayload,
)


BASE_DIR = Path(__file__).resolve().parent
CLOB_URL = "https://clob.polymarket.com"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        print(f"env_file_missing path={path}")
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ["'", '"']:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"missing_env {name}")
    return value


def optional_manual_creds() -> Optional[ApiCreds]:
    api_key = os.getenv("CLOB_API_KEY", "").strip()
    secret = os.getenv("CLOB_SECRET", "").strip()
    passphrase = os.getenv("CLOB_PASS_PHRASE", "").strip()
    filled = [bool(api_key), bool(secret), bool(passphrase)]
    if all(filled):
        print("clob_api_creds_source=manual")
        return ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=passphrase)
    if any(filled):
        raise SystemExit("partial_manual_creds CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE must be filled together")
    print("clob_api_creds_source=auto_derive")
    return None


def build_client(use_manual_only: bool = False) -> ClobClient:
    private_key = env_required("POLY_PRIVATE_KEY")
    funder = env_required("POLY_FUNDER")
    signature_type = int(os.getenv("POLY_SIGNATURE_TYPE", "3"))
    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    manual_creds = optional_manual_creds()

    derived = "<eth_account missing>"
    if Account is not None:
        derived = Account.from_key(private_key).address
    print(
        f"wallet_check funder={funder} derived={derived} "
        f"funder_matches_private_key={str(derived).lower() == funder.lower()} signature_type={signature_type}"
    )

    if manual_creds is not None:
        return ClobClient(
            host=CLOB_URL,
            chain_id=chain_id,
            key=private_key,
            creds=manual_creds,
            signature_type=signature_type,
            funder=funder,
            use_server_time=True,
        )

    if use_manual_only:
        raise SystemExit("manual_creds_required fill CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE first")

    base_client = ClobClient(
        host=CLOB_URL,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
        use_server_time=True,
    )
    creds = base_client.create_or_derive_api_key()
    print("auth_l2_creds_ok source=auto_derive")
    return ClobClient(
        host=CLOB_URL,
        chain_id=chain_id,
        key=private_key,
        creds=creds,
        signature_type=signature_type,
        funder=funder,
        use_server_time=True,
    )


def print_json(label: str, value: Any) -> None:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        text = str(value)
    print(f"{label}={text}")


def extract_order_id(resp: Any) -> Optional[str]:
    if isinstance(resp, dict):
        for key in ["orderID", "orderId", "id"]:
            if resp.get(key):
                return str(resp[key])
        nested = resp.get("order")
        if isinstance(nested, dict):
            for key in ["orderID", "orderId", "id"]:
                if nested.get(key):
                    return str(nested[key])
    return None


def quantize_shares(dollars: Decimal, price: Decimal) -> Decimal:
    return (dollars / price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def normalize_tick(price: Decimal) -> str:
    if price < Decimal("0.01"):
        return "0.001"
    return "0.01"


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket CLOB auth/order smoke test. Defaults to auth-only.")
    parser.add_argument("--env-file", default=str(BASE_DIR / ".env"))
    parser.add_argument("--manual-creds-only", action="store_true", help="Fail unless CLOB_API_KEY/SECRET/PASS_PHRASE are filled.")
    parser.add_argument("--skip-balance-update", action="store_true")
    parser.add_argument("--place-order", action="store_true", help="Actually post a tiny post-only order, then cancel it.")
    parser.add_argument("--token-id", default="", help="Outcome token id to test with. Required with --place-order.")
    parser.add_argument("--price", default="0.01", help="Limit price. Keep far from market for smoke tests.")
    parser.add_argument("--dollars", default="1.00", help="Approx notional dollars for the test order.")
    args = parser.parse_args()

    print("execution_mode=auth_smoke_test live_order=false unless --place-order is set")
    load_dotenv(Path(args.env_file))
    client = build_client(use_manual_only=args.manual_creds_only)

    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    if not args.skip_balance_update:
        update_resp = client.update_balance_allowance(params)
        print_json("balance_allowance_update_ok", update_resp)
    balance_resp = client.get_balance_allowance(params)
    print_json("balance_allowance_ok", balance_resp)

    if not args.place_order:
        print("result=AUTH_ONLY_OK no_order_posted=true")
        return

    if not args.token_id:
        raise SystemExit("--token-id is required with --place-order")
    price = Decimal(args.price)
    dollars = Decimal(args.dollars)
    if price <= 0:
        raise SystemExit("invalid_price")
    shares = quantize_shares(dollars, price)
    if shares <= 0:
        raise SystemExit("invalid_shares")

    print(
        f"order_test_start post_only=true cancel_immediately=true token_id={args.token_id} "
        f"price={price} shares={shares} dollars={dollars}"
    )
    options = PartialCreateOrderOptions(tick_size=normalize_tick(price))
    resp = client.create_and_post_order(
        order_args=OrderArgs(
            token_id=args.token_id,
            price=float(price),
            size=float(shares),
            side=Side.BUY,
        ),
        options=options,
        order_type=OrderType.GTC,
        post_only=True,
    )
    print_json("order_post_ok", resp)

    order_id = extract_order_id(resp)
    if not order_id:
        print("cancel_skip reason=order_id_not_found")
        return
    cancel_resp = client.cancel_order(OrderPayload(orderID=order_id))
    print_json("cancel_ok", cancel_resp)
    print("result=ORDER_POST_AND_CANCEL_OK")


if __name__ == "__main__":
    main()
