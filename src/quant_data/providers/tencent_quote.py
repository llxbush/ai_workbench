from __future__ import annotations

import urllib.request


def tencent_quote(codes: list[str], timeout_seconds: float = 10.0) -> dict[str, dict]:
    prefixed = [_to_tencent_symbol(code) for code in codes]
    if not prefixed:
        return {}

    url = "https://qt.gtimg.cn/q=" + ",".join(prefixed)
    request = urllib.request.Request(url)
    request.add_header("User-Agent", "Mozilla/5.0")
    response = urllib.request.urlopen(request, timeout=timeout_seconds)
    data = response.read().decode("gbk", errors="ignore")

    result: dict[str, dict] = {}
    for line in data.strip().split(";"):
        if not line.strip() or "=" not in line or '"' not in line:
            continue
        key = line.split("=")[0].split("_")[-1]
        values = line.split('"')[1].split("~")
        if len(values) < 53:
            continue
        code = key[2:]
        result[code] = {
            "name": values[1],
            "price": _to_float(values[3]),
            "last_close": _to_float(values[4]),
            "open": _to_float(values[5]),
            "change_amount": _to_float(values[31]),
            "change_pct": _to_float(values[32]),
            "high": _to_float(values[33]),
            "low": _to_float(values[34]),
            "quote_datetime": values[30] if len(values) > 30 else "",
            "quote_date": _to_date(values[30]) if len(values) > 30 else None,
            "amount_yuan": _to_float(values[37]) * 10000 if values[37] else None,
            "turnover_pct": _to_float(values[38]),
            "pe_ttm": _to_float(values[39]),
            "amplitude_pct": _to_float(values[43]),
            "mcap_yi": _to_float(values[44]),
            "float_mcap_yi": _to_float(values[45]),
            "pb": _to_float(values[46]),
            "limit_up": _to_float(values[47]),
            "limit_down": _to_float(values[48]),
            "volume_ratio": _to_float(values[49]),
            "pe_static": _to_float(values[52]),
        }
    return result


def _to_tencent_symbol(code: str) -> str:
    normalized = str(code).zfill(6)
    if normalized.startswith(("4", "8", "9")):
        return f"bj{normalized}"
    if normalized.startswith("6"):
        return f"sh{normalized}"
    return f"sz{normalized}"


def _to_float(value: str) -> float | None:
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_date(value: str) -> str | None:
    if len(value) < 8:
        return None
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
