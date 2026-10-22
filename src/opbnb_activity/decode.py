from decimal import Decimal, localcontext

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def decimal_units(raw_amount, decimals):
    with localcontext() as context:
        context.prec = max(100, len(str(raw_amount)) + decimals + 2)
        return format(Decimal(str(raw_amount)) / (Decimal(10) ** decimals), "f")


def decode_transfer(log, assets):
    topics = log.get("topics", [])
    if not topics or topics[0].lower() != TRANSFER_TOPIC:
        return {"kind": "other"}
    if len(topics) != 3:
        return {"kind": "nft_or_ambiguous"}
    asset = assets.get(log["address"].lower())
    if not asset or asset.get("verified") is not True:
        return {"kind": "unverified_transfer"}
    data = log.get("data", "0x")
    if not data.startswith("0x") or not 0 < len(data[2:]) <= 64:
        return {"kind": "nft_or_ambiguous"}
    try:
        amount = int(data, 16)
        decimals = asset["decimals"]
        if not isinstance(decimals, int) or not 0 <= decimals <= 255:
            raise ValueError("invalid decimals")
    except (KeyError, TypeError, ValueError):
        return {"kind": "nft_or_ambiguous"}
    return {"kind": "verified_erc20", "address": log["address"].lower(), "raw_amount": str(amount),
            "amount": decimal_units(amount, decimals), "symbol": asset.get("symbol", ""), "decimals": decimals}
