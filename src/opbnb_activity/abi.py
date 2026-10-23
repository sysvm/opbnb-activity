import hashlib
import json
import re

from Crypto.Hash import keccak

MAX_ABI_BYTES = 512 * 1024
MAX_ABI_ENTRIES = 1000


def topic(signature):
    return "0x" + keccak.new(digest_bits=256, data=signature.encode()).hexdigest()


def selector(signature):
    return topic(signature)[:10]


def canonical_type(parameter, depth=0):
    if not isinstance(parameter, dict) or depth > 8:
        raise ValueError("invalid ABI type")
    kind = parameter.get("type")
    if not isinstance(kind, str) or len(kind) > 128:
        raise ValueError("invalid ABI type")
    matched = re.fullmatch(r"([A-Za-z][A-Za-z0-9]*)(\[\d*\])*", kind)
    if not matched:
        raise ValueError("invalid ABI type")
    base = matched[1]
    suffix = kind[len(base):]
    if base == "tuple":
        components = parameter.get("components")
        if not isinstance(components, list) or len(components) > 100:
            raise ValueError("invalid ABI tuple")
        base = "(" + ",".join(canonical_type(part, depth + 1) for part in components) + ")"
    else:
        base = {"uint": "uint256", "int": "int256", "fixed": "fixed128x18", "ufixed": "ufixed128x18"}.get(base, base)
        if not re.fullmatch(r"address|bool|string|bytes(?:[1-9]|[12][0-9]|3[0-2])?|u?int(?:8|16|24|32|40|48|56|64|72|80|88|96|104|112|120|128|136|144|152|160|168|176|184|192|200|208|216|224|232|240|248|256)|u?fixed\d+x\d+|function", base):
            raise ValueError("invalid ABI type")
    return base + suffix


def parse_abi(value):
    if isinstance(value, str):
        if len(value.encode()) > MAX_ABI_BYTES:
            raise ValueError("ABI too large")
        value = json.loads(value)
    if not isinstance(value, list) or len(value) > MAX_ABI_ENTRIES:
        raise ValueError("invalid ABI entries")
    text = json.dumps(value, sort_keys=True, separators=(",", ":"))
    if len(text.encode()) > MAX_ABI_BYTES:
        raise ValueError("ABI too large")
    maps = {"functions": {}, "events": {}}
    for entry in value:
        if not isinstance(entry, dict):
            raise ValueError("invalid ABI entry")
        kind = entry.get("type")
        if kind not in ("function", "event") or (kind == "event" and entry.get("anonymous")):
            continue
        name, inputs = entry.get("name"), entry.get("inputs")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]{0,127}", name) or not isinstance(inputs, list) or len(inputs) > 100:
            raise ValueError("invalid ABI signature")
        signature = name + "(" + ",".join(canonical_type(parameter) for parameter in inputs) + ")"
        target = maps["functions" if kind == "function" else "events"]
        key = selector(signature) if kind == "function" else topic(signature)
        target.setdefault(key, []).append(signature)
    for mapping in maps.values():
        for key, signatures in mapping.items():
            mapping[key] = sorted(set(signatures))
    return dict(maps, abi_sha256=hashlib.sha256(text.encode()).hexdigest())


STANDARD_FUNCTIONS = {selector(signature): [signature] for signature in (
    "transfer(address,uint256)", "approve(address,uint256)", "transferFrom(address,address,uint256)",
    "checkIn()", "checkIn(uint256)", "dailyCheckIn()", "update()", "signIn(string)")}
STANDARD_EVENTS = {topic(signature): [signature] for signature in ("Transfer(address,address,uint256)", "Approval(address,address,uint256)")}
