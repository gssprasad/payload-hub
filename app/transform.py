"""Payload parsing, JMESPath mapping and serialisation (the "Camel route" logic)."""
import json

import jmespath
import xmltodict
from jmespath import functions


class MappingFunctions(functions.Functions):
    """Extra functions available inside mapping expressions."""

    @functions.signature({"types": [], "variadic": True})
    def _func_concat(self, *parts):
        return "".join("" if p is None else str(p) for p in parts)

    @functions.signature({"types": ["string", "null"]})
    def _func_upper(self, s):
        return None if s is None else s.upper()

    @functions.signature({"types": ["string", "null"]})
    def _func_lower(self, s):
        return None if s is None else s.lower()

    @functions.signature({"types": ["number", "string"]}, {"types": ["number", "string"]})
    def _func_multiply(self, a, b):
        return round(float(a) * float(b), 2)

    @functions.signature({"types": ["number", "string"]})
    def _func_to_float(self, v):
        return float(v)


_OPTIONS = jmespath.Options(custom_functions=MappingFunctions())


class TransformError(Exception):
    """Raised when a payload cannot be parsed, mapped or serialised."""


def parse(payload: str, fmt: str):
    try:
        if fmt == "json":
            return json.loads(payload)
        if fmt == "xml":
            return xmltodict.parse(payload)
    except Exception as e:  # noqa: BLE001 - surfaced to the user as a failed transaction
        raise TransformError(f"Could not parse input as {fmt.upper()}: {e}") from e
    raise TransformError(f"Unsupported input format: {fmt}")


def apply_mapping(data, expression: str):
    try:
        return jmespath.search(expression, data, options=_OPTIONS)
    except Exception as e:  # noqa: BLE001
        raise TransformError(f"Mapping failed: {e}") from e


def serialise(data, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(data, indent=2, ensure_ascii=False)
    if fmt == "xml":
        if isinstance(data, list):
            data = {"result": {"item": data}}  # XML needs one root element; list entries become <item>s
        elif not (isinstance(data, dict) and len(data) == 1):
            data = {"result": data}
        try:
            return xmltodict.unparse(data, pretty=True, indent="  ")
        except Exception as e:  # noqa: BLE001
            raise TransformError(f"Could not write output as XML: {e}") from e
    raise TransformError(f"Unsupported output format: {fmt}")


def transform(payload: str, input_format: str, expression: str, output_format: str) -> str:
    mapped = apply_mapping(parse(payload, input_format), expression)
    if mapped is None:
        raise TransformError("Mapping produced no output - check the field paths match the input")
    return serialise(mapped, output_format)


def to_csv(payload: str, fmt: str, delimiter: str = ",") -> str:
    """Rows for the target system as CSV: a list of objects, the first list inside an object, or one object = one row.
    Nested objects become dotted column names (customer.name)."""
    import csv
    import io

    data = parse(payload, fmt)
    if isinstance(data, dict):
        if len(data) == 1 and isinstance(next(iter(data.values())), (dict, list)):
            data = next(iter(data.values()))  # XML root element, or {"orders": [...]}
        lists = [v for v in data.values() if isinstance(v, list) and v and all(isinstance(x, dict) for x in v)] \
            if isinstance(data, dict) else []
        if isinstance(data, dict) and len(lists) == 1:
            data = lists[0]
    rows = data if isinstance(data, list) else [data]

    def flat(obj, prefix=""):
        out = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                out.update(flat(v, f"{prefix}{k}."))
        elif isinstance(obj, list):
            out[prefix[:-1]] = json.dumps(obj, ensure_ascii=False)
        else:
            out[prefix[:-1] or "value"] = obj
        return out

    flat_rows = [flat(r) for r in rows]
    columns = list(dict.fromkeys(k for r in flat_rows for k in r))
    buf = io.StringIO()
    w = csv.DictWriter(buf, columns, delimiter=delimiter or ",", lineterminator="\n")
    w.writeheader()
    w.writerows(flat_rows)
    return buf.getvalue()


def query_target(target: str, payload: str, fmt: str) -> str:
    """Encode the mapped JSON object as query parameters, preserving the configured query."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    if fmt != "json":
        raise TransformError("Query parameters require JSON output from the request mapping.")
    data = parse(payload, fmt)
    if not isinstance(data, dict):
        raise TransformError("Query parameters require a JSON object.")
    pairs = []
    for key, value in data.items():
        for item in value if isinstance(value, list) else [value]:
            if item is None:
                continue
            if isinstance(item, (dict, list)):
                raise TransformError(f"Query parameter '{key}' must contain scalar values, not objects or nested lists.")
            pairs.append((key, str(item).lower() if isinstance(item, bool) else str(item)))
    parts = urlsplit(target)
    existing = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in data]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(existing + pairs), parts.fragment))


def convert_response(body: str, content_type: str, expression: str | None, response_format: str,
                     request_format: str) -> tuple[str, str]:
    """Turn an upstream API's response into the format the original caller expects.

    response_format: 'input' (same as the caller's request format), 'json', 'xml' or 'unchanged'.
    Returns (body, format) where format is 'json', 'xml' or 'unchanged'.
    """
    if response_format == "unchanged" and not expression:
        return body, "unchanged"
    target = request_format if response_format == "input" else response_format
    if target == "unchanged":
        target = "json"
    ctype = (content_type or "").lower()
    order = ["xml", "json"] if "xml" in ctype else ["json", "xml"]
    data = None
    for fmt in order:
        try:
            data = parse(body, fmt)
            break
        except TransformError:
            continue
    if data is None:
        data = {"response": body}  # plain text: wrap it so it can still be returned as JSON or XML
    if expression and expression.strip():
        data = apply_mapping(data, expression)
        if data is None:
            raise TransformError("Response mapping produced no output - check the field paths match the API's response")
    return serialise(data, target), target


SAMPLES = {
    "order-to-invoice": {
        "label": "Order (JSON) → Invoice (JSON)",
        "input_format": "json",
        "output_format": "json",
        "expression": """{
  invoiceNumber: concat('INV-', order.id),
  customerName: concat(order.customer.firstName, ' ', order.customer.lastName),
  email: lower(order.customer.email),
  currency: order.currency,
  lines: order.items[].{sku: sku, description: name, quantity: qty, lineTotal: multiply(qty, price)},
  total: sum(order.items[].multiply(qty, price))
}""",
        "input": json.dumps(
            {
                "order": {
                    "id": "1042",
                    "currency": "USD",
                    "customer": {"firstName": "Asha", "lastName": "Rao", "email": "Asha.Rao@Example.com"},
                    "items": [
                        {"sku": "YM-01", "name": "Yoga mat", "qty": 2, "price": 24.5},
                        {"sku": "BL-07", "name": "Yoga block", "qty": 1, "price": 12},
                    ],
                }
            },
            indent=2,
        ),
    },
    "customer-xml-to-json": {
        "label": "Customer (XML) → Profile (JSON)",
        "input_format": "xml",
        "output_format": "json",
        "expression": """{
  customerId: customer."@id",
  fullName: upper(customer.name),
  email: lower(customer.email),
  location: concat(customer.address.city, ', ', customer.address.country),
  vip: customer."@tier" == 'gold'
}""",
        "input": """<customer id="C-1001" tier="gold">
  <name>Ravi Kumar</name>
  <email>RAVI.KUMAR@EXAMPLE.COM</email>
  <address>
    <city>Hyderabad</city>
    <country>IN</country>
  </address>
</customer>""",
    },
    "payment-json-to-xml": {
        "label": "Payment (JSON) → Transfer (XML)",
        "input_format": "json",
        "output_format": "xml",
        "expression": """{
  transfer: {
    reference: txnRef,
    amount: to_string(amount),
    currency: ccy,
    debtor: from.name,
    debtorAccount: from.account,
    creditor: to.name,
    creditorAccount: to.account
  }
}""",
        "input": json.dumps(
            {
                "txnRef": "PAY-2026-0091",
                "amount": 1500.75,
                "ccy": "INR",
                "from": {"name": "Sai Prasad", "account": "XX11-2233"},
                "to": {"name": "Acme Supplies", "account": "XX99-8877"},
            },
            indent=2,
        ),
    },
}
