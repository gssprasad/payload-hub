"""Mapping agent: turns a plain-English request into a tested, saved mapping using Claude.

The agent inspects the sample payload, writes a JMESPath mapping, runs it against the real
transformer until the output matches the request, then saves it so the pipeline can use it.
"""
import json
import logging
import re
import time

from . import ai, db
from .transform import TransformError, parse, transform

log = logging.getLogger("agent")

MODEL = "claude-opus-5-5"
MAX_PREVIEW = 20_000

SYSTEM_PROMPT = """You are the mapping agent for Payload Hub, a Kafka pipeline that transforms payloads.
A user describes, in plain English, how an input payload should be transformed. Your job is to
produce a mapping the pipeline can run, verify it, and save it.

How the transformer works:
- The input is parsed (JSON as-is; XML via xmltodict: attributes become "@name" keys, element text
  next to attributes is "#text", repeated elements become lists, a single element is NOT a list,
  and every XML value is a string).
- A JMESPath expression is applied to the parsed data. Quote keys containing special characters,
  e.g. customer."@id".
- The result is written as JSON, or as XML (XML output needs a single root key; otherwise it is
  wrapped in <result>).
- Besides standard JMESPath functions (join, sum, length, to_string, to_number, max_by, sort_by,
  contains, starts_with, merge, ...), these extra functions exist: concat(a, b, ...) joins any
  values as a string; upper(s); lower(s); multiply(a, b) returns a number rounded to 2 decimals
  (accepts numeric strings); to_float(v).
- For XML lists that may hold one item, use to_array(...) so both shapes work.

Process:
1. Call inspect_input to see the parsed structure.
2. Write the expression and call test_mapping. Read the output critically against the request
   and fix it until it matches - field names, nesting, types and formats.
3. Call save_mapping with the final version. Pick a short kebab-case name.
4. Finish with 2-4 sentences: what the mapping does and any assumptions you made.

Mapping documents:
The user may attach a mapping specification exported from Excel, shown as <mapping_document>
with numbered rows per sheet. Treat it as the source of truth: it usually lists source fields,
target fields and transformation rules (column names vary - work out which column is which).
Implement every target field it defines, with the names and nesting it specifies, and follow
its rules (defaults, constants, formats, concatenations, conditions). After saving, check your
output against the document row by row. In your final message, list any rows you could not
implement or had to interpret, citing sheet and row number. The written request, if any, adds
to or overrides the document.

If the request cannot be expressed with this transformer (for example it needs data that is not
in the payload, or a calculation the functions cannot do), say so plainly instead of saving an
approximate mapping."""


TOOLS = [
    {"name": "inspect_input", "description": "Parse the user's sample payload and return its structure as JSON.",
     "parameters": {"type": "object", "properties": {"input_format": {"type": "string", "enum": ["json", "xml"]}},
                    "required": ["input_format"], "additionalProperties": False}},
    {"name": "test_mapping",
     "description": "Run a mapping against the sample payload with the real transformer and return the output.",
     "parameters": {"type": "object", "properties": {
         "expression": {"type": "string", "description": "JMESPath expression to apply."},
         "input_format": {"type": "string", "enum": ["json", "xml"]},
         "output_format": {"type": "string", "enum": ["json", "xml"]}},
         "required": ["expression", "input_format", "output_format"], "additionalProperties": False}},
    {"name": "save_mapping",
     "description": "Save a tested mapping so the pipeline and UI can use it. Re-tests it before saving.",
     "parameters": {"type": "object", "properties": {
         "name": {"type": "string", "description": "Short kebab-case identifier, e.g. order-to-shipment."},
         "label": {"type": "string", "description": "Human-readable label, e.g. Order (JSON) -> Shipment (XML)."},
         "expression": {"type": "string", "description": "The final JMESPath expression."},
         "input_format": {"type": "string", "enum": ["json", "xml"]},
         "output_format": {"type": "string", "enum": ["json", "xml"]}},
         "required": ["name", "label", "expression", "input_format", "output_format"], "additionalProperties": False}},
]


async def run(request: str, payload: str, input_format: str | None, mapping_doc: dict | None = None,
              partner_code: str | None = None, provider: str | None = None, model: str | None = None):
    """Run the agent for one request and return its result (also logged to agent_runs).
    provider/model default to the active choice set under AI providers."""
    started = time.perf_counter()
    steps: list[dict] = []
    saved: dict = {}
    if not provider:
        provider, default_model = await ai.active()
        model = model or default_model
    model = model or ai.PROVIDERS.get(provider, {}).get("default_model", "")

    async def inspect_input(input_format: str) -> str:
        try:
            data = parse(payload, input_format)
        except TransformError as e:
            steps.append({"tool": "inspect_input", "input_format": input_format, "ok": False, "result": str(e)})
            return f"Error: {e}"
        steps.append({"tool": "inspect_input", "input_format": input_format, "ok": True})
        return json.dumps(data, indent=2, ensure_ascii=False)[:MAX_PREVIEW]

    async def test_mapping(expression: str, input_format: str, output_format: str) -> str:
        try:
            out = transform(payload, input_format, expression, output_format)
            steps.append({"tool": "test_mapping", "expression": expression, "ok": True, "result": out[:2000]})
            return out[:MAX_PREVIEW]
        except TransformError as e:
            steps.append({"tool": "test_mapping", "expression": expression, "ok": False, "result": str(e)})
            return f"Error: {e}"

    async def save_mapping(name: str, label: str, expression: str, input_format: str, output_format: str) -> str:
        name = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")[:60] or "agent-mapping"
        try:
            preview = transform(payload, input_format, expression, output_format)
        except TransformError as e:
            steps.append({"tool": "save_mapping", "name": name, "ok": False, "result": str(e)})
            return f"Not saved - the mapping fails: {e}"
        if partner_code:  # partner mappings are stored on the partner, under the partner's code
            name = partner_code
            await db.update_partner(partner_code, expression=expression, input_format=input_format,
                                    output_format=output_format, mapping_source="agent")
        await db.save_mapping(name, label, input_format, output_format, expression,
                              request or f"From mapping document {mapping_doc['filename']}")
        saved.update(name=name, label=label, expression=expression, input_format=input_format,
                     output_format=output_format, preview=preview)
        steps.append({"tool": "save_mapping", "name": name, "ok": True})
        return f"Saved as '{name}'."

    impl = {"inspect_input": inspect_input, "test_mapping": test_mapping, "save_mapping": save_mapping}

    async def call_tool(name: str, args: dict) -> str:
        fn = impl.get(name)
        if fn is None:
            return f"Error: unknown tool '{name}'."
        try:
            return await fn(**{k: v for k, v in (args or {}).items() if k in fn.__code__.co_varnames})
        except TypeError as e:
            return f"Error: wrong arguments for {name}: {e}"

    hint = f"The payload is {input_format.upper()}." if input_format else "Work out whether the payload is JSON or XML."
    parts = []
    if mapping_doc:
        parts.append(f'<mapping_document filename="{mapping_doc["filename"]}">\n{mapping_doc["text"]}\n</mapping_document>')
    parts.append(f"<request>\n{request or 'Build the mapping described in the mapping document.'}\n</request>")
    parts.append(hint)
    parts.append(f"<sample_payload>\n{payload[:MAX_PREVIEW]}\n</sample_payload>")

    result = {"status": "FAILED", "mapping": None, "explanation": None, "steps": steps,
              "provider": provider, "model": model}
    usage = {"input_tokens": 0, "output_tokens": 0}
    try:
        out = await ai.run_agent(provider, model, SYSTEM_PROMPT, "\n\n".join(parts), TOOLS, call_tool)
        usage = out["usage"]
        result["explanation"] = out["text"] or ("The model declined this request." if out["refused"] else "")
        if saved:
            result.update(status="SAVED", mapping=saved)
        elif not out["refused"]:
            result["status"] = "NOT_SAVED"
    except ai.AIError as e:
        result["explanation"] = str(e)
    except Exception as e:  # noqa: BLE001
        log.exception("agent run failed")
        result["explanation"] = f"Agent failed: {e}"

    result["usage"] = usage
    result["duration_ms"] = round((time.perf_counter() - started) * 1000)
    result["id"] = await db.log_agent_run(request, result, mapping_doc, partner_code)
    return result
