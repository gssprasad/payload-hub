"""AI providers for the mapping agent: Claude, OpenAI, Gemini, GitHub Models and Azure OpenAI.

Each provider runs the same tool-using loop: the model calls our tools (inspect_input, test_mapping,
save_mapping) until it is done, then answers in text. API keys are pasted in the UI and stored
encrypted (see vault.py); environment variables work as a fallback.
"""
import json
import logging
import os

from . import db, vault

log = logging.getLogger("ai")

MAX_TURNS = 15

PROVIDERS = {
    "anthropic": {
        "label": "Claude (Anthropic)", "icon": "🟠", "env": "ANTHROPIC_API_KEY",
        "key_help": "Create a key at console.anthropic.com → API keys.",
        "models": ["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-4-5", "claude-fable-5-1"],
        "default_model": "claude-opus-5-5", "fields": [],
    },
    "openai": {
        "label": "OpenAI (GPT)", "icon": "🟢", "env": "OPENAI_API_KEY",
        "key_help": "Create a key at platform.openai.com → API keys.",
        "models": [], "default_model": "", "fields": [],
    },
    "gemini": {
        "label": "Gemini (Google)", "icon": "🔵", "env": "GEMINI_API_KEY",
        "key_help": "Create a key in Google AI Studio (aistudio.google.com) → Get API key.",
        "models": [], "default_model": "", "fields": [],
    },
    "github": {
        "label": "GitHub Models (Copilot family)", "icon": "⚫", "env": "GITHUB_TOKEN",
        "key_help": "GitHub Copilot itself has no public API for apps. GitHub Models is GitHub's official API for "
                    "the same model families: create a fine-grained personal access token with the "
                    "'Models: read' permission (github.com → Settings → Developer settings).",
        "models": [], "default_model": "", "fields": [
            {"key": "base_url", "label": "Endpoint", "default": "https://models.github.ai/inference"}],
    },
    "azure": {
        "label": "Azure OpenAI (Microsoft)", "icon": "🔷", "env": "AZURE_OPENAI_API_KEY",
        "key_help": "In the Azure portal: your Azure OpenAI resource → Keys and Endpoint. The model is your "
                    "deployment name.",
        "models": [], "default_model": "", "fields": [
            {"key": "endpoint", "label": "Endpoint", "default": "https://<resource>.openai.azure.com"},
            {"key": "api_version", "label": "API version", "default": "2024-10-21"}],
    },
}

# Claude request options differ by model (see Anthropic's model docs).
_CLAUDE_FALLBACKS = {"claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5", "claude-fable-5-1"}
_CLAUDE_EFFORT = ("claude-opus-5", "claude-sonnet-5", "claude-fable-5", "claude-opus-4-8", "claude-opus-4-7",
                  "claude-opus-4-6", "claude-sonnet-4-6")


class AIError(Exception):
    pass


# ---------- settings and keys ----------

async def credentials(provider: str) -> tuple[str | None, dict]:
    row = await db.get_ai_provider(provider)
    key = vault.decrypt(row["api_key_enc"]) if row else None
    config = (row or {}).get("config") or {}
    if not key:
        env = PROVIDERS[provider]["env"]
        key = os.getenv(env) or (os.getenv("GOOGLE_API_KEY") if provider == "gemini" else None)
    return key, config


async def active() -> tuple[str, str]:
    s = await db.get_setting("agent_model") or {}
    provider = s.get("provider") or "anthropic"
    model = s.get("model") or PROVIDERS[provider]["default_model"]
    return provider, model


# ---------- model lists ----------

async def list_models(provider: str) -> list[str]:
    key, cfg = await credentials(provider)
    if not key:
        raise AIError("No API key saved for this provider.")
    try:
        if provider == "anthropic":
            import anthropic

            client = anthropic.AsyncAnthropic(api_key=key)
            return sorted([m.id async for m in client.models.list()], reverse=True)
        if provider == "openai":
            import openai

            client = openai.AsyncOpenAI(api_key=key)
            ids = [m.id async for m in client.models.list()]
            chat = [i for i in ids if i.startswith(("gpt", "o1", "o3", "o4", "o5", "chatgpt"))
                    and not any(x in i for x in ("audio", "realtime", "transcribe", "tts", "image", "search"))]
            return sorted(chat or ids, reverse=True)
        if provider == "gemini":
            from google import genai

            client = genai.Client(api_key=key)
            out = []
            async for m in await client.aio.models.list():
                if "generateContent" in (m.supported_actions or []):
                    out.append(m.name.removeprefix("models/"))
            return sorted(out, reverse=True)
        if provider == "github":
            import httpx

            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get("https://models.github.ai/catalog/models",
                                headers={"Authorization": f"Bearer {key}", "Accept": "application/vnd.github+json"})
            if r.status_code in (401, 403):
                raise AIError("GitHub rejected the token - it needs the 'Models: read' permission.")
            try:
                data = r.json()
            except ValueError:
                data = None
            if r.status_code >= 400 or not isinstance(data, list):
                raise AIError(f"GitHub Models gave an unexpected answer (HTTP {r.status_code}) - check the token.")
            return sorted(m["id"] for m in data if isinstance(m, dict) and "id" in m)
        if provider == "azure":
            raise AIError("Azure OpenAI lists deployments only in the Azure portal - type your deployment name.")
    except AIError:
        raise
    except Exception as e:  # noqa: BLE001 - shown to the admin as the test result
        raise AIError(_friendly(e)) from e
    return []


def _friendly(e: Exception) -> str:
    text = str(e)
    if any(s in text for s in ("401", "403", "invalid_api_key", "API key not valid", "authentication", "Unauthorized")):
        return "The API key was rejected - check it and paste it again."
    return f"{type(e).__name__}: {text[:300]}"


# ---------- the tool-using loop, per provider ----------

async def run_agent(provider: str, model: str, system: str, user_text: str, tools: list[dict], call_tool) -> dict:
    """tools: [{name, description, parameters (JSON Schema)}]; call_tool(name, args) -> str.
    Returns {"text", "usage": {"input_tokens", "output_tokens"}, "refused": bool}."""
    if provider not in PROVIDERS:
        raise AIError(f"Unknown AI provider '{provider}'.")
    if not model:
        raise AIError(f"Choose a model for {PROVIDERS[provider]['label']} (AI providers → Load models).")
    key, cfg = await credentials(provider)
    if not key:
        raise AIError(f"No API key for {PROVIDERS[provider]['label']} - an administrator can paste one under "
                      "AI Agent → AI providers.")
    try:
        if provider == "anthropic":
            return await _anthropic(key, model, system, user_text, tools, call_tool)
        if provider == "gemini":
            return await _gemini(key, model, system, user_text, tools, call_tool)
        return await _openai_style(provider, key, cfg, model, system, user_text, tools, call_tool)
    except AIError:
        raise
    except Exception as e:  # noqa: BLE001
        log.exception("%s agent call failed", provider)
        raise AIError(_friendly(e)) from e


async def _anthropic(key, model, system, user_text, tools, call_tool):
    import anthropic

    client = anthropic.AsyncAnthropic(api_key=key)
    kwargs = {"model": model, "max_tokens": 16000, "system": system,
              "tools": [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]}
                        for t in tools]}
    if model.startswith(_CLAUDE_EFFORT):
        kwargs["output_config"] = {"effort": "medium"}
    if model in _CLAUDE_FALLBACKS:
        kwargs.update(betas=["server-side-fallback-2026-07-01"], fallbacks="default")
        create = client.beta.messages.create
    else:
        create = client.messages.create
    messages = [{"role": "user", "content": user_text}]
    usage = {"input_tokens": 0, "output_tokens": 0}
    resp = None
    for _ in range(MAX_TURNS):
        resp = await create(messages=messages, **kwargs)
        usage["input_tokens"] += resp.usage.input_tokens
        usage["output_tokens"] += resp.usage.output_tokens
        if resp.stop_reason == "refusal":
            return {"text": "The model declined this request.", "usage": usage, "refused": True}
        calls = [b for b in resp.content if b.type == "tool_use"]
        if resp.stop_reason != "tool_use" or not calls:
            break
        messages.append({"role": "assistant", "content": resp.content})  # unchanged, keeps thinking blocks valid
        results = []
        for b in calls:
            results.append({"type": "tool_result", "tool_use_id": b.id, "content": await call_tool(b.name, b.input)})
        messages.append({"role": "user", "content": results})  # all results in one message
    text = "".join(b.text for b in (resp.content if resp else []) if b.type == "text").strip()
    return {"text": text, "usage": usage, "refused": False}


async def _openai_style(provider, key, cfg, model, system, user_text, tools, call_tool):
    import openai

    if provider == "azure":
        client = openai.AsyncAzureOpenAI(api_key=key, azure_endpoint=cfg.get("endpoint") or "",
                                         api_version=cfg.get("api_version") or "2024-10-21")
    elif provider == "github":
        client = openai.AsyncOpenAI(api_key=key, base_url=cfg.get("base_url") or "https://models.github.ai/inference")
    else:
        client = openai.AsyncOpenAI(api_key=key)
    specs = [{"type": "function", "function": {"name": t["name"], "description": t["description"],
                                               "parameters": t["parameters"]}} for t in tools]
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user_text}]
    usage = {"input_tokens": 0, "output_tokens": 0}
    msg = None
    for _ in range(MAX_TURNS):
        resp = await client.chat.completions.create(model=model, messages=messages, tools=specs)
        if resp.usage:
            usage["input_tokens"] += resp.usage.prompt_tokens or 0
            usage["output_tokens"] += resp.usage.completion_tokens or 0
        msg = resp.choices[0].message
        if getattr(msg, "refusal", None):
            return {"text": msg.refusal, "usage": usage, "refused": True}
        if not msg.tool_calls:
            break
        messages.append({"role": "assistant", "content": msg.content,
                         "tool_calls": [{"id": tc.id, "type": "function",
                                         "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                                        for tc in msg.tool_calls]})
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                out = "Error: the tool arguments weren't valid JSON."
            else:
                out = await call_tool(tc.function.name, args)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": out})
    return {"text": (msg.content or "").strip() if msg else "", "usage": usage, "refused": False}


async def _gemini(key, model, system, user_text, tools, call_tool):
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=key)
    config = types.GenerateContentConfig(
        system_instruction=system,
        tools=[types.Tool(function_declarations=[
            types.FunctionDeclaration(name=t["name"], description=t["description"],
                                      parameters_json_schema=t["parameters"]) for t in tools])],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True))
    contents = [types.Content(role="user", parts=[types.Part(text=user_text)])]
    usage = {"input_tokens": 0, "output_tokens": 0}
    resp = None
    for _ in range(MAX_TURNS):
        resp = await client.aio.models.generate_content(model=model, contents=contents, config=config)
        if resp.usage_metadata:
            usage["input_tokens"] += resp.usage_metadata.prompt_token_count or 0
            usage["output_tokens"] += resp.usage_metadata.candidates_token_count or 0
        calls = resp.function_calls or []
        if not calls:
            break
        contents.append(resp.candidates[0].content)
        parts = [types.Part.from_function_response(name=fc.name, response={"result": await call_tool(fc.name, dict(fc.args or {}))})
                 for fc in calls]
        contents.append(types.Content(role="user", parts=parts))
    text = ""
    if resp is not None:
        try:
            text = (resp.text or "").strip()
        except ValueError:
            text = ""
    return {"text": text, "usage": usage, "refused": False}
