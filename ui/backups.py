"""Backups page (administrators): database backups, the backup schedule, schema version and how to restore."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api


def _when(v) -> str:
    return pd.to_datetime(v).tz_convert(common.TZ).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S") if v else "—"


def page():
    common.hero("💾 Backups", "Database backups, their schedule, and how to restore.")
    data = api("GET", "/api/backups")
    schema = api("GET", "/api/system/schema")
    tasks = [t for t in api("GET", "/api/schedules")["tasks"] if t["action"]["type"] == "backup"]
    backups = data["backups"]

    top = st.columns([3, 1], vertical_alignment="center")
    newest = backups[0] if backups else None
    top[0].caption(f"{len(backups)} backup(s) in {data['folder']} · keeping the newest {data['keep']} · "
                   f"schema {schema['current']}" + (f" · last {_when(newest.get('created_at'))}" if newest else ""))
    if top[1].button("💾 Back up now", type="primary", width="stretch"):
        with st.spinner("Backing up…"):
            try:
                m = api("POST", "/api/backups", timeout=3600)
            except ApiError as e:
                st.error(str(e))
            else:
                st.session_state["backups_selected"] = m["file"]
                st.rerun()

    if not tasks:
        c = st.columns([3, 1], vertical_alignment="center")
        c[0].warning("No backup is scheduled.")
        if c[1].button("⏰ Nightly at 02:00", width="stretch", help="Creates a scheduled task (Schedulers page)"):
            api("POST", "/api/schedules", json={"name": "Nightly database backup", "action": {"type": "backup"},
                                                "schedule": {"kind": "daily", "at": "02:00"}})
            st.rerun()
    else:
        t = tasks[0]
        st.caption(f"⏰ Scheduled: {t['schedule_text']} ({t['name']})" + ("" if t["enabled"] else " - ⏸️ paused")
                   + f" · next {_when(t['next_run_at'])}")

    if backups:
        name = common.pick_row([{"Backup": b["file"], "Taken": _when(b.get("created_at")),
                                 "Size": f"{b.get('size', 0) / 1e6:.1f} MB", "Schema": b.get("schema_version", "?"),
                                 "Key": str(b.get("key_fingerprint", "?"))[:8], "Note": b.get("note", "")}
                                for b in backups], "backups", [b["file"] for b in backups])
        b = next(x for x in backups if x["file"] == name)
        common.detail_title(name)
        if b.get("key_fingerprint") and b["key_fingerprint"] != data["key_fingerprint"]:
            st.warning("Taken with a different master key than the current one - restoring it needs that key.")
        a = st.columns([1, 1, 1, 2])
        if a[0].button("🔍 Verify", key=f"bk_verify_{name}", width="stretch"):
            r = api("POST", f"/api/backups/{name}/verify", timeout=600)
            (st.success if r["ok"] else st.error)(r["detail"])
        if a[1].button("⬇️ Download", key=f"bk_dl_{name}", width="stretch"):
            st.session_state["bk_download"] = (name, api("GET", f"/api/backups/{name}/download", timeout=600))
        if a[2].button("🗑️ Delete", key=f"bk_del_{name}", width="stretch"):
            api("DELETE", f"/api/backups/{name}")
            st.session_state.pop("backups_selected", None)
            st.rerun()
        if (dl := st.session_state.get("bk_download")) and dl[0] == name:
            st.download_button(f"⬇️ Save {name}", dl[1], file_name=name, mime="application/octet-stream")
    else:
        st.info("No backups yet.")

    with st.expander("🛟 How to restore"):
        st.markdown(f"""
1. **Stop Payload Hub** (the backend and the web console).
2. Make sure the **master key** matches the backup's *Key* column (current key `{data['key_fingerprint'][:8]}`) -
   put the right `data/secret.key` back, or set `PAYLOAD_HUB_SECRET_KEY`.
3. Check the file, then restore it over the database:
""")
        st.code("python -m app.backup verify  <backup.dump>\npython -m app.backup restore <backup.dump> --yes",
                language="bash")
        st.markdown("4. **Start Payload Hub.** It applies any newer migrations automatically.")
        st.markdown("**Practise it** without touching the live database - restore into a scratch database and look:")
        st.code("python -m app.backup restore <backup.dump> --into postgresql://localhost/restore_drill --yes",
                language="bash")
        st.caption("A backup holds the Payload Hub database only - not the master key, other databases you "
                   "connected, or files in polled folders. Copy backups and the key to separate, safe places.")
