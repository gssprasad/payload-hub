"""Export, check and deploy Payload Hub configuration packages from the command line (or a CI pipeline).

    # export from DEV: everything, or chosen assets (dependencies are added)
    python scripts/deploy_package.py export https://hub-dev.company.local --all -o deploy/packages/release-7.json
    python scripts/deploy_package.py export URL --partners acme-retail,globex --workflows order-invoice -o pkg.json

    # check what deploying it to TEST would do (changes nothing; exit code 2 if it can't be deployed)
    python scripts/deploy_package.py plan  https://hub-test.company.local pkg.json --overrides deploy/environments/test.json

    # deploy it (takes a backup first)
    python scripts/deploy_package.py apply https://hub-test.company.local pkg.json --overrides deploy/environments/test.json

Sign-in: PAYLOAD_HUB_USER (default Administrator) and PAYLOAD_HUB_PASSWORD - from the environment, so passwords
don't end up in shell history or CI logs.
"""
import argparse
import json
import os
import sys

import httpx

TYPES = ("partners", "connections", "workflows", "schedules", "ports", "alert_channels", "alert_rules", "secrets")


def client(url: str) -> httpx.Client:
    password = os.getenv("PAYLOAD_HUB_PASSWORD")
    if not password:
        sys.exit("Set PAYLOAD_HUB_PASSWORD (and PAYLOAD_HUB_USER if it isn't Administrator).")
    c = httpx.Client(base_url=url.rstrip("/"), timeout=600)
    r = c.post("/api/auth/login", json={"username": os.getenv("PAYLOAD_HUB_USER", "Administrator"),
                                         "password": password})
    if r.status_code != 200:
        sys.exit(f"Sign-in to {url} failed: {r.text}")
    c.headers["Authorization"] = "Bearer " + r.json()["token"]
    return c


def show_plan(plan: dict):
    print(f"Package {plan['package'].get('name')} from {plan['package'].get('source_environment')} "
          f"-> {plan['target_environment']}")
    for i in plan["items"]:
        changes = f" ({', '.join(i['changes'])})" if i["changes"] else ""
        print(f"  {i['action']:9} {i['label']:20} {i['id']}{changes}")
        for p in i["problems"]:
            print(f"            ! {p}")
    c = plan["counts"]
    print(f"{c['create']} to create, {c['update']} to update, {c['unchanged']} unchanged"
          + (f", {c['missing']} missing secret(s)" if c["missing"] else ""))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["export", "plan", "apply"])
    ap.add_argument("url")
    ap.add_argument("package", nargs="?", help="package file (plan / apply)")
    ap.add_argument("-o", "--output", help="export: where to write the package")
    ap.add_argument("--name", help="export: package name (default: the file name)")
    ap.add_argument("--all", action="store_true", help="export: every asset")
    ap.add_argument("--overrides", help="plan / apply: the target environment's overrides file")
    ap.add_argument("--no-backup", action="store_true", help="apply: skip the backup before deploying")
    for t in TYPES:
        ap.add_argument(f"--{t.replace('_', '-')}", help=f"export: {t} to include (comma-separated ids)")
    args = ap.parse_args()
    c = client(args.url)

    if args.command == "export":
        if args.all:
            inv = c.get("/api/deploy/inventory").raise_for_status().json()["assets"]
            selection = {t: [a["id"] for a in inv.get(t, [])] for t in TYPES}
        else:
            selection = {t: [x for x in (getattr(args, t) or "").split(",") if x] for t in TYPES}
        if not any(selection.values()):
            sys.exit("Choose what to export (--all, or e.g. --partners acme-retail).")
        name = args.name or (os.path.splitext(os.path.basename(args.output))[0] if args.output else "package")
        r = c.post("/api/deploy/export", json={"name": name, "selection": selection})
        if r.status_code != 200:
            sys.exit(f"Export failed: {r.json().get('detail', r.text)}")
        text = json.dumps(r.json(), indent=2, ensure_ascii=False) + "\n"
        (open(args.output, "w").write(text) if args.output else sys.stdout.write(text))
        counts = {k: len(v) for k, v in r.json()["assets"].items()}
        print(f"exported {counts}" + (f" to {args.output}" if args.output else ""), file=sys.stderr)
        return 0

    if not args.package:
        sys.exit("Give the package file.")
    body = {"package": json.load(open(args.package)),
            "overrides": json.load(open(args.overrides)) if args.overrides else None,
            "take_backup": not args.no_backup}
    plan = c.post("/api/deploy/plan", json=body)
    if plan.status_code != 200:
        sys.exit(f"Check failed: {plan.json().get('detail', plan.text)}")
    show_plan(plan.json())
    if not plan.json()["can_apply"]:
        print("Can't be deployed yet - fix the problems above.")
        return 2
    if args.command == "plan":
        return 0
    r = c.post("/api/deploy/apply", json=body)
    if r.status_code != 200:
        sys.exit(f"Deployment failed: {r.json().get('detail', r.text)}")
    result = r.json()
    print(f"{result['status']} - deployment #{result['deployment']}, backup {result['backup']}"
          + (f" - error: {result['error']}" if result["error"] else ""))
    return 0 if result["status"] == "DEPLOYED" else 1


if __name__ == "__main__":
    sys.exit(main())
