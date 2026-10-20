"""A partner's HTTP endpoint: payload in -> Kafka -> transformer -> result back to the caller."""
import json

import httpx
import pytest

from .conftest import wait_for

pytestmark = pytest.mark.integration

ORDER = {"order": {"id": "A-100", "total": 42.5, "customer": {"name": "Asha"}}}
MAPPING = "{order_id: order.id, total: order.total, customer: upper(order.customer.name)}"


def make_partner(admin, name, output_format="json", queue_mode="shared", expression=MAPPING):
    r = admin.post("/api/partners", json={"name": name, "input_format": "json", "output_format": output_format,
                                          "expression": expression, "queue_mode": queue_mode,
                                          "sample_input": json.dumps(ORDER)})
    assert r.status_code == 201, r.text
    code = r.json()["code"]
    return {**admin.get(f"/api/partners/{code}").json(), "api_key": admin.get(f"/api/partners/{code}/api-key").json()["api_key"]}


def call(base_url, partner, body=ORDER, key=None):
    return httpx.post(f"{base_url}/inbound/{partner['code']}", content=json.dumps(body).encode(), timeout=60,
                      headers={"X-API-Key": key or partner["api_key"], "Content-Type": "application/json"})


@pytest.fixture(scope="module")
def partner(admin):
    return make_partner(admin, "Flow Test Retail")


def test_transformed_payload_comes_back(base_url, admin, partner):
    r = call(base_url, partner)
    assert r.status_code == 200, r.text
    assert r.json() == {"order_id": "A-100", "total": 42.5, "customer": "ASHA"}
    txn = wait_for(lambda: admin.get(f"/api/transactions/{r.headers['X-Transaction-Id']}").json(),
                   lambda t: t["status"] == "COMPLETED")
    assert txn["status"] == "COMPLETED" and txn["partner_code"] == partner["code"]


def test_wrong_api_key_is_rejected(base_url, partner):
    assert call(base_url, partner, key="wrong").status_code == 401


def test_bad_payload_fails_and_is_logged(base_url, admin, partner):
    r = httpx.post(f"{base_url}/inbound/{partner['code']}", content=b"{not json", timeout=60,
                   headers={"X-API-Key": partner["api_key"], "Content-Type": "application/json"})
    assert r.status_code == 422
    failed = wait_for(lambda: admin.get("/api/transactions", params={"status": "FAILED",
                                                                      "partner": partner["code"]}).json(), bool)
    assert failed and "parse" in (failed[0].get("error") or "").lower()


def test_xml_output_on_dedicated_queues(base_url, admin):
    p = make_partner(admin, "Flow Test Logistics", output_format="xml", queue_mode="dedicated")
    assert p["topic_in"].endswith(f"partner.{p['code']}.in")
    r = call(base_url, p)
    assert r.status_code == 200, r.text
    assert "application/xml" in r.headers["content-type"] and "<order_id>A-100</order_id>" in r.text
