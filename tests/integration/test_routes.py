"""Source -> Target system -> Source routes: API, database (delivery receipt), scope of endpoints."""
import pytest

from .conftest import query
from .test_partner_flow import call, make_partner

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def partner(admin):
    return make_partner(admin, "Route Test Partner")


def route(admin, partner, **kw):
    return admin.put(f"/api/partners/{partner['code']}/reply", json={"reply_mode": "forward", **kw})


def test_api_answer_as_is(base_url, admin, partner, receiver):
    assert route(admin, partner, reply_connection=receiver, reply_path="orders", response_format="unchanged").status_code == 200
    r = call(base_url, partner)
    assert r.status_code == 200, r.text
    assert "application/json" in r.headers["content-type"]
    assert r.json()["status"] == "ACCEPTED" and r.headers["X-Status"] == "RESPONDED"


def test_api_answer_translated(base_url, admin, partner, receiver):
    assert route(admin, partner, reply_connection=receiver, reply_path="orders", response_format="xml",
                 response_expression="{confirmation: {ref: reference}}").status_code == 200
    r = call(base_url, partner)
    assert r.status_code == 200 and "<ref>TR-" in r.text


def test_database_target_returns_translated_receipt(base_url, admin, shop_db):
    p = make_partner(admin, "Route Test DB", expression="{order_id: order.id, total: order.total}")
    r = route(admin, p, reply_connection=shop_db["id"], reply_path="orders_in", response_format="json",
              response_expression="{ok: status, where: target}")
    assert r.status_code == 200, r.text
    r = call(base_url, p)
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": "DELIVERED", "where": "orders_in"}
    assert query(shop_db["path"], "SELECT order_id, total FROM orders_in") == [("A-100", 42.5)]


def test_database_target_refuses_csv(admin, partner, shop_db):
    r = route(admin, partner, reply_connection=shop_db["id"], reply_path="orders_in", send_format="csv")
    assert r.status_code == 422 and "CSV" in r.json()["detail"]


def test_unreachable_target_gives_502(base_url, admin):
    p = make_partner(admin, "Route Test Down")
    c = admin.post("/api/connections", json={"name": "Nowhere API", "type": "http",
                                             "config": {"base_url": "http://127.0.0.1:9", "retry_attempts": 1,
                                                        "timeout": 2}}).json()
    assert route(admin, p, reply_connection=c["id"]).status_code == 200
    r = call(base_url, p)
    assert r.status_code == 502 and "unreachable" in r.json()["error"]


def test_dedicated_endpoints_stay_with_their_partner(admin, partner, base_url):
    other = make_partner(admin, "Route Test Other")
    c = admin.post("/api/connections", json={"name": "Own API", "type": "http", "owner_partner": partner["code"],
                                             "config": {"base_url": f"{base_url}/test-receiver"}}).json()
    assert c["owner_partner"] == partner["code"]
    r = route(admin, other, reply_connection=c["id"])
    assert r.status_code == 422 and "belongs to" in r.json()["detail"]
    assert route(admin, partner, reply_connection=c["id"], reply_path="x").status_code == 200
    # once used by one partner it can be made common, and then be dedicated again
    assert admin.put(f"/api/connections/{c['id']}/owner", json={"owner_partner": None}).status_code == 200
    assert admin.put(f"/api/connections/{c['id']}/owner", json={"owner_partner": partner["code"]}).status_code == 200


def test_back_to_transformed_reply(base_url, admin, partner):
    assert admin.put(f"/api/partners/{partner['code']}/reply", json={"reply_mode": "transformed"}).status_code == 200
    assert call(base_url, partner).json()["order_id"] == "A-100"
