"""Every page of the Streamlit app renders without an error, signed in as the administrator."""
import pytest
from streamlit.testing.v1 import AppTest

from .conftest import ADMIN

pytestmark = pytest.mark.integration

PAGES = ["overview", "partners", "connections", "ports", "workflows", "schedulers", "agent", "send", "transactions", "data", "monitoring",
         "users", "secrets", "alerts", "backups", "deployments"]

SCRIPT = """
import requests
import streamlit as st
from ui import common, {page}

common.API = "{base_url}"
if "auth_token" not in st.session_state:
    r = requests.post(common.API + "/api/auth/login", json={{"username": "{user}", "password": "{password}"}}).json()
    st.session_state.auth_token, st.session_state.auth_user = r["token"], r["user"]
{page}.page()
"""


@pytest.mark.parametrize("page", PAGES)
def test_page_renders(base_url, page, shop_db, receiver):  # fixtures: the pages have some data to show
    at = AppTest.from_string(SCRIPT.format(page=page, base_url=base_url, user=ADMIN[0], password=ADMIN[1]),
                             default_timeout=60)
    at.run()
    assert not at.exception, [e.value for e in at.exception]  # crashes; st.error messages are the app's own
