"""Payload parsing, mapping, serialising, response conversion and CSV output."""
import json

import pytest

from app.transform import (SAMPLES, TransformError, apply_mapping, convert_response, parse, serialise, to_csv,
                           transform)

ORDER = {"order": {"id": "A1", "customer": {"firstName": "Asha", "lastName": "Rao", "email": "ASHA@EXAMPLE.COM"},
                   "items": [{"sku": "S1", "qty": 2, "price": 3.5}, {"sku": "S2", "qty": 1, "price": 10}]}}


def test_custom_functions():
    data = apply_mapping(ORDER, "{name: concat(order.customer.firstName, ' ', order.customer.lastName), "
                                "email: lower(order.customer.email), code: upper('abc'), "
                                "totals: order.items[].multiply(qty, price), n: to_float('2.5')}")
    assert data == {"name": "Asha Rao", "email": "asha@example.com", "code": "ABC", "totals": [7.0, 10.0], "n": 2.5}


def test_concat_treats_null_as_empty():
    assert apply_mapping({}, "concat('a', missing, 'b')") == "ab"


def test_json_to_xml_and_back():
    xml = transform(json.dumps(ORDER), "json", "{invoice: {id: order.id, lines: length(order.items)}}", "xml")
    assert "<invoice>" in xml and "<id>A1</id>" in xml
    assert parse(xml, "xml") == {"invoice": {"id": "A1", "lines": "2"}}


def test_xml_output_wraps_lists_and_multi_key_objects():
    xml = serialise([{"id": 1}, {"id": 2}], "xml")
    assert parse(xml, "xml") == {"result": {"item": [{"id": "1"}, {"id": "2"}]}}
    assert parse(serialise({"a": 1, "b": 2}, "xml"), "xml") == {"result": {"a": "1", "b": "2"}}


@pytest.mark.parametrize("payload, fmt", [("{not json", "json"), ("<a><b></a>", "xml")])
def test_bad_input_raises(payload, fmt):
    with pytest.raises(TransformError, match="Could not parse"):
        parse(payload, fmt)


def test_mapping_with_no_result_raises():
    with pytest.raises(TransformError, match="no output"):
        transform(json.dumps(ORDER), "json", "nothing.here", "json")


def test_invalid_expression_raises():
    with pytest.raises(TransformError, match="Mapping failed"):
        apply_mapping(ORDER, "order.[[")


@pytest.mark.parametrize("key", list(SAMPLES))
def test_built_in_samples_transform(key):
    s = SAMPLES[key]
    out = transform(s["input"], s["input_format"], s["expression"], s["output_format"])
    assert out.strip()


class TestConvertResponse:
    BODY = json.dumps({"reference": "R-1", "status": "OK"})

    def test_translated_into_callers_format(self):
        body, fmt = convert_response(self.BODY, "application/json", None, "input", "xml")
        assert fmt == "xml" and "<reference>R-1</reference>" in body

    def test_as_is_keeps_body(self):
        assert convert_response(self.BODY, "application/json", None, "unchanged", "xml") == (self.BODY, "unchanged")

    def test_mapping_reshapes_answer(self):
        body, fmt = convert_response(self.BODY, "application/json", "{ref: reference}", "json", "xml")
        assert fmt == "json" and json.loads(body) == {"ref": "R-1"}

    def test_plain_text_is_wrapped(self):
        body, _ = convert_response("accepted", "text/plain", None, "json", "json")
        assert json.loads(body) == {"response": "accepted"}

    def test_mapping_with_no_result_raises(self):
        with pytest.raises(TransformError):
            convert_response(self.BODY, "application/json", "missing", "json", "json")


class TestCsv:
    def test_list_of_objects(self):
        out = to_csv(json.dumps([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]), "json")
        assert out.splitlines() == ["id,name", "1,a", "2,b"]

    def test_nested_fields_become_dotted_columns(self):
        out = to_csv(json.dumps([{"id": 1, "customer": {"name": "Asha", "city": "Pune"}}]), "json")
        assert out.splitlines() == ["id,customer.name,customer.city", "1,Asha,Pune"]

    def test_single_list_inside_object_is_the_rows(self):
        out = to_csv(json.dumps({"batch": "B1", "orders": [{"id": 1}, {"id": 2}]}), "json")
        assert out.splitlines() == ["id", "1", "2"]

    def test_xml_root_is_unwrapped(self):
        out = to_csv("<invoice><id>7</id><total>9.5</total></invoice>", "xml")
        assert out.splitlines() == ["id,total", "7,9.5"]

    def test_separator_and_missing_columns(self):
        out = to_csv(json.dumps([{"a": 1}, {"b": 2}]), "json", ";")
        assert out.splitlines() == ["a;b", "1;", ";2"]
