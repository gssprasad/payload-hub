"""Mapped request query parameters and weather response translation."""
import json
from urllib.parse import parse_qs, urlsplit

import pytest

from app.transform import TransformError, convert_response, query_target, transform


def test_weather_request_and_response():
    mapped = transform(json.dumps({'location': {'latitude': 41.88, 'longitude': -87.63}}), 'json',
                       "{latitude: location.latitude, longitude: location.longitude, "
                       "current: 'temperature_2m,precipitation,wind_speed_10m'}", 'json')
    target = query_target('/v1/forecast?timezone=auto', mapped, 'json')
    assert parse_qs(urlsplit(target).query) == {
        'latitude': ['41.88'], 'longitude': ['-87.63'], 'timezone': ['auto'],
        'current': ['temperature_2m,precipitation,wind_speed_10m']}
    body, fmt = convert_response('{"current":{"temperature_2m":12.5,"precipitation":3.2,"wind_speed_10m":38}}',
                                'application/json',
                                "{carryUmbrella: current.precipitation > `0`, "
                                "wearJacket: current.temperature_2m < `15`, "
                                "recommendation: ((current.precipitation > `0` || "
                                "current.wind_speed_10m > `30`) && 'INDOOR_RECOMMENDED') || 'OUTDOOR_SUITABLE'}",
                                'json', 'json')
    assert fmt == 'json'
    assert json.loads(body) == {'carryUmbrella': True, 'wearJacket': True, 'recommendation': 'INDOOR_RECOMMENDED'}


def test_encoding_lists_nulls_and_overrides():
    target = query_target('/search?fixed=yes&q=old', json.dumps({
        'q': 'dogs & cats/雪', 'enabled': True, 'tag': ['a', 'b'], 'skip': None, 'zero': 0}), 'json')
    assert parse_qs(urlsplit(target).query) == {
        'fixed': ['yes'], 'q': ['dogs & cats/雪'], 'enabled': ['true'], 'tag': ['a', 'b'], 'zero': ['0']}


@pytest.mark.parametrize('payload,fmt', [('[]', 'json'), ('{"nested":{}}', 'json'),
                                           ('{"nested":[[]]}', 'json'), ('{}', 'xml')])
def test_invalid_query_mapping(payload, fmt):
    with pytest.raises(TransformError):
        query_target('/forecast', payload, fmt)


@pytest.mark.asyncio
async def test_forward_sends_query_without_body(monkeypatch):
    from app import adapters, connectors

    class WeatherHTTP(adapters.HTTP):
        async def request(self, target, body, headers, content_type, method=None):
            assert method == 'GET'
            assert body == b''
            assert parse_qs(urlsplit(target).query) == {'latitude': ['41.88']}
            return 200, 'application/json', '{"current":{"temperature_2m":12.5}}', 'https://weather.test' + target

    adapter = WeatherHTTP.__new__(WeatherHTTP)
    adapter.config = {}

    async def connection(*args, **kwargs):
        return {'name': 'Weather', 'type': 'http'}

    async def adapter_for(*args, **kwargs):
        return adapter

    events = []

    async def event(txn_id, name, detail):
        events.append(name)

    async def response(*args):
        pass

    monkeypatch.setattr(connectors.db, 'get_connection', connection)
    monkeypatch.setattr(connectors, 'adapter_for', adapter_for)
    monkeypatch.setattr(connectors.db, 'record_event', event)
    monkeypatch.setattr(connectors.db, 'set_response', response)
    result = await connectors.forward({
        'code': 'weather', 'reply_connection': 'weather', 'reply_path': '/forecast',
        'reply_method': 'GET', 'send_format': 'query', 'response_format': 'json',
        'response_expression': '{wearJacket: current.temperature_2m < `15`}'}, {
        'id': 'weather-test', 'input_format': 'json', 'output_format': 'json',
        'output_payload': '{"latitude":41.88}'})
    assert result['ok']
    assert json.loads(result['body']) == {'wearJacket': True}
    assert events == ['FORWARDED', 'RESPONDED']
