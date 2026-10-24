# Weather request/reply example

Partner A sends JSON to `POST /inbound/<partner-code>` with its `X-API-Key` header:

```json
{"location":{"latitude":41.88,"longitude":-87.63},"requestType":"OUTDOOR_ACTIVITY_CHECK"}
```

On the partner's Mapping tab select JSON input and JSON output, and use:

```text
{latitude: location.latitude, longitude: location.longitude, current: 'temperature_2m,precipitation,wind_speed_10m'}
```

Create an HTTP connection with base URL `https://api.open-meteo.com`. On the
partner's Route tab choose **Send to the target system and reply with its answer**,
select that connection, method **GET**, path `/v1/forecast`, and **URL query
parameters (mapped JSON fields)**. Select JSON as the response format and use:

```text
{
  activityAssessment: {
    recommendation: ((current.precipitation > `0` || current.wind_speed_10m > `30`) && 'INDOOR_RECOMMENDED') || 'OUTDOOR_SUITABLE',
    carryUmbrella: current.precipitation > `0`,
    wearJacket: current.temperature_2m < `15`,
    windWarning: (current.wind_speed_10m > `30` && 'STRONG_WIND') || 'NORMAL',
    temperature: {value: current.temperature_2m, unit: 'CELSIUS'},
    assessedAt: current.time
  }
}
```

Save the route and send A's request. The mapped latitude, longitude and current
fields become URL query parameters; no JSON body is sent to B. B returns actual
weather observations, which the response mapping converts into A's assessment.
Inspect Transactions for the mapped output, response payload, FORWARDED and
RESPONDED events. Weather values and advice vary with the live response.

Query values are URL encoded. Nulls are omitted, booleans use true/false, and
lists create repeated parameters. Nested objects/lists are rejected. Mapped keys
replace matching keys in the path's existing query; other existing keys remain.
Use a JSON object as the request mapping output. Existing body/CSV routes are
unchanged. The existing text database column needs no schema migration.

Open-Meteo's public endpoint is free for noncommercial use within its limits:
https://open-meteo.com/en/terms
