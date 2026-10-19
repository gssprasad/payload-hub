# Test pack: two example partners

| Partner | Direction | Queue | Mapping spec |
|---------|-----------|-------|--------------|
| **Zenith Pharma** | Purchase Order **XML → JSON** Sales Order | Dedicated | `zenith-pharma/zenith-pharma-mapping-spec.xlsx` |
| **Nova Retail** | Customer Order **JSON → XML** Shipment Request | Shared | `nova-retail/nova-retail-mapping-spec.xlsx` |

Each folder has:

- `*-mapping-spec.xlsx`: the mapping specification (Interface Info, Field Mapping, Sample Input,
  Expected Output, Test Cases)
- `sample-input.*`: the payload to send
- `expected-output.*`: what the mapping must produce
- `mapping-expression.txt`: a mapping that produces exactly the expected output (use it if
  you are testing without the AI agent)
- `negative-input.*`: a malformed payload, which should fail and go to the failure queue (DLQ)

## How to test (Streamlit app → Partners)

1. **Onboard**: *Onboard a new partner*. Use the name from the table, matching input/output
   formats and queue mode, and paste `sample-input.*` and `expected-output.*`.
2. **Upload the spec**: open the partner, go to *Mapping documents*, upload the `.xlsx` and click *Save document*.
3. **Create the mapping**, either
   - with the AI agent: *Mapping* → *Generate mapping* (needs an Anthropic API key), or
   - by hand: paste `mapping-expression.txt` into the mapping expression and click *Save mapping*.
4. **Check it**: *Test mapping on the sample input* should say *Output matches the expected output*.
5. **Run the test cases** in the spec's *Test Cases* sheet: send the sample and the negative
   input on *Send & Test*, then look them up on *Transactions* and *Monitoring*.
