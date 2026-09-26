# Make edits prepared 2026-09-27 (plan steps 2, 8, 12)

Built from the live blueprints read on 2026-09-26 (identical to `../baseline-2026-09-26/`),
which remain the rollback points. Each file is a complete scenario blueprint: import it in
the scenario editor (three dots > Import Blueprint) and save, or PATCH `/scenarios/{id}`
with a token that carries `scenarios:write`.

| File | Scenario | Change |
|---|---|---|
| `6893541_...retry` | Salla \| Order Delivery Status (GCP) | Retry handler (10 tries, 15 min) on the sheet append; store incomplete executions ON; sequential OFF (a stored run must not pause the capture) |
| `6892982_...retry` | Salla \| Customer Created Capture (GCP) | Same retry handler and storage; the dead placeholder module removed. The column H template is unchanged here (step 8 waits on the escape test) |
| `5563154_...conflict_route` | Salla \| Customer Updated | On the create: Sleep 1 s, parse the holder id from the unique-phone 400, update that contact with the event's fields (consent included), otherwise retry 10 x 15 min. On the update: retry 10 x 15 min instead of stopping |

**Applied 2026-09-27 18:30 UTC through `PATCH /scenarios/{id}`** (a new Make token with write
scopes on the VM), each read back equal to the file here, all three scenarios active, settings
(`dlq`, `sequential`) taken from the blueprint metadata. The 8 stored incomplete executions on
`5563154` were deleted first (every holder verified to carry Salla id, phone and name; the
rejected contact ids no longer existed). Step 8 (a JSON-escaped column H template on `6892982`)
is NOT applied: Make strings take no backslash escapes, a quote cannot appear inside `{{ }}`,
and regex escapes are stripped; a candidate built on `decodeURL("%22")` / `decodeURL("%5C")`
with split/join still needs a one-operation check in the editor. The engine's salvage reader
covers the gap (no held customer rows since 26 Sep).
