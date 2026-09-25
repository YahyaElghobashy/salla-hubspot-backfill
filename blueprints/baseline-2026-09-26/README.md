# Make blueprints, baseline 2026-09-26

Exported read-only through the Make API before the capture-hardening work, so any Make
edit in that work can be rolled back by re-importing the matching file.

| Scenario id | Name | Writes | Store incomplete | Sequential |
|---|---|---|---|---|
| 6892982 | Salla / Customer Created Capture (GCP) | Customer Queue tab, A:H | off | off |
| 6893541 | Salla / Order Delivery Status (GCP) | Status Queue tab, A:H | off | on |
| 6568689 | Salla / Local Live Relay | Live Queue tab (intake) | on | on |
| 5563154 | Salla / Customer Updated | HubSpot contacts directly | on | off |
| 5780791 | Salla / Watch Abandoned Cart | HubSpot contacts directly | on | on |

Checked before committing: no secret values from the engine environment appear in these
files.
