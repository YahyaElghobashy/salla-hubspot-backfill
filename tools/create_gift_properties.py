#!/usr/bin/env python3
"""Create the gift-order properties on the Orders object. Idempotent: a 409
means the property already exists and is left exactly as it is.

Run:  set -a && . "<Order Backfill>/.env" && set +a && python3 tools/create_gift_properties.py
"""
import json, os, sys, urllib.request

TOKEN = os.environ["HUBSPOT_ACCESS_TOKEN"]
GROUP = "order_information"          # where the salla_* properties live

PROPS = [
    {"name": "is_gift_order", "label": "Is gift order", "type": "bool",
     "fieldType": "booleancheckbox",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}],
     "description": "Set by the sync engine when Salla marks the order type gift / buy_as_gift. The customer on the order is the BUYER; the receiver lives in the gift_* properties."},
    {"name": "gift_receiver_name", "label": "Gift receiver name", "type": "string", "fieldType": "text",
     "description": "Name of the person receiving the gift, exactly as the buyer entered it."},
    {"name": "gift_receiver_phone", "label": "Gift receiver phone", "type": "string", "fieldType": "text",
     "description": "Receiver's phone in +E.164 where possible. The receiver is deliberately NOT a contact; WhatsApp/SMS workflows read this property."},
    {"name": "gift_receiver_email", "label": "Gift receiver email", "type": "string", "fieldType": "text",
     "description": "Receiver's email if the buyer provided one (usually empty)."},
    {"name": "gift_message", "label": "Gift message", "type": "string", "fieldType": "textarea",
     "description": "The gift card message the buyer wrote."},
    {"name": "gift_card_image_url", "label": "Gift card image URL", "type": "string", "fieldType": "text",
     "description": "The gift card image the buyer picked, hosted on Salla's CDN."},
    {"name": "gift_confirmation_url", "label": "Gift confirmation URL", "type": "string", "fieldType": "text",
     "description": "Link the receiver opens to confirm their delivery address. Support can resend this."},
    {"name": "gift_confirmation_expiry", "label": "Gift confirmation expires", "type": "date", "fieldType": "date",
     "description": "When the address-confirmation link expires. Drives the nudge workflow."},
    {"name": "gift_deliver_at", "label": "Gift scheduled delivery", "type": "date", "fieldType": "date",
     "description": "Buyer-chosen future delivery date, when scheduled."},
    {"name": "gift_receiver_salla_notified", "label": "Receiver notified by Salla", "type": "bool",
     "fieldType": "booleancheckbox",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}],
     "description": "Whether Salla itself notifies the receiver. When false, OUR WhatsApp/SMS workflow is the only notification they get."},
    {"name": "gift_address_incomplete", "label": "Gift address incomplete", "type": "bool",
     "fieldType": "booleancheckbox",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}],
     "description": "True when the receiver had not yet confirmed their address at sync time. Drives the confirm-your-address nudge."},
]

def main():
    ok = exists = failed = 0
    for p in PROPS:
        p["groupName"] = GROUP
        req = urllib.request.Request(
            "https://api.hubapi.com/crm/v3/properties/orders",
            data=json.dumps(p).encode(),
            headers={"Authorization": "Bearer " + TOKEN,
                     "Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60):
                ok += 1; print("created %-32s" % p["name"])
        except urllib.error.HTTPError as e:
            if e.code == 409:
                exists += 1; print("exists  %-32s" % p["name"])
            else:
                failed += 1; print("FAILED  %-32s %s %s" % (p["name"], e.code, e.read()[:150]))
    print("\ncreated=%d existing=%d failed=%d" % (ok, exists, failed))
    if failed: sys.exit(1)

if __name__ == "__main__":
    main()
