## Retention Decisions

| Dataset | Retention | Rationale |
|---|---|---|
| bronze.trips | Indefinite | Public data, no PII |
| silver.silver_trips | Indefinite | Public data, no PII |
| gold.fact_trips | Indefinite | Public data, no PII |

## Future Trigger for Review

If this pipeline is extended to include any of the following,
retention policy must be revisited and TTL enforced immediately:

- User identity or demographic data
- Payment or subscription data
- Location data linked to identifiable individuals
- Any data subject to GDPR, CCPA, or equivalent regulation
