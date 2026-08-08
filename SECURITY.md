# Security policy

## Supported versions

The current default branch is supported. Older snapshots and unmerged branches are not supported.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability and do not include Ozon, Wildberries,
SonarQube, Tunnel, or other credentials in reports, screenshots, logs, commits, or pull requests.

Use GitHub private vulnerability reporting for the repository. If that feature is unavailable,
contact the repository owner through an approved private corporate channel. Include affected
versions, impact, reproduction steps with sanitized data, and a suggested remediation when known.

Revoke and rotate any credential that may have been exposed before continuing the investigation.
Production credentials must never be used in CI; tests use local mocks only.
