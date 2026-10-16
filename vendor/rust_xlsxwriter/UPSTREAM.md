# rust_xlsxwriter ZIP compatibility patch

- Upstream crate: rust_xlsxwriter 0.99.1
- Repository: https://github.com/jmcnamara/rust_xlsxwriter
- Registry archive SHA-256: 34f2d2cda7223168a14fb86bc7411dbc3379bd7d49749ecc53b91ed98585b6b0
- Upstream library source and license files are unchanged.
- Cargo manifests select zip 9.0.3 instead of ^8.3 because the 8.x releases
  are yanked. The service also selects zip 9.0.3, avoiding a yanked duplicate.
- Remove this metadata-only patch when an upstream release accepts zip 9.
- Validation: report/XLSX roundtrip tests, all-feature workspace CI and
  cargo audit --deny warnings; no advisory suppression.
