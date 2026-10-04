# TianGong reference data

The original Nebula LCA public release includes the TianGong 1.0 reference-data
foundation: `ILCD_Unit_Groups.xlsx`, `elementary_flows_sample.csv`,
`intermediate_flows_sample.csv`, and `tiangong_processes.zip`. These are existing
bundled assets, not newly introduced commercial inventories. The process archive
is retained byte-exact (SHA-256
`fe5acc26f8a693a6ac221cae284e29972bd366fbe82ca5b50008721469f06222`).

Source: [TianGong LCA Database](https://github.com/linancn/TianGong-LCA-Data).
The official database/resource repository publishes an MIT licence with
`Copyright (c) 2023 linancn`. A fixed source reference, predating retirement of
its download channel, is commit `1ee8a1827dace69700b64354adc53cc9660177ac`:

- [Database README](https://github.com/linancn/TianGong-LCA-Data/blob/1ee8a1827dace69700b64354adc53cc9660177ac/README.md)
- [MIT licence](https://github.com/linancn/TianGong-LCA-Data/blob/1ee8a1827dace69700b64354adc53cc9660177ac/LICENSE)

The complete copyright, permission and warranty notice is reproduced in
`LICENSE.TianGong`. Individual Process, Flow or Unit records do not need to repeat
that database-level notice. Their missing licence fields do not by themselves
exclude these historical TianGong assets from distribution.

The source reference above pins the licence evidence; it does not claim that the
current upstream `release.json` version or an upstream archive byte hash has been
matched to this historical TianGong 1.0 conversion. Local CSV/XLSX conversions
and the ILCD/TIDAS reference catalog and seed retain their original source/version
descriptions. JRC/ILCD reference records and EF 3.1 method data retain their
respective attribution and conditions; this notice does not relicense them.
New platform exports retain their exact version and source declarations.

No commercial ecoinvent inventories, characterization-factor tables or licensed
runtime vectors are covered by this notice. Users import their authorized data
separately. Redistribution of these TianGong references does not certify the
scientific quality or completeness of the source inventories.
