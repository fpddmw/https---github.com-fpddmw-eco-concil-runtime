# USGS Water IV API Notes

- Official service documentation:
  - `https://waterservices.usgs.gov/docs/instantaneous-values/instantaneous-values-details/`
- Water Services migration notice:
  - `https://waterdata.usgs.gov/blog/api-waterservices-decom/`

- Endpoint:
  - `https://waterservices.usgs.gov/nwis/iv/`
- Recommended eco-council query style:
  - `format=json`
  - one major filter:
    - `bBox=minLon,minLat,maxLon,maxLat`
    - or `sites=site1,site2,...`
  - one time selector:
    - `period=P1D`
    - or `startDT=...` with `endDT=...`
  - hydrology parameters:
    - `parameterCd=00060` for discharge
    - `parameterCd=00065` for gage height
  - site narrowing:
    - `siteType=ST`
    - `siteStatus=active`

- The JSON response is WaterML-style JSON.
- Relevant nested fields:
  - `value.queryInfo`
  - `value.timeSeries[]`
  - `timeSeries[].sourceInfo`
  - `timeSeries[].variable`
  - `timeSeries[].values[].value[]`

- Useful site-property fields commonly present in `sourceInfo.siteProperty[]`:
  - `siteTypeCd`
  - `hucCd`
  - `stateCd`
  - `countyCd`

- Useful variable codes for the first eco-council integration:
  - `00060` -> discharge / streamflow
  - `00065` -> gage height

## Agent-facing Caveats

- Every IV query needs one major filter such as `sites` or `bBox`; do not mix major filters in one request.
- Parameter codes are five-character numeric codes, and not every parameter is served at every site.
- Returned IV records may include provisional or operational data; use them as evidence inputs with provenance, not final regulatory adjudication.
