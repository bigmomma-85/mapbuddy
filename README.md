# StormKML — Asset → KML (Vercel Node Functions)

- `/api/convert` (POST JSON `{ assetId, dataset }`) → returns a KML download
- `/api/hello` (GET) → quick check that functions are routing

## Deploy
1) Push to GitHub.
2) Import on Vercel → Framework: **Other**, no build command, no output dir.
3) Visit `/api/hello` to confirm JSON.
4) Test convert:
```bash
curl -X POST https://YOUR-APP.vercel.app/api/convert   -H "Content-Type: application/json"   --data '{"assetId":"1373DP","dataset":"fairfax_bmps"}'   -o 1373DP.kml
```
