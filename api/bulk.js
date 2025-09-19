// api/bulk.js
// Bulk: fetch many ArcGIS features by ID and return a zip containing
// one file per asset (KML / GeoJSON / Shapefile), plus mapbuddy_links.csv

import axios from "axios";
import AdmZip from "adm-zip";

// IMPORTANT: Node runtime (NOT Edge)
export const config = { runtime: "nodejs" };

// ---------- helpers ----------

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  if (!chunks.length) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8")); }
  catch { return {}; }
}

// mapshaper dynamic loader (works for both ESM/CJS shapes)
async function getApply() {
  const ms = await import("mapshaper");
  const apply = ms.applyCommands || (ms.default && ms.default.applyCommands) || null;
  if (!apply) throw new Error("Mapshaper API not available (applyCommands missing)");
  return apply;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const isOk = (v) => v !== undefined && v !== null && v !== "";
const sanitizeName = (s) => String(s).replace(/[^\w\-]+/g, "_");

// super-light centroid to build a Google Maps link
function centroidOf(feature) {
  const g = feature?.geometry;
  if (!g) return null;
  const type = g.type;

  function avg(coords) {
    let sx = 0, sy = 0, n = 0;
    for (const [x, y] of coords) { sx += x; sy += y; n++; }
    return n ? [sx / n, sy / n] : null;
  }

  if (type === "Point") return { lng: g.coordinates[0], lat: g.coordinates[1] };

  if (type === "MultiPoint") {
    const c = avg(g.coordinates);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "LineString") {
    const c = avg(g.coordinates);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "MultiLineString") {
    const flat = g.coordinates.flat();
    const c = avg(flat);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "Polygon") {
    const ring = g.coordinates?.[0] || [];
    const c = avg(ring);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "MultiPolygon") {
    const ring = g.coordinates?.[0]?.[0] || [];
    const c = avg(ring);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  return null;
}

function mapsUrl(lat, lng) {
  return `https://www.google.com/maps/search/?api=1&query=${lat},${lng}`;
}

// ---------- datasets ----------
//
// Key is what you send in "dataset" or select as defaultDataset.
// You may also pass the human label (the UI text) — we map both.

const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities
  fairfax_bmps: {
    label: "Fairfax — Stormwater Facilities (FACILITY_ID)",
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },

  // MDOT SHA Managed Landscape
  mdsha_landscape: {
    label: "MDOT SHA — Managed Landscape (LOD_ID)",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },

  // TMDL (MD SHA)
  "MDOT SHA — TMDL (Auto-detect)": { alias: true }, // UI group label
  "TMDL — Stormwater Control Structures": {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },
  "TMDL — Retrofits": {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },
  "TMDL — Tree Plantings": {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Pavement Removals": {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Stream Restorations": {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Outfall Stabilizations": {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
};

// allow referencing by label or by key
function resolveDatasetKey(input) {
  if (DATASETS[input] && !DATASETS[input].alias) return input;
  // match by label
  for (const [k, v] of Object.entries(DATASETS)) {
    if (!v.alias && (v.label === input)) return k;
  }
  return null;
}

// ---------- ArcGIS fetch ----------

async function fetchGeoJSONFeature(datasetKey, assetId) {
  const def = DATASETS[datasetKey];
  if (!def) return null;

  // try f=geojson first, then fallback to f=json
  for (const fld of def.idFields || []) {
    // f=geojson attempt
    try {
      const p = new URLSearchParams({
        where: `${fld}='${assetId}'`,
        outFields: "*",
        returnGeometry: "true",
        outSR: "4326",
        f: "geojson",
      });
      const url = `${def.base}/query?${p.toString()}`;
      const r = await axios.get(url, { timeout: 15000 });
      const feats = r.data?.features || [];
      if (feats.length) return feats[0];
    } catch { /* fall through */ }

    // fallback f=json -> not usually needed for Fairfax/TMDL, but safe
    try {
      const p = new URLSearchParams({
        where: `${fld}='${assetId}'`,
        outFields: "*",
        returnGeometry: "true",
        outSR: "4326",
        f: "json",
      });
      const url = `${def.base}/query?${p.toString()}`;
      const r = await axios.get(url, { timeout: 15000 });
      const feats = r.data?.features || [];
      if (feats.length) {
        // lightweight conversion
        const f = feats[0];
        const geom = f.geometry;
        let gj = null;

        if (geom?.x !== undefined && geom?.y !== undefined) {
          gj = { type: "Point", coordinates: [geom.x, geom.y] };
        } else if (geom?.rings) {
          gj = { type: "Polygon", coordinates: geom.rings };
        } else if (geom?.paths) {
          gj = { type: "LineString", coordinates: geom.paths[0] };
        }

        if (gj) {
          return {
            type: "Feature",
            properties: f.attributes || {},
            geometry: gj,
          };
        }
      }
    } catch { /* continue */ }

    // be nice to the server if we loop many fields
    await sleep(80);
  }

  return null;
}

// ---------- converters ----------

async function featureToKmlBuffer(feature) {
  const apply = await getApply();
  const files = { "in.json": JSON.stringify(feature) };
  const out = await apply("-i in.json -o out.kml format=kml", files);
  return Buffer.from(out["out.kml"]);
}

async function featureToShapefileParts(feature, outBase) {
  const apply = await getApply();
  const files = { "in.json": JSON.stringify(feature) };
  const out = await apply("-i in.json -o out.shp format=shapefile", files);
  // rename "out.*" to "<outBase>.*"
  const parts = [];
  for (const [name, content] of Object.entries(out)) {
    const renamed = name.replace(/^out(\.|$)/, `${outBase}$1`);
    parts.push([renamed, Buffer.from(content)]);
  }
  return parts;
}

// ---------- main handler ----------

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Use POST with JSON body { items, defaultDataset, format }" });
    return;
  }

  const body = await readJsonBody(req);
  const items = Array.isArray(body.items) ? body.items : [];
  let defDataset = body.defaultDataset;
  const format = String(body.format || "").toLowerCase(); // kmlzip | geojsonzip | shpzip | kml | geojson

  if (!items.length) {
    res.status(400).json({ error: "No items provided" });
    return;
  }

  // resolve default dataset label or key to our key
  if (defDataset) {
    defDataset = resolveDatasetKey(defDataset) || defDataset; // keep if already a key
  }

  const zip = new AdmZip();
  const linkRows = [["assetId", "dataset", "lat", "lng", "googleMapsUrl", "status"]];
  let successCount = 0;

  for (const it of items) {
    const assetId = String(it.assetId || "").trim();
    if (!assetId) continue;

    // choose dataset: row.dataset > defaultDataset
    let datasetKey = it.dataset ? (resolveDatasetKey(it.dataset) || it.dataset) : defDataset;
    // if row-specific dataset missing and default missing, try a very light auto-detect:
    if (!datasetKey) {
      // heuristic: "UT" looks like TMDL tree/retrofit IDs; otherwise assume Fairfax
      datasetKey = assetId.endsWith("UT")
        ? "TMDL — Tree Plantings"
        : "fairfax_bmps";
    }

    const ds = DATASETS[datasetKey];
    if (!ds || ds.alias) {
      linkRows.push([assetId, datasetKey || "", "", "", "", "unknown dataset"]);
      continue;
    }

    let feature = null;
    try {
      feature = await fetchGeoJSONFeature(datasetKey, assetId);
    } catch (e) {
      // swallow, will be handled below
    }

    if (!feature) {
      linkRows.push([assetId, datasetKey, "", "", "", "not found"]);
      continue;
    }

    // centroid + Google Maps URL
    const c = centroidOf(feature);
    if (c) {
      linkRows.push([assetId, datasetKey, String(c.lat), String(c.lng), mapsUrl(c.lat, c.lng), "ok"]);
    } else {
      linkRows.push([assetId, datasetKey, "", "", "", "ok (no centroid)"]);
    }

    // file base name = just the assetId (as requested)
    const base = sanitizeName(assetId);

    // write according to requested format
    try {
      if (format === "kmlzip") {
        const buf = await featureToKmlBuffer(feature);
        zip.addFile(`${base}.kml`, buf);
      } else if (format === "geojsonzip") {
        zip.addFile(`${base}.geojson`, Buffer.from(JSON.stringify(feature)));
      } else if (format === "shpzip") {
        const parts = await featureToShapefileParts(feature, base);
        for (const [name, buf] of parts) zip.addFile(name, buf);
      } else if (format === "kml") {
        // merged single file (non-zip) — we’ll collect below
        // handled after the loop
      } else if (format === "geojson") {
        // merged single file — handled after the loop
      } else {
        // default to kmlzip if unrecognized
        const buf = await featureToKmlBuffer(feature);
        zip.addFile(`${base}.kml`, buf);
      }

      successCount++;
    } catch (e) {
      linkRows.push([assetId, datasetKey, "", "", "", "conversion error"]);
    }
  }

  // If format is kml or geojson (non-zip, merged), return single file directly
  if (format === "kml" || format === "geojson") {
    // For merged exports, we re-fetch features that succeeded above to avoid holding all in memory,
    // but here we can simply tell users bulk merged is zip-only to keep the endpoint predictable.
    res.status(400).json({ error: "For bulk, use kmlzip, geojsonzip or shpzip." });
    return;
  }

  // add the google maps CSV
  const csv = linkRows.map(r => r.map(v => /[",\n]/.test(v) ? `"${String(v).replace(/"/g, '""')}"` : v).join(",")).join("\n");
  zip.addFile("mapbuddy_links.csv", Buffer.from(csv, "utf8"));

  if (!successCount) {
    res.status(404).json({ error: "No features found for supplied items." });
    return;
  }

  // send zip
  const zipName =
    format === "geojsonzip" ? "mapbuddy_geojson.zip" :
    format === "shpzip"     ? "mapbuddy_shapefile.zip" :
                              "mapbuddy_kml.zip";

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename=${zipName}`);
  res.status(200).send(zip.toBuffer());
}
