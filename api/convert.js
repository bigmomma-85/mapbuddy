// api/convert.js
// Convert ONE ArcGIS feature (by ID) to KML / GeoJSON / Shapefile using Mapshaper

import axios from "axios";
export const config = { runtime: "nodejs" };

// ---------- Dataset registry ----------
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7) — MapServer OK
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
    label: "Fairfax — Stormwater Facilities",
  },

  // MDOT SHA Managed Landscape (Layer 0) — MapServer OK
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
    label: "MDOT SHA — Managed Landscape",
  },

  // TMDL layers — USE FeatureServer (supports JSON, stable for queries)
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
    label: "TMDL — Stormwater Control Structures",
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
    label: "TMDL — Retrofits",
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/2",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"],
    label: "TMDL — Tree Plantings",
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/3",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"],
    label: "TMDL — Pavement Removals",
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/4",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"],
    label: "TMDL — Stream Restorations",
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/5",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"],
    label: "TMDL — Outfall Stabilizations",
  }
};

// Special “any TMDL” search order
const TMDL_ANY = [
  "mdsha_tmdl_structures",
  "mdsha_tmdl_retrofits",
  "mdsha_tmdl_tree_plantings",
  "mdsha_tmdl_pavement_removals",
  "mdsha_tmdl_stream_restorations",
  "mdsha_tmdl_outfall_stabilizations",
];

// ---------- helpers ----------
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

function isTmdlIdLike(s) {
  const v = String(s || "").trim().toUpperCase();
  return /^(\d+)\s*-?\s*(UT|TR|SR|OF|PR)$/.test(v);
}

function guessDatasetFromId(id) {
  if (!id) return null;
  const s = String(id).trim().toUpperCase();
  if (/^WP\d{3,}$/.test(s)) return "fairfax_bmps";
  if (s.startsWith("LOD_")) return "mdsha_landscape";
  if (isTmdlIdLike(s)) return "mdsha_tmdl_any";
  return null;
}

function tmdlVariants(assetId) {
  const s = String(assetId || "").trim().toUpperCase();
  const m = s.match(/^(\d+)\s*-?\s*([A-Z]{2})$/);
  if (m) {
    const num = m[1], suf = m[2];
    return [ `${num}${suf}`, `${num}-${suf}`, `${num} ${suf}` ];
  }
  return [s, s.replace(/-/g,""), s.replace(/\s+/g,""), s.replace(/(\d+)([A-Za-z]+)/,'$1-$2'), s.replace(/(\d+)([A-Za-z]+)/,'$1 $2')];
}

// Case-insensitive where, with optional TMDL variants
function buildWhere(idFields, assetId, useVariants) {
  const vals = useVariants ? tmdlVariants(assetId) : [String(assetId)];
  const esc = (v) => String(v).replace(/'/g, "''");
  const ors = [];
  for (const f of idFields) {
    for (const v of vals) {
      ors.push(`UPPER(${f}) = UPPER('${esc(v)}')`);
    }
  }
  return ors.join(" OR ");
}

async function fetchArcgisAsGeoJSON(base, where) {
  // FeatureServer usually supports only JSON → convert here
  const url = `${base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=json`;
  const r = await axios.get(url, { validateStatus: () => true, timeout: 20000 });
  if (r.status !== 200) throw new Error(`ArcGIS request failed (${r.status})`);
  const data = r.data || {};
  const arr = Array.isArray(data.features) ? data.features : [];
  const features = arr.map(esriToGeoJSONFeature).filter(Boolean);
  return { type: "FeatureCollection", features };
}

function esriToGeoJSONFeature(f) {
  const a = f.attributes || {};
  const g = f.geometry || {};
  let geom = null;
  if (Array.isArray(g.rings)) {
    geom = { type: "Polygon", coordinates: g.rings };
  } else if (Array.isArray(g.paths)) {
    geom = g.paths.length === 1
      ? { type: "LineString", coordinates: g.paths[0] }
      : { type: "MultiLineString", coordinates: g.paths };
  } else if (typeof g.x === "number" && typeof g.y === "number") {
    geom = { type: "Point", coordinates: [g.x, g.y] };
  }
  return geom ? { type: "Feature", properties: a, geometry: geom } : null;
}

async function getMapshaperApply() {
  const ms = await import("mapshaper");
  return ms.applyCommands || (ms.default && ms.default.applyCommands);
}

// ---------- main ----------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST { assetId, dataset?, format? }" });
      return;
    }

    const { assetId, dataset: inputDataset, format } = await readJson(req);
    if (!assetId) { res.status(400).json({ error: "assetId is required" }); return; }

    let datasetKey = inputDataset || guessDatasetFromId(assetId);
    if (!datasetKey) { res.status(400).json({ error: "Unknown dataset. Pick one or use a recognizable ID." }); return; }

    const targets = datasetKey === "mdsha_tmdl_any" ? TMDL_ANY : [datasetKey];

    // Find first layer that returns a match
    let fc = null, usedKey = null, usedLabel = null;
    for (const key of targets) {
      const ds = DATASETS[key]; if (!ds) continue;
      const useVariants = key.startsWith("mdsha_tmdl_");
      const where = buildWhere(ds.idFields, assetId, useVariants);
      const gj = await fetchArcgisAsGeoJSON(ds.base, where);
      if (gj.features.length) {
        usedKey = key; usedLabel = ds.label;
        const feat = gj.features[0];
        feat.properties = { ...feat.properties, _assetId: assetId, _dataset: key, _sourceLabel: ds.label };
        fc = { type: "FeatureCollection", features: [feat] };
        break;
      }
    }

    if (!fc) {
      res.status(404).json({ error: `No feature found for '${assetId}' in ${targets.join(", ")}` });
      return;
    }

    // Convert via mapshaper
    const outFmt = (format || "kml").toLowerCase();
    const msApply = await getMapshaperApply();
    if (!msApply) throw new Error("Mapshaper API not available");

    const input = { "in.json": Buffer.from(JSON.stringify(fc)) };
    const fmt = outFmt === "geojson" ? "geojson" : (outFmt === "shapefile" || outFmt === "shp" || outFmt === "zip" || outFmt === "shpzip" ? "shapefile" : "kml");
    const outName = fmt === "kml" ? "out.kml" : (fmt === "geojson" ? "out.geojson" : "out.zip");
    const cmd = `-i in.json -clean -o ${outName} format=${fmt}`;
    const outputs = await msApply(cmd, input);
    const buf = outputs[outName];
    if (!buf) throw new Error(`Mapshaper produced no ${outName}`);

    const safe = String(assetId).replace(/[^\w.-]+/g, "_");
    if (fmt === "kml") {
      res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.kml"`);
    } else if (fmt === "geojson") {
      res.setHeader("Content-Type", "application/geo+json");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.geojson"`);
    } else {
      res.setHeader("Content-Type", "application/zip");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.zip"`);
    }
    res.status(200).send(buf);

  } catch (err) {
    res.status(500).json({ error: "Conversion failed", details: String(err?.message || err) });
  }
}
