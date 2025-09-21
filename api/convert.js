// api/convert.js
// Convert ONE ArcGIS feature (by ID) to KML / GeoJSON / Shapefile using Mapshaper

import axios from "axios";

// Force Node runtime (not Edge)
export const config = { runtime: "nodejs" };

// ---------- Dataset registry ----------
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7)
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
    label: "Fairfax — Stormwater Facilities",
  },

  // MDOT SHA Managed Landscape (Layer 0)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
    label: "MDOT SHA — Managed Landscape",
  },

  // TMDL layers
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
    label: "TMDL — Stormwater Control Structures",
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
    label: "TMDL — Retrofits",
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
    label: "TMDL — Tree Plantings",
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
    label: "TMDL — Pavement Removals",
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
    label: "TMDL — Stream Restorations",
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
    label: "TMDL — Outfall Stabilizations",
  },
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

// Recognize TMDL-style IDs even with dash/space variants
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

// Generate robust variants for TMDL IDs: raw, with dash, with space
function tmdlVariants(assetId) {
  const s = String(assetId || "").trim().toUpperCase();
  const m = s.match(/^(\d+)\s*-?\s*([A-Z]{2})$/);
  if (m) {
    const num = m[1], suf = m[2];
    return [ `${num}${suf}`, `${num}-${suf}`, `${num} ${suf}` ];
  }
  // If it already has letters+digits, still try common normalizations
  return [s, s.replace(/-/g,""), s.replace(/\s+/g,""), s.replace(/(\d+)([A-Za-z]+)/,'$1-$2'), s.replace(/(\d+)([A-Za-z]+)/,'$1 $2')];
}

// Build WHERE with optional variants (for TMDL)
function buildWhere(idFields, assetId, useVariants) {
  const esc = (v) => `'${String(v).replace(/'/g, "''")}'`;
  const vals = useVariants ? tmdlVariants(assetId) : [String(assetId)];
  const ors = [];
  for (const f of idFields) {
    for (const val of vals) ors.push(`${f}=${esc(val)}`);
  }
  return ors.join(" OR ");
}

async function fetchArcgisGeoJSON(base, where) {
  // try f=geojson first
  const u1 = `${base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  let r = await axios.get(u1, { validateStatus: () => true, timeout: 20000 });
  if (r.status === 200 && r.data && (r.data.type === "FeatureCollection" || r.data.type === "Feature")) {
    return r.data;
  }

  // fallback to f=json then convert
  const u2 = `${base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=json`;
  r = await axios.get(u2, { validateStatus: () => true, timeout: 20000 });
  if (r.status !== 200) throw new Error(`ArcGIS request failed (${r.status})`);
  if (!r.data || !Array.isArray(r.data.features)) return { type: "FeatureCollection", features: [] };

  const features = r.data.features.map(esriToGeoJSONFeature).filter(Boolean);
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

function wrapSingleFeature(fc) {
  if (!fc) return { type: "FeatureCollection", features: [] };
  if (fc.type === "Feature") return { type: "FeatureCollection", features: [fc] };
  if (fc.type === "FeatureCollection") return fc;
  return { type: "FeatureCollection", features: [] };
}

function castOutput(format) {
  const f = (format || "kml").toLowerCase();
  if (f === "geojson") return "geojson";
  if (f === "shapefile" || f === "shp" || f === "zip") return "shapefile";
  return "kml";
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

    // Resolve dataset
    let datasetKey = inputDataset || guessDatasetFromId(assetId);
    if (!datasetKey) { res.status(400).json({ error: "Unknown dataset. Pick one or use a recognizable ID." }); return; }

    // Handle TMDL any
    const targets = datasetKey === "mdsha_tmdl_any" ? TMDL_ANY : [datasetKey];

    // Find the first layer that returns a feature
    let featureFC = null, usedKey = null, usedLabel = null;

    for (const key of targets) {
      const ds = DATASETS[key];
      if (!ds) continue;

      // Use robust variants only for TMDL layers
      const useVariants = key.startsWith("mdsha_tmdl_");
      const where = buildWhere(ds.idFields, assetId, useVariants);

      const fc = wrapSingleFeature(await fetchArcgisGeoJSON(ds.base, where));
      if (fc.features.length) {
        usedKey = key; usedLabel = ds.label;
        const feat = fc.features[0];
        feat.properties = { ...feat.properties, _assetId: assetId, _dataset: key, _sourceLabel: ds.label };
        featureFC = { type: "FeatureCollection", features: [feat] };
        break;
      }
    }

    if (!featureFC) {
      res.status(404).json({ error: `No feature found for '${assetId}' in ${targets.join(", ")}` });
      return;
    }

    // Mapshaper conversion
    const outFmt = castOutput(format);
    const msApply = await getMapshaperApply();
    if (!msApply) throw new Error("Mapshaper API not available");

    const input = { "in.json": Buffer.from(JSON.stringify(featureFC)) };
    const outName = outFmt === "kml" ? "out.kml" : (outFmt === "geojson" ? "out.geojson" : "out.zip");
    const cmd = `-i in.json -clean -o ${outName} format=${outFmt}`;

    const outputs = await msApply(cmd, input);
    const buf = outputs[outName];
    if (!buf) throw new Error(`Mapshaper produced no ${outName}`);

    // headers + filename
    const safe = String(assetId).replace(/[^\w.-]+/g, "_");
    if (outFmt === "kml") {
      res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.kml"`);
    } else if (outFmt === "geojson") {
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
