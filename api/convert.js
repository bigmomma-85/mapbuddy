// api/convert.js
// Convert one ArcGIS feature (found by ID) to KML via Mapshaper

import axios from "axios";

// IMPORTANT: run on Node runtime (NOT edge)
export const config = { runtime: "nodejs" };

// ---- tiny helpers ----------------------------------------------------------
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    return {};
  }
}

// load mapshaper and get a stable "apply" function no matter how it's exported
async function getMsApply() {
  const mod = await import("mapshaper");
  const ms = mod.default || mod;
  const apply =
    ms.applyCommands || // common on CJS
    ms.runCommands ||   // some builds expose this name
    (ms.default && (ms.default.applyCommands || ms.default.runCommands));

  if (!apply) throw new Error("Mapshaper API not available (applyCommands missing)");
  return apply;
}

// ---- datasets ---------------------------------------------------------------
// Keep IDs in sync with your UI <select> values
const DATASETS = {
  // Fairfax County (layer 7) — FACILITY_ID
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },

  // MDOT SHA — Managed Landscape (layer 0) — LOD_ID (VPN required)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"]
  },

  // MDOT SHA — TMDL: control structures / retrofits / plantings / removals / streams / outfalls (VPN)
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  }
};

// compose a GeoJSON query URL for a single-id lookup
function buildUrl(base, field, value) {
  const where = `${field}='${value.replace(/'/g, "''")}'`;
  const qs = new URLSearchParams({
    where,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "geojson"
  });
  return `${base}/query?${qs.toString()}`;
}

// Try each id field until one returns a non-empty FeatureCollection
async function fetchOneFeatureAsGeoJSON(datasetKey, assetId) {
  const cfg = DATASETS[datasetKey];
  if (!cfg) throw new Error(`Unknown dataset '${datasetKey}'`);

  const fields = cfg.idFields || (cfg.idField ? [cfg.idField] : []);
  if (!fields.length) throw new Error(`Dataset '${datasetKey}' has no id field mapping`);

  for (const f of fields) {
    const url = buildUrl(cfg.base, f, assetId);
    try {
      const r = await axios.get(url, { timeout: 20000 });
      if (r.status === 200 && r.data && r.data.type === "FeatureCollection" && r.data.features?.length) {
        // Return just the first feature to keep KML small and deterministic
        return {
          type: "FeatureCollection",
          features: [r.data.features[0]]
        };
      }
    } catch (_) {
      // ignore and try next field
    }
  }
  return null;
}

// ---- main handler -----------------------------------------------------------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const { assetId, dataset } = await readJson(req);
    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    // 1) fetch GeoJSON for the feature
    const geojson = await fetchOneFeatureAsGeoJSON(dataset, assetId);
    if (!geojson) {
      res.status(404).json({ error: "No feature found for that ID in the selected dataset." });
      return;
    }

    // 2) KML via mapshaper (robust export handling)
    const apply = await getMsApply();
    const cmd = `-i in.json -o format=kml precision=0.000001 encoding=utf8 out.kml`;
    // NOTE: apply returns an object mapping filenames → file contents
    const outFiles = await apply(cmd, { "in.json": JSON.stringify(geojson) });
    const kml = outFiles["out.kml"];
    if (!kml) throw new Error("KML not generated");

    // 3) send KML back
    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}.kml"`);
    res.status(200).send(kml);
  } catch (err) {
    // surface a concise message to the UI
    const msg = err?.message || "Conversion failed";
    res.status(500).json({ error: `Conversion failed: ${msg}` });
  }
}
