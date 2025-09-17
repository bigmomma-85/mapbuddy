// api/convert.js
// Convert a single ArcGIS feature (by ID) to KML using Mapshaper

import axios from "axios";

// IMPORTANT: ensure we run on the Node runtime (NOT Edge)
export const config = { runtime: "nodejs" };

// tiny helper to read JSON body
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

// Try both ESM/CJS shapes for mapshaper so `applyCommands` is always found
async function getMapshaperApply() {
  const ms = await import("mapshaper");
  const apply =
    ms.applyCommands ||
    (ms.default && ms.default.applyCommands) ||
    null;
  if (!apply) {
    throw new Error("Mapshaper API not available (applyCommands missing)");
  }
  return apply;
}

/**
 * Build an ArcGIS REST query URL for the chosen dataset.
 * We always ask for WGS84 (outSR=4326) and GeoJSON (f=geojson).
 */
function buildQueryUrl(datasetKey, assetId) {
  const enc = encodeURIComponent;

  // Fairfax BMPs (FACILITY_ID)
  if (datasetKey === "fairfax_bmps") {
    const base =
      "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7";
    return `${base}/query?where=FACILITY_ID%3D'${enc(assetId)}'&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  }

  // MDOT SHA Managed Landscape (LOD_ID)
  if (datasetKey === "mdsha_landscape") {
    const base =
      "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0";
    return `${base}/query?where=LOD_ID%3D'${enc(assetId)}'&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  }

  // TMDL layers:
  //  - structures (layer 0) & retrofits (layer 1): ID field SWM_FAC_NO
  //  - tree plantings (2), pavement removals (3), stream restorations (4), outfall stabilizations (5): ID field STRU_ID
  const tmdlBase =
    "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer";

  const layerDefs = {
    mdsha_tmdl_structures: { layer: 0, field: "SWM_FAC_NO" },
    mdsha_tmdl_retrofits: { layer: 1, field: "SWM_FAC_NO" },
    mdsha_tmdl_tree_plantings: { layer: 2, field: "STRU_ID" },
    mdsha_tmdl_pavement_removals: { layer: 3, field: "STRU_ID" },
    mdsha_tmdl_stream_restorations: { layer: 4, field: "STRU_ID" },
    mdsha_tmdl_outfall_stabilizations: { layer: 5, field: "STRU_ID" },
  };

  const def = layerDefs[datasetKey];
  if (!def) return null;

  return `${tmdlBase}/${def.layer}/query?where=${def.field}%3D'${enc(
    assetId
  )}'&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
}

/**
 * ArcGIS can sometimes hang behind VPN. We add a short global timeout and
 * give a helpful error if it times out or returns no features.
 */
async function fetchGeoJSON(url) {
  try {
    const { data } = await axios.get(url, { timeout: 15000, responseType: "json" });
    return data;
  } catch (err) {
    const msg = err?.code === "ECONNABORTED"
      ? "ArcGIS request timed out (VPN required for MDOT SHA layers?)"
      : `ArcGIS request failed`;
    throw Object.assign(new Error(msg), { details: err?.message || "" });
  }
}

/**
 * Validate GeoJSON FeatureCollection and ensure at least one feature exists.
 */
function ensureAtLeastOneFeature(geojson) {
  if (!geojson || geojson.type !== "FeatureCollection") {
    throw new Error("ArcGIS did not return a FeatureCollection (VPN needed?)");
  }
  if (!geojson.features || geojson.features.length === 0) {
    throw new Error("No feature found for that ID.");
  }
}

/**
 * Convert GeoJSON -> KML using mapshaper
 */
async function geojsonToKml(geojson) {
  const applyCommands = await getMapshaperApply();
  const inputs = { "in.json": JSON.stringify(geojson) };

  // Keep geography as-is; write KML
  const cmd = `-i in.json -o out.kml format=kml`;

  const outputs = await applyCommands(cmd, inputs);
  const kml = outputs["out.kml"];
  if (!kml) throw new Error("Mapshaper did not produce KML.");
  return Buffer.isBuffer(kml) ? kml : Buffer.from(kml);
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJson(req);
    const assetId = (body.assetId || "").trim();
    const dataset = (body.dataset || "").trim();

    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const url = buildQueryUrl(dataset, assetId);
    if (!url) {
      res.status(400).json({ error: `Unknown dataset '${dataset}'` });
      return;
    }

    const geojson = await fetchGeoJSON(url);
    ensureAtLeastOneFeature(geojson);

    const kmlBuffer = await geojsonToKml(geojson);

    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}.kml"`);
    res.status(200).send(kmlBuffer);
  } catch (err) {
    res
      .status(500)
      .json({
        error: `Conversion failed: ${err?.message || "Unknown error"}`,
        details: err?.details || undefined
      });
  }
}
