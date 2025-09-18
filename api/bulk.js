// api/bulk.js
// Combine many assets into one download (KML / GeoJSON / Shapefile) with a REQUIRED dataset,
// either from the "defaultDataset" form field or per-row override in items[].dataset.
// NO auto-detect here.

import axios from "axios";

// Make sure we're on Node (NOT Edge)
export const config = { runtime: "nodejs" };

// Try both ESM/CJS shims so mapshaper.applyCommands is always available
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

// ---- Dataset registry (same keys you use in /api/convert.js & UI) ----
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
  },

  // TMDL group + individual layers
  mdsha_tmdl_any: {
    // pseudo-dataset to allow "any TMDL layer" — we’ll search in the layer order below
    layers: [
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2", idField: "STRU_ID" }, // Tree Plantings
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0", idField: "SWM_FAC_NO" }, // Control Structures
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1", idField: "SWM_FAC_NO" }, // Retrofits
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3", idField: "STRU_ID" }, // Pavement Removals
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4", idField: "STRU_ID" }, // Stream Restorations
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5", idField: "STRU_ID" }, // Outfall Stabilizations
    ],
  },

  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idField: "SWM_FAC_NO",
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idField: "SWM_FAC_NO",
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idField: "STRU_ID",
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idField: "STRU_ID",
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idField: "STRU_ID",
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idField: "STRU_ID",
  },
};

// ---- helpers ----
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

function buildQueryUrl(base, idField, idVal) {
  const params = new URLSearchParams({
    where: `${idField}='${encodeURIComponent(idVal).replace(/'/g, "''")}'`,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "json",
  });
  return `${base}/query?${params.toString()}`;
}

// Search mdsha_tmdl_any across its sublayers in order
async function fetchFromAnyTMDL(assetId) {
  for (const layer of DATASETS.mdsha_tmdl_any.layers) {
    const url = buildQueryUrl(layer.base, layer.idField, assetId);
    const r = await axios.get(url);
    if (r.data?.features?.length) return r.data.features;
  }
  return [];
}

async function fetchFeatures(datasetKey, assetId) {
  const def = DATASETS[datasetKey];
  if (!def) throw new Error(`Unknown dataset '${datasetKey}'`);

  if (datasetKey === "mdsha_tmdl_any") {
    return await fetchFromAnyTMDL(assetId);
  }

  const url = buildQueryUrl(def.base, def.idField, assetId);
  const r = await axios.get(url);
  return r.data?.features || [];
}

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Use POST with JSON body { items: [{assetId, dataset?}], defaultDataset?, format }" });
    return;
  }

  try {
    const body = await readJson(req);
    const items = Array.isArray(body.items) ? body.items : [];
    const defaultDataset = body.defaultDataset || null; // used for all rows without an explicit dataset
    const format = (body.format || "kml").toLowerCase(); // kml | geojson | shp

    if (!items.length) {
      res.status(400).json({ error: "No items provided." });
      return;
    }

    // Collect all features
    const allFeatures = [];
    const skipped = [];

    for (const [idx, row] of items.entries()) {
      const assetId = String(row.assetId || "").trim();
      const dataset = String(row.dataset || defaultDataset || "").trim();
      if (!assetId || !dataset) {
        skipped.push({ index: idx, assetId, reason: "missing assetId or dataset" });
        continue;
      }

      try {
        const feats = await fetchFeatures(dataset, assetId);
        if (!feats.length) {
          skipped.push({ index: idx, assetId, dataset, reason: "no feature found" });
        } else {
          // tag each feature with the source id for traceability
          for (const f of feats) {
            f.properties = f.properties || {};
            f.properties.__assetId = assetId;
            f.properties.__dataset = dataset;
          }
          allFeatures.push(...feats);
        }
      } catch (e) {
        skipped.push({ index: idx, assetId, dataset, reason: e.message || String(e) });
      }
    }

    if (!allFeatures.length) {
      res.status(404).json({ error: "No features found for any row.", skipped });
      return;
    }

    // Build a FeatureCollection
    const fc = { type: "FeatureCollection", features: allFeatures };

    const apply = await getMapshaperApply();
    const input = { "in.json": JSON.stringify(fc) };

    let cmd = "-i in.json -proj wgs84 ";
    let outName = "mapbuddy";

    if (format === "geojson") {
      cmd += "-o out.geojson";
    } else if (format === "shp" || format === "shapefile") {
      cmd += "-o format=shapefile out.zip";
      outName += ".zip";
    } else {
      // KML default
      cmd += "-o out.kml";
      outName += ".kml";
    }

    const outputs = await apply(cmd, input);
    const key =
      format === "geojson" ? "out.geojson" :
      (format === "shp" || format === "shapefile") ? "out.zip" :
      "out.kml";

    const buf = outputs[key];
    if (!buf) {
      res.status(500).json({ error: "Mapshaper did not return an output file." });
      return;
    }

    // Content-Type & download name
    const ctype =
      format === "geojson" ? "application/geo+json" :
      (format === "shp" || format === "shapefile") ? "application/zip" :
      "application/vnd.google-earth.kml+xml";

    res.setHeader("Content-Type", ctype);
    res.setHeader("Content-Disposition", `attachment; filename="${outName}"`);
    res.status(200).send(Buffer.isBuffer(buf) ? buf : Buffer.from(buf));
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  }
}
