// api/bulk.js
// Bulk convert many ArcGIS features into a ZIP of files (KML | GeoJSON | Shapefile)
// One file per asset inside the final ZIP for KML/GeoJSON.
// Shapefile uses Mapshaper's own zipped output per asset (nested into our master zip).

export const config = { runtime: "nodejs" };

import axios from "axios";
import JSZip from "jszip";

// Load mapshaper applyCommands in a way that works on Vercel
async function getMapshaperApply() {
  const ms = await import("mapshaper");
  return (
    ms.applyCommands ||
    (ms.default && ms.default.applyCommands) ||
    null
  );
}

// Datasets (same keys as the UI)
const DATASETS = {
  fairfax_bmps: {
    base:
      "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },
  mdsha_landscape: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },
  mdsha_tmdl_structures: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },
  mdsha_tmdl_retrofits: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },
  mdsha_tmdl_tree_plantings: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  mdsha_tmdl_pavement_removals: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  mdsha_tmdl_stream_restorations: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  mdsha_tmdl_outfall_stabilizations: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
};

// --- helpers ---
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

async function fetchGeoJSON(datasetKey, assetId) {
  const ds = DATASETS[datasetKey];
  if (!ds) throw new Error(`Unknown dataset '${datasetKey}'`);
  const { base, idFields } = ds;

  for (const field of idFields) {
    const url =
      `${base}/query` +
      `?where=${encodeURIComponent(`${field}='${assetId}'`)}` +
      `&outFields=*` +
      `&returnGeometry=true` +
      `&outSR=4326` +
      `&f=json`;

    const r = await axios.get(url, { timeout: 20000 });
    const data = r.data || {};
    const feats = data.features || [];
    if (feats.length > 0) {
      // Minimal GeoJSON FeatureCollection; mapshaper consumes it fine
      return {
        type: "FeatureCollection",
        features: feats.map((f) => ({
          type: "Feature",
          properties: f.attributes || {},
          // let mapshaper handle geometry parsing from esri json via input
          geometry: null,
          _esri: f,
        })),
        _esriSpatialReference: data.spatialReference || { wkid: 4326 },
      };
    }
  }
  return null;
}

async function runMapshaper(msApply, featureCollection, format, outName) {
  const inputName = "in.json";
  const input = JSON.stringify(featureCollection);

  let cmd;
  if (format === "kml") cmd = `-i ${inputName} -o format=kml ${outName}`;
  else if (format === "geojson") cmd = `-i ${inputName} -o format=geojson ${outName}`;
  else if (format === "shp") cmd = `-i ${inputName} -o format=shapefile ${outName}`;
  else throw new Error(`Unsupported format '${format}'`);

  const result = await msApply(input, cmd, { [inputName]: input });
  const buf = result[outName];
  if (!buf) throw new Error(`Mapshaper did not produce ${outName}`);
  return Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
}

// --- handler ---
export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Use POST with JSON body: { items, defaultDataset, format }" });
    return;
  }

  try {
    const body = await readJson(req);
    const items = Array.isArray(body.items) ? body.items : [];
    const defaultDataset = body.defaultDataset || "fairfax_bmps";
    const format = (body.format || "kml").toLowerCase();

    if (items.length === 0) {
      res.status(400).json({ error: "No items provided" });
      return;
    }

    const apply = await getMapshaperApply();
    if (!apply) throw new Error("Mapshaper API not available (applyCommands missing)");

    const zip = new JSZip();
    const report = [];

    for (const [i, it] of items.entries()) {
      const assetId = String(it.assetId || "").trim();
      const dataset = String(it.dataset || defaultDataset).trim();
      if (!assetId) {
        report.push({ i, assetId, dataset, status: "skipped", reason: "missing assetId" });
        continue;
      }

      try {
        const fc = await fetchGeoJSON(dataset, assetId);
        if (!fc) {
          report.push({ i, assetId, dataset, status: "not_found" });
          continue;
        }

        if (format === "shp") {
          const out = await runMapshaper(apply, fc, "shp", "out.zip");
          zip.file(`${assetId}.shp.zip`, out);
        } else if (format === "kml") {
          const out = await runMapshaper(apply, fc, "kml", "out.kml");
          zip.file(`${assetId}.kml`, out);
        } else if (format === "geojson") {
          const out = await runMapshaper(apply, fc, "geojson", "out.geojson");
          zip.file(`${assetId}.geojson`, out);
        }

        report.push({ i, assetId, dataset, status: "ok" });
      } catch (e) {
        report.push({ i, assetId, dataset, status: "error", error: String(e?.message || e) });
      }
    }

    const zipBuf = await zip.generateAsync({ type: "nodebuffer" });
    const name =
      format === "kml" ? "mapbuddy_kml.zip" :
      format === "geojson" ? "mapbuddy_geojson.zip" :
      "mapbuddy_shapefile.zip";

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename=${name}`);
    res.status(200).end(zipBuf);
  } catch (err) {
    res.status(500).json({ error: "Bulk conversion failed", details: String(err?.message || err) });
  }
}
