// api/bulk.js
// Bulk convert many ArcGIS features into a ZIP of files (KML | GeoJSON | Shapefile)
// One file per asset inside the final ZIP for KML/GeoJSON.
// Shapefile uses Mapshaper's own zipped output per asset (nested into our master zip).

export const config = { runtime: "nodejs" };

import axios from "axios";
import JSZip from "jszip";

// Load mapshaper in a way that works for both ESM and CJS bundles on Vercel
async function getMapshaper() {
  const ms = await import("mapshaper");
  return (
    ms.applyCommands ||
    (ms.default && ms.default.applyCommands) ||
    null
  );
}

// --- Datasets used in your app (same keys the UI sends) ---
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7)
  fairfax_bmps: {
    base:
      "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },

  // MDOT SHA Managed Landscape (Layer 0)
  mdsha_landscape: {
    base:
      "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },

  // --- TMDL Bay Restoration layers ---
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

// ---- helpers ----
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try {
    return JSON.parse(raw || "{}");
  } catch {
    return {};
  }
}

// Try each idField until we get a hit
async function fetchFeatureGeoJSON(datasetKey, assetId) {
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
      // Convert ESRI JSON to GeoJSON FeatureCollection
      const gj = {
        type: "FeatureCollection",
        features: feats.map((f) => {
          const geom = f.geometry || {};
          // ArcGIS returns rings/paths; mapshaper can ingest ESRI JSON or GeoJSON.
          // Easiest path: call mapshaper with ESRI JSON directly.
          // But we'll convert minimal geometry => let mapshaper parse ESRI JSON if we feed whole object.
          return { type: "Feature", properties: f.attributes || {}, geometry: null, _esri: f };
        }),
        // Keep the raw esri to let mapshaper read it properly
        _esriSpatialReference: data.spatialReference || { wkid: 4326 },
      };
      return gj;
    }
  }
  return null;
}

// Build an in-memory KML/GeoJSON using mapshaper for one asset
async function buildVectorWithMapshaper(ms, geojson, format, outName) {
  // Feed an ESRI-ish FeatureCollection safely to mapshaper by writing directly as GeoJSON,
  // letting mapshaper reproject if needed.
  const inputName = "in.json";
  const json = JSON.stringify(geojson);

  // Commands differ by format
  let cmd;
  if (format === "kml") {
    cmd = `-i ${inputName} -o format=kml ${outName}`;
  } else if (format === "geojson") {
    cmd = `-i ${inputName} -o format=geojson ${outName}`;
  } else if (format === "shp") {
    // Mapshaper writes a zipped shapefile when outName ends with .zip
    cmd = `-i ${inputName} -o format=shapefile ${outName}`;
  } else {
    throw new Error(`Unsupported format '${format}'`);
  }

  const result = await ms(json, cmd, { "in.json": json });
  const buf = result[outName];
  if (!buf) throw new Error(`Mapshaper did not produce ${outName}`);
  return Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
}

// ---- main handler ----
export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Use POST with JSON body" });
    return;
  }

  try {
    const body = await readJson(req);
    // body.items: [{ assetId, dataset? }, ...]
    // body.defaultDataset?: string
    // body.format: "kml" | "geojson" | "shp"
    const items = Array.isArray(body.items) ? body.items : [];
    const defaultDataset = body.defaultDataset || "fairfax_bmps";
    const format = (body.format || "kml").toLowerCase();

    if (items.length === 0) {
      res.status(400).json({ error: "No items provided" });
      return;
    }

    const applyCommands =
      (await getMapshaper()) ||
      (() => {
        throw new Error("Mapshaper API not available (applyCommands missing)");
      });

    // zip that we will stream back
    const zip = new JSZip();

    // collect per-asset logs
    const report = [];

    for (const [i, it] of items.entries()) {
      const assetId = String(it.assetId || "").trim();
      const dataset = String(it.dataset || defaultDataset).trim();

      if (!assetId) {
        report.push({ index: i, assetId, dataset, status: "skipped", reason: "missing assetId" });
        continue;
      }

      try {
        const gj = await fetchFeatureGeoJSON(dataset, assetId);
        if (!gj) {
          report.push({ index: i, assetId, dataset, status: "not_found" });
          continue;
        }

        if (format === "shp") {
          // make a zipped shapefile for each asset and nest it into the master zip
          const outName = "out.zip";
          const buf = await buildVectorWithMapshaper(applyCommands, gj, "shp", outName);
          zip.file(`${assetId}.shp.zip`, buf);
        } else if (format === "kml") {
          const outName = "out.kml";
          const buf = await buildVectorWithMapshaper(applyCommands, gj, "kml", outName);
          zip.file(`${assetId}.kml`, buf);
        } else if (format === "geojson") {
          const outName = "out.geojson";
          const buf = await buildVectorWithMapshaper(applyCommands, gj, "geojson", outName);
          zip.file(`${assetId}.geojson`, buf);
        } else {
          throw new Error(`Unsupported format '${format}'`);
        }

        report.push({ index: i, assetId, dataset, status: "ok" });
      } catch (e) {
        report.push({
          index: i,
          assetId,
          dataset,
          status: "error",
          error: String(e && e.message ? e.message : e),
        });
      }
    }

    const zipBuf = await zip.generateAsync({ type: "nodebuffer" });

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename=mapbuddy_${format}.zip`);
    // Include a tiny JSON report inside the zip too (helps with debugging)
    // (Optional) If you prefer not to, comment these two lines:
    // const rep = JSON.stringify({ generatedAt: new Date().toISOString(), report }, null, 2);
    // zip.file("_report.json", rep);

    res.status(200).end(zipBuf);
  } catch (err) {
    res.status(500).json({ error: "Bulk conversion failed", details: String(err?.message || err) });
  }
}
