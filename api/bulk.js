// api/bulk.js
// Bulk: fetch multiple ArcGIS features and return a .zip containing
// per-asset KML files, per-asset GeoJSON files, or per-asset Shapefile .zip files.

import axios from "axios";
import AdmZip from "adm-zip";

// Ensure Node runtime (not Edge)
export const config = { runtime: "nodejs" };

// ---------- helpers ----------
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); }
  catch { return {}; }
}

async function getMapshaperApply() {
  const ms = await import("mapshaper");
  const apply = ms.applyCommands || (ms.default && ms.default.applyCommands) || null;
  if (!apply) throw new Error("Mapshaper API not available (applyCommands missing)");
  return apply;
}

function esc(str) {
  return String(str).replace(/'/g, "''");
}

// ---------- dataset registry (kept in-sync with convert/locate) ----------
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7)
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },

  // MDOT SHA Managed Landscape (Layer 0)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"]
  },

  // MDOT SHA — TMDL (5 layers)
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

// Build ArcGIS 'where' from idFields
function makeWhere(fields, assetId) {
  const v = esc(assetId);
  return fields.map((f) => `${f}='${v}'`).join(" OR ");
}

async function fetchGeoJSON(datasetKey, assetId) {
  const ds = DATASETS[datasetKey];
  if (!ds) throw new Error(`Unknown dataset '${datasetKey}'`);
  const where = makeWhere(ds.idFields, assetId);

  const url = `${ds.base}/query`;
  const params = {
    where,
    outFields: "*",
    returnGeometry: true,
    outSR: 4326,
    f: "geojson"
  };

  const { data } = await axios.get(url, { params, timeout: 20000 });
  // Expecting a FeatureCollection; make sure we found *something*
  if (!data || !data.features || !data.features.length) return null;
  return data; // GeoJSON FeatureCollection
}

// Convert GeoJSON → per-asset KML (Buffer)
async function geojsonToKml(assetId, geojson) {
  const apply = await getMapshaperApply();
  const inName = "in.geojson";
  const outName = `${assetId}.kml`;
  const cmd = [
    `-i ${inName}`,
    // Keep WGS84, light precision to keep files small
    `-o format=kml encoding=UTF-8 "${outName}"`
  ].join(" ");

  const files = await apply(cmd, { [inName]: Buffer.from(JSON.stringify(geojson)) });
  const buf = files[outName] || files[outName.toLowerCase()];
  if (!buf) throw new Error(`Mapshaper did not produce ${outName}`);
  return Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
}

// Convert GeoJSON → per-asset Shapefile ZIP (Buffer)
async function geojsonToShapefileZip(assetId, geojson) {
  const apply = await getMapshaperApply();
  const inName = "in.geojson";
  const outZip = `${assetId}.zip`;

  // Mapshaper can directly emit a zipped shapefile by giving a .zip output name.
  const cmd = [
    `-i ${inName}`,
    `-o format=shapefile encoding=UTF-8 "${outZip}"`
  ].join(" ");

  const files = await apply(cmd, { [inName]: Buffer.from(JSON.stringify(geojson)) });
  const buf = files[outZip] || files[outZip.toLowerCase()];
  if (!buf) throw new Error(`Mapshaper did not produce ${outZip}`);
  return Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { items: [{assetId,dataset?}, ...], defaultDataset?, format }" });
      return;
    }

    const body = await readJson(req);
    const items = Array.isArray(body.items) ? body.items : [];
    const defaultDataset = body.defaultDataset || null;
    const formatIn = (body.format || "kml").toLowerCase(); // "kml" | "geojson" | "shapefile"

    if (!items.length) {
      res.status(400).json({ error: "No items provided" });
      return;
    }

    // Prepare the output zip (outer zip)
    const outerZip = new AdmZip();

    // Process sequentially (safer with ArcGIS throttling; easy to parallelize later)
    for (const it of items) {
      const assetId = String(it.assetId || "").trim();
      const dataset = String(it.dataset || defaultDataset || "").trim();
      if (!assetId || !dataset) continue;

      // Fetch
      const fc = await fetchGeoJSON(dataset, assetId);
      if (!fc) continue; // skip not-found

      if (formatIn === "kml") {
        const kmlBuf = await geojsonToKml(assetId, fc);
        outerZip.addFile(`${assetId}.kml`, kmlBuf);
      } else if (formatIn === "geojson") {
        // Use the ArcGIS GeoJSON as-is
        const gjBuf = Buffer.from(JSON.stringify(fc));
        outerZip.addFile(`${assetId}.geojson`, gjBuf);
      } else if (formatIn === "shapefile") {
        // Inner zip per asset (e.g. 210049UT.zip), then added into the outer zip
        const shpZip = await geojsonToShapefileZip(assetId, fc);
        outerZip.addFile(`${assetId}.zip`, shpZip);
      } else {
        // Unknown format → skip
      }
    }

    const outBuf = outerZip.toBuffer();
    const fname = `mapbuddy_${formatIn}.zip`;

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename=${fname}`);
    res.status(200).send(outBuf);
  } catch (err) {
    const msg = err?.message || String(err);
    res.status(500).json({ error: msg });
  }
}
