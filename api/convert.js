// api/convert.js
import axios from "axios";
import mapshaperModule from "mapshaper";

export const config = { runtime: "nodejs", maxDuration: 20 };

// Make Mapshaper API resilient to different export styles
const MS = mapshaperModule?.default ?? mapshaperModule;
const run =
  MS?.applyCommands
    ? (cmds, files) => MS.applyCommands(cmds, files)
    : (cmds, files) => MS.runCommands(cmds, files);

// Robust JSON body reader (works on Vercel Node functions)
async function readJsonBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") {
    try { return JSON.parse(req.body); } catch { return {}; }
  }
  return await new Promise((resolve) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => { try { resolve(JSON.parse(data || "{}")); } catch { resolve({}); } });
  });
}

const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7) | field: FACILITY_ID
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
  },
  // MDOT SHA Managed Landscape (Layer 0) | field: LOD_ID
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
  },
};

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJsonBody(req);
    const assetId = body?.assetId;
    const dataset = body?.dataset;

    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const cfg = DATASETS[dataset];
    if (!cfg) {
      res.status(400).json({ error: `Unsupported dataset '${dataset}'` });
      return;
    }

    // Query ArcGIS for GeoJSON
    const safeId = String(assetId).replace(/'/g, "''");
    const where = encodeURIComponent(cfg.idField) + "='" + encodeURIComponent(safeId) + "'";
    const url = `${cfg.base}/query?where=${where}&outFields=*&f=geojson`;

    const { data } = await axios.get(url, { timeout: 20000 });
    if (!data || !data.features || data.features.length === 0) {
      res.status(404).json({ error: `No feature found for ${assetId} in ${dataset}` });
      return;
    }

    // Convert GeoJSON -> KML with Mapshaper (name an output file)
    const inputName = "asset.geojson";
    const outputName = "out.kml";
    const geojsonStr = JSON.stringify(data);
    const commands = `-i ${inputName} -o ${outputName} format=kml precision=0.000001`;

    const out = await run(commands, { [inputName]: geojsonStr });
    const kmlText = out?.[outputName];
    if (!kmlText) {
      throw new Error("Mapshaper produced no KML (out.kml not found)");
    }

    // Return as a download
    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}.kml"`);
    res.status(200).send(Buffer.from(kmlText));
  } catch (err) {
    const details = (err && (err.stack || err.message)) ? (err.stack || err.message) : String(err);
    console.error("[/api/convert] ERROR:", details);
    res.status(500).json({ error: "Conversion failed", details });
  }
}
