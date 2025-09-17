import axios from "axios";
import * as mapshaper from "mapshaper";

export const config = { runtime: "nodejs", maxDuration: 20 };

async function readJsonBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") { try { return JSON.parse(req.body); } catch { return {}; } }
  return await new Promise((resolve) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => { try { resolve(JSON.parse(data || "{}")); } catch { resolve({}); } });
  });
}

const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID"
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID"
  }
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

    const safeId = String(assetId).replace(/'/g, "''");
    const where = encodeURIComponent(cfg.idField) + "='" + encodeURIComponent(safeId) + "'";
    const url = `${cfg.base}/query?where=${where}&outFields=*&f=geojson`;

    const { data } = await axios.get(url, { timeout: 20000 });
    if (!data || !data.features || data.features.length === 0) {
      res.status(404).json({ error: `No feature found for ${assetId} in ${dataset}` });
      return;
    }

    const inputName = "asset.geojson";
    const geojsonStr = JSON.stringify(data);
    const commands = `-i ${inputName} -o format=kml precision=0.000001 stdout`;
    const out = await mapshaper.applyCommands(commands, { [inputName]: geojsonStr });

    const kmlBuf = Buffer.from(out["stdout"]);
    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}.kml"`);
    res.status(200).send(kmlBuf);
  } catch (err) {
    res.status(500).json({ error: "Conversion failed", details: err?.message || String(err) });
  }
}
