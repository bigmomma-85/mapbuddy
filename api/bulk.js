// api/bulk.js
// Bulk conversion: return ONE ZIP that contains ONE FILE PER ASSET.
// KML  -> <ASSET>.kml
// GeoJSON -> <ASSET>.geojson
// Shapefile -> <ASSET>/{asset.shp, asset.shx, asset.dbf, asset.prj[, asset.cpg]}
import axios from "axios";
import AdmZip from "adm-zip";

export const config = { runtime: "nodejs" };

// Mapshaper loader: make sure applyCommands is available regardless of ESM/CJS shape.
async function getMapshaperApply() {
  const ms = await import("mapshaper");
  const apply = ms.applyCommands || (ms.default && ms.default.applyCommands);
  if (!apply) throw new Error("Mapshaper API not available (applyCommands missing)");
  return apply;
}

// --------- DATASETS (same keys as UI) ----------
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
  },

  // “Any” within TMDL layers only
  mdsha_tmdl_any: {
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

// --------- helpers ----------
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

function qUrl(base, idField, idVal) {
  const params = new URLSearchParams({
    where: `${idField}='${encodeURIComponent(idVal).replace(/'/g, "''")}'`,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "json",
  });
  return `${base}/query?${params.toString()}`;
}

async function fetchAnyTMDL(assetId) {
  for (const layer of DATASETS.mdsha_tmdl_any.layers) {
    const url = qUrl(layer.base, layer.idField, assetId);
    const r = await axios.get(url, { timeout: 20000 });
    if (r.data?.features?.length) return r.data.features;
  }
  return [];
}

async function fetchFeatures(datasetKey, assetId) {
  const def = DATASETS[datasetKey];
  if (!def) throw new Error(`Unknown dataset '${datasetKey}'`);
  if (datasetKey === "mdsha_tmdl_any") return await fetchAnyTMDL(assetId);
  const url = qUrl(def.base, def.idField, assetId);
  const r = await axios.get(url, { timeout: 20000 });
  return r.data?.features || [];
}

function safeName(s) {
  return String(s || "")
    .replace(/[^\w.-]+/g, "_")
    .replace(/^_+/, "")
    .slice(0, 80) || "asset";
}

// simple concurrency limiter
async function runPool(items, limit, worker) {
  const ret = [];
  let i = 0, active = 0;
  return new Promise((resolve, reject) => {
    const next = () => {
      if (i >= items.length && active === 0) return resolve(ret);
      while (active < limit && i < items.length) {
        const idx = i++;
        active++;
        Promise.resolve(worker(items[idx], idx))
          .then(v => { ret[idx] = v; active--; next(); })
          .catch(err => reject(err));
      }
    };
    next();
  });
}

// ------------- MAIN -------------
export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Use POST with JSON body { items:[{assetId,dataset?}], defaultDataset?, format }" });
    return;
  }

  try {
    const body = await readJson(req);
    let items = Array.isArray(body.items) ? body.items : [];
    const defaultDataset = (body.defaultDataset || "").trim();
    const format = (body.format || "kml").toLowerCase(); // kml | geojson | shp

    if (!items.length) {
      res.status(400).json({ error: "No items provided." });
      return;
    }

    // Fill missing dataset from default (no auto-detect)
    items = items.map(r => ({
      assetId: String(r.assetId || "").trim(),
      dataset: String(r.dataset || defaultDataset || "").trim(),
    }));

    const toProcess = items.filter(r => r.assetId && r.dataset);
    if (!toProcess.length) {
      res.status(400).json({ error: "Every row needs a dataset. Choose a default or include a second CSV column." });
      return;
    }

    const apply = await getMapshaperApply();
    const zip = new AdmZip();
    const skipped = [];

    // Worker: build one file (or folder) per asset and add to the outer zip
    async function doOne(row) {
      const { assetId, dataset } = row;
      const feats = await fetchFeatures(dataset, assetId);
      if (!feats.length) {
        skipped.push({ assetId, dataset, reason: "no feature found" });
        return;
      }

      const fc = { type: "FeatureCollection", features: feats };
      const input = { "in.json": JSON.stringify(fc) };
      const outBase = safeName(assetId);

      if (format === "geojson") {
        const cmd = "-i in.json -proj wgs84 -o out.geojson";
        const files = await apply(cmd, input);
        const buf = files["out.geojson"];
        if (buf) zip.addFile(`${outBase}.geojson`, Buffer.isBuffer(buf) ? buf : Buffer.from(buf));
        return;
      }

      if (format === "kml") {
        const cmd = "-i in.json -proj wgs84 -o out.kml";
        const files = await apply(cmd, input);
        const buf = files["out.kml"];
        if (buf) zip.addFile(`${outBase}.kml`, Buffer.isBuffer(buf) ? buf : Buffer.from(buf));
        return;
      }

      // shapefile: write components and place them under a folder per asset
      if (format === "shp" || format === "shapefile") {
        const cmd = "-i in.json -proj wgs84 -o format=shapefile out.shp";
        const files = await apply(cmd, input);
        const partNames = ["out.shp", "out.shx", "out.dbf", "out.prj", "out.cpg"];
        for (const name of partNames) {
          if (!files[name]) continue;
          const ext = name.slice(name.lastIndexOf(".")); // ".shp"
          const target = `${outBase}/${outBase}${ext}`;
          const buf = files[name];
          zip.addFile(target, Buffer.isBuffer(buf) ? buf : Buffer.from(buf));
        }
        return;
      }

      throw new Error(`Unsupported format '${format}'`);
    }

    // Process with small concurrency
    await runPool(toProcess, 4, doOne);

    if (zip.getEntries().length === 0) {
      res.status(404).json({ error: "No features found for any row.", skipped });
      return;
    }

    const outName =
      format === "geojson" ? "mapbuddy_geojson.zip" :
      (format === "shp" || format === "shapefile") ? "mapbuddy_shapefile.zip" :
      "mapbuddy_kml.zip";

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="${outName}"`);
    res.status(200).send(zip.toBuffer());
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  }
}
