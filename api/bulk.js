// api/bulk.js
// Bulk convert many asset IDs into ONE download (KML / GeoJSON / Shapefile)

import axios from "axios";
export const config = { runtime: "nodejs" };

// ---- same dataset registry as convert.js (keep in sync) ----
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
    label: "Fairfax — Stormwater Facilities",
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
    label: "MDOT SHA — Managed Landscape",
  },
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
  }
};
const TMDL_ANY = [
  "mdsha_tmdl_structures",
  "mdsha_tmdl_retrofits",
  "mdsha_tmdl_tree_plantings",
  "mdsha_tmdl_pavement_removals",
  "mdsha_tmdl_stream_restorations",
  "mdsha_tmdl_outfall_stabilizations",
];

function guessDatasetFromId(id) {
  if (!id) return null;
  const s = id.trim().toUpperCase();
  if (/^WP\d{3,}$/.test(s)) return "fairfax_bmps";
  if (s.startsWith("LOD_")) return "mdsha_landscape";
  if (/(UT|TR|SR|OF|PR)\s*$/.test(s)) return "mdsha_tmdl_any";
  return null;
}

async function readJson(req) {
  const chunks = []; for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); } catch { return {}; }
}
function buildWhere(idFields, assetId) {
  const esc = (v) => `'${String(v).replace(/'/g, "''")}'`;
  return idFields.map((f) => `${f}=${esc(assetId)}`).join(" OR ");
}
async function fetchArcgisGeoJSON(base, where) {
  const u1 = `${base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  let r = await axios.get(u1, { validateStatus: () => true });
  if (r.status === 200 && r.data && (r.data.type === "FeatureCollection" || r.data.type === "Feature")) {
    return r.data;
  }
  const u2 = `${base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=json`;
  r = await axios.get(u2, { validateStatus: () => true });
  if (r.status !== 200 || !r.data || !Array.isArray(r.data.features)) return { type: "FeatureCollection", features: [] };
  const features = r.data.features.map(esriToGeoJSONFeature).filter(Boolean);
  return { type: "FeatureCollection", features };
}
function esriToGeoJSONFeature(f) {
  const a = f.attributes || {}, g = f.geometry || {};
  let geom = null;
  if (Array.isArray(g.rings)) geom = { type: "Polygon", coordinates: g.rings };
  else if (Array.isArray(g.paths)) geom = g.paths.length === 1
    ? { type: "LineString", coordinates: g.paths[0] }
    : { type: "MultiLineString", coordinates: g.paths };
  else if (typeof g.x === "number" && typeof g.y === "number")
    geom = { type: "Point", coordinates: [g.x, g.y] };
  return geom ? { type: "Feature", properties: a, geometry: geom } : null;
}
async function getMapshaperApply() {
  const ms = await import("mapshaper");
  return ms.applyCommands || (ms.default && ms.default.applyCommands);
}
function castOutput(format) {
  const f = (format || "kml").toLowerCase();
  if (f === "geojson") return "geojson";
  if (f === "shapefile" || f === "shp" || f === "zip") return "shapefile";
  return "kml";
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST { items: [{assetId,dataset?},...], format? }" });
      return;
    }
    const { items, format } = await readJson(req);
    if (!Array.isArray(items) || items.length === 0) {
      res.status(400).json({ error: "No items provided" }); return;
    }

    const outFmt = castOutput(format);
    const features = [];
    const missed = [];

    for (const row of items) {
      const assetId = String(row?.assetId || "").trim();
      if (!assetId) { missed.push({ assetId, reason: "empty id" }); continue; }

      let datasetKey = row?.dataset || guessDatasetFromId(assetId);
      const targets = datasetKey === "mdsha_tmdl_any"
        ? TMDL_ANY
        : (DATASETS[datasetKey] ? [datasetKey] : []);

      if (targets.length === 0) {
        missed.push({ assetId, reason: "unknown dataset" }); continue;
      }

      let found = false;
      for (const key of targets) {
        const ds = DATASETS[key];
        const where = buildWhere(ds.idFields, assetId);
        const fc = await fetchArcgisGeoJSON(ds.base, where);
        const list = fc.type === "Feature" ? [fc] : (fc.features || []);
        if (list.length > 0) {
          const ft = list[0];
          ft.properties = {
            ...ft.properties,
            _assetId: assetId,
            _dataset: key,
            _sourceLabel: ds.label
          };
          features.push(ft);
          found = true;
          break;
        }
      }
      if (!found) missed.push({ assetId, reason: "not found" });
    }

    if (!features.length) {
      res.status(404).json({ error: "No features found", missed }); return;
    }

    const fc = { type: "FeatureCollection", features };
    const msApply = await getMapshaperApply();
    if (!msApply) throw new Error("Mapshaper API not available");

    const input = { "in.json": Buffer.from(JSON.stringify(fc)) };
    const outName = outFmt === "kml" ? "out.kml" : (outFmt === "geojson" ? "out.geojson" : "out.zip");
    const cmd = `-i in.json -clean -o ${outName} format=${outFmt}`;

    const outputs = await msApply(cmd, input);
    const buf = outputs[outName];
    if (!buf) throw new Error(`Mapshaper produced no ${outName}`);

    // Attach a tiny JSON summary header via filename comment only (no body change)
    const ts = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0,14);
    const base = `mapbuddy_bulk_${ts}`;

    if (outFmt === "kml") {
      res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
      res.setHeader("Content-Disposition", `attachment; filename="${base}.kml"`);
    } else if (outFmt === "geojson") {
      res.setHeader("Content-Type", "application/geo+json");
      res.setHeader("Content-Disposition", `attachment; filename="${base}.geojson"`);
    } else {
      res.setHeader("Content-Type", "application/zip");
      res.setHeader("Content-Disposition", `attachment; filename="${base}.zip"`);
    }
    // Include X-MapBuddy-Missed header (count)
    res.setHeader("X-MapBuddy-Missed", String(missed.length));
    if (missed.length) res.setHeader("X-MapBuddy-Missed-Example", JSON.stringify(missed.slice(0,3)));

    res.status(200).send(buf);

  } catch (err) {
    res.status(500).json({ error: "Bulk conversion failed", details: String(err?.message || err) });
  }
}
