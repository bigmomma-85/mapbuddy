// /api/convert.js
// Converts an ArcGIS Feature (from dataset+assetId) OR client-supplied GeoJSON
// into a downloadable KML. Also returns X-Google-Maps-URL header for convenience.

export const runtime = "nodejs";               // <-- ensure Node on Vercel
export const config = { api: { bodyParser: false } };

// ---------- helpers ----------
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try { return JSON.parse(raw); } catch { throw new Error("Invalid JSON body"); }
}
function isFC(g) { return g && g.type === "FeatureCollection" && Array.isArray(g.features); }

function centroid(fc) {
  const pts = [];
  const push = ([x, y]) => Number.isFinite(x) && Number.isFinite(y) && pts.push([x, y]);
  function walk(g) {
    if (!g || !g.coordinates) return;
    const { type, coordinates } = g;
    switch (type) {
      case "Point": push(coordinates); break;
      case "MultiPoint":
      case "LineString": coordinates.forEach(push); break;
      case "MultiLineString":
      case "Polygon": coordinates.flat(1).forEach(push); break;
      case "MultiPolygon": coordinates.flat(2).forEach(push); break;
    }
  }
  (fc.features || []).forEach(f => walk(f.geometry));
  if (!pts.length) return null;
  const [sx, sy] = pts.reduce((a, [x, y]) => [a[0] + x, a[1] + y], [0, 0]);
  return { lng: sx / pts.length, lat: sy / pts.length };
}
function gmapsLink(c) { return c ? `https://www.google.com/maps?q=${c.lat},${c.lng}&z=19` : null; }

function whereExpr(idFields, value) {
  const safe = String(value ?? "").trim().replace(/'/g, "''");
  if (!safe) return "1=2";
  const enc = s => encodeURIComponent(s);
  const exact = idFields.map(f => `${enc(f)}='${enc(safe)}'`);
  const like  = idFields.map(f => `${enc(f)} like '%25${enc(safe)}%25'`);
  return `(${exact.join(" OR ")}) OR (${like.join(" OR ")})`;
}
async function fetchArcgisGeoJSON(base, where) {
  const url = `${base}/query?where=${where}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`ArcGIS request failed (${r.status})`);
  return await r.json();
}

// ---------- datasets ----------
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID","SITE_ID","SITE_CODE","NAME","PROJECT_ID"]
  },
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO","NAME","PROJECT_ID"]
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO","NAME","PROJECT_ID"]
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID","ASSET_ID","NAME","PROJECT_ID"]
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID","ASSET_ID","NAME","PROJECT_ID"]
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID","ASSET_ID","NAME","PROJECT_ID"]
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID","ASSET_ID","NAME","PROJECT_ID"]
  }
};

// ---------- robust Mapshaper runner ----------
async function toKmlWithMapshaper(featureCollection) {
  const mod = await import("mapshaper");
  const ms = mod.default || mod;

  // feed as Buffer and try every known signature
  const inputsBuf = { "in.json": Buffer.from(JSON.stringify(featureCollection), "utf8") };
  const outputs = {};
  const cmd = `-i in.json -o format=kml precision=0.000001 out.kml`;

  // try 1: modern applyCommands(opts)
  if (typeof ms.applyCommands === "function") {
    try {
      await ms.applyCommands(cmd, { inputs: inputsBuf, outputs });
    } catch (e1) {
      // try 2: legacy applyCommands(cmd, inputs)
      try {
        const out2 = await ms.applyCommands(cmd, inputsBuf);
        if (out2) Object.assign(outputs, out2);
      } catch (e2) {
        // try 3: legacy runCommands(cmd, inputs, cb)
        if (typeof ms.runCommands === "function") {
          await new Promise((resolve, reject) => {
            ms.runCommands(cmd, inputsBuf, (err, out) => {
              if (err) return reject(err);
              Object.assign(outputs, out || {});
              resolve();
            });
          });
        } else {
          throw e2;
        }
      }
    }
  } else if (typeof ms.runCommands === "function") {
    // only legacy available
    await new Promise((resolve, reject) => {
      ms.runCommands(cmd, inputsBuf, (err, out) => {
        if (err) return reject(err);
        Object.assign(outputs, out || {});
        resolve();
      });
    });
  } else {
    throw new Error("Mapshaper API not found (no applyCommands/runCommands)");
  }

  const kmlFile = outputs["out.kml"];
  if (!kmlFile) throw new Error("KML conversion failed (no out.kml produced)");
  return Buffer.isBuffer(kmlFile) ? kmlFile : Buffer.from(String(kmlFile), "utf8");
}

// ---------- handler ----------
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    if (req.method !== "POST") {
      return res.status(405).json({
        error: "POST required. Body: { assetId?, dataset?, geojson?, geojsonUrl?, filename? }"
      });
    }

    const { assetId, dataset, geojson, geojsonUrl, filename } = await readJson(req);

    // 1) Get FeatureCollection (either client-sent GeoJSON or server fetch from ArcGIS)
    let fc;
    let whereUsed;

    if (geojson || geojsonUrl) {
      const data = geojson
        ? (typeof geojson === "string" ? JSON.parse(geojson) : geojson)
        : await (await fetch(geojsonUrl)).json();
      if (!isFC(data)) return res.status(400).json({ error: "Provided GeoJSON is not a FeatureCollection" });
      fc = data;
    } else {
      const cfg = DATASETS[dataset];
      if (!cfg) return res.status(400).json({ error: `Unknown dataset: ${dataset}` });
      if (!assetId) return res.status(400).json({ error: "assetId is required when not sending geojson/geojsonUrl" });
      const where = whereExpr(cfg.idFields, assetId);
      whereUsed = where;
      const data = await fetchArcgisGeoJSON(cfg.base, where);
      if (!isFC(data) || !data.features.length) {
        return res.status(404).json({ error: "No feature found", dataset, where: decodeURIComponent(where) });
      }
      fc = data;
    }

    // 2) Extra goodies (centroid -> Google Maps)
    const c = centroid(fc);
    const mapsUrl = gmapsLink(c);

    // 3) Convert to KML via Mapshaper (robust)
    const kml = await toKmlWithMapshaper(fc);

    // 4) Download response
    const baseName =
      filename ||
      fc.features?.[0]?.properties?.FACILITY_ID ||
      fc.features?.[0]?.properties?.LOD_ID ||
      "export";

    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${baseName}.kml"`);
    if (mapsUrl) res.setHeader("X-Google-Maps-URL", mapsUrl);
    if (whereUsed) res.setHeader("X-Where", decodeURIComponent(whereUsed));
    return res.status(200).send(kml);

  } catch (err) {
    console.error("convert error:", err);
    return res.status(500).json({ error: "Conversion failed", details: String(err?.message || err) });
  }
}
