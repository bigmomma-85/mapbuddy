// /api/convert.js
// Converts an ArcGIS Feature (looked up by Asset/BMP ID) *or* raw GeoJSON into a KML download.
// Also returns a Google Maps link via header X-Google-Maps-URL for convenience.

export const config = {
  api: { bodyParser: false },
};

// ------------------ helpers ------------------
function enableCORS(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try { return JSON.parse(raw); } catch { throw new Error("Invalid JSON body"); }
}

function buildWhere(idFields, input) {
  const safe = String(input ?? "").trim().replace(/'/g, "''");
  if (!safe) return "1=2";
  const exact = idFields.map(f => `${encodeURIComponent(f)}='${encodeURIComponent(safe)}'`);
  const like  = idFields.map(f => `${encodeURIComponent(f)} like '%25${encodeURIComponent(safe)}%25'`);
  return `(${exact.join(" OR ")}) OR (${like.join(" OR ")})`;
}

async function fetchArcgisGeoJSON(base, where) {
  const url = `${base}/query?where=${where}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`ArcGIS request failed (${r.status})`);
  return await r.json();
}

function isFeatureCollection(g) {
  return g && g.type === "FeatureCollection" && Array.isArray(g.features);
}

function centroidOfFeatureCollection(fc) {
  const pts = [];
  const push = ([x, y]) => Number.isFinite(x) && Number.isFinite(y) && pts.push([x, y]);

  function walk(geom) {
    if (!geom || !geom.coordinates) return;
    const { type, coordinates } = geom;
    switch (type) {
      case "Point": push(coordinates); break;
      case "MultiPoint":
      case "LineString": coordinates.forEach(push); break;
      case "MultiLineString":
      case "Polygon": coordinates.flat(1).forEach(push); break;
      case "MultiPolygon": coordinates.flat(2).forEach(push); break;
    }
  }

  for (const f of fc.features || []) walk(f.geometry);
  if (!pts.length) return null;
  const [sx, sy] = pts.reduce((a, [x, y]) => [a[0] + x, a[1] + y], [0, 0]);
  return { lng: sx / pts.length, lat: sy / pts.length };
}

function googleMapsLinkFromCentroid(c) {
  return c ? `https://www.google.com/maps?q=${c.lat},${c.lng}&z=19` : null;
}

// ------------------ datasets ------------------
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7)
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },

  // MDOT SHA Managed Landscape (Layer 0)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID", "SITE_ID", "SITE_CODE", "NAME", "PROJECT_ID"]
  },

  // TMDL Bay Restoration — Stormwater Control Structures
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },

  // TMDL Bay Restoration — Retrofits
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },

  // TMDL Bay Restoration — Tree Plantings
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },

  // TMDL Bay Restoration — Pavement Removals
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },

  // TMDL Bay Restoration — Stream Restorations
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },

  // TMDL Bay Restoration — Outfall Stabilizations
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  }
};

// ------------------ handler ------------------
export default async function handler(req, res) {
  enableCORS(res);
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    if (req.method !== "POST") {
      return res.status(405).json({
        error: "Use POST with JSON body { assetId?, dataset?, geojson?, geojsonUrl?, filename? }"
      });
    }

    const body = await readJsonBody(req);
    const { assetId, dataset, geojson, geojsonUrl, filename } = body;

    // Build a FeatureCollection (fc) either from client-supplied GeoJSON (VPN mode) or by server fetching ArcGIS
    let fc;
    let whereUsed = null;

    if (geojson || geojsonUrl) {
      const data = geojson
        ? (typeof geojson === "string" ? JSON.parse(geojson) : geojson)
        : await (await fetch(geojsonUrl)).json();

      if (!isFeatureCollection(data)) {
        return res.status(400).json({ error: "Provided GeoJSON is not a FeatureCollection" });
      }
      fc = data;
    } else {
      const cfg = DATASETS[dataset];
      if (!cfg) return res.status(400).json({ error: `Unknown dataset: ${dataset}` });
      if (!assetId) return res.status(400).json({ error: "assetId is required when not sending geojson/geojsonUrl" });

      const where = buildWhere(cfg.idFields, assetId);
      whereUsed = where;
      const data = await fetchArcgisGeoJSON(cfg.base, where);
      if (!isFeatureCollection(data) || !data.features.length) {
        return res.status(404).json({ error: "No feature found for given assetId", dataset, where: decodeURIComponent(where) });
      }
      fc = data;
    }

    // Google Maps link (centroid)
    const center = centroidOfFeatureCollection(fc);
    const mapsUrl = googleMapsLinkFromCentroid(center);

    // ---- Mapshaper conversion (robust, Buffer inputs, both APIs supported) ----
    const mod = await import("mapshaper");
    const ms = mod.default || mod;

    const inputs = { "in.json": Buffer.from(JSON.stringify(fc), "utf8") };
    const outputs = {};
    const cmd = `-i in.json -o format=kml precision=0.000001 out.kml`;

    async function runMapshaper() {
      if (typeof ms.applyCommands === "function") {
        await ms.applyCommands(cmd, { inputs, outputs });
      } else if (typeof ms.runCommands === "function") {
        await new Promise((resolve, reject) => {
          ms.runCommands(cmd, inputs, (err, out) => {
            if (err) return reject(err);
            Object.assign(outputs, out || {});
            resolve();
          });
        });
      } else {
        throw new Error("Mapshaper API not found (no applyCommands/runCommands)");
      }
    }

    await runMapshaper();

    const kmlBuf = outputs["out.kml"];
    if (!kmlBuf) throw new Error("KML conversion failed (no out.kml produced)");
    const kml = Buffer.isBuffer(kmlBuf) ? kmlBuf : Buffer.from(String(kmlBuf), "utf8");

    // Send as attachment
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
