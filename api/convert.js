// api/convert.js
// Convert ONE ArcGIS feature (by ID) to KML / GeoJSON / Shapefile using Mapshaper

import axios from "axios";
export const config = { runtime: "nodejs" };

// ---------- Dataset registry ----------
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
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/0",
    idFields: ["SWM_FAC_NO", "ASSET_ID", "STRU_ID", "NAME", "PROJECT_ID", "STRUCTURE_ID", "STRUCT_ID", "FACILITY_ID", "FACILITYID"],
    label: "TMDL — Stormwater Control Structures",
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/1",
    idFields: ["SWM_FAC_NO", "ASSET_ID", "STRU_ID", "NAME", "PROJECT_ID", "STRUCTURE_ID", "STRUCT_ID", "FACILITY_ID", "FACILITYID"],
    label: "TMDL — Retrofits",
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/2",
    idFields: ["STRU_ID"],
    label: "TMDL — Tree Plantings",
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/3",
    idFields: ["ASSET_ID", "STRU_ID", "NAME", "PROJECT_ID", "STRUCTURE_ID", "STRUCT_ID", "FACILITY_ID", "FACILITYID"],
    label: "TMDL — Pavement Removals",
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/4",
    idFields: ["ASSET_ID", "STRU_ID", "NAME", "PROJECT_ID", "STRUCTURE_ID", "STRUCT_ID", "FACILITY_ID", "FACILITYID"],
    label: "TMDL — Stream Restorations",
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/5",
    idFields: ["ASSET_ID", "STRU_ID", "NAME", "PROJECT_ID", "STRUCTURE_ID", "STRUCT_ID", "FACILITY_ID", "FACILITYID"],
    label: "TMDL — Outfall Stabilizations",
  },

  // ✅ Unified Montgomery County (stormwater + buildings + communities + municipalities + tree inventory)
  // NOTE: we include address-friendly fields so spreadsheet addresses can match if present in attributes.
  montgomery_county: {
    // This "virtual" dataset has multiple underlying layers tried in order.
    // Each entry: { base, idFields, type: "point"|"polygon"|"line" }
    layers: [
      // DEP Stormwater Facilities (points) — address-friendly fields included
      {
        base: "https://depgis.montgomerycountymd.gov/arcgis/rest/services/DEP_Public/DEP_Stormwater/MapServer/2",
        idFields: ["ASSET", "SEQNO", "PROP_NAME", "P_ST_ADD", "Acct", "P_ACCT", "GRID", "INSP_REG", "Type_Desc"],
        type: "point",
        label: "Montgomery — DEP Stormwater Facilities (points)"
      },
      // County Buildings (polygons)
      {
        base: "https://gis.montgomerycountymd.gov/arcgis/rest/services/General/BLDG_PS/MapServer/0",
        idFields: ["NAME", "ALIAS_NAME", "AGENCYNAME", "ADDRESS", "FULLADDR"],
        type: "polygon",
        label: "Montgomery — County Buildings"
      },
      // Communities with municipalities (polygons)
      {
        base: "https://gis.montgomerycountymd.gov/arcgis/rest/services/General/communities_w_muni/MapServer/0",
        idFields: ["NAME", "COMMUNITY", "MUNICIPALITY", "MUNI_NAME"],
        type: "polygon",
        label: "Montgomery — Communities & Municipalities"
      },
      // Municipalities (polygons)
      {
        base: "https://gis.montgomerycountymd.gov/arcgis/rest/services/General/municipalities/MapServer/0",
        idFields: ["NAME", "MUNI_NAME"],
        type: "polygon",
        label: "Montgomery — Municipalities"
      },
      // Tree Inventory (points)
      {
        base: "https://gis.montgomerycountymd.gov/arcgis/rest/services/DOT/MCDOT_Tree_Inventory_FY22/MapServer/0",
        idFields: ["TREE_ID", "SITE_NAME", "STREETNAME", "ADDRNUM", "FULLADDR", "COMMUNITY"],
        type: "point",
        label: "Montgomery — Tree Inventory (FY22)"
      }
    ],
    label: "Montgomery County (Unified)"
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

// ---------- helpers ----------
async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

function isTmdlLike(id) {
  const s = String(id || "").trim().toUpperCase();
  return /^(\d+)\s*-?\s*(UT|TR|SR|OF|PR)$/.test(s);
}
function guessDatasetFromId(id) {
  if (!id) return null;
  const s = String(id).trim().toUpperCase();
  if (/^WP\d{3,}$/.test(s)) return "fairfax_bmps";
  if (s.startsWith("LOD_")) return "mdsha_landscape";
  if (isTmdlLike(s)) return "mdsha_tmdl_any";
  return null;
}

function looksLikeAddress(s) {
  const t = String(s || "").trim();
  // crude heuristic: has a number + a space + a word → likely an address
  return /\d+\s+\S+/.test(t);
}

function tmdlVariants(assetId) {
  const s = String(assetId || "").trim().toUpperCase();
  const m = s.match(/^(\d+)\s*-?\s*([A-Z]{2})$/);
  if (m) {
    const num = m[1], suf = m[2];
    return [ `${num}${suf}`, `${num}-${suf}`, `${num} ${suf}` ];
  }
  return [s, s.replace(/-/g,""), s.replace(/\s+/g,""), s.replace(/(\d+)([A-Za-z]+)/,'$1-$2'), s.replace(/(\d+)([A-Za-z]+)/,'$1 $2')];
}

const esc = (v) => String(v).replace(/'/g, "''");

function buildWhereExact(fields, value, useVariants) {
  const vals = useVariants ? tmdlVariants(value) : [String(value)];
  const ors = [];
  for (const f of fields) for (const v of vals) ors.push(`UPPER(${f}) = UPPER('${esc(v)}')`);
  return ors.join(" OR ");
}
function buildWhereLike(fields, value, useVariants) {
  const vals = useVariants ? tmdlVariants(value) : [String(value)];
  const patterns = new Set();
  for (const v of vals) {
    patterns.add(`%${v}%`);
    patterns.add(`%${v.replace(/-/g," ")}%`);
    patterns.add(`%${v.replace(/\s+/g,"-")}%`);
  }
  const ors = [];
  for (const f of fields) for (const p of patterns) ors.push(`UPPER(${f}) LIKE UPPER('${esc(p)}')`);
  return ors.join(" OR ");
}

function esriToGeoJSONFeature(f) {
  const a = f.attributes || {};
  const g = f.geometry || {};
  let geom = null;
  if (Array.isArray(g.rings)) {
    geom = { type: "Polygon", coordinates: g.rings };
  } else if (Array.isArray(g.paths)) {
    geom = g.paths.length === 1
      ? { type: "LineString", coordinates: g.paths[0] }
      : { type: "MultiLineString", coordinates: g.paths };
  } else if (typeof g.x === "number" && typeof g.y === "number") {
    geom = { type: "Point", coordinates: [g.x, g.y] };
  }
  return geom ? { type: "Feature", properties: a, geometry: geom } : null;
}

async function fetchArcgisAsGeoJSON(base, where) {
  const url = `${base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=json`;
  const r = await axios.get(url, { validateStatus: () => true, timeout: 20000 });
  if (r.status !== 200) throw new Error(`ArcGIS request failed (${r.status})`);
  const data = r.data || {};
  const arr = Array.isArray(data.features) ? data.features : [];
  const features = arr.map(esriToGeoJSONFeature).filter(Boolean);
  return { type: "FeatureCollection", features };
}

async function getMapshaperApply() {
  const ms = await import("mapshaper");
  return ms.applyCommands || (ms.default && ms.default.applyCommands);
}

// ---------- Montgomery County Geocoder (fallback for addresses) ----------
async function geocodeMontgomery(singleLine) {
  const url = "https://gis.montgomerycountymd.gov/geocoding/rest/services/Locators/CompositeLocator/GeocodeServer/findAddressCandidates";
  const params = {
    SingleLine: singleLine,
    outSR: 4326,
    f: "pjson",
    maxLocations: 1
  };
  const r = await axios.get(url, { params, timeout: 20000, validateStatus: () => true });
  if (r.status !== 200) return null;
  const cands = r.data?.candidates || [];
  const top = cands[0];
  if (!top?.location) return null;
  const { x, y } = top.location;
  if (typeof x !== "number" || typeof y !== "number") return null;
  return {
    type: "FeatureCollection",
    features: [{
      type: "Feature",
      properties: {
        _geocoded: true,
        _sourceLabel: "Montgomery County Geocoder",
        MatchedAddress: top.address || singleLine,
        Score: top.score
      },
      geometry: { type: "Point", coordinates: [x, y] }
    }]
  };
}

// ---------- main ----------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST { assetId, dataset?, format? }" });
      return;
    }

    const { assetId, dataset: inputDataset, format } = await readJson(req);
    if (!assetId) { res.status(400).json({ error: "assetId is required" }); return; }

    let datasetKey = inputDataset || guessDatasetFromId(assetId);
    if (!datasetKey) { res.status(400).json({ error: "Unknown dataset. Pick one or use a recognizable ID." }); return; }

    const targets = datasetKey === "mdsha_tmdl_any" ? TMDL_ANY : [datasetKey];

    // Pass A: exact; Pass B: LIKE
    const passes = ["exact", "like"];
    let fc = null;

    for (const pass of passes) {
      for (const key of targets) {
        const ds = DATASETS[key];
        if (!ds) continue;

        // Unified Montgomery: iterate its inner layers
        if (key === "montgomery_county") {
          for (const lyr of ds.layers) {
            const where = pass === "exact"
              ? buildWhereExact(lyr.idFields, assetId, false)
              : buildWhereLike (lyr.idFields, assetId, false);

            try {
              const gj = await fetchArcgisAsGeoJSON(lyr.base, where);
              if (gj.features.length) {
                const feat = gj.features[0];
                feat.properties = {
                  ...feat.properties,
                  _assetId: assetId,
                  _dataset: key,
                  _sourceLabel: lyr.label,
                  _matchMode: pass
                };
                fc = { type: "FeatureCollection", features: [feat] };
                break;
              }
            } catch { /* keep trying */ }
          }
          if (fc) break;
          continue;
        }

        // All other single-layer datasets
        const useVariants = key.startsWith("mdsha_tmdl_");
        const where = pass === "exact"
          ? buildWhereExact(DATASETS[key].idFields, assetId, useVariants)
          : buildWhereLike (DATASETS[key].idFields, assetId, useVariants);

        const gj = await fetchArcgisAsGeoJSON(DATASETS[key].base, where);
        if (gj.features.length) {
          const feat = gj.features[0];
          feat.properties = { ...feat.properties, _assetId: assetId, _dataset: key, _sourceLabel: DATASETS[key].label, _matchMode: pass };
          fc = { type: "FeatureCollection", features: [feat] };
          break;
        }
      }
      if (fc) break;
    }

    // ✅ Geocoder fallback for Montgomery County if no GIS feature was matched
    if (!fc && targets.length === 1 && targets[0] === "montgomery_county" && looksLikeAddress(assetId)) {
      const gj = await geocodeMontgomery(assetId);
      if (gj?.features?.length) {
        fc = gj;
      }
    }

    if (!fc) {
      res.status(404).json({ error: `No feature found for '${assetId}' in ${targets.join(", ")}` });
      return;
    }

    // Convert via mapshaper
    const outFmtRaw = (format || "kml").toLowerCase();
    const outFmt = outFmtRaw === "geojson" ? "geojson"
                  : (["shapefile","shp","zip","shpzip"].includes(outFmtRaw) ? "shapefile" : "kml");

    const msApply = await getMapshaperApply();
    if (!msApply) throw new Error("Mapshaper API not available");

    const input = { "in.json": Buffer.from(JSON.stringify(fc)) };
    const outName = outFmt === "kml" ? "out.kml" : (outFmt === "geojson" ? "out.geojson" : "out.zip");
    const cmd = `-i in.json -clean -o ${outName} format=${outFmt}`;

    let outputs;
    try {
      outputs = await msApply(cmd, input);
    } catch (err) {
      throw new Error("Mapshaper failed: " + (err.message || err));
    }

    if (outputs['stderr']) {
      throw new Error('Mapshaper error: ' + outputs['stderr'].toString());
    }
    const buf = outputs[outName];
    if (!buf) throw new Error(`Mapshaper produced no ${outName}, outputs: ${Object.keys(outputs).join(", ")}`);

    const safe = String(assetId).replace(/[^\w.-]+/g, "_");
    if (outFmt === "kml") {
      res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.kml"`);
    } else if (outFmt === "geojson") {
      res.setHeader("Content-Type", "application/geo+json");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.geojson"`);
    } else {
      res.setHeader("Content-Type", "application/zip");
      res.setHeader("Content-Disposition", `attachment; filename="${safe}.zip"`);
    }
    res.status(200).send(buf);

  } catch (err) {
    res.status(500).json({ error: "Conversion failed", details: String(err?.message || err), stack: err?.stack });
  }
}
