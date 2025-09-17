// api/convert.js
// Fetch a feature from ArcGIS (GeoJSON), convert to KML via Mapshaper, return as a download.
export const config = { runtime: "nodejs18.x" };

import axios from "axios";
import * as mapshaper from "mapshaper";

/* --------------------------- DATASETS (must match locate.js) --------------------------- */
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7) | field: FACILITY_ID
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },

  // MDOT SHA Managed Landscape (Layer 0) | field: LOD_ID  (VPN may be required)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },

  // -------- MDOT SHA — TMDL (VPN may be required) --------
  // virtual "any" dataset to auto-detect a match
  mdsha_tmdl_any: {
    any: true,
    layers: [
      "mdsha_tmdl_structures",
      "mdsha_tmdl_retrofits",
      "mdsha_tmdl_tree_plantings",
      "mdsha_tmdl_pavement_removals",
      "mdsha_tmdl_stream_restorations",
      "mdsha_tmdl_outfall_stabilizations",
    ],
  },

  // 0: Stormwater Control Structures
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },

  // 1: Retrofits
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },

  // 2: Tree Plantings
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },

  // 3: Pavement Removals
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },

  // 4: Stream Restorations
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },

  // 5: Outfall Stabilizations
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },
};
/* --------------------------------------------------------------------------------------- */

function buildWhere(idFields, value) {
  const esc = String(value).replace(/'/g, "''");
  return idFields.map((f) => `${f}= '${esc}'`).join(" OR ");
}

async function queryGeoJSON(base, idFields, assetId) {
  const url =
    `${base}/query?` +
    `where=${encodeURIComponent(buildWhere(idFields, assetId))}` +
    `&outFields=*` +
    `&returnGeometry=true` +
    `&outSR=4326` +
    `&f=geojson`;
  const { data } = await axios.get(url, { timeout: 20000 });
  if (!data || !data.features || !data.features.length) return null;
  // Return only the first match as a single-feature FeatureCollection
  return {
    type: "FeatureCollection",
    features: [data.features[0]],
  };
}

async function fetchFeatureFC(datasetKey, assetId) {
  const ds = DATASETS[datasetKey];
  if (!ds) throw new Error(`Unknown dataset '${datasetKey}'`);

  if (ds.any) {
    for (const key of ds.layers) {
      const fc = await queryGeoJSON(DATASETS[key].base, DATASETS[key].idFields, assetId);
      if (fc) return fc;
    }
    return null;
  }
  return queryGeoJSON(ds.base, ds.idFields, assetId);
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const text = Buffer.concat(chunks).toString("utf8");
  return JSON.parse(text || "{}");
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJsonBody(req);
    const assetId = (body.assetId || "").trim();
    const dataset = (body.dataset || "").trim();
    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    // 1) Get a single-feature FeatureCollection from ArcGIS
    const featureFC = await fetchFeatureFC(dataset, assetId);
    if (!featureFC) {
      res.status(404).json({ error: `No feature found for '${assetId}' in '${dataset}'` });
      return;
    }

    // 2) Convert GeoJSON → KML using Mapshaper (ESM: use runCommands)
    const ms = mapshaper.default || mapshaper; // be robust to ESM/CJS shapes
    const filesIn = { "in.json": JSON.stringify(featureFC) };

    // Simplest robust command: read geojson and write kml
    const cmd = `-i in.json -o out.kml format=kml`;
    const out = await ms.runCommands(cmd, filesIn);

    // Varying shapes depending on version; pull the bytes safely
    const outFile = out["out.kml"];
    let buf;
    if (outFile?.content instanceof Uint8Array) {
      buf = Buffer.from(outFile.content);
    } else if (outFile instanceof Uint8Array) {
      buf = Buffer.from(outFile);
    } else if (typeof outFile === "string") {
      buf = Buffer.from(outFile, "utf8");
    } else {
      throw new Error("Unexpected Mapshaper output format");
    }

    // 3) Return file
    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}.kml"`);
    res.status(200).send(buf);
  } catch (e) {
    res.status(500).json({
      error: "Conversion failed",
      details: e?.message || String(e),
    });
  }
}
