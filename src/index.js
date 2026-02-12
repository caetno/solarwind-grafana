// NOAA -> Influx line protocol -> Grafana Cloud, with KV state + cron
const OVERLAP_MS = 10 * 60 * 1000;
const REQUEST_TIMEOUT_MS = 15000;

const MAP_WIND = {
  speed_km_s: "proton_speed",
  density_cm3: "proton_density",
  temperature_k: "proton_temperature",
};

const MAP_MAG = {
  bx_gsm_nt: "bx_gsm",
  by_gsm_nt: "by_gsm",
  bz_gsm_nt: "bz_gsm",
  bt_nt: "bt",
};

function toMs(t) {
  const ms = Date.parse(String(t));
  return Number.isFinite(ms) ? ms : null;
}

function isFiniteNumber(x) {
  if (x === null || x === undefined) return false;
  if (typeof x === "number") return Number.isFinite(x);
  const s = String(x).trim().toLowerCase();
  if (s === "" || s === "null" || s === "nan") return false;
  const n = Number(s);
  return Number.isFinite(n);
}

function toNumber(x) {
  return typeof x === "number" ? x : Number(String(x).trim());
}

function pickNumericFields(row, fieldMap) {
  const out = {};
  for (const [destKey, srcKey] of Object.entries(fieldMap)) {
    const v = row?.[srcKey];
    if (isFiniteNumber(v)) out[destKey] = toNumber(v);
  }
  return out;
}

function normalizePoints(rows, { fieldMap, tagKeys = [] }) {
  return (rows || []).flatMap((r) => {
    const tms = toMs(r?.time_tag);
    if (!tms) return [];

    const fields = pickNumericFields(r, fieldMap);
    if (!Object.keys(fields).length) return [];

    const tags = {};
    for (const k of tagKeys) tags[k] = r?.[k] ?? "unknown";

    return [{ tms, ...tags, ...fields }];
  });
}

function sliceNew(rows, lastTsMs, bootstrapSinceMs) {
  if (!Array.isArray(rows)) return [];
  const since = lastTsMs
    ? lastTsMs - OVERLAP_MS
    : (bootstrapSinceMs ?? null);

  const seen = new Set();
  const out = [];

  for (const r of rows) {
    const t = r?.time_tag;
    const tms = t ? toMs(t) : null;
    if (!tms) continue;
    if (since !== null && tms <= since) continue;

    if (seen.has(t)) continue;
    seen.add(t);
    out.push(r);
  }

  out.sort((a, b) => (toMs(a.time_tag) ?? 0) - (toMs(b.time_tag) ?? 0));
  return out;
}

function latestTsMs(rows) {
  if (!rows?.length) return null;
  const tms = rows.map((r) => toMs(r.time_tag)).filter(Boolean);
  return tms.length ? Math.max(...tms) : null;
}

async function fetchJson(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: { accept: "application/json" },
      signal: controller.signal,
    });
    if (!r.ok) throw new Error(`Fetch failed ${r.status}: ${await r.text()}`);
    return await r.json();
  } finally {
    clearTimeout(timer);
  }
}

// Influx line protocol encoder (your Step2)
function escTag(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/,/g, "\\,").replace(/ /g, "\\ ").replace(/=/g, "\\=");
}
function escMeas(m) {
  return String(m).replace(/\\/g, "\\\\").replace(/,/g, "\\,").replace(/ /g, "\\ ");
}
function line(measurement, tags, fields, tms) {
  const tagStr = Object.entries(tags || {}).map(([k, v]) => `${k}=${escTag(v)}`).join(",");
  const fieldStr = Object.entries(fields || {})
    .map(([k, v]) => (Number.isFinite(v) ? `${k}=${v}` : null))
    .filter(Boolean)
    .join(",");
  if (!fieldStr) return null;

  const tsNs = BigInt(tms) * 1000000n; // ms -> ns
  return `${escMeas(measurement)}${tagStr ? "," + tagStr : ""} ${fieldStr} ${tsNs}`;
}

function basicAuthHeader(user, pass) {
  const bytes = new TextEncoder().encode(`${user}:${pass}`);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return "Basic " + btoa(bin);
}

async function pushToGrafana(env, influxBody) {
  const r = await fetch(env.GRAFANA_INFLUX_URL, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      Authorization: basicAuthHeader(env.GRAFANA_USER, env.GRAFANA_API_KEY),
    },
    body: influxBody,
  });

  if (!r.ok) {
    const body = await r.text();
    throw new Error(
      JSON.stringify(
        { status: r.status, url: env.GRAFANA_INFLUX_URL, body: body.slice(0, 500) },
        null,
        2
      )
    );
  }
}

async function run(env) {
  // Load last timestamps from KV
  const state = (await env.STATE.get("noaa_state", "json")) || {};
  const last = {
    k: state.k ?? null,
    wind: state.wind ?? null,
    mag: state.mag ?? null,
  };

  // Optional bootstrap lookback for first run (limits initial payload size)
  const lookbackMin = Number(env.BOOTSTRAP_LOOKBACK_MINUTES ?? 180);
  const bootstrapSinceMs =
    (!last.k && !last.wind && !last.mag && lookbackMin > 0)
      ? Date.now() - lookbackMin * 60 * 1000
      : null;

  // Fetch in parallel
  const [kRaw, windRaw, magRaw] = await Promise.all([
    fetchJson(env.NOAA_KP_URL),
    fetchJson(env.NOAA_WIND_URL),
    fetchJson(env.NOAA_MAG_URL),
  ]);

  // Slice new with overlap + dedupe
  const kNewRaw = sliceNew(kRaw, last.k, bootstrapSinceMs);
  const windNewRaw = sliceNew(windRaw, last.wind, bootstrapSinceMs);
  const magNewRaw = sliceNew(magRaw, last.mag, bootstrapSinceMs);

  // Normalize points (your Step1)
  const kPoints = kNewRaw.flatMap((r) => {
    const tms = toMs(r?.time_tag);
    if (!tms) return [];

    let kpVal = null;
    if (isFiniteNumber(r?.kp)) kpVal = toNumber(r.kp);
    else if (isFiniteNumber(r?.kp_index)) kpVal = toNumber(r.kp_index);

    return kpVal === null ? [] : [{ tms, kp: kpVal }];
  });

  const windPoints = normalizePoints(windNewRaw, { fieldMap: MAP_WIND, tagKeys: ["source"] });
  const magPoints = normalizePoints(magNewRaw, { fieldMap: MAP_MAG, tagKeys: ["source"] });

  // Encode Influx (your Step2)
  const lines = [];

  for (const p of kPoints) {
    const l = line("spaceweather_kp", { source: "noaa" }, { kp: p.kp }, p.tms);
    if (l) lines.push(l);
  }

  for (const p of windPoints) {
    const { tms, source, ...fields } = p;
    const l = line("solarwind_plasma", { source }, fields, tms);
    if (l) lines.push(l);
  }

  for (const p of magPoints) {
    const { tms, source, ...fields } = p;
    const l = line("solarwind_mag", { source }, fields, tms);
    if (l) lines.push(l);
  }

  const proposedState = {
    k: latestTsMs(kNewRaw) ?? last.k,
    wind: latestTsMs(windNewRaw) ?? last.wind,
    mag: latestTsMs(magNewRaw) ?? last.mag,
  };

  console.log(
    `Fetched new: kp=${kPoints.length}, wind=${windPoints.length}, mag=${magPoints.length} | encoded=${lines.length}`
  );

  if (!lines.length) {
    return { pushed: 0, committed: false, state: last };
  }

  const influxBody = lines.join("\n") + "\n";

  // Push + commit state only after success (your Step3)
  await pushToGrafana(env, influxBody);
  await env.STATE.put("noaa_state", JSON.stringify(proposedState));

  console.log(`Pushed ${lines.length} lines and committed state`);
  return { pushed: lines.length, committed: true, state: proposedState };
}

export default {
  // Cron trigger entry point
  async scheduled(_event, env, ctx) {
    ctx.waitUntil(run(env));
  },

  // Optional manual trigger endpoint (for testing without CLI)
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return new Response("ok\n", { status: 200 });
    }

    if (url.pathname === "/run") {
      const token = url.searchParams.get("token");
      if (!env.RUN_TOKEN || token !== env.RUN_TOKEN) {
        return new Response("unauthorized\n", { status: 401 });
      }
      try {
        const result = await run(env);
        return new Response(JSON.stringify(result, null, 2) + "\n", {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      } catch (e) {
        return new Response(String(e?.message || e) + "\n", { status: 500 });
      }
    }

    return new Response("not found\n", { status: 404 });
  },
};
