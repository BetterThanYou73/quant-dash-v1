// QuantEdge frontend logic.
// Plain vanilla JS — no build step, no framework. Loads config.js first,
// then runs on DOMContentLoaded.
//
// Conventions:
//   - One fetch* function per endpoint.
//   - One render* function per panel.
//   - All DOM access goes through document.getElementById (no querySelector
//     soup) so it's obvious which IDs in index.html each function owns.

const API = window.QD_CONFIG.API_BASE;

// --- tiny utilities -------------------------------------------------------

const fmtPct = (x, digits = 2) =>
  (x === null || x === undefined || Number.isNaN(x))
    ? "—"
    : (x * 100).toFixed(digits) + "%";

const fmtPrice = (x) =>
  (x === null || x === undefined || Number.isNaN(x))
    ? "—"
    : "$" + Number(x).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

const fmtNum = (x, digits = 3) =>
  (x === null || x === undefined || Number.isNaN(x))
    ? "—"
    : Number(x).toFixed(digits);

// Wraps fetch with: JSON parsing, error handling, JSON-error body extraction.
// Throws on non-2xx so callers can use try/catch instead of nested .then chains.
async function apiGet(path) {
  const url = `${API}${path}`;
  const res = await fetch(url);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json();
}

// --- status dot -----------------------------------------------------------

function setStatus(state, title) {
  // state: "live" | "stale" | "dead"
  const dot = document.getElementById("status-dot");
  dot.classList.remove("stale", "dead");
  if (state === "stale") dot.classList.add("stale");
  if (state === "dead")  dot.classList.add("dead");
  dot.title = title || state;
}

// --- /api/health ----------------------------------------------------------

async function pingHealth() {
  try {
    const data = await apiGet("/api/health");
    setStatus("live", `API ${data.version} · ${data.time_utc}`);
  } catch (e) {
    setStatus("dead", `API unreachable: ${e.message}`);
  }
}

// --- /api/signals → table + stat strip ------------------------------------

// Map our backend's "Long Candidate" / "Watch" / "High Risk" to the
// template's BUY / HOLD / SELL visual language. We're not actually changing
// the model — just translating to the language traders are used to.
function signalClass(signal) {
  switch (signal) {
    case "Long Candidate": return { label: "▲ BUY",  cls: "signal-buy"  };
    case "High Risk":      return { label: "▼ AVOID", cls: "signal-sell" };
    default:               return { label: "● WATCH", cls: "signal-hold" };
  }
}

function confidenceTier(score) {
  // Score is in [0,1]. Bin it into colors that match the template.
  if (score >= 0.65) return "high";
  if (score >= 0.40) return "mid";
  return "low";
}

function renderSignalsTable(rows) {
  // Build the table as one HTML string and assign once. Faster + simpler than
  // creating elements one-by-one and avoids partial-render flicker.
  const head = `
    <table class="signals-table">
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Signal</th>
          <th>Profitability Score</th>
          <th>Price</th>
          <th>21d Mom</th>
          <th>20d Vol</th>
          <th>5% CVaR</th>
          <th>63d DD</th>
          <th>Sector</th>
        </tr>
      </thead>
      <tbody>`;

  const body = rows.map(r => {
    const sig = signalClass(r.Signal);
    const score = r["Profitability Score"];
    const tier = confidenceTier(score);
    const mom = r["21d Momentum Numeric"];
    const dd = r["Max Drawdown 63d Numeric"];
    return `
      <tr>
        <td>
          <div class="sym-col">${r.Ticker}</div>
          <div class="sym-sub">${r.Sector || ""}</div>
        </td>
        <td><span class="${sig.cls}">${sig.label}</span></td>
        <td>
          <div style="display:flex;align-items:center;gap:8px">
            <div class="conf-bar"><div class="conf-fill ${tier}" style="width:${(score * 100).toFixed(0)}%"></div></div>
            <span style="font-size:10px">${(score * 100).toFixed(0)}%</span>
          </div>
        </td>
        <td class="price-col">${fmtPrice(r.Price)}</td>
        <td class="${mom >= 0 ? 'chg-col up' : 'chg-col dn'}">${fmtPct(mom)}</td>
        <td>${fmtNum(r["20d Volatility Numeric"], 3)}</td>
        <td class="chg-col dn">${fmtPct(r["5% CVaR Numeric"])}</td>
        <td class="chg-col dn">${fmtPct(dd)}</td>
        <td><span class="card-badge badge-blue">${r.Sector || "Unknown"}</span></td>
      </tr>`;
  }).join("");

  document.getElementById("signals-body").innerHTML = head + body + "</tbody></table>";
}

function renderStatStrip(payload) {
  const rows = payload.results;
  if (!rows.length) return;

  // Top candidate = first row (already sorted by score desc on the server).
  const top = rows[0];

  // Highest volatility ticker
  const mostVol = rows.reduce((a, b) =>
    (b["20d Volatility Numeric"] > a["20d Volatility Numeric"]) ? b : a
  );

  // Worst CVaR (most negative)
  const worstCvar = rows.reduce((a, b) =>
    (b["5% CVaR Numeric"] < a["5% CVaR Numeric"]) ? b : a
  );

  const longCount = rows.filter(r => r.Signal === "Long Candidate").length;

  document.getElementById("stat-top").textContent = top.Ticker;
  document.getElementById("stat-top-sub").textContent = `${top.Signal} · score ${fmtNum(top["Profitability Score"], 2)}`;

  document.getElementById("stat-vol").textContent = mostVol.Ticker;
  document.getElementById("stat-vol-sub").textContent = `${fmtNum(mostVol["20d Volatility Numeric"], 3)} ann.`;

  document.getElementById("stat-cvar").textContent = worstCvar.Ticker;
  document.getElementById("stat-cvar-sub").textContent = `${fmtPct(worstCvar["5% CVaR Numeric"])} on bad days`;

  document.getElementById("stat-universe").textContent = payload.scored_count;
  document.getElementById("stat-universe-sub").textContent = `Lookback ${payload.lookback_days}d`;

  document.getElementById("stat-long").textContent = longCount;

  document.getElementById("stat-cache").textContent = payload.as_of_utc
    ? new Date(payload.as_of_utc).toLocaleString()
    : "unknown";
}

async function loadSignals() {
  try {
    const data = await apiGet("/api/signals");
    renderSignalsTable(data.results);
    renderStatStrip(data);
  } catch (e) {
    document.getElementById("signals-body").innerHTML =
      `<div class="placeholder err">Failed to load signals: ${e.message}</div>`;
  }
}

// --- /api/quote (lightweight) for the ticker bar -------------------------

const TICKER_BAR_SYMS = ["SPY", "QQQ", "NVDA", "AAPL", "TSLA"];

async function loadTickerBar() {
  // Fire all requests in parallel; render whichever come back.
  // Failures for a single ticker shouldn't blank the whole bar.
  const results = await Promise.allSettled(
    TICKER_BAR_SYMS.map(s => apiGet(`/api/quote/${s}?lookback=21`))
  );

  const bar = document.getElementById("ticker-bar");
  bar.innerHTML = results.map((r, i) => {
    const sym = TICKER_BAR_SYMS[i];
    if (r.status !== "fulfilled") {
      return `<div class="tick"><span class="tick-sym">${sym}</span><span class="tick-val">—</span></div>`;
    }
    const d = r.value;
    const up = (d.change_pct ?? 0) >= 0;
    return `
      <div class="tick">
        <span class="tick-sym">${sym}</span>
        <span class="tick-val">${fmtPrice(d.latest)}</span>
        <span class="tick-chg ${up ? 'up' : 'dn'}">${up ? '+' : ''}${fmtPct(d.change_pct)}</span>
      </div>`;
  }).join("");
}

// --- Boot -----------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  pingHealth();          // colors the status dot
  loadSignals();         // signals table + stat strip
  loadTickerBar();       // top nav prices
});
