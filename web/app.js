// QuantDash frontend — vanilla JS, no build step.
// Loads after config.js and Chart.js (see <script> order in index.html).
//
// File layout (top → bottom):
//   1. tiny utils         — formatters, fetch wrapper, status dot
//   2. AppState           — selected ticker, sort state (single source of truth)
//   3. Watchlist module   — localStorage CRUD + render
//   4. Signals table      — fetch, sort, render, row-click selection
//   5. Stat strip + ticker bar (top nav)
//   6. Price chart        — Chart.js, driven by AppState.selectedTicker
//   7. Pairs chart        — two y-axes (spread, z-score)
//   8. Correlation heatmap — CSS-grid heatmap, no plugin
//   9. Boot
//
// Every fetch goes through apiGet(); every chart instance is tracked in
// chartRegistry so we can destroy() before re-rendering (Chart.js complains
// about leaking canvases otherwise).

const API = window.QD_CONFIG.API_BASE;

// =========================================================================
// 1. TINY UTILS
// =========================================================================

const fmtPct = (x, d = 2) => (x == null || Number.isNaN(x)) ? "—" : (x * 100).toFixed(d) + "%";
const fmtPrice = (x) => (x == null || Number.isNaN(x))
  ? "—"
  : "$" + Number(x).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtNum = (x, d = 3) => (x == null || Number.isNaN(x)) ? "—" : Number(x).toFixed(d);

async function apiGet(path) {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json();
}

async function apiPost(path, body) {
  const res = await fetch(`${API}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json();
}

function setStatus(state, title) {
  const dot = document.getElementById("status-dot");
  dot.classList.remove("stale", "dead");
  if (state === "stale") dot.classList.add("stale");
  if (state === "dead")  dot.classList.add("dead");
  dot.title = title || state;
}

async function pingHealth() {
  try {
    const data = await apiGet("/api/health");
    setStatus("live", `API ${data.version} · ${data.time_utc}`);
  } catch (e) {
    setStatus("dead", `API unreachable: ${e.message}`);
  }
}

// Track Chart.js instances so we can destroy before re-creating.
// Without this, Chart.js raises "Canvas is already in use".
const chartRegistry = {};
function destroyChart(key) {
  if (chartRegistry[key]) { chartRegistry[key].destroy(); delete chartRegistry[key]; }
}

// =========================================================================
// 2. APP STATE — minimal global state, mutated only via setters below
// =========================================================================

const AppState = {
  selectedTicker: null,           // currently-selected row in signals table
  priceLookback: 63,              // days; controlled by chart-tab buttons
  sort: { col: "Composite_Z", dir: "desc" },  // signals table sort
  lastSignals: [],                // last fetched rows, kept for re-sort without refetch
  nameMap: {},                    // ticker -> company name, populated from /api/signals + autocomplete picks
  acItems: [],                    // autocomplete current results
  acActiveIdx: -1,                // keyboard-highlighted index in autocomplete
  acTimer: null,                  // debounce timer for the autocomplete fetch
};

// Cache the company name we discovered for a ticker so the watchlist
// shows it even before the next /api/signals refresh.
function rememberName(symbol, name) {
  if (!symbol || !name) return;
  AppState.nameMap[symbol.toUpperCase()] = name;
}
function nameOf(symbol) {
  return AppState.nameMap[(symbol || "").toUpperCase()] || "";
}

// =========================================================================
// 3. WATCHLIST MODULE — localStorage CRUD
// =========================================================================
//
// Schema in localStorage:
//   key: "qd.watchlist.v1"
//   value: [{ symbol: "AAPL", enabled: true }, ...]
//
// The version suffix lets us migrate later without trashing user data.

const WATCHLIST_KEY = "qd.watchlist.v1";
const DEFAULT_WATCHLIST = ["AAPL","MSFT","GOOGL","AMZN","META","INTC","AMD","NVDA","TSLA"];

const Watchlist = {
  load() {
    try {
      const raw = localStorage.getItem(WATCHLIST_KEY);
      if (!raw) return DEFAULT_WATCHLIST.map(s => ({ symbol: s, enabled: true }));
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) throw new Error("not an array");
      return parsed.filter(x => x && typeof x.symbol === "string");
    } catch (_) {
      return DEFAULT_WATCHLIST.map(s => ({ symbol: s, enabled: true }));
    }
  },
  save(items) { localStorage.setItem(WATCHLIST_KEY, JSON.stringify(items)); },
  add(symbol) {
    const sym = symbol.trim().toUpperCase();
    if (!sym || !/^[A-Z.\-]{1,6}$/.test(sym)) return false;
    const items = Watchlist.load();
    if (items.some(x => x.symbol === sym)) return false;
    items.push({ symbol: sym, enabled: true });
    Watchlist.save(items);
    return true;
  },
  remove(symbol) {
    Watchlist.save(Watchlist.load().filter(x => x.symbol !== symbol));
  },
  toggle(symbol) {
    const items = Watchlist.load();
    const item = items.find(x => x.symbol === symbol);
    if (item) { item.enabled = !item.enabled; Watchlist.save(items); }
  },
  reset() { Watchlist.save(DEFAULT_WATCHLIST.map(s => ({ symbol: s, enabled: true }))); },
  enabledSymbols() { return Watchlist.load().filter(x => x.enabled).map(x => x.symbol); },
};

function renderWatchlist() {
  const items = Watchlist.load();
  const list = document.getElementById("watchlist-list");
  const meta = document.getElementById("watchlist-meta");
  if (!list) return;  // watchlist UI not present in this build of index.html

  if (!items.length) {
    list.innerHTML = `<div class="wl-empty">No tickers. Add one or reset.</div>`;
  } else {
    list.innerHTML = items.map(it => {
      const nm = nameOf(it.symbol);
      return `
      <div class="wl-item ${it.enabled ? "" : "disabled"}" data-sym="${it.symbol}">
        <span class="wl-sym">${it.symbol}</span>
        <span class="wl-name">${nm}</span>
        <button class="wl-toggle" data-action="toggle" data-sym="${it.symbol}">
          ${it.enabled ? "ON" : "OFF"}
        </button>
        <button class="wl-remove" data-action="remove" data-sym="${it.symbol}">×</button>
      </div>`;
    }).join("");
  }

  const enabled = items.filter(i => i.enabled).length;
  if (meta) meta.textContent = `${enabled} / ${items.length} ON`;
}

// One delegated click handler — cheaper than per-button listeners and
// survives re-renders without re-binding.
//
// Defensive: every getElementById is null-checked because the watchlist
// markup is not rendered on every page state (e.g. early shell builds).
// Without these guards a missing node would throw and halt all later JS.
function bindWatchlist() {
  const list = document.getElementById("watchlist-list");
  if (list) {
    list.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      const action = btn.dataset.action;
      const sym = btn.dataset.sym;
      if (action === "toggle") Watchlist.toggle(sym);
      if (action === "remove") Watchlist.remove(sym);
      renderWatchlist();
      refreshDataForWatchlistChange();
    });
  }

  const addBtn = document.getElementById("watchlist-add");
  if (addBtn) addBtn.addEventListener("click", addFromInput);

  const input = document.getElementById("watchlist-input");
  if (input) {
    // Debounced live search against /api/universe.
    input.addEventListener("input", () => {
      clearTimeout(AppState.acTimer);
      const q = input.value.trim();
      if (!q) { hideAutocomplete(); return; }
      AppState.acTimer = setTimeout(() => fetchAutocomplete(q), 200);
    });
    // Keyboard navigation in the dropdown. Enter without an active
    // suggestion falls through to the legacy add-typed-text behavior.
    input.addEventListener("keydown", (e) => {
      const list = AppState.acItems;
      if (e.key === "ArrowDown" && list.length) {
        e.preventDefault();
        AppState.acActiveIdx = (AppState.acActiveIdx + 1) % list.length;
        renderAutocomplete();
      } else if (e.key === "ArrowUp" && list.length) {
        e.preventDefault();
        AppState.acActiveIdx = (AppState.acActiveIdx - 1 + list.length) % list.length;
        renderAutocomplete();
      } else if (e.key === "Escape") {
        hideAutocomplete();
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (AppState.acActiveIdx >= 0 && list[AppState.acActiveIdx]) {
          pickAutocomplete(list[AppState.acActiveIdx]);
        } else {
          addFromInput();
        }
      }
    });
    // Click outside closes the dropdown.
    document.addEventListener("click", (e) => {
      if (!e.target.closest(".autocomplete-wrap")) hideAutocomplete();
    });
  }

  // Suggestion clicks are wired here (delegated) so they survive re-renders.
  const sug = document.getElementById("watchlist-suggestions");
  if (sug) {
    sug.addEventListener("mousedown", (e) => {
      // mousedown so it fires before input blur swallows the click
      const item = e.target.closest(".autocomplete-item");
      if (!item) return;
      const sym = item.dataset.sym;
      const picked = AppState.acItems.find(x => x.symbol === sym);
      if (picked) pickAutocomplete(picked);
    });
  }

  const resetBtn = document.getElementById("watchlist-reset");
  if (resetBtn) resetBtn.addEventListener("click", () => {
    Watchlist.reset();
    renderWatchlist();
    refreshDataForWatchlistChange();
  });
}

function addFromInput() {
  const input = document.getElementById("watchlist-input");
  if (!input) return;
  const raw = (input.value || "").trim().toUpperCase();
  if (!raw) return;
  // Route through the ensure-cache path so a hand-typed ticker (e.g. POET)
  // gets fetched + persisted before scoring.
  pickAutocomplete({ symbol: raw, name: raw, sector: "Unknown" });
}

// =========================================================================
// 3b. AUTOCOMPLETE — /api/universe (search) + /api/cache/ensure (hydrate)
// =========================================================================

async function fetchAutocomplete(q) {
  const sug = document.getElementById("watchlist-suggestions");
  if (!sug) return;
  try {
    sug.hidden = false;
    sug.innerHTML = `<div class="ac-loading">searching…</div>`;
    const data = await apiGet(`/api/universe?q=${encodeURIComponent(q)}&limit=15`);
    AppState.acItems = data.results || [];
    AppState.acActiveIdx = AppState.acItems.length ? 0 : -1;
    renderAutocomplete();
  } catch (e) {
    sug.innerHTML = `<div class="ac-empty">search failed: ${e.message}</div>`;
  }
}

function renderAutocomplete() {
  const sug = document.getElementById("watchlist-suggestions");
  if (!sug) return;
  const items = AppState.acItems;
  if (!items.length) {
    sug.innerHTML = `<div class="ac-empty">no matches</div>`;
    return;
  }
  sug.innerHTML = items.map((x, i) => `
    <div class="autocomplete-item ${i === AppState.acActiveIdx ? "active" : ""}" data-sym="${x.symbol}">
      <span class="ac-sym">${x.symbol}</span>
      <span class="ac-name">${x.name || ""}</span>
      <span class="ac-sec">${x.sector || ""}</span>
    </div>`).join("");
}

function hideAutocomplete() {
  const sug = document.getElementById("watchlist-suggestions");
  if (sug) { sug.hidden = true; sug.innerHTML = ""; }
  AppState.acItems = [];
  AppState.acActiveIdx = -1;
}

// User picked a suggestion (click or Enter). Hydrate the cache, then add
// to the watchlist. The hydrate call is what makes non-S&P 500 tickers
// scorable — it tells the backend to fetch + persist them.
async function pickAutocomplete(item) {
  const input = document.getElementById("watchlist-input");
  const sym = (item.symbol || "").toUpperCase();
  if (!sym) return;

  rememberName(sym, item.name);
  hideAutocomplete();
  if (input) { input.value = ""; input.disabled = true; }

  try {
    // Best-effort: tell the backend to make sure this ticker is in the cache.
    // 503 / network failures are tolerated — the watchlist add still happens
    // and the ticker will simply be flagged "Insufficient Data" until the
    // worker (or a manual ensure) catches up.
    await apiPost("/api/cache/ensure", { tickers: [sym], period: "2y" });
  } catch (e) {
    console.warn("cache ensure failed:", e);
  } finally {
    if (input) input.disabled = false;
  }

  if (Watchlist.add(sym)) {
    renderWatchlist();
    refreshDataForWatchlistChange();
  }
}

// Fired whenever the watchlist changes. Keeps everything in sync.
function refreshDataForWatchlistChange() {
  loadSignals();
  loadCorrelation();
}

// =========================================================================
// 4. SIGNALS TABLE — sortable, row-click → selectTicker()
// =========================================================================

// Map the MFC label set to a display label + CSS class.
// Five tiers (plus Insufficient Data) — see core/signals.py classification rules.
function signalClass(signal) {
  switch (signal) {
    case "Strong Buy":        return { label: "▲▲ STRONG BUY", cls: "signal-buy"  };
    case "Buy":               return { label: "▲ BUY",         cls: "signal-buy"  };
    case "Watch":             return { label: "● WATCH",       cls: "signal-hold" };
    case "Avoid":             return { label: "▼ AVOID",       cls: "signal-sell" };
    case "High Risk":         return { label: "▼▼ HIGH RISK",  cls: "signal-sell" };
    case "Insufficient Data": return { label: "— N/A",         cls: "signal-hold" };
    default:                  return { label: signal || "—",   cls: "signal-hold" };
  }
}
// Tier off the percentile (0..100) — used for the bar fill color.
function confidenceTier(pct) {
  if (pct >= 75) return "high";
  if (pct >= 40) return "mid";
  return "low";
}

// Signal label → strength rank. Higher = more bullish. Used so clicking
// the Signal header sorts by conviction (Strong Buy on top in desc) instead
// of alphabetically (which would put "Avoid" first — useless).
const SIGNAL_RANK = {
  "Strong Buy":        5,
  "Buy":               4,
  "Watch":             3,
  "Avoid":             2,
  "High Risk":         1,
  "Insufficient Data": 0,
};

// Maps display column → key in the row payload (MFC schema).
// Setting `numeric: true` triggers numeric sort instead of string sort.
// Setting `sortValue` lets a column compute a custom sort key (e.g. Signal
// uses the strength rank above instead of the raw label string).
const SIGNAL_COLUMNS = [
  { key: "Ticker",            label: "Symbol",        numeric: false },
  { key: "Signal",            label: "Signal",        numeric: true,
    sortValue: r => SIGNAL_RANK[r.Signal] ?? -1 },
  { key: "Composite_Z",       label: "Composite",     numeric: true  },
  { key: "Price",             label: "Price",         numeric: true  },
  { key: "Momentum_12_1",     label: "Mom 12-1",      numeric: true  },
  { key: "Sortino",           label: "Sortino",       numeric: true  },
  { key: "Alpha_Annualized",  label: "α vs SPY",      numeric: true  },
  { key: "CVaR_5",            label: "CVaR 5%",       numeric: true  },
  { key: "Max_Drawdown_252d", label: "DD 252d",       numeric: true  },
  { key: "Sector",            label: "Sector",        numeric: false },
];

function sortRows(rows) {
  const { col, dir } = AppState.sort;
  const meta = SIGNAL_COLUMNS.find(c => c.key === col);
  if (!meta) return rows;
  const mult = dir === "asc" ? 1 : -1;
  // Custom resolver wins; otherwise fall back to the raw column value.
  const resolve = meta.sortValue || (r => r[col]);
  return [...rows].sort((a, b) => {
    const av = resolve(a), bv = resolve(b);
    if (av == null) return 1;            // nulls sink to bottom regardless of direction
    if (bv == null) return -1;
    if (meta.numeric) return (av - bv) * mult;
    return String(av).localeCompare(String(bv)) * mult;
  });
}

function renderSignalsTable(rows) {
  const sorted = sortRows(rows);
  const { col, dir } = AppState.sort;

  const head = `
    <table class="signals-table">
      <thead>
        <tr>
          ${SIGNAL_COLUMNS.map(c => {
            const cls = c.key === col ? `sortable sort-${dir}` : "sortable";
            const arrow = c.key === col ? (dir === "asc" ? "▲" : "▼") : "↕";
            return `<th class="${cls}" data-col="${c.key}">${c.label}<span class="sort-arrow">${arrow}</span></th>`;
          }).join("")}
        </tr>
      </thead>
      <tbody>`;

  const body = sorted.map(r => {
    const sig = signalClass(r.Signal);
    const pct = r.Composite_Percentile;            // 0..100, may be null
    const tier = confidenceTier(pct == null ? 0 : pct);
    const mom = r.Momentum_12_1;
    const alpha = r.Alpha_Annualized;
    const dd = r.Max_Drawdown_252d;
    const z = r.Composite_Z;
    const isSelected = r.Ticker === AppState.selectedTicker ? "selected" : "";
    // Composite bar width: percentile 0..100 → 0..100% fill.
    // Showing both the bar and the raw z keeps the relative ranking visible
    // alongside the absolute score (z=+1 means "1 stdev above universe mean").
    const barWidth = pct == null ? 0 : Math.max(0, Math.min(100, pct));
    return `
      <tr class="${isSelected}" data-ticker="${r.Ticker}">
        <td>
          <div class="sym-col">${r.Ticker}</div>
          <div class="sym-sub">${r.Sector || ""}</div>
        </td>
        <td><span class="${sig.cls}">${sig.label}</span></td>
        <td>
          <div style="display:flex;align-items:center;gap:8px">
            <div class="conf-bar"><div class="conf-fill ${tier}" style="width:${barWidth.toFixed(0)}%"></div></div>
            <span style="font-size:10px">${z == null ? "—" : (z >= 0 ? "+" : "") + fmtNum(z, 2)}</span>
          </div>
        </td>
        <td class="price-col">${fmtPrice(r.Price)}</td>
        <td class="${(mom != null && mom >= 0) ? 'chg-col up' : 'chg-col dn'}">${fmtPct(mom)}</td>
        <td>${fmtNum(r.Sortino, 2)}</td>
        <td class="${(alpha != null && alpha >= 0) ? 'chg-col up' : 'chg-col dn'}">${fmtPct(alpha)}</td>
        <td class="chg-col dn">${fmtPct(r.CVaR_5)}</td>
        <td class="chg-col dn">${fmtPct(dd)}</td>
        <td><span class="card-badge badge-blue">${r.Sector || "Unknown"}</span></td>
      </tr>`;
  }).join("");

  document.getElementById("signals-body").innerHTML = head + body + "</tbody></table>";

  // Bind sort headers
  document.querySelectorAll(".signals-table th[data-col]").forEach(th => {
    th.addEventListener("click", () => {
      const newCol = th.dataset.col;
      if (AppState.sort.col === newCol) {
        AppState.sort.dir = AppState.sort.dir === "asc" ? "desc" : "asc";
      } else {
        AppState.sort = { col: newCol, dir: "desc" };
      }
      renderSignalsTable(AppState.lastSignals);
    });
  });

  // Bind row clicks → ticker selection
  document.querySelectorAll(".signals-table tbody tr").forEach(tr => {
    tr.addEventListener("click", () => selectTicker(tr.dataset.ticker));
  });
}

function selectTicker(ticker) {
  if (!ticker || ticker === AppState.selectedTicker) return;
  AppState.selectedTicker = ticker;
  // Re-render the table to update the "selected" highlight without refetching.
  renderSignalsTable(AppState.lastSignals);
  loadPriceChart();
  loadRegime();
  loadNews();
}

// =========================================================================
// 5. STAT STRIP + TICKER BAR
// =========================================================================

// Pick the best-by row defensively: ignore nulls, return null if no candidates.
function _best(rows, field, dir = "max") {
  const filtered = rows.filter(r => r[field] != null && !Number.isNaN(r[field]));
  if (!filtered.length) return null;
  return filtered.reduce((a, b) => (dir === "max" ? (b[field] > a[field] ? b : a)
                                                  : (b[field] < a[field] ? b : a)));
}

function renderStatStrip(payload) {
  const rows = payload.results || [];
  if (!rows.length) return;

  const top       = _best(rows, "Composite_Z",       "max");
  const bestAlpha = _best(rows, "Alpha_Annualized",  "max");
  const worstDD   = _best(rows, "Max_Drawdown_252d", "min");
  const strongBuy = rows.filter(r => r.Signal === "Strong Buy" || r.Signal === "Buy").length;

  const setText = (id, value) => { const el = document.getElementById(id); if (el) el.textContent = value; };

  if (top) {
    setText("stat-top", top.Ticker);
    setText("stat-top-sub", `${top.Signal} · z ${(top.Composite_Z >= 0 ? "+" : "")}${fmtNum(top.Composite_Z, 2)}`);
  }
  if (bestAlpha) {
    setText("stat-vol", bestAlpha.Ticker);
    setText("stat-vol-sub", `α ${fmtPct(bestAlpha.Alpha_Annualized)} ann. vs SPY`);
  }
  if (worstDD) {
    setText("stat-cvar", worstDD.Ticker);
    setText("stat-cvar-sub", `${fmtPct(worstDD.Max_Drawdown_252d)} max drawdown 252d`);
  }
  setText("stat-universe", payload.universe_size ?? payload.scored_count ?? rows.length);
  setText("stat-universe-sub", `${payload.scored_count ?? rows.length} watchlist scored`);
  setText("stat-long", strongBuy);
  setText("stat-cache", payload.as_of_utc ? new Date(payload.as_of_utc).toLocaleString() : "unknown");
}

async function loadSignals() {
  const enabled = Watchlist.enabledSymbols();
  if (!enabled.length) {
    document.getElementById("signals-body").innerHTML =
      `<div class="placeholder">Enable at least one ticker in the watchlist.</div>`;
    return;
  }
  try {
    const qs = `?watchlist=${encodeURIComponent(enabled.join(","))}`;
    const data = await apiGet(`/api/signals${qs}`);
    AppState.lastSignals = data.results;
    // Stash the symbol -> name map so the watchlist can show company names
    // without an extra round-trip.
    for (const r of data.results) rememberName(r.Ticker, r.Name);
    renderWatchlist();

    // Default selection: top-scored ticker (or keep current if still in results)
    if (!AppState.selectedTicker || !data.results.find(r => r.Ticker === AppState.selectedTicker)) {
      const top = [...data.results].sort((a,b) => (b.Composite_Z ?? -Infinity) - (a.Composite_Z ?? -Infinity))[0];
      AppState.selectedTicker = top ? top.Ticker : null;
    }

    renderSignalsTable(data.results);
    renderStatStrip(data);
    loadPriceChart();
    loadRegime();
    loadNews();
  } catch (e) {
    document.getElementById("signals-body").innerHTML =
      `<div class="placeholder err">Failed to load signals: ${e.message}</div>`;
  }
}

const TICKER_BAR_SYMS = ["SPY", "QQQ", "NVDA", "AAPL", "TSLA"];
async function loadTickerBar() {
  const results = await Promise.allSettled(
    TICKER_BAR_SYMS.map(s => apiGet(`/api/quote/${s}?lookback=21`))
  );
  document.getElementById("ticker-bar").innerHTML = results.map((r, i) => {
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

// =========================================================================
// 6. PRICE CHART — Chart.js line, single y-axis
// =========================================================================

// Default Chart.js styling that matches the dark theme.
// Set once at boot; every chart inherits these.
function configureChartDefaults() {
  if (!window.Chart) return;
  Chart.defaults.color = "#7a9e94";
  Chart.defaults.font.family = "'Space Mono', monospace";
  Chart.defaults.font.size = 10;
  Chart.defaults.borderColor = "rgba(0,255,170,0.08)";
}

async function loadPriceChart() {
  const ticker = AppState.selectedTicker;
  if (!ticker) return;
  // The price chart canvas may not exist in shell builds — bail early.
  if (!document.getElementById("price-chart")) return;

  document.getElementById("price-chart-ticker").textContent = ticker;
  document.getElementById("price-chart-meta").textContent = `Loading…`;

  try {
    const data = await apiGet(`/api/quote/${ticker}?lookback=${AppState.priceLookback}`);
    const labels = data.series.map(p => p.date);
    const closes = data.series.map(p => p.close);
    const up = (data.change_pct ?? 0) >= 0;
    const color = up ? "#00ffaa" : "#ff4060";

    document.getElementById("price-chart-meta").textContent =
      `${fmtPrice(data.latest)} · ${up ? "+" : ""}${fmtPct(data.change_pct)} (${data.lookback_days}d)`;

    destroyChart("price");
    chartRegistry["price"] = new Chart(document.getElementById("price-chart"), {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: ticker,
          data: closes,
          borderColor: color,
          backgroundColor: color + "20",          // semi-transparent fill
          fill: true,
          tension: 0.2,
          pointRadius: 0,
          borderWidth: 1.5,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { ticks: { maxTicksLimit: 8 }, grid: { display: false } },
          y: { grid: { color: "rgba(0,255,170,0.06)" } },
        },
        plugins: { legend: { display: false } },
      },
    });
  } catch (e) {
    document.getElementById("price-chart-meta").textContent = `error: ${e.message}`;
  }
}

function bindPriceChartTabs() {
  if (!document.querySelector(".chart-tab[data-days]")) return;
  document.querySelectorAll(".chart-tab[data-days]").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".chart-tab[data-days]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      AppState.priceLookback = Number(btn.dataset.days);
      loadPriceChart();
    });
  });
}

// =========================================================================
// 7. PAIRS CHART — dual y-axis (spread on left, z-score on right)
// =========================================================================

async function loadPairs() {
  const aEl = document.getElementById("pairs-a");
  const bEl = document.getElementById("pairs-b");
  if (!aEl || !bEl) return;  // pairs UI not in this build
  const a = aEl.value.trim().toUpperCase();
  const b = bEl.value.trim().toUpperCase();
  const lookback = document.getElementById("pairs-lookback").value;
  const zwin     = document.getElementById("pairs-zwin").value;
  const entry    = document.getElementById("pairs-entry").value;
  const exitv    = document.getElementById("pairs-exit").value;

  const qs = `?a=${a}&b=${b}&lookback=${lookback}&z_window=${zwin}&entry=${entry}&exit=${exitv}`;
  const meta = document.getElementById("pairs-meta");
  const sigBadge = document.getElementById("pairs-signal");
  meta.textContent = "Loading…";

  try {
    const data = await apiGet(`/api/pairs${qs}`);
    const labels = data.series.map(p => p.date);
    const spread = data.series.map(p => p.spread);
    const z      = data.series.map(p => p.z);

    meta.textContent = `β=${fmtNum(data.hedge_ratio_beta, 3)} · z_now=${fmtNum(data.current_z, 2)}`;
    sigBadge.textContent = data.signal;
    sigBadge.className = "card-badge " + (
      data.signal && data.signal !== "No Trade" ? "badge-green" : "badge-warn"
    );

    destroyChart("pairs");
    chartRegistry["pairs"] = new Chart(document.getElementById("pairs-chart"), {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: `${a} − β·${b} (spread)`,
            data: spread,
            borderColor: "#00cfff",
            yAxisID: "y",
            pointRadius: 0, borderWidth: 1.4, tension: 0.15,
          },
          {
            label: "z-score",
            data: z,
            borderColor: "#ff6b35",
            yAxisID: "y1",
            pointRadius: 0, borderWidth: 1.4, tension: 0.15,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x:  { ticks: { maxTicksLimit: 10 }, grid: { display: false } },
          y:  { position: "left",  grid: { color: "rgba(0,207,255,0.06)" },
                title: { display: true, text: "Spread", color: "#00cfff" } },
          y1: { position: "right", grid: { drawOnChartArea: false },
                title: { display: true, text: "Z", color: "#ff6b35" } },
        },
        plugins: { legend: { labels: { boxWidth: 10 } } },
      },
    });
  } catch (e) {
    meta.textContent = `error: ${e.message}`;
    sigBadge.textContent = "—";
    sigBadge.className = "card-badge badge-danger";
  }
}

function bindPairs() {
  const btn = document.getElementById("pairs-run");
  if (btn) btn.addEventListener("click", loadPairs);
}

// =========================================================================
// 8. CORRELATION HEATMAP — pure CSS grid
// =========================================================================

// Map a correlation value in [-1, 1] to a color.
// Positive → green, negative → red, near-zero → muted gray.
// Uses a simple linear blend; good enough without a color library.
function corrColor(v) {
  if (v == null || Number.isNaN(v)) return "var(--bg3)";
  const a = Math.min(1, Math.abs(v));
  if (v >= 0) return `rgba(0, 255, 170, ${0.10 + a * 0.55})`;
  return `rgba(255, 64, 96, ${0.10 + a * 0.55})`;
}

function renderCorrelationMatrix(payload) {
  const tickers = payload.tickers;
  const matrix = payload.matrix;
  const n = tickers.length;
  const body = document.getElementById("corr-body");

  if (!n) { body.innerHTML = `<div class="placeholder">No correlation data.</div>`; return; }

  // Grid: (n+1) x (n+1) — first row is column labels, first col is row labels.
  const cells = [];
  cells.push(`<div class="heatmap-cell"></div>`);                // top-left empty
  tickers.forEach(t => cells.push(`<div class="heatmap-cell label-col">${t}</div>`));

  tickers.forEach(rt => {
    cells.push(`<div class="heatmap-cell label-row">${rt}</div>`);
    tickers.forEach(ct => {
      const v = matrix[rt] ? matrix[rt][ct] : null;
      const isDiag = rt === ct;
      const text = v == null ? "—" : v.toFixed(2);
      const style = isDiag ? "" : `background:${corrColor(v)}`;
      cells.push(`<div class="heatmap-cell ${isDiag ? "diag" : ""}" style="${style}">${text}</div>`);
    });
  });

  body.innerHTML = `
    <div class="heatmap-wrap">
      <div class="heatmap-grid" style="grid-template-columns: 46px repeat(${n}, minmax(34px, 1fr))">
        ${cells.join("")}
      </div>
      <div class="heatmap-legend">
        <span><span class="swatch" style="background:rgba(255,64,96,0.65)"></span> -1</span>
        <span><span class="swatch" style="background:rgba(150,150,150,0.2)"></span> 0</span>
        <span><span class="swatch" style="background:rgba(0,255,170,0.65)"></span> +1</span>
        <span style="margin-left:auto">Lookback ${payload.lookback_days}d · returns</span>
      </div>
    </div>`;
  document.getElementById("corr-meta").textContent = `${n} tickers`;
}

async function loadCorrelation() {
  const body = document.getElementById("corr-body");
  if (!body) return;  // correlation UI not in this build
  const enabled = Watchlist.enabledSymbols();
  if (enabled.length < 2) {
    body.innerHTML = `<div class="placeholder">Enable at least 2 tickers in the watchlist.</div>`;
    return;
  }
  try {
    const qs = `?watchlist=${encodeURIComponent(enabled.join(","))}&lookback=63`;
    const data = await apiGet(`/api/risk/correlation${qs}`);
    renderCorrelationMatrix(data);
  } catch (e) {
    document.getElementById("corr-body").innerHTML =
      `<div class="placeholder err">Correlation failed: ${e.message}</div>`;
  }
}

// =========================================================================
// 9. SECTORS — GICS rollups of the MFC composite
// =========================================================================

async function loadSectors() {
  const body = document.getElementById("sectors-body");
  const meta = document.getElementById("sectors-meta");
  if (!body) return;
  try {
    const data = await apiGet("/api/sectors");
    if (!data.results || !data.results.length) {
      body.innerHTML = `<div class="placeholder">No sectors yet.</div>`;
      return;
    }
    if (meta) meta.textContent = `${data.sector_count} sectors`;
    // Find max |avg_composite| to scale the divergent bar widths.
    const maxAbs = Math.max(...data.results.map(r => Math.abs(r.avg_composite || 0)), 0.5);
    const header = `
      <div class="sector-row header">
        <div>Sector</div>
        <div>Avg Z</div>
        <div>Med Mom</div>
        <div>Buys</div>
        <div>N</div>
      </div>`;
    const rows = data.results.map(r => {
      const z = r.avg_composite || 0;
      const pct = Math.min(100, Math.abs(z) / maxAbs * 50);  // half-width centered
      const cls = z >= 0 ? "pos" : "neg";
      const buyCount = (r.strong_buy || 0) + (r.buy || 0);
      const buyCls = buyCount > 0 ? "sector-buys" : "sector-buys empty";
      return `
        <div class="sector-row">
          <div class="sec-name">${r.Sector}</div>
          <div>
            <div style="display:flex;align-items:center;gap:6px">
              <span class="sector-bar-wrap" style="flex:1">
                <span class="sector-bar ${cls}" style="width:${pct}%"></span>
              </span>
              <span style="color:${z >= 0 ? "var(--success)" : "var(--danger)"};min-width:38px;text-align:right">${z >= 0 ? "+" : ""}${z.toFixed(2)}</span>
            </div>
          </div>
          <div>${fmtPct(r.median_momentum)}</div>
          <div class="${buyCls}">${buyCount}</div>
          <div style="color:var(--text3)">${r.constituent_count}</div>
        </div>`;
    }).join("");
    body.innerHTML = header + rows;
  } catch (e) {
    body.innerHTML = `<div class="placeholder err">${e.message}</div>`;
  }
}

// =========================================================================
// 10. REGIME & VOLATILITY — SMA trend + EWMA vol forecast
// =========================================================================

async function loadRegime() {
  const body = document.getElementById("regime-body");
  const tickerEl = document.getElementById("regime-ticker");
  const badge = document.getElementById("regime-badge");
  if (!body) return;

  // Use selected ticker; fall back to SPY (the market).
  const sym = AppState.selectedTicker || "SPY";
  if (tickerEl) tickerEl.textContent = sym;

  try {
    body.innerHTML = `<div class="placeholder">Loading regime…</div>`;
    const data = await apiGet(`/api/regime?ticker=${encodeURIComponent(sym)}`);
    const code = data.regime.code;
    const cls = code === "BULL" ? "bull" : code === "BEAR" ? "bear" : "mixed";
    if (badge) {
      badge.className = "card-badge badge-" + cls;
      badge.textContent = code;
    }
    body.innerHTML = `
      <div class="regime-cell ${cls}">
        <div class="lbl">Trend</div>
        <div class="val">${code}</div>
        <div class="sub">${data.regime.description}</div>
      </div>
      <div class="regime-cell">
        <div class="lbl">Realized 21d</div>
        <div class="val">${data.vol.realized_21d_annualized}%</div>
        <div class="sub">annualized stdev</div>
      </div>
      <div class="regime-cell">
        <div class="lbl">EWMA Forecast</div>
        <div class="val">${data.vol.ewma_today_annualized}%</div>
        <div class="sub">RiskMetrics λ=${data.vol.lambda}</div>
      </div>
      ${data.anomaly.flagged
        ? `<div class="regime-anomaly" style="grid-column: span 3">
             ⚠ Anomaly: today's move ${data.anomaly.last_return_pct >= 0 ? "+" : ""}${data.anomaly.last_return_pct}% exceeds 3σ
           </div>`
        : ""}
    `;
    renderRegimeChart(data.ewma_series);
  } catch (e) {
    body.innerHTML = `<div class="placeholder err">${e.message}</div>`;
    if (badge) { badge.className = "card-badge"; badge.textContent = "—"; }
  }
}

function renderRegimeChart(series) {
  const canvas = document.getElementById("regime-chart");
  if (!canvas || !series || !series.length || typeof Chart === "undefined") return;
  destroyChart("regime");
  chartRegistry.regime = new Chart(canvas.getContext("2d"), {
    type: "line",
    data: {
      labels: series.map(p => p.date),
      datasets: [{
        label: "EWMA Vol (annualized %)",
        data: series.map(p => p.vol_pct),
        borderColor: "rgba(0,207,255,0.9)",
        backgroundColor: "rgba(0,207,255,0.1)",
        borderWidth: 1.4,
        fill: true,
        pointRadius: 0,
        tension: 0.2,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: false },
        y: { ticks: { color: "rgba(255,255,255,0.4)", font: { size: 9 } }, grid: { color: "rgba(255,255,255,0.04)" } },
      },
    },
  });
}

// =========================================================================
// 11. MACRO FACTORS — VIX, 10Y, oil, gold, DXY, S&P
// =========================================================================

async function loadMacro() {
  const body = document.getElementById("macro-body");
  const meta = document.getElementById("macro-meta");
  if (!body) return;
  try {
    const data = await apiGet("/api/macro");
    if (meta) meta.textContent = `${data.results.length} indicators`;
    const colorClass = (n) => n == null ? "flat" : n > 0 ? "up" : n < 0 ? "dn" : "flat";
    const fmtChg = (n) => n == null ? "—" : (n >= 0 ? "+" : "") + n.toFixed(2) + "%";
    const header = `
      <div class="macro-row header">
        <div>Indicator</div>
        <div>Level</div>
        <div>1d</div>
        <div>5d</div>
        <div>21d</div>
        <div>1y</div>
      </div>`;
    const rows = data.results.map(r => `
      <div class="macro-row" title="${r.description}">
        <div class="macro-lbl">${r.label}</div>
        <div class="macro-px">${r.price == null ? "—" : r.price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
        <div class="macro-chg ${colorClass(r.change_1d_pct)}">${fmtChg(r.change_1d_pct)}</div>
        <div class="macro-chg ${colorClass(r.change_5d_pct)}">${fmtChg(r.change_5d_pct)}</div>
        <div class="macro-chg ${colorClass(r.change_21d_pct)}">${fmtChg(r.change_21d_pct)}</div>
        <div class="macro-chg ${colorClass(r.change_252d_pct)}">${fmtChg(r.change_252d_pct)}</div>
      </div>`).join("");
    body.innerHTML = header + rows;
  } catch (e) {
    body.innerHTML = `<div class="placeholder err">${e.message}</div>`;
  }
}

// =========================================================================
// 12. NEWS — headlines for the selected ticker
// =========================================================================

function _relativeTime(iso) {
  if (!iso) return "";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return "";
  const diffMin = Math.max(0, Math.floor((Date.now() - t) / 60000));
  if (diffMin < 60)   return `${diffMin}m ago`;
  if (diffMin < 1440) return `${Math.floor(diffMin / 60)}h ago`;
  return `${Math.floor(diffMin / 1440)}d ago`;
}

async function loadNews() {
  const body = document.getElementById("news-body");
  const tickerEl = document.getElementById("news-ticker");
  const meta = document.getElementById("news-meta");
  if (!body) return;
  const sym = AppState.selectedTicker;
  if (tickerEl) tickerEl.textContent = sym || "—";
  if (!sym) {
    body.innerHTML = `<div class="placeholder">Select a ticker to see headlines.</div>`;
    return;
  }
  try {
    body.innerHTML = `<div class="placeholder">Loading headlines for ${sym}…</div>`;
    const data = await apiGet(`/api/news?ticker=${encodeURIComponent(sym)}&limit=8`);
    if (meta) meta.textContent = data.filtered ? `${data.count} items` : `${data.count} (loose)`;
    if (!data.results.length) {
      body.innerHTML = `<div class="placeholder">No recent headlines for ${sym}.</div>`;
      return;
    }
    const note = data.filtered ? "" : `
      <div class="placeholder" style="padding:6px 16px;font-size:9px">
        No headlines explicitly mention ${sym}; showing related industry items.
      </div>`;
    body.innerHTML = note + data.results.map(n => `
      <div class="news-item">
        <a class="news-title" href="${n.link || "#"}" target="_blank" rel="noopener noreferrer">${n.title}</a>
        <div class="news-meta">
          <span class="news-publisher">${n.publisher || "—"}</span>
          <span>·</span>
          <span>${_relativeTime(n.published_utc)}</span>
        </div>
      </div>`).join("");
  } catch (e) {
    body.innerHTML = `<div class="placeholder err">${e.message}</div>`;
  }
}

// =========================================================================
// 13. BOOT
// =========================================================================

document.addEventListener("DOMContentLoaded", () => {
  configureChartDefaults();

  // Watchlist must render before signals (signals reads enabled symbols).
  renderWatchlist();
  bindWatchlist();

  bindPriceChartTabs();
  bindPairs();

  pingHealth();
  loadTickerBar();
  loadSignals();          // → loads price chart + regime + news for top ticker
  loadCorrelation();
  loadPairs();            // initial run with default KO/PEP
  loadSectors();
  loadRegime();           // defaults to SPY until a ticker is selected
  loadMacro();
});
