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
  const res = await fetch(`${API}${path}`, { credentials: "include" });
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json();
}

async function apiSend(path, method, body) {
  const res = await fetch(`${API}${path}`, {
    method,
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: body == null ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json();
}

async function apiPost(path, body)   { return apiSend(path, "POST", body || {}); }
async function apiPut(path, body)    { return apiSend(path, "PUT",  body || {}); }
async function apiPatch(path, body)  { return apiSend(path, "PATCH", body || {}); }
async function apiDelete(path)       { return apiSend(path, "DELETE", null); }

// =========================================================================
// 1a. SNAPSHOT BOOTSTRAP — single-request page warm-up
// =========================================================================
//
// /api/snapshot bundles the slow, read-mostly cards (signals, sectors,
// regime SPY, macro, ticker bar quotes, default pair) into one payload
// that the server pre-computes and caches. By fetching it ONCE on boot
// and consuming the bundled payloads in each loadX() call, we replace
// ~10 parallel HTTP round-trips with a single ~300 ms request.
//
// `consume(key)` is one-shot per key: the first loader to ask gets the
// snapshot payload and clears that slot. The Scheduler's subsequent
// refresh ticks then go through the live per-card endpoints as normal.
// This keeps "freshness on user interaction" while killing the cold-start
// thundering-herd that was tripping Heroku's H12/H13 router timeouts.
const Snapshot = (() => {
  let cards = null;          // { signals, sectors, regime_spy, macro, quotes, pair_default }
  let builtAt = null;        // server-side build timestamp
  let watchlistAtBuild = null;

  async function bootstrap() {
    try {
      const t0 = performance.now();
      const data = await apiGet("/api/snapshot");
      cards = {
        signals:      data.signals,
        sectors:      data.sectors,
        regime_spy:   data.regime_spy,
        macro:        data.macro,
        quotes:       data.quotes || {},
        pair_default: data.pair_default,
      };
      builtAt = data.built_at_utc;
      watchlistAtBuild = (data.default_watchlist || []).map(s => s.toUpperCase()).sort().join(",");
      console.log(`[snapshot] bootstrap ok in ${(performance.now() - t0).toFixed(0)}ms — keys=${Object.keys(cards).filter(k => cards[k]).join(",")}`);
    } catch (e) {
      // Non-fatal: each loadX falls back to its individual endpoint.
      console.warn(`[snapshot] bootstrap failed (will fall back to per-card endpoints): ${e.message}`);
    }
  }

  // Returns the bundled payload for `key` once, then clears it. Subsequent
  // calls return null so the live endpoint takes over.
  function consume(key) {
    if (!cards) return null;
    const v = cards[key];
    cards[key] = null;
    return v || null;
  }

  // Same as consume() but only returns the value if the caller's watchlist
  // matches the watchlist the snapshot was built for. Used by loadSignals
  // to avoid serving the wrong tickers when the user has a custom set.
  function consumeIfWatchlistMatches(key, callerWatchlist) {
    if (!cards) return null;
    const wantKey = (callerWatchlist || []).map(s => s.toUpperCase()).sort().join(",");
    if (wantKey !== watchlistAtBuild) return null;
    return consume(key);
  }

  return { bootstrap, consume, consumeIfWatchlistMatches };
})();

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
// 1b. REFRESH SCHEDULER — single-flight, visibility-aware, per-task TTL
// =========================================================================
//
// Borrows three OS-style ideas:
//   - Cooperative scheduling: each "task" declares a min interval; the loop
//     ticks at a coarse 5s heartbeat and only runs tasks whose deadline has
//     passed. (Avoids hammering setInterval per resource and keeps the
//     wakeups coherent — like a tickless kernel batching timer events.)
//   - Single-flight (mutex per key): if a task is already in flight, skip
//     the next tick instead of stacking concurrent requests. Prevents
//     thundering-herd when an endpoint is slow.
//   - Process suspension: when document.hidden (tab in background), the
//     loop is paused entirely. On resume, any task whose deadline elapsed
//     while hidden runs once immediately, then resettles into its cadence.
//     Saves API quota + CPU when the user isn't looking.
//
// Each task can also depend on a "key" derived from AppState (e.g. the
// selected ticker). When the key changes, we run immediately AND reset the
// deadline — so clicking a new ticker repaints news/regime instantly but
// then continues to refresh on its normal interval, not faster.
//
// Tasks that should NOT auto-refresh (price chart, pairs, correlation —
// they're either user-driven or only change on watchlist edits) stay
// outside the scheduler and are called directly.

const Scheduler = (() => {
  const tasks = new Map();   // name -> { fn, intervalMs, keyFn, lastRun, lastKey, inflight, errors }
  let timer = null;
  const HEARTBEAT_MS = 5000; // coarse tick — enough granularity for human dashboards

  function register(name, fn, intervalMs, opts = {}) {
    tasks.set(name, {
      fn,
      intervalMs,
      keyFn: opts.keyFn || (() => null),
      lastRun: 0,
      lastKey: undefined,
      inflight: false,
      errors: 0,
    });
  }

  async function _run(name) {
    const t = tasks.get(name);
    if (!t || t.inflight) return;       // single-flight guard
    t.inflight = true;
    try {
      await t.fn();
      t.errors = 0;
      t.lastRun = Date.now();
      t.lastKey = t.keyFn();
    } catch (e) {
      // Exponential-ish backoff: each consecutive failure pushes lastRun
      // forward by an extra interval (capped). Surfaced in the status dot.
      t.errors = Math.min(t.errors + 1, 5);
      t.lastRun = Date.now() + t.intervalMs * t.errors;
      console.warn(`[scheduler] ${name} failed:`, e.message);
    } finally {
      t.inflight = false;
    }
  }

  function _tick() {
    if (document.hidden) return;        // pause when tab not visible
    const now = Date.now();
    for (const [name, t] of tasks) {
      const curKey = t.keyFn();
      const keyChanged = curKey !== t.lastKey;
      const due = (now - t.lastRun) >= t.intervalMs;
      if (keyChanged || due) _run(name);
    }
  }

  // "Kick" a task by name: forces an immediate run if not already in flight.
  // Used when the user picks a new ticker — we don't want them to wait for
  // the next heartbeat for News/Regime to repaint.
  function kick(name) { _run(name); }

  function start() {
    if (timer) return;
    timer = setInterval(_tick, HEARTBEAT_MS);
    // When the tab regains focus, run a tick right away so stale data
    // refreshes immediately instead of after the next heartbeat.
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) _tick();
    });
    _tick();  // first tick right away
  }

  return { register, kick, start };
})();

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
    // Match the backend's _TICKER_RE: 1-10 chars, starts with a letter,
    // allows digits, dots and dashes (BRK.B, TLO.TO, RY-B etc.).
    if (!sym || !/^[A-Z][A-Z0-9.\-]{0,9}$/.test(sym)) return false;
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
// We kick the scheduler so signals + sectors + correlation refresh
// immediately rather than waiting for the next heartbeat. Sectors are
// kicked too because the per-watchlist filter (enabled symbols) feeds
// into the sector aggregation indirectly via the universe.
function refreshDataForWatchlistChange() {
  Scheduler.kick("signals");
  Scheduler.kick("sectors");
  Scheduler.kick("corr");
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
  // Scheduler will see the key change on its next heartbeat, but we kick
  // explicitly so the user gets instant feedback.
  Scheduler.kick("regime");
  Scheduler.kick("news");
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
    // Snapshot fast-path: if the user's watchlist still matches the
    // server-baked default, render from the bundled payload instead of
    // making an expensive cross-sectional rank request.
    const fromSnap = Snapshot.consumeIfWatchlistMatches("signals", enabled);
    const qs = `?watchlist=${encodeURIComponent(enabled.join(","))}`;
    const data = fromSnap || await apiGet(`/api/signals${qs}`);
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
    // Regime + News follow the selected ticker via Scheduler key-change
    // detection, so we don't call them directly here.
  } catch (e) {
    document.getElementById("signals-body").innerHTML =
      `<div class="placeholder err">Failed to load signals: ${e.message}</div>`;
  }
}

const TICKER_BAR_SYMS = ["SPY", "QQQ", "NVDA", "AAPL", "TSLA"];
async function loadTickerBar() {
  // Snapshot fast-path: the server pre-bakes the same tickers.
  const baked = Snapshot.consume("quotes");
  const results = baked
    ? TICKER_BAR_SYMS.map(s => baked[s]
        ? { status: "fulfilled", value: baked[s] }
        : { status: "rejected" })
    : await Promise.allSettled(
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
    const data = Snapshot.consume("sectors") || await apiGet("/api/sectors");
    if (!data.results || !data.results.length) {
      body.innerHTML = `<div class="placeholder">No sectors yet.</div>`;
      return;
    }
    if (meta) meta.textContent = `${data.sector_count} sectors`;
    // Compact GICS labels for the tile face. (Tiles are narrow.)
    const SHORT = {
      "Information Technology": "TECH",
      "Communication Services": "COMMS",
      "Consumer Discretionary": "DISC",
      "Consumer Staples": "STAPL",
      "Health Care": "HLTH",
      "Financials": "FINS",
      "Industrials": "INDU",
      "Energy": "ENRG",
      "Materials": "MATR",
      "Real Estate": "REAL",
      "Utilities": "UTIL",
    };
    body.innerHTML = data.results.map(r => {
      const z = r.avg_composite || 0;
      const cls = z > 0.05 ? "pos" : z < -0.05 ? "neg" : "flat";
      const buyCount = (r.strong_buy || 0) + (r.buy || 0);
      const buyCls = buyCount > 0 ? "buys" : "buys empty";
      const short = SHORT[r.Sector] || r.Sector.slice(0, 5).toUpperCase();
      return `
        <div class="sector-tile" title="${r.Sector} — ${r.constituent_count} tickers">
          <div class="st-name">${short}</div>
          <div class="st-z ${cls}">${z >= 0 ? "+" : ""}${z.toFixed(2)}</div>
          <div class="st-meta">
            <span>n=${r.constituent_count}</span>
            <span class="${buyCls}">▲${buyCount}</span>
          </div>
        </div>`;
    }).join("");
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
    // Snapshot fast-path is only valid for SPY — the bundled regime card
    // is always built against SPY since that's the dashboard default.
    const fromSnap = (sym === "SPY") ? Snapshot.consume("regime_spy") : null;
    const data = fromSnap || await apiGet(`/api/regime?ticker=${encodeURIComponent(sym)}`);
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
    const data = Snapshot.consume("macro") || await apiGet("/api/macro");
    if (meta) meta.textContent = `${data.results.length} indicators`;
    const colorClass = (n) => n == null ? "flat" : n > 0 ? "up" : n < 0 ? "dn" : "flat";
    const fmtChg = (n) => n == null ? "—" : (n >= 0 ? "+" : "") + n.toFixed(1) + "%";
    const fmtPx = (n) => n == null ? "—" : n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    body.innerHTML = data.results.map(r => `
      <div class="macro-tile" title="${r.description}">
        <div class="mt-lbl">${r.label}</div>
        <div class="mt-px">${fmtPx(r.price)}</div>
        <div class="mt-chgs">
          <span><span class="k">1d</span><span class="v ${colorClass(r.change_1d_pct)}">${fmtChg(r.change_1d_pct)}</span></span>
          <span><span class="k">21d</span><span class="v ${colorClass(r.change_21d_pct)}">${fmtChg(r.change_21d_pct)}</span></span>
          <span><span class="k">1y</span><span class="v ${colorClass(r.change_252d_pct)}">${fmtChg(r.change_252d_pct)}</span></span>
        </div>
      </div>`).join("");
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
        <div class="news-meta-top">
          <span class="news-publisher">${(n.publisher || "—").toUpperCase()}</span>
          <span>·</span>
          <span>${_relativeTime(n.published_utc)}</span>
        </div>
        <a class="news-title" href="${n.link || "#"}" target="_blank" rel="noopener noreferrer">${n.title}</a>
      </div>`).join("");
  } catch (e) {
    body.innerHTML = `<div class="placeholder err">${e.message}</div>`;
  }
}

// =========================================================================
// 13. NAV BUTTONS — Modal, Screener, Portfolio, API Docs
// =========================================================================
//
// Phase 1 keeps the Portfolio fully browser-local (localStorage). Phase 2
// will swap localStorage for a Postgres-backed `/api/portfolio` resource
// behind auth. We've kept the read/write surface tiny on purpose so that
// migration is a one-file change.

// ----- Modal helpers -----
const Modal = (() => {
  const root = () => document.getElementById("modal-root");
  const titleEl = () => document.getElementById("modal-title");
  const bodyEl = () => document.getElementById("modal-body");

  function open(title, html) {
    titleEl().textContent = title;
    bodyEl().innerHTML = html;
    root().hidden = false;
    document.addEventListener("keydown", _esc);
  }
  function close() {
    root().hidden = true;
    bodyEl().innerHTML = "";
    document.removeEventListener("keydown", _esc);
  }
  function _esc(e) { if (e.key === "Escape") close(); }

  // Wire backdrop + close button once.
  function bind() {
    root().addEventListener("click", (e) => {
      if (e.target.dataset.close !== undefined) close();
    });
  }
  return { open, close, bind, body: bodyEl };
})();

// ----- Screener -----
async function openScreener() {
  Modal.open("Screener — Full Universe", `
    <div class="modal-controls">
      <label>Min Z
        <input id="scr-minz" class="field" type="number" step="0.1" value="0.5">
      </label>
      <label>Signal
        <select id="scr-signal" class="field">
          <option value="">Any</option>
          <option value="STRONG_BUY">Strong Buy only</option>
          <option value="STRONG_BUY,BUY" selected>Buy+ (Buy or Strong Buy)</option>
          <option value="BUY">Buy only</option>
          <option value="HOLD">Hold</option>
          <option value="AVOID">Avoid</option>
        </select>
      </label>
      <label>Sector
        <select id="scr-sector" class="field">
          <option value="">Any</option>
        </select>
      </label>
      <label>Limit
        <input id="scr-limit" class="field" type="number" min="5" max="200" value="50">
      </label>
      <button id="scr-run" class="pill cta" style="padding:5px 14px">Run</button>
    </div>
    <div id="scr-results"><div class="placeholder">Adjust filters and hit Run.</div></div>
  `);

  // Populate sector dropdown from the sectors endpoint we already have.
  try {
    const sec = await apiGet("/api/sectors");
    const sel = document.getElementById("scr-sector");
    for (const s of sec.results) {
      const opt = document.createElement("option");
      opt.value = s.Sector; opt.textContent = s.Sector;
      sel.appendChild(opt);
    }
  } catch (_) { /* non-fatal */ }

  document.getElementById("scr-run").addEventListener("click", _runScreener);
  _runScreener();  // initial run
}

async function _runScreener() {
  const out = document.getElementById("scr-results");
  if (!out) return;
  const minz = document.getElementById("scr-minz").value || 0;
  const sig = document.getElementById("scr-signal").value;
  const sec = document.getElementById("scr-sector").value;
  const lim = document.getElementById("scr-limit").value || 50;
  const params = new URLSearchParams({ min_z: minz, limit: lim });
  if (sig) params.set("signal", sig);
  if (sec) params.set("sector", sec);
  out.innerHTML = `<div class="placeholder">Screening…</div>`;
  try {
    const data = await apiGet(`/api/screener?${params.toString()}`);
    if (!data.results.length) {
      out.innerHTML = `<div class="placeholder">No tickers match those filters.</div>`;
      return;
    }
    const rows = data.results.map(r => `
      <tr>
        <td class="lcol"><strong>${r.Ticker}</strong> <span style="color:var(--text3)">${r.Name || ""}</span></td>
        <td class="lcol">${r.Sector || "—"}</td>
        <td>${r.Composite_Z >= 0 ? "+" : ""}${r.Composite_Z}</td>
        <td>${r.Composite_Percentile}%</td>
        <td class="lcol">${r.Signal}</td>
        <td class="${(r.Momentum_12_1||0)>=0?'pos':'neg'}">${fmtPct(r.Momentum_12_1)}</td>
        <td>${fmtPrice(r.Price)}</td>
      </tr>`).join("");
    out.innerHTML = `
      <div style="color:var(--text3);font-size:10px;margin-bottom:6px">
        ${data.count} matches · as of ${data.as_of_utc || "—"}
      </div>
      <table class="modal-table">
        <thead><tr>
          <th class="lcol">Ticker</th>
          <th class="lcol">Sector</th>
          <th>Composite Z</th><th>Pctl</th>
          <th class="lcol">Signal</th>
          <th>Mom 12-1</th><th>Price</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  } catch (e) {
    out.innerHTML = `<div class="placeholder err">${e.message}</div>`;
  }
}

// ----- Portfolio (server-backed via /api/portfolio) -----
//
// Phase 2a: positions live in Postgres, scoped by an httpOnly device-id
// cookie (qd_device). Phase 2b will migrate device-owned rows to the
// signed-in user with one UPDATE.
//
// Migration: legacy positions stored under "qd.portfolio.v1" in
// localStorage are PUT to the server on first load (when the server
// returns count=0), then localStorage is cleared. This keeps existing
// users from "losing" their positions when this module deploys.

const PORTFOLIO_KEY = "qd.portfolio.v1";
let _PF_MIGRATED = false;  // session flag — only attempt migration once per page load

const Portfolio = {
  // Returns array of {ticker, shares, avg_cost, opened_at?}.
  // Performs the one-shot localStorage migration the first time it sees
  // an empty server-side portfolio for this device.
  async load() {
    try {
      const data = await apiGet("/api/portfolio");
      if (!_PF_MIGRATED && data.count === 0) {
        _PF_MIGRATED = true;
        let legacy = [];
        try { legacy = JSON.parse(localStorage.getItem(PORTFOLIO_KEY) || "[]"); }
        catch (_) { legacy = []; }
        if (Array.isArray(legacy) && legacy.length) {
          try {
            await apiPut("/api/portfolio", { items: legacy });
            localStorage.removeItem(PORTFOLIO_KEY);
            const after = await apiGet("/api/portfolio");
            return after.positions || [];
          } catch (_) { /* fall through to empty list */ }
        }
      }
      return data.positions || [];
    } catch (_) {
      // Offline fallback — read whatever's still in localStorage so the
      // page is at least usable until the network recovers.
      try { return JSON.parse(localStorage.getItem(PORTFOLIO_KEY) || "[]"); }
      catch (__) { return []; }
    }
  },
  async add(ticker, shares, avgCost) {
    const sym = (ticker || "").toUpperCase().trim();
    if (!sym || !(shares > 0) || !(avgCost >= 0)) return { ok: false, error: "Need a ticker, positive shares, and non-negative avg cost." };
    try {
      const r = await apiPost("/api/portfolio", { ticker: sym, shares, avg_cost: avgCost });
      // Mirror to the local watchlist so the dashboard signals view also
      // tracks anything the user holds. Best-effort \u2014 if the symbol
      // doesn't pass Watchlist.add()'s regex (e.g. exotic foreign listings)
      // we just skip it. No-op if already there.
      try { Watchlist.add(sym); if (typeof renderWatchlist === "function") renderWatchlist(); } catch (_) {}
      return { ok: true, position: r.position };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  },
  async remove(ticker) {
    const sym = (ticker || "").toUpperCase().trim();
    if (!sym) return false;
    try { await apiDelete(`/api/portfolio/${encodeURIComponent(sym)}`); return true; }
    catch (_) { return false; }
  },
  // Set shares to an exact number (used by partial-sell). Server keeps
  // the existing avg_cost — selling shares doesn't change the cost basis
  // of what's left.
  async setShares(ticker, shares) {
    const sym = (ticker || "").toUpperCase().trim();
    if (!sym || !(shares > 0)) return { ok: false, error: "Need a positive share count." };
    try {
      const r = await apiSend(`/api/portfolio/${encodeURIComponent(sym)}`, "PATCH", { shares });
      return { ok: true, position: r.position };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  },
};
// Expose for the Auth module so login/logout can refresh the page after
// a successful identity change. Auth lives in its own IIFE later in the
// file, so it can't see lexical `Portfolio`.
window.Portfolio = Portfolio;

async function openPortfolio() {
  // Page-based now (was a modal). Render into the static markup in
  // index.html. The Router handles showing/hiding the page itself; this
  // function only attaches the "Add" handler and renders content.
  const addBtn = document.getElementById("pf-add");
  if (addBtn && !addBtn._wired) {
    addBtn.addEventListener("click", _addPortfolioPosition);
    addBtn._wired = true;  // bindNav can call us repeatedly without dup-binding
  }
  const refreshBtn = document.getElementById("pf-refresh");
  if (refreshBtn && !refreshBtn._wired) {
    refreshBtn.addEventListener("click", _refreshPortfolioPrices);
    refreshBtn._wired = true;
  }
  await _renderPortfolio();
}

async function _refreshPortfolioPrices() {
  const btn = document.getElementById("pf-refresh");
  if (!btn) return;
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Refreshing\u2026";
  try {
    const r = await apiPost("/api/portfolio/refresh", {});
    const parts = [];
    if (r.refreshed?.length) parts.push(`Loaded ${r.refreshed.join(", ")}`);
    if (r.missing?.length)   parts.push(`Could not find: ${r.missing.join(", ")}`);
    if (!parts.length)       parts.push("All positions already have prices.");
    btn.textContent = parts.join(" \u00b7 ").slice(0, 80);
    setTimeout(() => { btn.textContent = original; btn.disabled = false; }, 3500);
  } catch (e) {
    btn.textContent = `Failed: ${e.message || "error"}`;
    setTimeout(() => { btn.textContent = original; btn.disabled = false; }, 3500);
    return;
  }
  await _renderPortfolio();
}

async function _addPortfolioPosition() {
  const t = document.getElementById("pf-ticker").value;
  const s = parseFloat(document.getElementById("pf-shares").value);
  const c = parseFloat(document.getElementById("pf-cost").value);
  const btn = document.getElementById("pf-add");
  btn.disabled = true;
  const res = await Portfolio.add(t, s, c);
  btn.disabled = false;
  if (!res.ok) {
    alert(res.error || "Could not add position.");
    return;
  }
  document.getElementById("pf-ticker").value = "";
  document.getElementById("pf-shares").value = "";
  document.getElementById("pf-cost").value = "";
  _renderPortfolio();
}

// Donut palette — wraps modulo when there are more positions than colors.
// Colors picked to read well on the dark bg and stay distinct from each other.
const PF_DONUT_COLORS = [
  "#00ffaa", "#00cfff", "#ff6b35", "#ffcc00", "#9b59b6",
  "#e74c3c", "#3498db", "#1abc9c", "#f39c12", "#ec407a",
];

// Stat-strip card factory — mirrors the dashboard's .stat-card pattern so the
// portfolio page reads as one consistent design system. `tone` colors the sub-
// label: "up" green, "dn" red, "neu" muted, "stub" dimmed for placeholders.
function _pfStat(label, value, sub, tone) {
  const valColor = tone === "up" ? "var(--success)" : (tone === "dn" ? "var(--danger)" : "");
  const subCls   = tone === "up" ? "up" : (tone === "dn" ? "dn" : "neu");
  const dim      = tone === "stub" ? "opacity:0.55" : "";
  return `<div class="stat-card" style="${dim}">
    <div class="stat-label">${label}</div>
    <div class="stat-val" style="${valColor ? `color:${valColor}` : ""}">${value}</div>
    <div class="stat-sub ${subCls}">${sub}</div>
  </div>`;
}

// Donut as inline SVG. We compute cumulative offsets along the circumference
// for each slice — no chart library, just math, keeps the page deps-free.
function _pfDonut(slices, total) {
  const r = 64;
  const circ = 2 * Math.PI * r;
  let offset = 0;
  const arcs = slices.map((s, i) => {
    const len = circ * (s.weight || 0);
    const arc = `<circle cx="85" cy="85" r="${r}" fill="none"
      stroke="${PF_DONUT_COLORS[i % PF_DONUT_COLORS.length]}" stroke-width="26"
      stroke-dasharray="${len.toFixed(2)} ${(circ - len).toFixed(2)}"
      stroke-dashoffset="${(-offset).toFixed(2)}"
      transform="rotate(-90 85 85)"
      style="transition: stroke-dasharray 600ms"/>`;
    offset += len;
    return arc;
  }).join("");
  const totalLabel = "$" + (Math.round(total / 100) / 10).toFixed(1) + "K";
  return `<svg width="170" height="170" viewBox="0 0 170 170" xmlns="http://www.w3.org/2000/svg">
    <circle cx="85" cy="85" r="${r}" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="26"/>
    ${arcs}
    <text x="85" y="80" text-anchor="middle" fill="var(--text)" font-family="Syne,sans-serif" font-size="18" font-weight="700">${totalLabel}</text>
    <text x="85" y="96" text-anchor="middle" fill="var(--text3)" font-family="Space Mono,monospace" font-size="9" letter-spacing="1">TOTAL</text>
  </svg>`;
}

// Risk panel — three real metrics from analytics totals (beta, composite Z,
// concentration via top-2 weight) + four labeled stubs that will light up in
// later phases. Stubs use .stub class which dims them so the user sees what's
// coming without thinking the value is real.
function _pfRiskGrid(t, positions) {
  // Concentration: sum of top-2 weights, expressed as % of portfolio.
  const top2Weight = [...positions]
    .map(p => p.weight || 0)
    .sort((a, b) => b - a)
    .slice(0, 2)
    .reduce((a, b) => a + b, 0);
  const concPct = (top2Weight * 100).toFixed(0);
  const concTone = top2Weight > 0.4 ? "var(--danger)" : (top2Weight > 0.25 ? "#ffcc00" : "var(--success)");

  const beta = t.weighted_beta;
  const betaTone = beta == null ? "var(--text3)" : (beta > 1.2 ? "#ffcc00" : (beta < 0.8 ? "var(--accent2, #00cfff)" : "var(--success)"));
  const betaDesc = beta == null ? "no data"
    : (beta > 1.05 ? `${((beta - 1) * 100).toFixed(0)}% more volatile than market`
    : (beta < 0.95 ? `${((1 - beta) * 100).toFixed(0)}% less volatile than market` : "tracks market closely"));

  const cz = t.weighted_composite_z;
  const czTone = cz == null ? "var(--text3)" : (cz > 0.3 ? "var(--success)" : (cz < -0.3 ? "var(--danger)" : "var(--text2)"));
  const czDesc = cz == null ? "no data"
    : (cz > 0.3 ? "factor signals lean bullish" : (cz < -0.3 ? "factor signals lean bearish" : "factor signals neutral"));

  const items = [
    { name: "Portfolio Beta",    val: fmtNum(beta, 2),                  fill: Math.min(100, Math.max(5, (beta || 1) * 50)), color: betaTone, desc: betaDesc },
    { name: "Composite Z",       val: fmtNum(cz, 2),                    fill: Math.min(100, Math.max(5, ((cz || 0) + 2) * 25)), color: czTone, desc: czDesc },
    { name: "Concentration",     val: concPct + "%",                    fill: Math.min(100, top2Weight * 100), color: concTone, desc: positions.length >= 2 ? `Top 2 = ${concPct}% of portfolio` : "single position" },
    { name: "Positions",         val: String(positions.length),         fill: Math.min(100, positions.length * 10), color: "var(--accent2, #00cfff)", desc: positions.length < 5 ? "consider diversifying" : "good spread" },
  ];

  // Real risk metrics from /api/portfolio/analytics totals (Phase 2c).
  // Fixed-weight basket simulated over the last ~252 trading days.
  const sharpe = t.sharpe_ratio;
  const sharpeTone = sharpe == null ? "var(--text3)" : (sharpe >= 1.0 ? "var(--success)" : (sharpe >= 0.3 ? "#ffcc00" : "var(--danger)"));
  const sharpeDesc = sharpe == null ? "needs price history"
    : (sharpe >= 1.5 ? "excellent risk-adjusted return"
    : sharpe >= 1.0 ? "solid risk-adjusted return"
    : sharpe >= 0.3 ? "modest risk-adjusted return"
    : sharpe >= 0   ? "barely beats sitting in cash"
    : "risk not being rewarded");

  const mdd = t.max_drawdown;  // negative number
  const mddPct = mdd == null ? null : Math.abs(mdd * 100);
  const mddTone = mdd == null ? "var(--text3)" : (mddPct >= 30 ? "var(--danger)" : (mddPct >= 15 ? "#ffcc00" : "var(--success)"));
  const mddDesc = mdd == null ? "needs price history"
    : `worst peak\u2192trough drop in last ${t.risk_lookback_days || 252}d`;

  const var95 = t.var_95;  // positive fraction
  const var95Pct = var95 == null ? null : var95 * 100;
  const var95Dol = t.var_95_dollar;
  const varTone = var95 == null ? "var(--text3)" : (var95Pct >= 4 ? "var(--danger)" : (var95Pct >= 2 ? "#ffcc00" : "var(--success)"));
  const varDesc = var95 == null ? "needs price history"
    : `1-in-20 day loss \u2248 ${var95Dol != null ? "$" + Math.round(var95Dol).toLocaleString() : ""}`;

  const liq = t.liquidity_score;
  const liqAdv = t.liquidity_min_adv;
  const liqTone = liq == null ? "var(--text3)" : (liq >= 70 ? "var(--success)" : (liq >= 30 ? "#ffcc00" : "var(--danger)"));
  const liqDesc = liq == null ? "needs volume data"
    : `worst leg ADV \u2248 $${liqAdv >= 1e9 ? (liqAdv/1e9).toFixed(1) + "B" : liqAdv >= 1e6 ? (liqAdv/1e6).toFixed(0) + "M" : (liqAdv/1e3).toFixed(0) + "K"}/day`;

  items.push(
    { name: "Sharpe Ratio",    val: sharpe == null ? "\u2014" : sharpe.toFixed(2),
      fill: Math.min(100, Math.max(5, ((sharpe || 0) + 1) * 33)), color: sharpeTone, desc: sharpeDesc },
    { name: "Max Drawdown",    val: mddPct == null ? "\u2014" : "-" + mddPct.toFixed(1) + "%",
      fill: Math.min(100, mddPct || 5), color: mddTone, desc: mddDesc },
    { name: "VaR (95%)",       val: var95Pct == null ? "\u2014" : "-" + var95Pct.toFixed(2) + "%",
      fill: Math.min(100, (var95Pct || 0) * 15), color: varTone, desc: varDesc },
    { name: "Liquidity Score", val: liq == null ? "\u2014" : Math.round(liq) + "/100",
      fill: Math.min(100, liq || 5), color: liqTone, desc: liqDesc },
  );

  return items.map(it => `
    <div class="pf-risk-item${it.stub ? " stub" : ""}">
      <div class="pf-risk-name">${it.name}</div>
      <div class="pf-risk-val" style="color:${it.color}">${it.val}</div>
      <div class="pf-risk-track"><div class="pf-risk-fill" style="width:${it.fill}%;background:${it.color}"></div></div>
      <div class="pf-risk-desc">${it.desc}</div>
    </div>`).join("");
}

// ---- Live alerts -------------------------------------------------------
// Computed from the cached /api/portfolio/analytics payload. No backend, no
// cron, no email - alerts are surfaced when the user actually loads the page,
// which is the right time to act on them anyway.

function _computePortfolioAlerts() {
  const a = window._PF_ANALYTICS;
  if (!a) return [];
  const t = a.totals || {};
  const positions = a.positions || [];
  const out = [];

  // Day move alert - portfolio swing > 2% one way.
  if (t.value && t.day_change != null) {
    const prev = t.value - t.day_change;
    const pct = prev > 0 ? (t.day_change / prev) : 0;
    if (Math.abs(pct) >= 0.02) {
      out.push({
        kind: pct >= 0 ? "good" : "warn",
        icon: pct >= 0 ? "\u25B2" : "\u25BC",
        title: `Portfolio ${pct >= 0 ? "up" : "down"} ${(Math.abs(pct)*100).toFixed(1)}% today`,
        msg: `${pct >= 0 ? "+" : "-"}$${Math.abs(Math.round(t.day_change)).toLocaleString()} on $${Math.round(t.value).toLocaleString()} basket.`,
      });
    }
  }

  // Concentration alert - top single position > 30% of book.
  const sorted = positions.filter(p => p.weight != null).sort((x,y) => (y.weight||0) - (x.weight||0));
  if (sorted.length && sorted[0].weight >= 0.30) {
    out.push({
      kind: "warn",
      icon: "\u26A0",
      title: `${sorted[0].ticker} is ${(sorted[0].weight*100).toFixed(0)}% of your portfolio`,
      msg: "Single-name concentration above 30%. A bad earnings print here moves the whole book.",
    });
  }

  // Drawdown alert - any position down >15% from cost.
  positions.forEach(p => {
    if (p.unrealized_pl_pct != null && p.unrealized_pl_pct <= -0.15) {
      out.push({
        kind: "bad",
        icon: "\u25BC",
        title: `${p.ticker} down ${(p.unrealized_pl_pct*100).toFixed(0)}% from cost`,
        msg: `Unrealized loss of $${Math.abs(Math.round(p.unrealized_pl||0)).toLocaleString()}. Decide: thesis intact, or cut?`,
      });
    }
  });

  // Big winner alert - any position up >50% (consider trimming).
  positions.forEach(p => {
    if (p.unrealized_pl_pct != null && p.unrealized_pl_pct >= 0.50) {
      out.push({
        kind: "good",
        icon: "\u25B2",
        title: `${p.ticker} up ${(p.unrealized_pl_pct*100).toFixed(0)}% from cost`,
        msg: `Unrealized gain of $${Math.round(p.unrealized_pl||0).toLocaleString()}. Consider trimming back to target weight.`,
      });
    }
  });

  // Risk regime alert - portfolio Sharpe below 0 over last year.
  if (t.sharpe_ratio != null && t.sharpe_ratio < 0) {
    out.push({
      kind: "bad",
      icon: "\u26A0",
      title: `Negative Sharpe (${t.sharpe_ratio.toFixed(2)}) on current basket`,
      msg: "Last 252d risk-adjusted return is below cash. Rotate or rebalance.",
    });
  }

  // Drawdown alert - simulated max DD > 25% over last year.
  if (t.max_drawdown != null && t.max_drawdown <= -0.25) {
    out.push({
      kind: "warn",
      icon: "\u26A0",
      title: `Basket would have drawn down ${(Math.abs(t.max_drawdown)*100).toFixed(0)}% in the last year`,
      msg: "If you can't stomach that, the weights are too risky for you.",
    });
  }

  return out;
}

function _renderDashboardAlerts() {
  const banner = document.getElementById("pf-alerts-banner");
  if (!banner) return;
  const show = (localStorage.getItem("qd_alerts_show") || "1") === "1";
  if (!show) { banner.hidden = true; banner.innerHTML = ""; return; }
  const alerts = _computePortfolioAlerts();
  if (!alerts.length) { banner.hidden = true; banner.innerHTML = ""; return; }
  banner.hidden = false;
  banner.innerHTML = `
    <div class="pf-alerts-head">
      <span class="pf-alerts-title">\uD83D\uDD14 ${alerts.length} alert${alerts.length===1?"":"s"}</span>
      <button class="pf-alerts-dismiss" id="pf-alerts-dismiss" title="Hide for this session">\u00d7</button>
    </div>
    <div class="pf-alerts-list">
      ${alerts.map(a => `
        <div class="pf-alert pf-alert-${a.kind}">
          <span class="pf-alert-icon">${a.icon}</span>
          <span class="pf-alert-text"><strong>${_esc(a.title)}</strong> \u2014 ${_esc(a.msg)}</span>
        </div>`).join("")}
    </div>`;
  document.getElementById("pf-alerts-dismiss")?.addEventListener("click", () => {
    banner.hidden = true;
  });
}

async function _renderPortfolio() {
  const tableEl     = document.getElementById("pf-table");
  const stripEl     = document.getElementById("pf-stat-strip");
  const donutEl     = document.getElementById("pf-donut");
  const donutLegEl  = document.getElementById("pf-donut-legend");
  const sectorsEl   = document.getElementById("pf-sectors");
  const riskEl      = document.getElementById("pf-risk-grid");
  const riskBadgeEl = document.getElementById("pf-risk-overall");
  const countEl     = document.getElementById("pf-holdings-count");
  const metaEl      = document.getElementById("pf-meta");

  tableEl.innerHTML = `<div class="placeholder">Loading portfolio analytics…</div>`;
  stripEl.innerHTML = "";
  donutEl.innerHTML = "";
  donutLegEl.innerHTML = "";
  sectorsEl.innerHTML = "";
  riskEl.innerHTML = "";

  let analytics;
  try {
    analytics = await apiGet("/api/portfolio/analytics");
  } catch (e) {
    tableEl.innerHTML = `<div class="placeholder err">Analytics fetch failed: ${e.message}</div>`;
    return;
  }
  // Cache for the Account modal's Alerts section + the dashboard banner.
  window._PF_ANALYTICS = analytics;
  // Compute and surface alerts derived from this analytics payload.
  try { _renderDashboardAlerts(); } catch {}

  const positions = analytics.positions || [];
  if (metaEl) {
    const ts = analytics.as_of_utc ? analytics.as_of_utc.replace("T", " ").slice(0, 16) + " UTC" : "no data";
    metaEl.textContent = `${positions.length} position${positions.length === 1 ? "" : "s"} · prices as of ${ts}`;
  }

  const t = analytics.totals || {};

  // Stat strip — always shown, even when empty, so the layout doesn't reflow
  // dramatically when the user adds their first position.
  const dayPct = (t.value && (t.value - (t.day_change || 0))) ? (t.day_change || 0) / (t.value - (t.day_change || 0)) : null;
  const totalSub = (t.unrealized_pl || 0) >= 0
    ? `▲ ${fmtPrice(t.unrealized_pl)} all-time`
    : `▼ ${fmtPrice(Math.abs(t.unrealized_pl || 0))} all-time`;
  stripEl.innerHTML = [
    _pfStat("Total Value",     fmtPrice(t.value),         totalSub,                                         (t.unrealized_pl || 0) >= 0 ? "up" : "dn"),
    _pfStat("Today's P&amp;L", fmtPrice(t.day_change),    fmtPct(dayPct) + " · " + positions.length + " holdings", (t.day_change || 0) >= 0 ? "up" : "dn"),
    _pfStat("Unrealized",      fmtPrice(t.unrealized_pl), fmtPct(t.unrealized_pl_pct) + " return",          (t.unrealized_pl || 0) >= 0 ? "up" : "dn"),
    _pfStat("Cost Basis",      fmtPrice(t.cost),          "across " + positions.length + " positions",      "neu"),
    _pfStat("Beta vs " + (analytics.benchmark || "SPY"), fmtNum(t.weighted_beta, 2), "value-weighted",       "neu"),
    _pfStat("Composite Z",     fmtNum(t.weighted_composite_z, 2), "factor score",                            (t.weighted_composite_z || 0) >= 0 ? "up" : "dn"),
    _pfStat("YTD (basket)",
      t.ytd_return == null ? "\u2014" : (t.ytd_return >= 0 ? "+" : "") + (t.ytd_return * 100).toFixed(2) + "%",
      t.ytd_dollar == null ? "current weights since Jan 1"
        : (t.ytd_dollar >= 0 ? "+" : "-") + "$" + Math.abs(Math.round(t.ytd_dollar)).toLocaleString() + " on current basket",
      t.ytd_return == null ? "neu" : (t.ytd_return >= 0 ? "up" : "dn")),
  ].join("");

  if (countEl) countEl.textContent = `${positions.length} HOLDING${positions.length === 1 ? "" : "S"}`;

  if (!positions.length) {
    tableEl.innerHTML = `<div class="placeholder" style="padding:30px 18px">No positions yet. Add one using the form above.</div>`;
    sectorsEl.innerHTML = `<div class="placeholder" style="padding:18px">No sector exposure yet.</div>`;
    donutEl.innerHTML = `<div class="placeholder" style="padding:30px 18px">Add positions to see allocation.</div>`;
    if (riskBadgeEl) riskBadgeEl.textContent = "NO DATA";
    riskEl.innerHTML = `<div class="placeholder" style="padding:18px;grid-column:1/-1">Risk metrics appear once you have positions.</div>`;
    return;
  }

  // Holdings table — reuses analytics rows. Action buttons: BUY pre-fills the
  // add-position form (so user can confirm shares/cost), SELL opens a partial-
  // sell modal so the user can enter how many shares to sell.
  const rows = positions.map((p, i) => {
    const dayCls   = (p.day_change_pct || 0) >= 0 ? "pos" : "neg";
    const uplCls   = (p.unrealized_pl   || 0) >= 0 ? "pos" : "neg";
    const weightPct = ((p.weight || 0) * 100).toFixed(1);
    const color    = PF_DONUT_COLORS[i % PF_DONUT_COLORS.length];
    const sigBadge = p.signal
      ? `<span class="pf-h-sigbadge sig-${(p.signal||"").toLowerCase().replace(/\s+/g,"-")}">${p.signal}</span>`
      : "";
    return `
      <tr>
        <td>
          <div class="pf-h-sym">${p.ticker} ${sigBadge}</div>
          <div class="pf-h-sub">${p.sector || "—"}</div>
        </td>
        <td>${(p.shares || 0).toLocaleString("en-US", {maximumFractionDigits: 4})}</td>
        <td>${fmtPrice(p.avg_cost)}</td>
        <td>${fmtPrice(p.price)}</td>
        <td class="${dayCls}">${fmtPct(p.day_change_pct)}</td>
        <td>${fmtPrice(p.value)}</td>
        <td class="${uplCls}">${fmtPrice(p.unrealized_pl)} <span style="font-size:9px;opacity:0.7">(${fmtPct(p.unrealized_pl_pct)})</span></td>
        <td>
          <div class="pf-h-wt">
            <div class="pf-h-bar-wrap"><div class="pf-h-bar" style="width:${weightPct}%;background:${color}"></div></div>
            <span class="pf-h-wt-num">${weightPct}%</span>
          </div>
        </td>
        <td>
          <div class="pf-h-actions">
            <button class="pf-act pf-act-buy"  data-buy="${p.ticker}"  data-price="${p.price ?? ""}">BUY</button>
            <button class="pf-act pf-act-sell" data-sell="${p.ticker}">SELL</button>
          </div>
        </td>
      </tr>`;
  }).join("");

  tableEl.innerHTML = `
    <table class="pf-holdings-table">
      <thead><tr>
        <th>Symbol</th><th>Shares</th><th>Avg</th><th>Price</th>
        <th>Day</th><th>Value</th><th>Unrl P&amp;L</th>
        <th>Weight</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;

  // Donut + legend. Sort by weight desc so the biggest slice starts at 12 o'clock.
  const sorted = [...positions].sort((a, b) => (b.weight || 0) - (a.weight || 0));
  donutEl.innerHTML = _pfDonut(sorted, t.value || 0);
  donutLegEl.innerHTML = sorted.slice(0, 8).map((p, i) => {
    const color = PF_DONUT_COLORS[i % PF_DONUT_COLORS.length];
    return `<div class="pf-leg">
      <div class="pf-leg-dot" style="background:${color}"></div>
      <div class="pf-leg-sym">${p.ticker}</div>
      <div class="pf-leg-pct">${((p.weight || 0) * 100).toFixed(1)}%</div>
      <div class="pf-leg-val">${fmtPrice(p.value)}</div>
    </div>`;
  }).join("") + (sorted.length > 8
    ? `<div class="pf-leg" style="opacity:0.6"><div class="pf-leg-dot" style="background:#666"></div><div class="pf-leg-sym">+${sorted.length - 8} more</div><div></div><div></div></div>`
    : "");

  // Sector exposure — same data as before, in the new card.
  const sectorRows = (analytics.sector_exposure || []).map(s => `
    <tr>
      <td><strong>${s.sector}</strong></td>
      <td>${fmtPrice(s.value)}</td>
      <td>${fmtPct(s.weight)}</td>
      <td><div style="background:rgba(0,255,170,0.35);height:5px;width:${Math.max(2, (s.weight||0)*100).toFixed(1)}%;border-radius:2px"></div></td>
    </tr>`).join("");
  sectorsEl.innerHTML = `
    <table>
      <thead><tr><th>Sector</th><th>Value</th><th>Weight</th><th></th></tr></thead>
      <tbody>${sectorRows || `<tr><td colspan="4" class="placeholder">no sectors</td></tr>`}</tbody>
    </table>`;

  // Risk panel
  riskEl.innerHTML = _pfRiskGrid(t, positions);
  if (riskBadgeEl) {
    // Crude overall: bad concentration OR high beta = "ELEVATED", otherwise "MODERATE".
    const top2 = [...positions].map(p => p.weight || 0).sort((a, b) => b - a).slice(0, 2).reduce((a, b) => a + b, 0);
    const elevated = top2 > 0.5 || (t.weighted_beta || 0) > 1.4;
    riskBadgeEl.textContent = elevated ? "ELEVATED" : "MODERATE";
    riskBadgeEl.className = "pf-badge " + (elevated ? "pf-badge-r" : "pf-badge-w");
  }

  // Wire BUY → prefill add-position form, scroll into view, focus shares input.
  // Wire SELL → confirm + remove the position entirely.
  tableEl.querySelectorAll("button[data-buy]").forEach(btn => {
    btn.addEventListener("click", () => {
      const ticker = btn.dataset.buy;
      const price  = btn.dataset.price;
      const tEl = document.getElementById("pf-ticker");
      const sEl = document.getElementById("pf-shares");
      const cEl = document.getElementById("pf-cost");
      if (tEl) tEl.value = ticker;
      if (cEl && price && !cEl.value) cEl.value = price;
      if (sEl) { sEl.focus(); sEl.select(); }
      tEl?.scrollIntoView({ behavior: "smooth", block: "center" });
    });
  });
  tableEl.querySelectorAll("button[data-sell]").forEach(btn => {
    btn.addEventListener("click", () => {
      const ticker = btn.dataset.sell;
      const row = positions.find(p => p.ticker === ticker);
      if (!row) return;
      _openSellModal(row);
    });
  });

  // Performance chart — fetched separately so a slow history call doesn't
  // block the rest of the page from rendering.
  _renderPortfolioPerf().catch(() => { /* errors shown inline */ });

  // Advisor card — independent fetch so it doesn't block holdings.
  _renderAdvisor().catch(() => { /* status updates inline */ });

  // Pairs Trading card — wires inputs once, pre-fills from holdings.
  _wirePortfolioPairs(positions);

  // Pairs Opportunities — curated pre-screen, AI commentary on demand.
  _renderPairsOpportunities();

  // Strategy cards — wire one-time clicks; results panel hidden until clicked.
  _wireStrategyCards();

  // Money Multiplier — Monte Carlo card. Wire once on first portfolio render.
  _wireMultiplier();

  // Auto-hydrate any positions that are missing prices (rows showing "\u2014").
  // Only triggers once per render cycle; the refresh endpoint will retry
  // foreign tickers via .TO/.V fallback. Re-renders when done.
  const missing = (analytics?.diagnostics?.missing_prices) || [];
  if (missing.length && !window._PF_AUTO_HYDRATED) {
    window._PF_AUTO_HYDRATED = true;
    apiPost("/api/portfolio/refresh", {})
      .then(() => _renderPortfolio())
      .catch(() => { /* leave em-dashes; user can click Refresh */ });
  }
}

// Active period for the equity curve. Persisted in-module so re-renders
// (after add/sell) keep the user's selection.
let _PF_PERF_PERIOD = "1y";

// ---------- Investment Strategies --------------------------------------
// Six clickable strategy cards each open a screener panel below them.
// Backend: GET /api/strategies/screen?strategy=X. Pure read; uses cached
// market data so calls are <100ms. AI commentary on demand via
// /api/advisor/explain_strategy (optional, costs 1 Haiku call).

let _STRAT_LAST_DATA = null;
let _STRAT_LAST_KEY  = null;

function _wireStrategyCards() {
  const grid = document.getElementById("pf-strat-grid");
  if (!grid || grid.dataset.wired) return;
  grid.dataset.wired = "1";
  grid.querySelectorAll(".pf-strat-card").forEach(btn => {
    btn.addEventListener("click", () => _runStrategy(btn.dataset.strat));
  });
  const closeBtn = document.getElementById("pf-strat-close");
  if (closeBtn) closeBtn.addEventListener("click", () => {
    document.getElementById("pf-strat-card").hidden = true;
    document.querySelectorAll(".pf-strat-card.active").forEach(b => b.classList.remove("active"));
    _STRAT_LAST_DATA = null;
    _STRAT_LAST_KEY = null;
  });
  const aiBtn = document.getElementById("pf-strat-ai");
  if (aiBtn) aiBtn.addEventListener("click", () => _runStrategyAI());
}

async function _runStrategy(key) {
  if (!key) return;
  const card  = document.getElementById("pf-strat-card");
  const title = document.getElementById("pf-strat-title");
  const count = document.getElementById("pf-strat-count");
  const thes  = document.getElementById("pf-strat-thesis");
  const list  = document.getElementById("pf-strat-list");
  const aiBtn = document.getElementById("pf-strat-ai");
  const aiBox = document.getElementById("pf-strat-ai-box");
  if (!card) return;

  // Highlight active card.
  document.querySelectorAll(".pf-strat-card").forEach(b => {
    b.classList.toggle("active", b.dataset.strat === key);
  });

  card.hidden = false;
  if (aiBox) aiBox.hidden = true;
  if (aiBtn) { aiBtn.disabled = true; aiBtn.textContent = "🧠 Quant's read"; }
  list.innerHTML = `<div class="pf-strat-empty">Scanning universe\u2026</div>`;
  title.textContent = "Strategy Results";
  count.textContent = "\u2014";
  thes.innerHTML = "";

  // Smooth scroll to the results card so it's clear something happened.
  setTimeout(() => card.scrollIntoView({ behavior: "smooth", block: "start" }), 50);

  try {
    const data = await apiGet(`/api/strategies/screen?strategy=${encodeURIComponent(key)}&limit=20`);
    _STRAT_LAST_DATA = data;
    _STRAT_LAST_KEY  = key;
    title.textContent = data.name || "Strategy Results";
    count.textContent = `${data.count || 0} picks`;
    thes.innerHTML = `<strong>Thesis:</strong> ${_esc(data.thesis || "")} <br><strong>Fits:</strong> ${_esc(data.fits || "")}`;
    _renderStrategyRows(data);
    if (aiBtn && data.results && data.results.length) aiBtn.disabled = false;
  } catch (e) {
    list.innerHTML = `<div class="pf-strat-empty">Scan failed: ${_esc((e && e.message) || "request failed")}</div>`;
    _STRAT_LAST_DATA = null;
  }
}

function _renderStrategyRows(data) {
  const list = document.getElementById("pf-strat-list");
  if (!list) return;
  const rows = data.results || [];
  if (!rows.length) {
    list.innerHTML = `<div class="pf-strat-empty">No names match this strategy right now. Market may be in a different regime \u2014 try another lens.</div>`;
    return;
  }

  // Pick a "headline metric" per strategy so the right-most number means
  // something specific to what the user clicked on, not just composite-Z.
  const headline = {
    momentum:         { key: "Momentum_12_1",     label: "12-1 Mom",  pct: true },
    mean_reversion:   { key: "Return_21d",        label: "21d Return", pct: true },
    breakout:         { key: "Dist_52w_High",     label: "From 52w Hi", pct: true },
    value:            { key: "Alpha_Annualized",  label: "Alpha",     pct: true },
    dividend_capture: { key: "Max_Drawdown_252d", label: "Max DD",    pct: true },
    quant_signals:    { key: "Composite_Z",       label: "Composite Z", pct: false },
  }[data.strategy] || { key: "Composite_Z", label: "Composite Z", pct: false };

  const head =
    `<div class="pf-strat-row head">` +
    `<div>Ticker</div><div>Name</div><div style="text-align:right;">Price</div>` +
    `<div style="text-align:right;">${headline.label}</div>` +
    `<div style="text-align:right;">Z-score</div><div style="text-align:right;">Signal</div>` +
    `</div>`;

  const fmt = (v, asPct) => {
    if (v === null || v === undefined || isNaN(v)) return "\u2014";
    if (asPct) return (v * 100).toFixed(1) + "%";
    return Number(v).toFixed(2);
  };
  const sigClass = (s) => {
    const u = (s || "").toUpperCase();
    if (u.includes("STRONG_BUY") || u.includes("STRONG BUY") || u === "BUY") return "buy";
    if (u.includes("AVOID") || u.includes("HIGH_RISK") || u.includes("HIGH RISK")) return "avoid";
    return "hold";
  };

  const body = rows.map(r => {
    const hv = r[headline.key];
    const hvCls = (typeof hv === "number" && hv < 0) ? "neg" : (typeof hv === "number" && hv > 0 ? "pos" : "");
    const z = r.Composite_Z;
    const zCls = (typeof z === "number" && z > 0) ? "pos" : (typeof z === "number" && z < 0 ? "neg" : "");
    return `<div class="pf-strat-row" data-tk="${_esc(r.Ticker)}">
      <div class="pf-strat-tk">${_esc(r.Ticker)}</div>
      <div class="pf-strat-name-cell" title="${_esc(r.Name || "")}">${_esc(r.Name || "")}</div>
      <div class="pf-strat-num">${r.Price != null ? "$" + Number(r.Price).toFixed(2) : "\u2014"}</div>
      <div class="pf-strat-num ${hvCls}">${fmt(hv, headline.pct)}</div>
      <div class="pf-strat-num ${zCls}">${fmt(z, false)}</div>
      <div class="pf-strat-sig ${sigClass(r.Signal)}">${_esc(r.Signal || "\u2014")}</div>
    </div>`;
  }).join("");

  list.innerHTML = head + body;
}

async function _runStrategyAI() {
  if (!_STRAT_LAST_DATA || !_STRAT_LAST_KEY) return;
  const aiBtn = document.getElementById("pf-strat-ai");
  const aiBox = document.getElementById("pf-strat-ai-box");
  const aiBody = document.getElementById("pf-strat-ai-body");
  if (!aiBtn || !aiBox || !aiBody) return;

  const orig = aiBtn.textContent;
  aiBtn.disabled = true;
  aiBtn.textContent = "🧠 Quant is thinking\u2026";

  try {
    const top = (_STRAT_LAST_DATA.results || []).slice(0, 10).map(r => ({
      ticker: r.Ticker,
      sector: r.Sector,
      composite_z: r.Composite_Z,
      signal: r.Signal,
    }));
    const res = await apiPost("/api/advisor/explain_strategy", {
      strategy: _STRAT_LAST_KEY,
      strategy_name: _STRAT_LAST_DATA.name,
      thesis: _STRAT_LAST_DATA.thesis,
      picks: top,
    });
    aiBox.hidden = false;
    aiBody.textContent = res.answer || "(no response)";
  } catch (e) {
    aiBox.hidden = false;
    aiBody.textContent = "Quant call failed: " + ((e && e.message) || "request failed");
  } finally {
    aiBtn.textContent = orig;
    aiBtn.disabled = false;
  }
}

function _esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}


// ---------- Money Multiplier (Phase 2E) -------------------------------
// Bootstrap Monte Carlo on a single ticker. Backend is /api/multiplier/simulate;
// pure numpy, ~3,000 paths, sub-second. AI commentary on demand via
// /api/advisor/explain_multiplier.

let _MULT_LAST = null;

function _wireMultiplier() {
  const btn = document.getElementById("pf-mult-go");
  if (!btn || btn.dataset.wired) return;
  btn.dataset.wired = "1";
  btn.addEventListener("click", () => _runMultiplier());
  // Run once on first load with the default NVDA/2x/5y so the card isn't empty.
  _runMultiplier();
}

async function _runMultiplier() {
  const tk     = (document.getElementById("pf-mult-ticker").value || "").trim().toUpperCase();
  const target = parseFloat(document.getElementById("pf-mult-target").value);
  const horiz  = parseInt(document.getElementById("pf-mult-horizon").value, 10);
  const lookb  = parseInt(document.getElementById("pf-mult-lookback").value, 10);
  const mode   = (document.getElementById("pf-mult-mode") || {}).value || "shrunk";
  const btn    = document.getElementById("pf-mult-go");
  const stats  = document.getElementById("pf-mult-stats");
  const charts = document.getElementById("pf-mult-charts");
  const aiBox  = document.getElementById("pf-mult-ai");
  const warn   = document.getElementById("pf-mult-warn");

  if (!tk) {
    stats.hidden = false;
    stats.innerHTML = `<div class="pf-mult-stat" style="grid-column:1/-1;color:#f87171;">Enter a ticker.</div>`;
    return;
  }

  const orig = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Running\u2026";
  stats.hidden = false;
  stats.innerHTML = `<div class="pf-mult-stat" style="grid-column:1/-1;">Sampling 3,000 paths over ${horiz} days from ${tk}'s last ${lookb} days (${mode} mode)\u2026</div>`;
  charts.hidden = true;
  if (warn) { warn.hidden = true; warn.innerHTML = ""; }
  if (aiBox) aiBox.hidden = true;

  try {
    const data = await apiPost("/api/multiplier/simulate", {
      ticker: tk,
      target_multiple: target,
      horizon_days: horiz,
      lookback_days: lookb,
      n_paths: 3000,
      mode: mode,
    });
    _MULT_LAST = data;
    _renderMultiplierWarnings(data);
    _renderMultiplierStats(data);
    _renderMultiplierCharts(data);
  } catch (e) {
    stats.innerHTML = `<div class="pf-mult-stat" style="grid-column:1/-1;color:#f87171;">Simulation failed: ${_esc((e && e.message) || "request failed")}</div>`;
    charts.hidden = true;
    _MULT_LAST = null;
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
}

function _daysToHuman(n) {
  if (n === null || n === undefined) return "n/a";
  const d = Math.round(n);
  const yrs = (d / 252);
  if (yrs < 1) return `${d}d (~${(d / 21).toFixed(1)}mo)`;
  return `${d}d (~${yrs.toFixed(1)}y)`;
}

function _renderMultiplierWarnings(d) {
  const box = document.getElementById("pf-mult-warn");
  if (!box) return;
  const ws = d.warnings || [];
  if (!ws.length) { box.hidden = true; box.innerHTML = ""; return; }
  const modeLabel = { shrunk: "Shrunk", blended: "Blended", naive: "Naive" }[d.mode] || d.mode;
  const raw = d.regime || {};
  const rawAnn = raw.raw_annual_return != null ? (raw.raw_annual_return * 100).toFixed(0) + "%" : "n/a";
  const effAnn = raw.annual_return_est != null ? (raw.annual_return_est * 100).toFixed(0) + "%" : "n/a";
  box.hidden = false;
  box.innerHTML = `
    <div style="margin-bottom:6px;"><strong>Regime check (${_esc(modeLabel)} mode):</strong>
      raw recent regime = <strong>${rawAnn}</strong>/yr,
      effective input to bootstrap = <strong>${effAnn}</strong>/yr.</div>
    <ul>${ws.map(w => `<li>${_esc(w)}</li>`).join("")}</ul>
  `;
}

function _renderMultiplierStats(d) {
  const stats = document.getElementById("pf-mult-stats");
  if (!stats) return;
  const r = d.results || {};
  const dd = d.drawdown || {};
  const reg = d.regime || {};
  const prob = (r.prob_reached || 0) * 100;
  const probCls = prob >= 70 ? "pos" : prob >= 35 ? "warn" : "neg";

  // The drawdown the median path suffers en route. p25 = "25% of paths
  // were AT LEAST this deep underwater"; the median draws ~p50.
  const ddMed = (dd.p50 || 0) * 100;
  const ddBad = (dd.p10 || 0) * 100;

  stats.innerHTML = `
    <div class="pf-mult-stat">
      <span class="pf-mult-stat-label">P(reach ${d.target_multiple}x)</span>
      <span class="pf-mult-stat-val ${probCls}">${prob.toFixed(0)}%</span>
      <span class="pf-mult-stat-sub">within ${d.horizon_days} trading days</span>
    </div>
    <div class="pf-mult-stat">
      <span class="pf-mult-stat-label">Median time</span>
      <span class="pf-mult-stat-val">${_daysToHuman(r.p50_days)}</span>
      <span class="pf-mult-stat-sub">50% of paths cross by here</span>
    </div>
    <div class="pf-mult-stat">
      <span class="pf-mult-stat-label">Optimistic (p25)</span>
      <span class="pf-mult-stat-val pos">${_daysToHuman(r.p25_days)}</span>
      <span class="pf-mult-stat-sub">fastest 25% of paths</span>
    </div>
    <div class="pf-mult-stat">
      <span class="pf-mult-stat-label">Pessimistic (p75)</span>
      <span class="pf-mult-stat-val warn">${_daysToHuman(r.p75_days)}</span>
      <span class="pf-mult-stat-sub">slowest 25% (that reach)</span>
    </div>
    <div class="pf-mult-stat">
      <span class="pf-mult-stat-label">Median max-DD</span>
      <span class="pf-mult-stat-val warn">${ddMed.toFixed(1)}%</span>
      <span class="pf-mult-stat-sub">cost of the journey</span>
    </div>
    <div class="pf-mult-stat">
      <span class="pf-mult-stat-label">Worst-decile max-DD</span>
      <span class="pf-mult-stat-val neg">${ddBad.toFixed(1)}%</span>
      <span class="pf-mult-stat-sub">~est. annual vol ${(reg.annual_vol_est * 100).toFixed(0)}%</span>
    </div>
  `;
}

function _renderMultiplierCharts(d) {
  const wrap = document.getElementById("pf-mult-charts");
  const aiBox = document.getElementById("pf-mult-ai");
  if (!wrap || !window.Chart) return;
  wrap.hidden = false;

  // Show "Quant's read" button alongside results (only one button so put it
  // in the AI body header dynamically). Simpler: add inline button under stats.
  if (aiBox) {
    let runBtn = document.getElementById("pf-mult-ai-go");
    if (!runBtn) {
      const stats = document.getElementById("pf-mult-stats");
      const wrapBtn = document.createElement("div");
      wrapBtn.style.cssText = "grid-column:1/-1;display:flex;justify-content:flex-end;margin-top:4px;";
      wrapBtn.innerHTML = `<button id="pf-mult-ai-go" class="pill cta" type="button">🧠 Get Quant's read</button>`;
      stats.appendChild(wrapBtn);
      document.getElementById("pf-mult-ai-go").addEventListener("click", () => _runMultiplierAI());
    }
  }

  // --- Equity-curve fan chart ---------------------------------------
  const eq = d.equity_curve || {};
  const fanCanvas = document.getElementById("pf-mult-fan");
  if (chartRegistry["pfmult_fan"]) chartRegistry["pfmult_fan"].destroy();
  chartRegistry["pfmult_fan"] = new Chart(fanCanvas, {
    type: "line",
    data: {
      labels: eq.days || [],
      datasets: [
        { label: "p90", data: eq.p90, borderColor: "rgba(74,222,128,0.8)", backgroundColor: "rgba(74,222,128,0.10)", fill: "+1", tension: 0.2, pointRadius: 0, borderWidth: 1 },
        { label: "p75", data: eq.p75, borderColor: "rgba(74,222,128,0.5)", backgroundColor: "rgba(74,222,128,0.10)", fill: "+1", tension: 0.2, pointRadius: 0, borderWidth: 1 },
        { label: "p50 (median)", data: eq.p50, borderColor: "#5ad6c7", borderWidth: 2, tension: 0.2, pointRadius: 0, fill: false },
        { label: "p25", data: eq.p25, borderColor: "rgba(248,113,113,0.5)", backgroundColor: "rgba(248,113,113,0.10)", fill: "+1", tension: 0.2, pointRadius: 0, borderWidth: 1 },
        { label: "p10", data: eq.p10, borderColor: "rgba(248,113,113,0.8)", backgroundColor: "rgba(248,113,113,0.10)", tension: 0.2, pointRadius: 0, borderWidth: 1 },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: {
        legend: { labels: { color: "#b8b8c5", font: { size: 10 }, boxWidth: 14 } },
        tooltip: { mode: "index", intersect: false, callbacks: { label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}x` } },
        annotation: undefined,
      },
      scales: {
        x: { ticks: { color: "#b8b8c5", maxTicksLimit: 8, font: { size: 9 } }, grid: { color: "rgba(255,255,255,0.04)" }, title: { display: true, text: "Trading days forward", color: "#b8b8c5", font: { size: 10 } } },
        y: { ticks: { color: "#b8b8c5", font: { size: 9 }, callback: (v) => v.toFixed(1) + "x" }, grid: { color: "rgba(255,255,255,0.04)" } },
      },
    },
  });

  // --- Histogram of days-to-target ----------------------------------
  const h = d.histogram || {};
  const histCanvas = document.getElementById("pf-mult-hist");
  if (chartRegistry["pfmult_hist"]) chartRegistry["pfmult_hist"].destroy();
  chartRegistry["pfmult_hist"] = new Chart(histCanvas, {
    type: "bar",
    data: {
      labels: (h.bin_centers || []).map(x => Math.round(x)),
      datasets: [{
        label: `Days to ${d.target_multiple}x`,
        data: h.counts || [],
        backgroundColor: "rgba(90,214,199,0.6)",
        borderColor: "rgba(90,214,199,1)",
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { title: (ctx) => `~${ctx[0].label} days`, label: (ctx) => `${ctx.parsed.y} paths` } },
      },
      scales: {
        x: { ticks: { color: "#b8b8c5", maxTicksLimit: 10, font: { size: 9 } }, grid: { display: false }, title: { display: true, text: "Trading days to target", color: "#b8b8c5", font: { size: 10 } } },
        y: { ticks: { color: "#b8b8c5", font: { size: 9 } }, grid: { color: "rgba(255,255,255,0.04)" }, title: { display: true, text: "# paths", color: "#b8b8c5", font: { size: 10 } } },
      },
    },
  });
}

async function _runMultiplierAI() {
  if (!_MULT_LAST) return;
  const aiBox = document.getElementById("pf-mult-ai");
  const aiBody = document.getElementById("pf-mult-ai-body");
  const btn = document.getElementById("pf-mult-ai-go");
  if (!aiBox || !aiBody) return;
  const orig = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "🧠 Quant is thinking\u2026"; }
  try {
    const r = await apiPost("/api/advisor/explain_multiplier", {
      ticker: _MULT_LAST.ticker,
      target_multiple: _MULT_LAST.target_multiple,
      horizon_days: _MULT_LAST.horizon_days,
      prob_reached: _MULT_LAST.results?.prob_reached,
      p50_days: _MULT_LAST.results?.p50_days,
      p25_days: _MULT_LAST.results?.p25_days,
      p75_days: _MULT_LAST.results?.p75_days,
      median_max_drawdown: _MULT_LAST.drawdown?.p50,
      worst_decile_drawdown: _MULT_LAST.drawdown?.p10,
      annual_return_est: _MULT_LAST.regime?.annual_return_est,
      annual_vol_est: _MULT_LAST.regime?.annual_vol_est,
      mode: _MULT_LAST.mode,
      raw_annual_return: _MULT_LAST.regime?.raw_annual_return,
      raw_annual_vol: _MULT_LAST.regime?.raw_annual_vol,
      warnings: _MULT_LAST.warnings || [],
    });
    aiBox.hidden = false;
    aiBody.textContent = r.answer || "(no response)";
  } catch (e) {
    aiBox.hidden = false;
    aiBody.textContent = "Quant call failed: " + ((e && e.message) || "request failed");
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = orig || "🧠 Get Quant's read"; }
  }
}


// ---------- Advisor ----------------------------------------------------
// BYOK Anthropic chat. Fetches /api/advisor/key to decide whether to show
// the gate (no key) or the chat (key on file). Send button POSTs the user's
// message to /api/advisor/chat with the selected model.

async function _renderAdvisor() {
  const card    = document.getElementById("pf-advisor-card");
  const gate    = document.getElementById("adv-gate");
  const chat    = document.getElementById("adv-chat");
  const status  = document.getElementById("adv-status");
  const gateBtn = document.getElementById("adv-gate-btn");
  if (!card || !status) return;

  // Anonymous device users can't BYOK — they need to sign in first.
  if (!Auth.user()) {
    gate.hidden = false; chat.hidden = true;
    status.textContent = "SIGN IN";
    status.className = "pf-badge pf-badge-o";
    gate.querySelector(".adv-gate-text").innerHTML =
      "<strong>Sign in to use the AI Advisor.</strong> Bring-your-own-key (Anthropic) " +
      "lets you chat with Claude about your portfolio. Your key stays encrypted on our server.";
    gateBtn.textContent = "Sign in";
    gateBtn.onclick = () => Auth.open("login");
    return;
  }

  let s;
  try { s = await apiGet("/api/advisor/key"); }
  catch { status.textContent = "OFFLINE"; status.className = "pf-badge pf-badge-o"; return; }

  if (!s.has_key) {
    gate.hidden = false; chat.hidden = true;
    status.textContent = "NO KEY";
    status.className = "pf-badge pf-badge-o";
    gateBtn.textContent = "Add API Key";
    gateBtn.onclick = () => Auth.openAccount();
    return;
  }

  gate.hidden = true; chat.hidden = false;
  status.textContent = "READY";
  status.className = "pf-badge pf-badge-g";
  _wireAdvChat();
  _renderAdvEmpty();
}
window._renderAdvisor = _renderAdvisor;

// Welcome / suggested-prompt block shown when the chat log is empty.
// Clickable chips drop a prompt into the textarea so first-time users
// don't have to invent a question from scratch.
function _renderAdvEmpty() {
  const log = document.getElementById("adv-log");
  if (!log || log.children.length) return;
  const empty = document.createElement("div");
  empty.className = "adv-empty";
  empty.innerHTML =
    `<div class="adv-empty-line">` +
    `<strong>Ask anything about your portfolio.</strong> ` +
    `I can see your holdings, factor scores, sector mix, and signals. Try:` +
    `</div>` +
    `<div class="adv-suggest-row">` +
    `<button type="button" class="adv-suggest" data-q="What's my biggest risk right now?">Biggest risk?</button>` +
    `<button type="button" class="adv-suggest" data-q="Which positions look weakest based on the factor signals?">Weakest positions?</button>` +
    `<button type="button" class="adv-suggest" data-q="Am I overconcentrated in any sector? Suggest a rebalance.">Sector rebalance</button>` +
    `<button type="button" class="adv-suggest" data-q="Summarize my portfolio in one paragraph.">Quick summary</button>` +
    `</div>`;
  log.appendChild(empty);
  empty.querySelectorAll(".adv-suggest").forEach(b => {
    b.addEventListener("click", () => {
      const inp = document.getElementById("adv-input");
      inp.value = b.dataset.q;
      inp.focus();
    });
  });
}

// Per-user (or per-device) chat history persistence. Keyed by the user's
// id when signed in so two accounts on one browser don't share threads;
// falls back to a stable "anon" key for the rare BYOK-but-anonymous flow
// (won't actually happen since /api/advisor requires auth, but harmless).
function _advHistKey() {
  const u = (typeof Auth !== "undefined" && Auth.user && Auth.user()) || null;
  return "qd.adv.hist." + (u && (u.id || u.email) ? String(u.id || u.email) : "anon");
}
const _ADV_HIST_MAX = 40;  // cap stored turns to keep localStorage small

function _advLoadHistory() {
  try {
    const raw = localStorage.getItem(_advHistKey());
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch (_) { return []; }
}
function _advSaveHistory(arr) {
  try {
    const trimmed = arr.slice(-_ADV_HIST_MAX);
    localStorage.setItem(_advHistKey(), JSON.stringify(trimmed));
  } catch (_) { /* quota exceeded - ignore */ }
}
function _advClearHistory() {
  try { localStorage.removeItem(_advHistKey()); } catch (_) {}
}

function _wireAdvChat() {
  const log    = document.getElementById("adv-log");
  const input  = document.getElementById("adv-input");
  const send   = document.getElementById("adv-send");
  const model  = document.getElementById("adv-model");
  const clearBtn = document.getElementById("adv-clear");
  if (!log || send.dataset.wired) return;
  send.dataset.wired = "1";

  function appendMsg(role, text) {
    // First user/bot message after the welcome block clears the welcome.
    const empty = log.querySelector(".adv-empty");
    if (empty) empty.remove();
    const div = document.createElement("div");
    div.className = "adv-msg adv-msg-" + role;
    div.innerHTML = `<div class="adv-role">${role === "user" ? "You" : "Advisor"}</div>` +
                    `<div class="adv-text"></div>`;
    div.querySelector(".adv-text").textContent = text;
    log.appendChild(div);
    log.scrollTop = log.scrollHeight;
    return div.querySelector(".adv-text");
  }

  // Restore prior turns from localStorage on first wire of this page-load.
  const history = _advLoadHistory();
  if (history.length) {
    const empty = log.querySelector(".adv-empty");
    if (empty) empty.remove();
    for (const turn of history) {
      appendMsg(turn.role, turn.text);
    }
  }

  async function submit() {
    const msg = input.value.trim();
    if (!msg) return;
    input.value = "";
    appendMsg("user", msg);
    const placeholder = appendMsg("bot", "Thinking…");
    send.disabled = true;
    try {
      const r = await apiPost("/api/advisor/chat", {
        message: msg,
        model: model.value,
        include_portfolio: true,
      });
      const answer = r.answer || "(no response)";
      placeholder.textContent = answer;
      // Persist on success only \u2014 errors aren't worth re-loading.
      const cur = _advLoadHistory();
      cur.push({ role: "user", text: msg });
      cur.push({ role: "bot",  text: answer });
      _advSaveHistory(cur);
    } catch (e) {
      placeholder.textContent = "Error: " + (e.message || "unknown");
      placeholder.parentElement.classList.add("err");
    } finally {
      send.disabled = false;
      input.focus();
    }
  }

  send.addEventListener("click", submit);
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) submit();
  });
  if (clearBtn && !clearBtn.dataset.wired) {
    clearBtn.dataset.wired = "1";
    clearBtn.addEventListener("click", () => {
      if (!confirm("Clear this advisor chat? This only affects your browser.")) return;
      _advClearHistory();
      log.innerHTML = "";
      _renderAdvEmpty();
    });
  }
}

// ---------- Pairs Trading (Portfolio page) ------------------------------
// Lightweight wrapper around /api/pairs. Pre-fills the two ticker inputs
// from the user's largest holdings on first wire (or KO/PEP as a sane
// default), then re-uses the same Chart.js instance via destroyChart()
// across runs so we don't leak canvas contexts.

function _wirePortfolioPairs(positions) {
  const card = document.getElementById("pf-pairs-card");
  if (!card) return;
  const aEl  = document.getElementById("pf-pairs-a");
  const bEl  = document.getElementById("pf-pairs-b");
  const lkEl = document.getElementById("pf-pairs-lookback");
  const goEl = document.getElementById("pf-pairs-go");
  const stEl = document.getElementById("pf-pairs-stats");
  const sgEl = document.getElementById("pf-pairs-signal");
  if (!aEl || !goEl) return;

  // Pre-fill from the two largest holdings (skip if user already typed).
  if (!aEl.value && positions && positions.length >= 2) {
    const sorted = [...positions].sort((x, y) => (y.value || 0) - (x.value || 0));
    aEl.value = sorted[0].ticker;
    bEl.value = sorted[1].ticker;
  } else if (!aEl.value) {
    aEl.value = "KO"; bEl.value = "PEP";
  }

  if (goEl.dataset.wired) return;
  goEl.dataset.wired = "1";

  async function run() {
    const a = (aEl.value || "").trim().toUpperCase();
    const b = (bEl.value || "").trim().toUpperCase();
    if (!a || !b) { _setPairsStatus(stEl, "Need two tickers.", "err"); return; }
    if (a === b)  { _setPairsStatus(stEl, "Pick two different tickers.", "err"); return; }

    const lookback = lkEl.value || "252";
    goEl.disabled = true;
    _setPairsStatus(stEl, `Loading ${a} vs ${b}\u2026`);
    sgEl.textContent = "\u2026"; sgEl.className = "pf-badge pf-badge-b";

    try {
      const data = await apiGet(`/api/pairs?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}&lookback=${lookback}`);
      const beta = data.hedge_ratio_beta;
      const z    = data.current_z;
      const sig  = data.signal || "\u2014";
      _setPairsStatus(stEl,
        `\u03b2=${fmtNum(beta, 3)} \u00b7 z\u2099\u2092\u1d65=${fmtNum(z, 2)} \u00b7 ${data.series.length} pts \u00b7 ${sig}`,
        "ok"
      );
      const sigU = (sig || "").toUpperCase();
      sgEl.textContent = sig;
      if (sigU.includes("LONG") || sigU.includes("SHORT")) {
        sgEl.className = "pf-badge pf-badge-g";
      } else if (sigU === "EXIT" || sigU.includes("CLOSE")) {
        sgEl.className = "pf-badge pf-badge-o";
      } else {
        sgEl.className = "pf-badge pf-badge-b";
      }

      const labels = data.series.map(p => p.date);
      const spread = data.series.map(p => p.spread);
      const zArr   = data.series.map(p => p.z);

      destroyChart("pfpairs");
      chartRegistry["pfpairs"] = new Chart(document.getElementById("pf-pairs-chart"), {
        type: "line",
        data: {
          labels,
          datasets: [
            { label: `${a} \u2212 \u03b2\u00b7${b} (spread)`, data: spread,
              borderColor: "#00cfff", yAxisID: "y",
              pointRadius: 0, borderWidth: 1.4, tension: 0.15 },
            { label: "z-score", data: zArr,
              borderColor: "#ff6b35", yAxisID: "y1",
              pointRadius: 0, borderWidth: 1.4, tension: 0.15 },
          ],
        },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          interaction: { mode: "index", intersect: false },
          scales: {
            x:  { ticks: { maxTicksLimit: 8, color: "#8a8a99" }, grid: { display: false } },
            y:  { position: "left",  ticks: { color: "#00cfff" },
                  grid: { color: "rgba(0,207,255,0.06)" },
                  title: { display: true, text: "Spread", color: "#00cfff", font: { size: 10 } } },
            y1: { position: "right", ticks: { color: "#ff6b35" },
                  grid: { drawOnChartArea: false },
                  title: { display: true, text: "Z", color: "#ff6b35", font: { size: 10 } } },
          },
          plugins: { legend: { labels: { boxWidth: 10, color: "#b8b8c5", font: { size: 10 } } } },
        },
      });

      // Fire-and-forget AI commentary. Non-blocking — chart renders first.
      _explainPairs({
        a, b,
        lookback_days: parseInt(lookback, 10) || 252,
        hedge_ratio_beta: beta,
        current_z: z,
        signal: sig,
        spread_mean: data.spread_mean,
        spread_std: data.spread_std,
      });
    } catch (e) {
      _setPairsStatus(stEl, "Error: " + (e.message || "request failed"), "err");
      sgEl.textContent = "\u2014"; sgEl.className = "pf-badge pf-badge-o";
    } finally {
      goEl.disabled = false;
    }
  }

  goEl.addEventListener("click", run);
  [aEl, bEl].forEach(el => el.addEventListener("keydown", (e) => { if (e.key === "Enter") run(); }));

  // Auto-run on first wire so the card isn't empty.
  run();
}

async function _explainPairs(payload) {
  const wrap = document.getElementById("pf-pairs-ai");
  const body = document.getElementById("pf-pairs-ai-body");
  if (!wrap || !body) return;
  wrap.hidden = false;
  body.className = "pf-pairs-ai-body muted";
  body.textContent = "Quant is thinking\u2026";
  try {
    const res = await apiPost("/api/advisor/explain_pairs", payload);
    body.className = "pf-pairs-ai-body";
    body.textContent = res.answer || "(no response)";
  } catch (e) {
    const msg = (e && e.message) || "request failed";
    if (msg.includes("412")) {
      body.className = "pf-pairs-ai-body muted";
      body.innerHTML = "Add an Anthropic API key in <strong>Account \u2192 API Keys</strong> to get Quant's read on this trade.";
    } else if (msg.includes("401")) {
      body.className = "pf-pairs-ai-body muted";
      body.textContent = "Sign in and add an Anthropic key to enable AI commentary.";
    } else {
      body.className = "pf-pairs-ai-body err";
      body.textContent = "Couldn't reach Quant: " + msg;
    }
  }
}

function _setPairsStatus(el, text, kind) {
  if (!el) return;
  el.textContent = text || "";
  el.className = "pf-pairs-stats" + (kind ? " " + kind : "");
}

// ---------- Pairs Opportunities (curated pre-screen) -------------------
// Renders /api/pairs/opportunities into a table. AI commentary is fetched
// only when the user clicks "Get Quant's read" — protects their token spend.

const _POPP_NICKNAMES = {
  NVDA: "NVIDIA", AMD: "AMD", AAPL: "Apple", MSFT: "Microsoft",
  META: "Meta", GOOGL: "Alphabet", XOM: "Exxon", CVX: "Chevron",
  KO: "Coca-Cola", PEP: "PepsiCo", V: "Visa", MA: "Mastercard",
  JPM: "JPMorgan", BAC: "Bank of America", HD: "Home Depot", LOW: "Lowe's",
  SNAP: "Snap", TSLA: "Tesla", F: "Ford", GM: "GM",
};

let _POPP_LAST_DATA = null;  // cached so the AI button can reuse the rows
let _POPP_MODE = "curated";  // "curated" | "bulk" | "custom"

async function _renderPairsOpportunities() {
  const card = document.getElementById("pf-popp-card");
  if (!card) return;
  const tabsEl = document.getElementById("pf-popp-tabs");
  const refreshBtn = document.getElementById("pf-popp-refresh");
  const aiBtn = document.getElementById("pf-popp-ai");
  const scanBtn = document.getElementById("pf-popp-scan");
  const bulkBtn = document.getElementById("pf-popp-bulk-go");
  const tickersEl = document.getElementById("pf-popp-tickers");

  // Wire tabs once.
  if (tabsEl && !tabsEl.dataset.wired) {
    tabsEl.dataset.wired = "1";
    tabsEl.querySelectorAll(".pf-popp-tab").forEach(btn => {
      btn.addEventListener("click", () => _setPoppMode(btn.dataset.mode));
    });
  }
  if (refreshBtn && !refreshBtn.dataset.wired) {
    refreshBtn.dataset.wired = "1";
    refreshBtn.addEventListener("click", () => _runPoppForCurrentMode());
  }
  if (aiBtn && !aiBtn.dataset.wired) {
    aiBtn.dataset.wired = "1";
    aiBtn.addEventListener("click", () => _runPairsOpportunitiesAI());
  }
  if (scanBtn && !scanBtn.dataset.wired) {
    scanBtn.dataset.wired = "1";
    scanBtn.addEventListener("click", () => _runCustomScan());
    tickersEl?.addEventListener("keydown", (e) => { if (e.key === "Enter") _runCustomScan(); });
  }
  if (bulkBtn && !bulkBtn.dataset.wired) {
    bulkBtn.dataset.wired = "1";
    bulkBtn.addEventListener("click", () => _runBulkScan());
  }
  const holdingsBtn = document.getElementById("pf-popp-holdings-go");
  if (holdingsBtn && !holdingsBtn.dataset.wired) {
    holdingsBtn.dataset.wired = "1";
    holdingsBtn.addEventListener("click", () => _runHoldingsScan());
  }

  await _loadPairsOpportunities();
}

function _setPoppMode(mode) {
  _POPP_MODE = mode;
  document.querySelectorAll("#pf-popp-tabs .pf-popp-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.mode === mode);
  });
  const customRow   = document.getElementById("pf-popp-custom");
  const bulkRow     = document.getElementById("pf-popp-bulk");
  const holdingsRow = document.getElementById("pf-popp-holdings");
  if (customRow)   customRow.hidden   = mode !== "custom";
  if (bulkRow)     bulkRow.hidden     = mode !== "bulk";
  if (holdingsRow) holdingsRow.hidden = mode !== "holdings";

  const listEl = document.getElementById("pf-popp-list");
  if (mode === "curated") {
    _loadPairsOpportunities();
  } else if (listEl) {
    const msgs = {
      bulk: "Click \u201cRun bulk scan\u201d to scan ~40 large-caps for active LONG/SHORT setups.",
      custom: "Enter your own tickers above and click Scan.",
      holdings: "Click \u201cScan my portfolio\u201d to find pair setups inside your current holdings.",
    };
    listEl.innerHTML = `<div class="pf-popp-empty">${msgs[mode] || ""}</div>`;
    _POPP_LAST_DATA = null;
    document.getElementById("pf-popp-ai").disabled = true;
  }
}

function _runPoppForCurrentMode() {
  if (_POPP_MODE === "curated")  return _loadPairsOpportunities();
  if (_POPP_MODE === "bulk")     return _runBulkScan();
  if (_POPP_MODE === "custom")   return _runCustomScan();
  if (_POPP_MODE === "holdings") return _runHoldingsScan();
}

async function _loadPairsOpportunities() {
  const listEl = document.getElementById("pf-popp-list");
  const aiBtn = document.getElementById("pf-popp-ai");
  if (!listEl) return;
  listEl.innerHTML = `<div class="pf-popp-empty">Loading curated pairs\u2026</div>`;
  if (aiBtn) aiBtn.disabled = true;

  try {
    const data = await apiGet("/api/pairs/opportunities");
    _POPP_LAST_DATA = data;
    _renderPairsOpportunitiesRows(data, /*notes*/ null);
    if (aiBtn) aiBtn.disabled = !(data.pairs && data.pairs.length);
  } catch (e) {
    listEl.innerHTML = `<div class="pf-popp-empty">Couldn't load pairs: ${(e.message || "request failed")}</div>`;
    if (aiBtn) aiBtn.disabled = true;
  }
}

async function _runBulkScan() {
  const listEl = document.getElementById("pf-popp-list");
  const aiBtn  = document.getElementById("pf-popp-ai");
  const btn    = document.getElementById("pf-popp-bulk-go");
  if (!listEl) return;
  listEl.innerHTML = `<div class="pf-popp-empty">Scanning \u2026 (~700 pairs across large-cap universe)</div>`;
  if (aiBtn) aiBtn.disabled = true;
  if (btn) { btn.disabled = true; }

  try {
    const data = await apiPost("/api/pairs/scan", {});
    _POPP_LAST_DATA = data;
    if (!data.pairs || !data.pairs.length) {
      listEl.innerHTML = `<div class="pf-popp-empty">No active LONG/SHORT setups in the universe right now (${data.candidates_scanned || 0} pairs above ${(data.min_correlation*100).toFixed(0)}% correlation; all sitting in neutral/exit zone). Try later.</div>`;
    } else {
      _renderPairsOpportunitiesRows(data, null);
      if (aiBtn) aiBtn.disabled = false;
    }
  } catch (e) {
    listEl.innerHTML = `<div class="pf-popp-empty">Scan failed: ${(e.message || "request failed")}</div>`;
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function _runHoldingsScan() {
  const listEl = document.getElementById("pf-popp-list");
  const aiBtn  = document.getElementById("pf-popp-ai");
  const btn    = document.getElementById("pf-popp-holdings-go");
  if (!listEl) return;
  listEl.innerHTML = `<div class="pf-popp-empty">Scanning your holdings\u2026</div>`;
  if (aiBtn) aiBtn.disabled = true;
  if (btn) btn.disabled = true;

  try {
    const data = await apiPost("/api/pairs/scan_holdings", {});
    _POPP_LAST_DATA = data;
    const usedNote = (data.holdings_used && data.holdings_used.length)
      ? `<div class="pf-popp-empty" style="text-align:left;">Scanned: <strong>${data.holdings_used.join(", ")}</strong></div>`
      : "";
    if (!data.pairs || !data.pairs.length) {
      listEl.innerHTML =
        usedNote +
        `<div class="pf-popp-empty">No active LONG/SHORT pair setups inside your holdings right now. Try the Large-Cap Universe scan for ideas to add.</div>`;
    } else {
      _renderPairsOpportunitiesRows(data, null);
      if (usedNote) listEl.insertAdjacentHTML("afterbegin", usedNote);
      if (aiBtn) aiBtn.disabled = false;
    }
  } catch (e) {
    const msg = (e && e.message) || "request failed";
    if (msg.includes("400") && msg.toLowerCase().includes("at least 2")) {
      listEl.innerHTML = `<div class="pf-popp-empty">Add at least 2 holdings to your portfolio to use this scan.</div>`;
    } else if (msg.includes("401")) {
      listEl.innerHTML = `<div class="pf-popp-empty">Sign in to scan your holdings.</div>`;
    } else {
      listEl.innerHTML = `<div class="pf-popp-empty">Scan failed: ${msg}</div>`;
    }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function _runCustomScan() {
  const listEl = document.getElementById("pf-popp-list");
  const aiBtn  = document.getElementById("pf-popp-ai");
  const tickersEl = document.getElementById("pf-popp-tickers");
  const btn    = document.getElementById("pf-popp-scan");
  if (!listEl || !tickersEl) return;

  const raw = (tickersEl.value || "").trim();
  if (!raw) {
    listEl.innerHTML = `<div class="pf-popp-empty">Enter at least 2 tickers above.</div>`;
    return;
  }
  const tickers = raw.split(/[\s,;]+/).map(s => s.trim().toUpperCase()).filter(Boolean);
  if (tickers.length < 2) {
    listEl.innerHTML = `<div class="pf-popp-empty">Need at least 2 tickers \u2014 you gave ${tickers.length}.</div>`;
    return;
  }

  listEl.innerHTML = `<div class="pf-popp-empty">Scanning ${tickers.length} tickers (${(tickers.length*(tickers.length-1)/2)|0} possible pairs)\u2026</div>`;
  if (aiBtn) aiBtn.disabled = true;
  if (btn) btn.disabled = true;

  try {
    const data = await apiPost("/api/pairs/scan", { tickers });
    _POPP_LAST_DATA = data;
    const missingNote = (data.missing_tickers && data.missing_tickers.length)
      ? `<div class="pf-popp-empty" style="text-align:left;">Not in cache: <strong>${data.missing_tickers.join(", ")}</strong></div>`
      : "";
    if (!data.pairs || !data.pairs.length) {
      listEl.innerHTML =
        missingNote +
        `<div class="pf-popp-empty">Scanned ${data.candidates_scanned || 0} pairs above ${(data.min_correlation*100).toFixed(0)}% correlation \u2014 none have an active LONG/SHORT setup right now.</div>`;
    } else {
      _renderPairsOpportunitiesRows(data, null);
      if (missingNote) {
        listEl.insertAdjacentHTML("afterbegin", missingNote);
      }
      if (aiBtn) aiBtn.disabled = false;
    }
  } catch (e) {
    listEl.innerHTML = `<div class="pf-popp-empty">Scan failed: ${(e.message || "request failed")}</div>`;
  } finally {
    if (btn) btn.disabled = false;
  }
}

function _renderPairsOpportunitiesRows(data, notes) {
  const listEl = document.getElementById("pf-popp-list");
  if (!listEl) return;
  const pairs = (data && data.pairs) || [];
  if (!pairs.length) {
    listEl.innerHTML = `<div class="pf-popp-empty">No pairs available right now.</div>`;
    return;
  }
  notes = notes || {};
  const rows = pairs.map(p => {
    const key = `${p.a}/${p.b}`;
    const corr = p.correlation;
    const corrCls = corr >= 0.85 ? "corr-hi" : corr >= 0.70 ? "corr-md" : "corr-lo";
    const z = p.current_z || 0;
    const sig = p.signal || "—";
    const sigU = sig.toUpperCase();
    const sigLines = _formatPairSignal(sigU, p.a, p.b);
    const note = notes[key];
    const noteHtml = note
      ? `<div class="pf-popp-note">${_escapeHtml(note)}</div>`
      : (notes && Object.keys(notes).length
          ? `<div class="pf-popp-note muted">No commentary returned for this pair.</div>`
          : "");
    return `
      <div class="pf-popp-row" data-pair="${key}" data-a="${p.a}" data-b="${p.b}" title="Click to load full chart + analysis below">
        <div class="pf-popp-tk">
          <span class="pf-popp-tk-sym">${p.a}</span>
          <span class="pf-popp-tk-sub">${_POPP_NICKNAMES[p.a] || p.a}</span>
        </div>
        <div class="pf-popp-vs">vs</div>
        <div class="pf-popp-tk">
          <span class="pf-popp-tk-sym">${p.b}</span>
          <span class="pf-popp-tk-sub">${_POPP_NICKNAMES[p.b] || p.b}</span>
        </div>
        <div>
          <span class="pf-popp-stat-label">Correlation</span>
          <div class="pf-popp-stat"><span class="${corrCls}">${(corr ?? 0).toFixed(2)}</span></div>
        </div>
        <div class="pf-popp-sig">
          ${sigLines}
          <span class="pf-popp-sig-spread">Spread: ${z >= 0 ? "+" : ""}${z.toFixed(2)}\u03c3</span>
        </div>
        ${noteHtml}
      </div>`;
  }).join("");
  listEl.innerHTML = rows;

  // Wire click-to-analyze: drops the pair into the detail card below
  // and runs it. Scrolls so the user sees the chart appear.
  listEl.querySelectorAll(".pf-popp-row").forEach(row => {
    row.addEventListener("click", () => {
      const a = row.dataset.a;
      const b = row.dataset.b;
      _analyzePairFromOpportunity(a, b);
    });
  });
}

function _analyzePairFromOpportunity(a, b) {
  const aEl  = document.getElementById("pf-pairs-a");
  const bEl  = document.getElementById("pf-pairs-b");
  const goEl = document.getElementById("pf-pairs-go");
  if (!aEl || !bEl || !goEl) return;
  aEl.value = a;
  bEl.value = b;
  document.getElementById("pf-pairs-card")?.scrollIntoView({ behavior: "smooth", block: "start" });
  goEl.click();
}

function _formatPairSignal(sigU, a, b) {
  // Map engine signals to two-line "▲ LONG X / ▼ SHORT Y" blocks.
  if (sigU.includes("LONG A") || sigU.includes("LONG_A") || sigU === "LONG A / SHORT B") {
    return `<span class="pf-popp-sig-line green">\u25b2 LONG ${a}</span><span class="pf-popp-sig-line red">\u25bc SHORT ${b}</span>`;
  }
  if (sigU.includes("SHORT A") || sigU.includes("SHORT_A") || sigU === "SHORT A / LONG B") {
    return `<span class="pf-popp-sig-line red">\u25bc SHORT ${a}</span><span class="pf-popp-sig-line green">\u25b2 LONG ${b}</span>`;
  }
  if (sigU === "EXIT" || sigU.includes("CLOSE")) {
    return `<span class="pf-popp-sig-line amber">\u25cf EXIT</span>`;
  }
  return `<span class="pf-popp-sig-line">\u25cf ${sigU || "NO TRADE"}</span>`;
}

async function _runPairsOpportunitiesAI() {
  const aiBtn = document.getElementById("pf-popp-ai");
  const hint = document.getElementById("pf-popp-hint");
  if (!_POPP_LAST_DATA || !_POPP_LAST_DATA.pairs?.length) return;

  const origText = aiBtn.textContent;
  aiBtn.disabled = true;
  aiBtn.textContent = "🧠 Quant is thinking…";
  if (hint) hint.textContent = "1 Haiku call in flight…";

  try {
    const payload = {
      pairs: _POPP_LAST_DATA.pairs.map(p => ({
        a: p.a, b: p.b,
        correlation: p.correlation,
        hedge_ratio_beta: p.hedge_ratio_beta,
        current_z: p.current_z,
        signal: p.signal,
      })),
    };
    const res = await apiPost("/api/advisor/explain_pairs_batch", payload);
    _renderPairsOpportunitiesRows(_POPP_LAST_DATA, res.notes || {});
    if (hint) hint.textContent = `Notes attached \u00b7 model: ${res.model || "haiku"}.`;
  } catch (e) {
    const msg = (e && e.message) || "request failed";
    if (hint) {
      if (msg.includes("412")) {
        hint.innerHTML = "Add an Anthropic key in <strong>Account \u2192 API Keys</strong> first.";
      } else if (msg.includes("401")) {
        hint.textContent = "Sign in to use Quant.";
      } else {
        hint.textContent = "Quant call failed: " + msg;
      }
    }
  } finally {
    aiBtn.disabled = false;
    aiBtn.textContent = origText;
  }
}

function _escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

async function _renderPortfolioPerf() {
  const canvas = document.getElementById("pf-perf-chart");
  const metaEl = document.getElementById("pf-perf-meta");
  const tabsEl = document.getElementById("pf-perf-tabs");
  if (!canvas || !window.Chart) return;

  // Wire tabs once.
  if (tabsEl && !tabsEl.dataset.bound) {
    tabsEl.dataset.bound = "1";
    tabsEl.querySelectorAll(".pf-perf-tab").forEach(btn => {
      btn.addEventListener("click", () => {
        _PF_PERF_PERIOD = btn.dataset.period || "1y";
        tabsEl.querySelectorAll(".pf-perf-tab").forEach(b => b.classList.toggle("active", b === btn));
        _renderPortfolioPerf();
      });
    });
  }
  // Reflect current period selection on tabs.
  tabsEl?.querySelectorAll(".pf-perf-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.period === _PF_PERF_PERIOD);
  });

  if (metaEl) metaEl.textContent = "LOADING\u2026";

  let hist;
  try {
    hist = await apiGet(`/api/portfolio/history?period=${_PF_PERF_PERIOD}`);
  } catch (e) {
    if (metaEl) metaEl.textContent = `ERROR: ${e.message}`;
    return;
  }

  const series = hist.series || [];
  if (!series.length) {
    if (metaEl) metaEl.textContent = "NO DATA YET";
    destroyChart("pfperf");
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    return;
  }

  const labels = series.map(p => p.date);
  const values = series.map(p => p.value);
  const bench  = series.map(p => p.benchmark ?? null);
  const hasBench = bench.some(v => v != null);

  const start = values[0];
  const end   = values[values.length - 1];
  const totalRet = start > 0 ? (end / start - 1) : 0;
  const up = totalRet >= 0;
  const portColor  = up ? "#00ffaa" : "#ff4060";
  const benchColor = "#7a9e94";

  if (metaEl) {
    let txt = `${_PF_PERF_PERIOD.toUpperCase()} \u00b7 ${up ? "+" : ""}${(totalRet * 100).toFixed(2)}%`;
    if (hasBench) {
      const bStart = bench.find(v => v != null);
      const bEnd   = [...bench].reverse().find(v => v != null);
      if (bStart && bEnd && bStart > 0) {
        const bRet = bEnd / bStart - 1;
        const alpha = totalRet - bRet;
        txt += ` \u00b7 vs ${hist.benchmark || "SPY"} ${(alpha >= 0 ? "+" : "")}${(alpha * 100).toFixed(2)}%`;
      }
    }
    if (hist.diagnostics?.skipped?.length) {
      txt += ` \u00b7 skipped ${hist.diagnostics.skipped.join(",")}`;
    }
    metaEl.textContent = txt;
  }

  destroyChart("pfperf");
  const datasets = [{
    label: "Portfolio",
    data: values,
    borderColor: portColor,
    backgroundColor: portColor + "18",
    fill: true,
    tension: 0.18,
    pointRadius: 0,
    borderWidth: 1.6,
  }];
  if (hasBench) {
    datasets.push({
      // Make it explicit in the legend + tooltip that SPY is rebased
      // to portfolio start (not the live SPY share price), otherwise
      // users read "SPY: $64" as "SPY costs $64" and panic.
      label: `${hist.benchmark || "SPY"} (rebased)`,
      data: bench,
      borderColor: benchColor,
      backgroundColor: "transparent",
      borderDash: [4, 4],
      fill: false,
      tension: 0.18,
      pointRadius: 0,
      borderWidth: 1.2,
    });
  }
  // Pre-compute % returns from index 0 so the tooltip can show both
  // the rebased dollar and the cumulative return at any hovered point.
  const startPort = values[0] || 0;
  const startBench = bench.find(v => v != null) || 0;
  chartRegistry["pfperf"] = new Chart(canvas, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { ticks: { maxTicksLimit: 8 }, grid: { display: false } },
        y: {
          grid: { color: "rgba(0,255,170,0.06)" },
          ticks: { callback: (v) => "$" + Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 }) },
        },
      },
      plugins: {
        legend: { display: hasBench, position: "bottom", labels: { boxWidth: 10, boxHeight: 2, padding: 12 } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const y = Number(ctx.parsed.y);
              const isBench = ctx.datasetIndex === 1;
              const baseline = isBench ? startBench : startPort;
              const ret = baseline > 0 ? (y / baseline - 1) : 0;
              const pct = `${ret >= 0 ? "+" : ""}${(ret * 100).toFixed(2)}%`;
              const dollar = "$" + y.toLocaleString("en-US", { maximumFractionDigits: 2 });
              return `${ctx.dataset.label}: ${dollar} (${pct})`;
            },
            afterBody: (items) => {
              // Footer line for the SPY series so users understand the
              // dollar value isn't the real SPY share price.
              if (items.length > 1) {
                return ["", "Both rebased to portfolio start value"];
              }
              return [];
            },
          },
        },
      },
    },
  });
}

// Custom sell modal \u2014 replaces the ugly browser prompt(). Shows current
// position context, lets the user enter shares to sell, and offers
// quick-pick buttons (25% / 50% / 75% / All). Reuses the global
// Modal (#modal-root) so it gets the same backdrop/blur as everything else.
function _openSellModal(pos) {
  const ticker = pos.ticker;
  const owned  = Number(pos.shares) || 0;
  const price  = Number(pos.price)  || 0;
  const avg    = Number(pos.avg_cost) || 0;
  if (owned <= 0) { alert(`No shares of ${ticker} on record.`); return; }

  const proceedsAt = (qty) => price > 0 ? price * qty : null;
  const plPerShare = price > 0 ? price - avg : null;

  Modal.open(`Sell ${ticker}`, `
    <div class="sell-modal">
      <div class="sell-summary">
        <div class="sell-summary-row">
          <span class="sell-k">Owned</span><span class="sell-v">${owned.toLocaleString("en-US",{maximumFractionDigits:4})}</span>
        </div>
        <div class="sell-summary-row">
          <span class="sell-k">Avg cost</span><span class="sell-v">${fmtPrice(avg)}</span>
        </div>
        <div class="sell-summary-row">
          <span class="sell-k">Live price</span><span class="sell-v">${price > 0 ? fmtPrice(price) : "\u2014"}</span>
        </div>
        ${plPerShare !== null ? `<div class="sell-summary-row">
          <span class="sell-k">P&amp;L / share</span>
          <span class="sell-v ${plPerShare >= 0 ? "pos" : "neg"}">${plPerShare >= 0 ? "+" : ""}${fmtPrice(plPerShare)}</span>
        </div>` : ""}
      </div>

      <label class="sell-label">Shares to sell</label>
      <div class="sell-input-row">
        <input id="sell-qty" class="field" type="number" step="any" min="0" max="${owned}" value="${owned}" autofocus>
        <div class="sell-quick">
          <button class="pill ghost" data-pct="0.25">25%</button>
          <button class="pill ghost" data-pct="0.5">50%</button>
          <button class="pill ghost" data-pct="0.75">75%</button>
          <button class="pill ghost" data-pct="1">All</button>
        </div>
      </div>

      <div id="sell-preview" class="sell-preview">\u2014</div>
      <div id="sell-error" class="sell-error" hidden></div>

      <div class="sell-actions">
        <button class="pill" data-close>Cancel</button>
        <button id="sell-confirm" class="pill cta sell-confirm">Confirm Sell</button>
      </div>
    </div>
  `);

  const qtyEl     = document.getElementById("sell-qty");
  const previewEl = document.getElementById("sell-preview");
  const errEl     = document.getElementById("sell-error");
  const confirmEl = document.getElementById("sell-confirm");

  function _normalize() {
    const raw = qtyEl.value.trim().toLowerCase();
    if (raw === "all" || raw === "max") return owned;
    const n = Number(raw);
    return Number.isFinite(n) ? n : NaN;
  }
  function _refreshPreview() {
    const q = _normalize();
    errEl.hidden = true;
    if (!Number.isFinite(q) || q <= 0) {
      previewEl.textContent = "Enter how many shares to sell.";
      previewEl.className = "sell-preview";
      return;
    }
    if (q > owned + 1e-9) {
      previewEl.textContent = `You only own ${owned} shares.`;
      previewEl.className = "sell-preview neg";
      return;
    }
    const remaining = Math.max(0, owned - q);
    const proceeds = proceedsAt(q);
    const realized = (price > 0 && avg >= 0) ? (price - avg) * q : null;
    const isFull = Math.abs(q - owned) < 1e-6;
    let txt = isFull ? `Sell ALL \u2014 position will be removed.` : `Sell ${q} \u2192 ${remaining.toLocaleString("en-US",{maximumFractionDigits:4})} shares remain.`;
    if (proceeds !== null) txt += ` \u00b7 Proceeds \u2248 ${fmtPrice(proceeds)}`;
    if (realized !== null) txt += ` \u00b7 Realized ${realized >= 0 ? "+" : ""}${fmtPrice(realized)}`;
    previewEl.textContent = txt;
    previewEl.className = "sell-preview " + ((realized !== null && realized < 0) ? "warn" : "ok");
  }

  qtyEl.addEventListener("input", _refreshPreview);
  document.querySelectorAll(".sell-quick button[data-pct]").forEach(b => {
    b.addEventListener("click", (e) => {
      e.preventDefault();
      const pct = Number(b.dataset.pct);
      const q = pct >= 1 ? owned : Math.round(owned * pct * 10000) / 10000;
      qtyEl.value = String(q);
      _refreshPreview();
      qtyEl.focus();
    });
  });
  qtyEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); confirmEl.click(); }
  });

  confirmEl.addEventListener("click", async () => {
    const q = _normalize();
    if (!Number.isFinite(q) || q <= 0) {
      errEl.textContent = "Enter a positive number of shares.";
      errEl.hidden = false; return;
    }
    if (q > owned + 1e-9) {
      errEl.textContent = `You only own ${owned} shares.`;
      errEl.hidden = false; return;
    }
    confirmEl.disabled = true; confirmEl.textContent = "Selling\u2026";
    let ok = true; let errMsg = null;
    if (Math.abs(q - owned) < 1e-6) {
      ok = await Portfolio.remove(ticker);
      if (!ok) errMsg = "server rejected the request";
    } else {
      const remaining = owned - q;
      const r = await Portfolio.setShares(ticker, remaining);
      ok = r.ok; errMsg = r.error;
    }
    if (!ok) {
      confirmEl.disabled = false; confirmEl.textContent = "Confirm Sell";
      errEl.textContent = `Failed: ${errMsg || "unknown error"}`;
      errEl.hidden = false; return;
    }
    Modal.close();
    _renderPortfolio();
  });

  _refreshPreview();
}

// ----- Router (hash-based SPA routing) -----
//
// We use the URL hash so the back/forward buttons work and users can
// bookmark/share /#/portfolio. There are only a few routes today
// (dashboard, portfolio); screener is a modal because it's a one-shot
// table view, and docs is FastAPI's auto Swagger on /docs (new tab).
//
// Each <main class="page" data-route="..."> in index.html registers as
// a route. Showing a page hides the others and runs the slide-in
// animation by re-applying the .page-in class.

const Router = (() => {
  const routes = new Map();        // "/portfolio" -> { el, onEnter }
  const navLinks = {};             // routeKey -> nav button (for active highlighting)

  function register(key, options = {}) {
    const el = document.querySelector(`main.page[data-route="${key}"]`);
    if (!el) return;
    routes.set(key, { el, onEnter: options.onEnter || null });
  }

  function _showPage(key) {
    let target = routes.get(key);
    if (!target) target = routes.get("/");  // default
    for (const [k, r] of routes.entries()) {
      r.el.hidden = (r !== target);
      r.el.classList.remove("page-in");
    }
    if (target) {
      // Force reflow so the animation re-runs even if the same class
      // is re-added before the next paint.
      target.el.offsetHeight;
      target.el.classList.add("page-in");
      try { target.onEnter && target.onEnter(); }
      catch (e) { console.error("[router] onEnter failed:", e); }
    }
    // Active nav button.
    for (const [k, btn] of Object.entries(navLinks)) {
      btn.classList.toggle("active", k === (target && target.el.dataset.route));
    }
    // Scroll to top so each page feels fresh.
    window.scrollTo({ top: 0, behavior: "instant" in window ? "instant" : "auto" });
  }

  function _parseHash() {
    const h = (location.hash || "").replace(/^#/, "");
    if (!h || h === "/") return "/";
    return h.startsWith("/") ? h : "/" + h;
  }

  function goto(key) {
    const target = key.startsWith("/") ? key : "/" + key;
    if (_parseHash() === target) {
      _showPage(target);  // re-trigger animation if user clicks same nav twice
    } else {
      location.hash = "#" + target;
    }
  }

  function bindNavLink(routeKey, buttonId) {
    const btn = document.getElementById(buttonId);
    if (!btn) return;
    navLinks[routeKey] = btn;
    btn.addEventListener("click", (e) => { e.preventDefault(); goto(routeKey); });
  }

  function start() {
    window.addEventListener("hashchange", () => _showPage(_parseHash()));
    _showPage(_parseHash());
  }

  return { register, bindNavLink, goto, start };
})();

// =========================================================================
// AUTH (Phase 2b)
// Drives the sign-in/sign-up modal and the nav auth slot.
// State flow:
//   1. on boot, refresh() calls /api/auth/me to learn whether the
//      qd_session cookie is valid. The "Sign In" pill is shown until
//      this resolves to avoid an auth-state flash.
//   2. submit() POSTs to /api/auth/{login,signup}. On success, the
//      backend has already migrated this device's portfolio into the
//      account; we re-pull the portfolio so the page reflects it.
//   3. logout() POSTs to /api/auth/logout and re-runs Portfolio.load()
//      so the (now empty) device portfolio is shown instead.
// =========================================================================
const Auth = (() => {
  let _user = null;          // {id, email} or null
  let _mode = "login";       // "login" | "signup"
  const els = {};

  function _cache() {
    els.modal      = document.getElementById("auth-modal");
    els.close      = document.getElementById("auth-close");
    els.title      = document.getElementById("auth-title");
    els.tabs       = document.querySelectorAll(".auth-tab");
    els.form       = document.getElementById("auth-form");
    els.name       = document.getElementById("auth-name");
    els.signupOnly = document.querySelectorAll("[data-signup-only]");
    els.email      = document.getElementById("auth-email");
    els.password   = document.getElementById("auth-password");
    els.hint       = document.getElementById("auth-hint");
    els.error      = document.getElementById("auth-error");
    els.submit     = document.getElementById("auth-submit");
    els.navAuth    = document.getElementById("nav-auth");
    els.navAccount = document.getElementById("nav-account");
  }

  function _renderNav() {
    if (!els.navAuth) return;
    if (_user) {
      els.navAuth.hidden    = true;
      if (els.navAccount) {
        els.navAccount.hidden = false;
        const label = _user.display_name || (_user.email || "").split("@")[0] || "Account";
        els.navAccount.textContent = label + " \u25BE";
        els.navAccount.title = _user.email || "";
      }
    } else {
      els.navAuth.hidden    = false;
      if (els.navAccount) els.navAccount.hidden = true;
    }
  }

  function _setMode(mode) {
    _mode = mode;
    const isSignup = mode === "signup";
    if (isSignup) {
      els.title.textContent  = "Create account";
      els.submit.textContent = "Create account";
      els.password.setAttribute("autocomplete", "new-password");
      els.hint.textContent   = "We'll move the portfolio you just built on this device into your new account.";
    } else {
      els.title.textContent  = "Sign in";
      els.submit.textContent = "Sign in";
      els.password.setAttribute("autocomplete", "current-password");
      els.hint.textContent   = "Sign in to access your portfolio from any device.";
    }
    // Show/hide signup-only fields (display name, etc).
    els.signupOnly.forEach(el => { el.hidden = !isSignup; });
    els.tabs.forEach(t => t.classList.toggle("active", t.dataset.authMode === mode));
    els.error.hidden = true;
  }

  function open(mode = "login") {
    if (!els.modal) return;
    _setMode(mode);
    els.modal.hidden = false;
    setTimeout(() => els.email.focus(), 50);
  }

  function close() {
    if (els.modal) els.modal.hidden = true;
  }

  async function refresh() {
    // Don't break the page if /api/auth/me fails \u2014 just stay anonymous.
    try {
      const r = await apiGet("/api/auth/me");
      _user = r && r.user ? r.user : null;
    } catch {
      _user = null;
    }
    _renderNav();
  }

  async function submit(ev) {
    ev?.preventDefault();
    const email    = (els.email.value || "").trim();
    const password = els.password.value || "";
    const name     = (els.name?.value || "").trim();
    if (!email || password.length < 8) {
      els.error.textContent = "Enter a valid email and password (8+ chars).";
      els.error.hidden = false;
      return;
    }
    els.submit.disabled = true;
    els.error.hidden = true;
    try {
      const path = _mode === "signup" ? "/api/auth/signup" : "/api/auth/login";
      const body = _mode === "signup"
        ? { email, password, display_name: name || null }
        : { email, password };
      const r = await apiPost(path, body);
      _user = r.user;
      _renderNav();
      close();
      // Backend already migrated the device portfolio; re-render so the
      // UI reflects whatever the account now owns.
      try { await _renderPortfolio(); } catch {}
    } catch (e) {
      // apiSend throws Error with .message containing the server detail.
      const msg = (e && e.message) || "Something went wrong.";
      els.error.textContent = msg.replace(/^.*?:\s*/, "");
      els.error.hidden = false;
    } finally {
      els.submit.disabled = false;
    }
  }

  async function logout() {
    try { await apiPost("/api/auth/logout", {}); } catch {}
    _user = null;
    _renderNav();
    // Re-render so the now-anonymous device portfolio (likely empty
    // after migration on this device) is shown instead of the user's.
    try { await _renderPortfolio(); } catch {}
  }

  function openAccount() {
    if (!_user) { open("login"); return; }
    const name  = _user.display_name || "";
    const email = _user.email || "";
    Modal.open("Account", `
      <div class="account-modal">
        <div class="acct-section">
          <div class="acct-section-title">Profile</div>
          <div class="acct-row"><span class="acct-k">Email</span><span class="acct-v">${_esc(email)}</span></div>
          <div class="acct-row acct-edit-row">
            <span class="acct-k">Display name</span>
            <input id="acct-name-input" class="field acct-name-input" type="text" maxlength="80" value="${_esc(name)}" placeholder="How we greet you">
            <button id="acct-name-save" class="pill cta acct-name-save">Save</button>
          </div>
          <div id="acct-name-msg" class="acct-key-msg"></div>
        </div>
        <div class="acct-section">
          <div class="acct-section-title">Alerts</div>
          <div class="acct-row acct-muted"><span>Computed live from your portfolio every page load. No email \u2014 you'll see them here and on the dashboard.</span></div>
          <div id="acct-alerts" class="acct-alerts"><div class="acct-muted">Loading\u2026</div></div>
          <div class="acct-row" style="margin-top:8px;">
            <label class="acct-toggle"><input type="checkbox" id="acct-alert-show" checked> Show alert banner on dashboard</label>
          </div>
        </div>
        <div class="acct-section">
          <div class="acct-section-title">API Keys <span class="acct-soon">Anthropic</span></div>
          <div class="acct-row acct-muted"><span>Bring your own Anthropic key to power the AI Advisor. The key is encrypted at rest and never sent back to your browser.</span></div>
          <div id="acct-key-status" class="acct-row acct-muted"><span>Loading\u2026</span></div>
          <div class="acct-key-form">
            <input id="acct-key-input" class="field" type="password" placeholder="sk-ant-..." autocomplete="off" spellcheck="false">
            <button id="acct-key-save" class="pill cta">Save</button>
            <button id="acct-key-clear" class="pill" hidden>Remove</button>
          </div>
          <div id="acct-key-msg" class="acct-key-msg"></div>
          <div class="acct-row acct-muted" style="margin-top:6px"><span>Get a key from <a href="https://console.anthropic.com/settings/keys" target="_blank" rel="noopener">console.anthropic.com</a>. Set a monthly spend cap there too.</span></div>
        </div>
        <div class="acct-actions">
          <button class="pill" data-close>Close</button>
          <button class="pill cta acct-logout" id="acct-logout">Sign out</button>
        </div>
      </div>
    `);
    document.getElementById("acct-logout")?.addEventListener("click", async () => {
      Modal.close();
      await logout();
    });
    _wireAcctName();
    _wireAcctAlerts();
    _wireAcctKey();
  }

  async function _wireAcctName() {
    const inp = document.getElementById("acct-name-input");
    const btn = document.getElementById("acct-name-save");
    const msg = document.getElementById("acct-name-msg");
    if (!inp || !btn) return;
    btn.addEventListener("click", async () => {
      const newName = (inp.value || "").trim();
      const orig = btn.textContent;
      btn.disabled = true;
      btn.textContent = "Saving\u2026";
      msg.textContent = "";
      msg.className = "acct-key-msg";
      try {
        const r = await apiPatch("/api/auth/me", { display_name: newName || null });
        _user = r.user;
        _renderNav();
        msg.textContent = "Saved.";
        msg.className = "acct-key-msg ok";
      } catch (e) {
        msg.textContent = "Save failed: " + e.message;
        msg.className = "acct-key-msg err";
      } finally {
        btn.disabled = false;
        btn.textContent = orig;
      }
    });
  }

  function _wireAcctAlerts() {
    const box = document.getElementById("acct-alerts");
    const tog = document.getElementById("acct-alert-show");
    if (!box) return;
    const alerts = _computePortfolioAlerts();
    if (!alerts.length) {
      box.innerHTML = `<div class="acct-muted" style="padding:8px 0;">No alerts \u2014 portfolio is within normal ranges.</div>`;
    } else {
      box.innerHTML = alerts.map(a => `
        <div class="acct-alert acct-alert-${a.kind}">
          <span class="acct-alert-icon">${a.icon}</span>
          <div class="acct-alert-body">
            <div class="acct-alert-title">${_esc(a.title)}</div>
            <div class="acct-alert-msg">${_esc(a.msg)}</div>
          </div>
        </div>`).join("");
    }
    if (tog) {
      tog.checked = (localStorage.getItem("qd_alerts_show") || "1") === "1";
      tog.addEventListener("change", () => {
        localStorage.setItem("qd_alerts_show", tog.checked ? "1" : "0");
        _renderDashboardAlerts();
      });
    }
  }

  async function _wireAcctKey() {
    const statusEl = document.getElementById("acct-key-status");
    const inputEl  = document.getElementById("acct-key-input");
    const saveBtn  = document.getElementById("acct-key-save");
    const clearBtn = document.getElementById("acct-key-clear");
    const msgEl    = document.getElementById("acct-key-msg");
    if (!statusEl) return;

    function flash(text, kind) {
      msgEl.textContent = text || "";
      msgEl.className = "acct-key-msg" + (kind ? " " + kind : "");
    }

    async function refresh() {
      try {
        const s = await apiGet("/api/advisor/key");
        if (s.has_key) {
          const when = s.updated_utc ? s.updated_utc.replace("T", " ").slice(0, 16) + " UTC" : "";
          statusEl.innerHTML = `<span>Key on file ending in <strong>…${s.last4}</strong>${when ? " · updated " + when : ""}.</span>`;
          clearBtn.hidden = false;
          inputEl.placeholder = "sk-ant-… (replace existing)";
        } else {
          statusEl.innerHTML = `<span>No key on file. Add one to enable the AI Advisor.</span>`;
          clearBtn.hidden = true;
          inputEl.placeholder = "sk-ant-...";
        }
      } catch (e) {
        statusEl.innerHTML = `<span class="err">Couldn't load key status: ${e.message}</span>`;
      }
    }

    saveBtn.addEventListener("click", async () => {
      const v = inputEl.value.trim();
      if (!v) { flash("Paste a key first.", "err"); return; }
      saveBtn.disabled = true; flash("Verifying with Anthropic…");
      try {
        await apiPut("/api/advisor/key", { api_key: v });
        inputEl.value = "";
        flash("Key saved and verified. The Advisor is now unlocked on the Portfolio page.", "ok");
        await refresh();
        // If the Portfolio page is mounted, refresh the advisor card so
        // the gate disappears without needing a tab switch.
        try { if (window._renderAdvisor) window._renderAdvisor(); } catch (_) {}
      } catch (e) {
        flash(e.message || "Save failed.", "err");
      } finally {
        saveBtn.disabled = false;
      }
    });

    clearBtn.addEventListener("click", async () => {
      if (!confirm("Remove your Anthropic key? The Advisor will stop working until you add a new one.")) return;
      clearBtn.disabled = true;
      try {
        await apiDelete("/api/advisor/key");
        flash("Key removed.", "ok");
        await refresh();
      } catch (e) {
        flash(e.message || "Delete failed.", "err");
      } finally {
        clearBtn.disabled = false;
      }
    });

    inputEl.addEventListener("keydown", (e) => { if (e.key === "Enter") saveBtn.click(); });
    refresh();
  }

  function bind() {
    _cache();
    if (!els.modal) return;
    els.close?.addEventListener("click", close);
    // Backdrop click closes (data-auth-close on the .modal-backdrop sibling).
    els.modal.addEventListener("click", (e) => {
      if (e.target === els.modal || e.target?.dataset?.authClose !== undefined) close();
    });
    els.tabs.forEach(t =>
      t.addEventListener("click", () => _setMode(t.dataset.authMode))
    );
    els.form?.addEventListener("submit", submit);
    els.navAuth?.addEventListener("click", () => open("login"));
    els.navAccount?.addEventListener("click", openAccount);
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !els.modal.hidden) close();
    });
  }

  function user() { return _user; }

  return { bind, refresh, open, close, logout, user, openAccount };
})();

// ----- Bind nav -----
function bindNav() {
  Modal.bind();

  // Routes.
  Router.register("/", {});
  Router.register("/portfolio", { onEnter: () => openPortfolio() });
  Router.bindNavLink("/", "nav-dashboard");
  Router.bindNavLink("/portfolio", "nav-portfolio");

  // Modal-based features stay buttons.
  document.getElementById("nav-screener")?.addEventListener("click", () => {
    // Don't change the route \u2014 screener is overlaid on whatever page
    // the user was looking at, and closing it returns them there.
    openScreener();
  });
  document.getElementById("nav-docs")?.addEventListener("click", () => {
    // FastAPI auto-mounts Swagger UI at /docs. Open in a new tab so the
    // dashboard stays where the user left it.
    window.open(`${API}/docs`, "_blank", "noopener,noreferrer");
  });

  Router.start();
}

// =========================================================================
// 14. BOOT
// =========================================================================

document.addEventListener("DOMContentLoaded", () => {
  configureChartDefaults();

  // Watchlist must render before signals (signals reads enabled symbols).
  renderWatchlist();
  bindWatchlist();

  bindPriceChartTabs();
  bindPairs();
  bindNav();

  // Auth must bind before Portfolio.load() runs so the nav slot is wired
  // by the time /api/auth/me resolves. refresh() runs async \u2014 it's
  // fire-and-forget; the nav just stays anonymous-looking until the
  // round-trip completes (~50ms typical).
  Auth.bind();
  Auth.refresh();

  // One-shot at boot: pairs runs only when user clicks Run.
  loadPairs();

  // Kick off the snapshot bootstrap fetch BEFORE registering scheduler tasks.
  // The Scheduler's first _tick() runs synchronously inside .start(); by the
  // time those load* functions hit Snapshot.consume(), this fetch needs to
  // have populated the cards. We await it so the first paint is one HTTP
  // round-trip instead of ~10 parallel cold-start requests.
  Snapshot.bootstrap().finally(() => {

  // -------- Scheduler tasks --------
  // Cadences are tuned per-resource to balance freshness vs API cost.
  // Picked roughly by "how often does this dataset actually change?":
  //   ticker bar : 30s   (live quotes, cheap, user always sees it)
  //   macro      : 60s   (market data; VIX/oil move minute-by-minute)
  //   regime     : 120s  (SMA/EWMA shift slowly; per-ticker)
  //   news       : 180s  (Yahoo refreshes headlines maybe every few min)
  //   signals    : 300s  (heavy compute server-side; sectors derived from same)
  //   sectors    : 300s  (recomputed alongside signals on the same cadence)
  //   health     : 30s   (cheap heartbeat to keep the status dot honest)
  //
  // keyFn returns null for tasks that don't depend on selected ticker.
  // For per-ticker tasks (news/regime), changing the key forces an
  // immediate refresh on the next heartbeat AND on Scheduler.kick().
  const tickerKey = () => AppState.selectedTicker;
  Scheduler.register("health",   pingHealth,      30_000);
  Scheduler.register("ticker",   loadTickerBar,   30_000);
  Scheduler.register("signals",  loadSignals,    300_000);
  Scheduler.register("sectors",  loadSectors,    300_000);
  Scheduler.register("macro",    loadMacro,       60_000);
  Scheduler.register("corr",     loadCorrelation, 600_000);
  Scheduler.register("regime",   loadRegime,     120_000, { keyFn: tickerKey });
  Scheduler.register("news",     loadNews,       180_000, { keyFn: tickerKey });
  Scheduler.start();
  });
});
