// Single source of truth for the API base URL.
//
// Strategy:
//   - If we're on localhost:5500 (the dev static server), point at the
//     local FastAPI on :8000.
//   - Otherwise (production: served by FastAPI/StaticFiles on the same
//     origin as quantdash.tech), use a relative URL so the browser hits
//     the same host that served the page. This eliminates CORS entirely.
//
// Loaded as a plain <script> before app.js so window.QD_CONFIG is available globally.
(function () {
  var isLocalDev =
    location.hostname === "localhost" || location.hostname === "127.0.0.1";
  var inferred = isLocalDev && location.port !== "8000"
    ? "http://127.0.0.1:8000"
    : "";  // empty string → fetch("/api/...") stays same-origin
  window.QD_CONFIG = { API_BASE: inferred };
})();
