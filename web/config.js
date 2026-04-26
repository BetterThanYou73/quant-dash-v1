// Single source of truth for the API base URL.
// Local development: FastAPI on 127.0.0.1:8000
// Production: replace with your deployed URL (e.g. https://quant-dash-api.onrender.com)
//
// Loaded as a plain <script> before app.js so window.QD_CONFIG is available globally.
window.QD_CONFIG = {
  API_BASE: "http://127.0.0.1:8000",
};
