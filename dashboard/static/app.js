/*
Frontend principal del dashboard logistico.

Diseno funcional:
1) Columna izquierda (Tiempo Real): mapa de flota + panel de vehiculo.
   - Filtros propios: Origen RT / Destino RT.
2) Columna derecha (Red Logistica): grafo geografico + tablas de rutas.
   - Filtros propios: Origen / Destino / Perfil.

Reglas clave:
- Ambas vistas estan desacopladas: cambios en una no reconfiguran la otra.
- La ruta por vehiculo prioriza planned_origin/planned_destination (backend).
- Se suaviza posicion para evitar saltos visuales y se estima heading/ETA.
*/

const state = {
  // Mapas y capas.
  map: null,
  baseLayer: null,
  networkMap: null,
  networkBaseLayer: null,
  warehouseLayer: null,
  networkLayer: null,
  markers: new Map(),
  trails: new Map(),
  trailArrows: new Map(),
  truckHeadings: new Map(),
  etaByVehicle: new Map(),
  allVehicles: [],
  playback: true,
  vehicles: [],
  graph: null,
  route: null,
  selectedVehicle: "",
  theme: "dark",
  weatherFactor: 0,
  weatherImpactLevel: "low",
  didFitToWarehouses: false,
  animationTimer: null,
  projectionLine: null,
  routeRequestInFlight: false,
  routeRecalcPending: false,
  lastRouteCalcAt: null,
  liveEdgeSummary: null,
  networkInsights: null,
  insightsProfile: "balanced",
  insightsMinCongestion: "all",
  insightsHistory: null
};

// Formateadores de UI centralizados para evitar inconsistencias visuales.
const fmt = {
  n: (v, d = 2) => Number(v || 0).toFixed(d),
  dt: (v) => (v ? new Date(v).toLocaleString("es-ES") : "-"),
  hm: (minutes) => {
    const total = Math.max(0, Math.round(Number(minutes || 0)));
    const h = Math.floor(total / 60);
    const m = total % 60;
    if (h <= 0) return `${m} min`;
    return `${h} h ${m} min`;
  }
};

const SORTABLE_TABLE_IDS = [
  "vehicle-table",
  "all-routes-table",
  "route-table",
  "weather-table",
  "bottlenecks-table",
  "critical-nodes-table",
  "insights-history-table"
];

const tableSortState = new Map();

function parseEsDateTime(text) {
  const m = String(text || "").trim().match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{2,4}),?\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$/
  );
  if (!m) return null;
  const day = Number(m[1]);
  const month = Number(m[2]) - 1;
  let year = Number(m[3]);
  if (year < 100) year += 2000;
  const hour = Number(m[4]);
  const minute = Number(m[5]);
  const second = Number(m[6] || 0);
  const dt = new Date(year, month, day, hour, minute, second);
  return Number.isNaN(dt.getTime()) ? null : dt.getTime();
}

function parseDurationMinutes(text) {
  const raw = String(text || "").toLowerCase().trim();
  if (!raw) return null;
  const hm = raw.match(/^(\d+)\s*h\s*(\d+)\s*min$/);
  if (hm) return Number(hm[1]) * 60 + Number(hm[2]);
  const onlyMin = raw.match(/^(\d+)\s*min$/);
  if (onlyMin) return Number(onlyMin[1]);
  return null;
}

function sortValueFromCellText(text) {
  const raw = String(text || "").trim();
  if (!raw) return { type: "empty", value: "" };
  const asDate = parseEsDateTime(raw);
  if (asDate !== null) return { type: "number", value: asDate };
  const asDuration = parseDurationMinutes(raw);
  if (asDuration !== null) return { type: "number", value: asDuration };
  if (!/^[A-Za-z]+[0-9]+$/.test(raw)) {
    const firstNumber = raw.match(/-?\d+(?:[.,]\d+)?/);
    if (firstNumber) {
      return { type: "number", value: Number(firstNumber[0].replace(",", ".")) };
    }
  }
  return { type: "text", value: raw.toUpperCase() };
}

function compareCellValues(a, b) {
  if (a.type === "empty" && b.type !== "empty") return 1;
  if (a.type !== "empty" && b.type === "empty") return -1;
  if (a.type === "number" && b.type === "number") return a.value - b.value;
  return String(a.value).localeCompare(String(b.value), "es");
}

function updateSortableHeaderState(tableId) {
  const table = document.getElementById(tableId);
  if (!table) return;
  const headers = table.querySelectorAll("thead th");
  headers.forEach((th, idx) => {
    th.classList.add("sortable-th");
    th.classList.remove("sort-asc", "sort-desc");
    const state = tableSortState.get(tableId);
    if (!state || state.col !== idx) return;
    th.classList.add(state.dir === "desc" ? "sort-desc" : "sort-asc");
  });
}

function applyTableSortState(tableId) {
  const table = document.getElementById(tableId);
  if (!table) return;
  const tbody = table.querySelector("tbody");
  if (!tbody) return;
  const state = tableSortState.get(tableId);
  if (!state) {
    updateSortableHeaderState(tableId);
    return;
  }
  const rows = Array.from(tbody.querySelectorAll("tr"));
  const sortableRows = rows
    .filter((r) => r.children.length > state.col && r.querySelectorAll("td").length > 0)
    .map((row, idx) => {
      const cell = row.children[state.col];
      const sortValue = sortValueFromCellText(cell?.textContent || "");
      return { row, idx, sortValue };
    });
  sortableRows.sort((left, right) => {
    const cmp = compareCellValues(left.sortValue, right.sortValue);
    if (cmp !== 0) return state.dir === "desc" ? -cmp : cmp;
    return left.idx - right.idx;
  });
  sortableRows.forEach((item) => tbody.appendChild(item.row));
  updateSortableHeaderState(tableId);
}

function applyAllTableSortStates() {
  SORTABLE_TABLE_IDS.forEach((tableId) => applyTableSortState(tableId));
}

function bindSortableTables() {
  SORTABLE_TABLE_IDS.forEach((tableId) => {
    const table = document.getElementById(tableId);
    if (!table) return;
    const headers = table.querySelectorAll("thead th");
    headers.forEach((th, colIdx) => {
      if (th.dataset.sortBound === "1") return;
      th.dataset.sortBound = "1";
      th.classList.add("sortable-th");
      th.addEventListener("click", () => {
        const prev = tableSortState.get(tableId);
        const dir = prev && prev.col === colIdx && prev.dir === "asc" ? "desc" : "asc";
        tableSortState.set(tableId, { col: colIdx, dir });
        applyTableSortState(tableId);
      });
    });
    updateSortableHeaderState(tableId);
  });
}

async function fetchJson(url) {
  // Wrapper fetch con mensaje de error enriquecido para diagnostico en UI.
  const res = await fetch(url);
  if (!res.ok) {
    let details = "";
    try {
      const payload = await res.json();
      details = payload?.error ? `: ${payload.error}` : "";
    } catch (_) {
      details = "";
    }
    throw new Error(`Error ${res.status} en ${url}${details}`);
  }
  return res.json();
}

function tileLayerUrl(theme) {
  if (theme === "light") return "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
  return "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png";
}

function initMap() {
  state.map = L.map("map", { zoomControl: true }).setView([40.4168, -3.7038], 6);
}

function initNetworkMap() {
  state.networkMap = L.map("network-map", { zoomControl: true }).setView([40.4168, -3.7038], 5);
}

function applyMapTheme() {
  if (state.map) {
    if (state.baseLayer) state.map.removeLayer(state.baseLayer);
    state.baseLayer = L.tileLayer(tileLayerUrl(state.theme), {
      attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
    }).addTo(state.map);
  }
  if (state.networkMap) {
    if (state.networkBaseLayer) state.networkMap.removeLayer(state.networkBaseLayer);
    state.networkBaseLayer = L.tileLayer(tileLayerUrl(state.theme), {
      attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
    }).addTo(state.networkMap);
  }
}

function applyTheme(theme) {
  state.theme = theme === "light" ? "light" : "dark";
  document.body.setAttribute("data-theme", state.theme);
  localStorage.setItem("dashboard_theme", state.theme);
  const btn = document.getElementById("theme-btn");
  if (btn) btn.textContent = state.theme === "dark" ? "Modo claro" : "Modo oscuro";
  applyMapTheme();
  if (state.graph) {
    renderAllRoutesTable(state.graph, state.route);
    renderNetworkMap(state.graph, state.route);
  }
}

function initTheme() {
  applyTheme(localStorage.getItem("dashboard_theme") || "dark");
}

function severityClass(delay) {
  return delay >= 15 ? "bad" : "";
}

function buildTruckIcon(vehicle, selected, angle = 0) {
  const delayed = Number(vehicle.delay_minutes || 0) >= 15;
  const classes = ["truck-marker", delayed ? "truck-alert" : "truck-normal", selected ? "truck-selected" : ""]
    .filter(Boolean)
    .join(" ");
  return L.divIcon({
    className: classes,
    html: `<span class=\"truck-glyph\" style=\"--truck-angle:${Math.round(Number(angle || 0))}deg\">
      <svg viewBox=\"0 0 64 64\" aria-hidden=\"true\">
        <rect x=\"6\" y=\"26\" width=\"30\" height=\"16\" rx=\"2.5\" fill=\"currentColor\"></rect>
        <path d=\"M36 28h11l9 8v6H36z\" fill=\"currentColor\"></path>
        <rect x=\"43\" y=\"30\" width=\"8\" height=\"5\" rx=\"1\" fill=\"#d9f1ff\"></rect>
        <circle cx=\"18\" cy=\"46\" r=\"5\" fill=\"#111827\"></circle>
        <circle cx=\"46\" cy=\"46\" r=\"5\" fill=\"#111827\"></circle>
        <circle cx=\"18\" cy=\"46\" r=\"2\" fill=\"#9ca3af\"></circle>
        <circle cx=\"46\" cy=\"46\" r=\"2\" fill=\"#9ca3af\"></circle>
      </svg>
    </span>`,
    iconSize: [36, 36],
    iconAnchor: [18, 18]
  });
}

function renderOverview(overview, liveEdgeSummary = null) {
  const cards = [
    ["Vehiculos activos", overview.vehicles_active],
    ["Eventos cargados", overview.events_loaded],
    ["Delay medio (min)", fmt.n(overview.avg_delay_minutes, 1)],
    ["Velocidad media (km/h)", fmt.n(overview.avg_speed_kmh, 1)],
    ["Factor meteo", fmt.n(overview.weather_factor, 2)],
    ["Impacto meteo", (overview.weather_impact_level || "low").toUpperCase()],
    ["Ultimo evento", fmt.dt(overview.latest_event_time)]
  ];
  if (liveEdgeSummary) {
    cards.push(["Aristas live", `${liveEdgeSummary.edges_with_live_samples || 0}`]);
  }
  document.getElementById("overview-cards").innerHTML = cards
    .map(([title, value]) => `<article class=\"card\"><h4>${title}</h4><div class=\"big\">${value}</div></article>`)
    .join("");
}

function sourceClass(source) {
  if (source === "cassandra") return "source-cassandra";
  if (source === "nifi_files" || source === "nifi_raw_archive") return "source-files";
  return "source-unknown";
}

function sourceLabel(source) {
  if (source === "cassandra") return "Cassandra";
  if (source === "nifi_files") return "Nifi Input";
  if (source === "nifi_raw_archive") return "Nifi Raw Archive";
  return "-";
}

function renderDataSources(vehicleSource, weatherSource) {
  const container = document.getElementById("data-source-badges");
  if (!container) return;
  container.innerHTML = `
    <span class="source-badge ${sourceClass(vehicleSource)}">Vehiculos: ${sourceLabel(vehicleSource)}</span>
    <span class="source-badge ${sourceClass(weatherSource)}">Clima: ${sourceLabel(weatherSource)}</span>
  `;
}

function renderFleetFreshness(latestEventTime) {
  const el = document.getElementById("fleet-freshness");
  if (!el) return;
  if (!latestEventTime) {
    el.textContent = "Flota: sin datos";
    el.className = "fleet-freshness stale";
    return;
  }

  const last = new Date(latestEventTime);
  const now = new Date();
  const ageSec = Math.max(0, Math.round((now - last) / 1000));
  let statusClass = "fresh";
  if (ageSec > 120) statusClass = "warn";
  if (ageSec > 300) statusClass = "stale";

  el.className = `fleet-freshness ${statusClass}`;
  el.textContent = `Flota: ultimo evento ${fmt.dt(latestEventTime)} (${ageSec}s de antiguedad)`;
}

function renderVehicleTable(items) {
  const tbody = document.querySelector("#vehicle-table tbody");
  tbody.innerHTML = items
    .map(
      (v) => {
        const eta = computeVehicleEta(v);
        return `
      <tr>
        <td>${v.vehicle_id}</td>
        <td>${v.warehouse_id || "-"}</td>
        <td>${displayRouteForVehicle(v)}</td>
        <td class=\"${severityClass(v.delay_minutes)}\">${v.delay_minutes}</td>
        <td>${fmt.n(v.speed_kmh, 1)}</td>
        <td>${eta.etaLabel}</td>
        <td>${fmt.dt(v.event_time)}</td>
      </tr>
    `;
      }
    )
    .join("");
  applyTableSortState("vehicle-table");
}

function computeBearing(from, to) {
  // Azimut geografico (0..360) entre dos coordenadas.
  if (!from || !to) return 0;
  const lat1 = (from.lat * Math.PI) / 180;
  const lat2 = (to.lat * Math.PI) / 180;
  const dLon = ((to.lng - from.lng) * Math.PI) / 180;
  const y = Math.sin(dLon) * Math.cos(lat2);
  const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
  const brng = (Math.atan2(y, x) * 180) / Math.PI;
  return (brng + 360) % 360;
}

function normalizeLatLng(point) {
  if (!point) return null;
  if (Array.isArray(point)) return { lat: Number(point[0]), lng: Number(point[1]) };
  return { lat: Number(point.lat), lng: Number(point.lng) };
}

function bearingToCompass(bearing) {
  if (bearing === null || bearing === undefined || Number.isNaN(bearing)) return "-";
  const dirs = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"];
  const idx = Math.round(((bearing % 360) / 45)) % 8;
  return dirs[idx];
}

function distanceKm(a, b) {
  // Distancia Haversine aproximada (km) entre dos puntos.
  const p1 = normalizeLatLng(a);
  const p2 = normalizeLatLng(b);
  if (!p1 || !p2) return Infinity;
  const dLat = ((p2.lat - p1.lat) * Math.PI) / 180;
  const dLon = ((p2.lng - p1.lng) * Math.PI) / 180;
  const lat1 = (p1.lat * Math.PI) / 180;
  const lat2 = (p2.lat * Math.PI) / 180;
  const h =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return 6371 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function angleDiff(a, b) {
  const diff = Math.abs((a - b) % 360);
  return diff > 180 ? 360 - diff : diff;
}

function interpolateLatLng(from, to, ratio) {
  const a = normalizeLatLng(from);
  const b = normalizeLatLng(to);
  if (!a || !b) return b || a || null;
  return {
    lat: a.lat + (b.lat - a.lat) * ratio,
    lng: a.lng + (b.lng - a.lng) * ratio
  };
}

function smoothTargetPosition(previousLatLng, incomingLatLng, speedKmh = 0) {
  // Antisalto visual: limita movimiento maximo por frame segun velocidad.
  const prev = normalizeLatLng(previousLatLng);
  const next = normalizeLatLng(incomingLatLng);
  if (!prev || !next) return incomingLatLng;
  const jumpKm = distanceKm(prev, next);
  const dynamicCap = clamp((Number(speedKmh || 0) / 16.0) + 1.6, 2.0, 8.0);
  if (jumpKm <= dynamicCap) return [next.lat, next.lng];
  const ratio = dynamicCap / jumpKm;
  const smoothed = interpolateLatLng(prev, next, ratio);
  return [smoothed.lat, smoothed.lng];
}

function getTrailBearing(vehicleId) {
  const trail = state.trails.get(vehicleId);
  if (!trail) return null;
  const pts = trail.getLatLngs();
  if (pts.length < 2) return null;
  const prev = normalizeLatLng(pts[pts.length - 2]);
  const curr = normalizeLatLng(pts[pts.length - 1]);
  if (!prev || !curr) return null;
  return computeBearing(prev, curr);
}

function estimateNextWarehouse(vehicle, bearing = null) {
  // Heuristica generica para siguiente nodo cuando no hay filtro/plan explicito.
  if (!state.graph?.vertices?.length || !vehicle) return null;
  const current = { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) };
  const candidates = state.graph.vertices
    .filter((v) => typeof v.latitude === "number" && typeof v.longitude === "number" && v.id !== vehicle.warehouse_id)
    .map((v) => {
      const node = { lat: v.latitude, lng: v.longitude };
      const dist = distanceKm(current, node);
      const nodeBearing = computeBearing(current, node);
      const turn = bearing === null ? 0 : angleDiff(bearing, nodeBearing);
      return { ...v, dist, turn, nodeBearing };
    });
  if (!candidates.length) return null;

  if (bearing === null) {
    return [...candidates].sort((a, b) => a.dist - b.dist)[0];
  }
  const forward = candidates.filter((c) => c.turn <= 115);
  const ranked = (forward.length ? forward : candidates).sort((a, b) => (a.turn * 1.6 + a.dist * 0.4) - (b.turn * 1.6 + b.dist * 0.4));
  return ranked[0];
}

function computeVehicleEta(vehicle) {
  // ETA local por vehiculo usando distancia al siguiente nodo + velocidad + delay.
  // Incluye estabilizacion temporal para evitar oscilaciones bruscas entre refrescos.
  if (!vehicle) return { etaLabel: "-", minutes: null, destination: null };
  const vehicleId = vehicle.vehicle_id;
  const nowMs = Date.now();

  const speed = Number(vehicle.speed_kmh || 0);
  const bearing = getTrailBearing(vehicle.vehicle_id);
  const nextNode = estimateNextNode(vehicle, bearing);
  if (!nextNode || speed <= 0) return { etaLabel: "-", minutes: null, destination: nextNode };

  const cached = state.etaByVehicle.get(vehicleId);
  const distanceToNext = distanceKm(
    { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) },
    { lat: Number(nextNode.latitude), lng: Number(nextNode.longitude) }
  );
  const travelMinutes = (distanceToNext / speed) * 60;
  const delayBuffer = Number(vehicle.delay_minutes || 0) * 0.35;
  const rawMinutes = travelMinutes + delayBuffer;
  const destinationChanged = Boolean(cached?.destinationId && cached.destinationId !== nextNode.id);
  const cachedMinutes = Number(cached?.minutes);
  const divergence = Number.isFinite(cachedMinutes) ? Math.abs(rawMinutes - cachedMinutes) : 0;
  const forceResync = destinationChanged || divergence > Math.max(45, rawMinutes * 0.5);

  if (cached && nowMs - Number(cached.updatedAt || 0) < 2000 && !forceResync) {
    const etaDateCached = new Date(nowMs + Number(cached.minutes || 0) * 60000);
    return {
      etaLabel: `${etaDateCached.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit", hour12: false })} (${fmt.hm(cached.minutes)})`,
      minutes: cached.minutes,
      destination: cached.destination || null
    };
  }

  let stableMinutes = rawMinutes;
  if (cached && Number.isFinite(cached.minutes) && !forceResync) {
    const elapsedMin = Math.max(0.2, (nowMs - Number(cached.updatedAt || nowMs)) / 60000);
    const maxStep = Math.max(7, elapsedMin * 18);
    const bounded = Number(cached.minutes) + clamp(rawMinutes - Number(cached.minutes), -maxStep, maxStep);
    const alpha = destinationChanged ? 0.20 : 0.35;
    stableMinutes = Number(cached.minutes) * (1 - alpha) + bounded * alpha;
  }

  state.etaByVehicle.set(vehicleId, {
    minutes: stableMinutes,
    destination: nextNode,
    destinationId: nextNode.id,
    updatedAt: nowMs
  });

  const etaDate = new Date(nowMs + stableMinutes * 60000);
  const etaLabel = `${etaDate.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit", hour12: false })} (${fmt.hm(stableMinutes)})`;
  return { etaLabel, minutes: stableMinutes, destination: nextNode };
}

function headingTowardRouteDestination(vehicle) {
  if (!vehicle) return null;
  const current = { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) };
  const bearing = getTrailBearing(vehicle.vehicle_id);
  const nextNode = estimateNextNode(vehicle, bearing);
  if (!nextNode) return bearing;
  return computeBearing(current, { lat: Number(nextNode.latitude), lng: Number(nextNode.longitude) });
}

function buildArrowIcon(angle, active) {
  return L.divIcon({
    className: "trail-arrow",
    html: `<span style=\"transform: rotate(${angle}deg); opacity:${active ? 1 : 0.35}\">➤</span>`,
    iconSize: [18, 18],
    iconAnchor: [9, 9]
  });
}

function updateTrailArrow(vehicleId, active) {
  const trail = state.trails.get(vehicleId);
  if (!trail) return;
  const pts = trail.getLatLngs();
  if (pts.length < 2) return;

  const prev = pts[pts.length - 2];
  const curr = pts[pts.length - 1];
  const angle = computeBearing(prev, curr);

  if (!state.trailArrows.has(vehicleId)) {
    const arrow = L.marker(curr, {
      icon: buildArrowIcon(angle, active),
      interactive: false,
      keyboard: false
    }).addTo(state.map);
    state.trailArrows.set(vehicleId, arrow);
  } else {
    const arrow = state.trailArrows.get(vehicleId);
    arrow.setLatLng(curr);
    arrow.setIcon(buildArrowIcon(angle, active));
  }
}

function routePathCoords(route, verticesById) {
  if (!route?.path?.length) return [];
  return route.path
    .map((id) => verticesById[id])
    .filter((v) => typeof v?.latitude === "number" && typeof v?.longitude === "number")
    .map((v) => ({ lat: Number(v.latitude), lng: Number(v.longitude) }));
}

function pointToSegmentDistanceKm(point, a, b) {
  const meanLat = (((a.lat + b.lat + point.lat) / 3) * Math.PI) / 180;
  const kmPerDegLat = 110.57;
  const kmPerDegLon = 111.32 * Math.cos(meanLat);
  const px = point.lng * kmPerDegLon;
  const py = point.lat * kmPerDegLat;
  const ax = a.lng * kmPerDegLon;
  const ay = a.lat * kmPerDegLat;
  const bx = b.lng * kmPerDegLon;
  const by = b.lat * kmPerDegLat;
  const abx = bx - ax;
  const aby = by - ay;
  const apx = px - ax;
  const apy = py - ay;
  const ab2 = abx * abx + aby * aby;
  const t = ab2 <= 0 ? 0 : clamp((apx * abx + apy * aby) / ab2, 0, 1);
  const cx = ax + t * abx;
  const cy = ay + t * aby;
  const dx = px - cx;
  const dy = py - cy;
  return Math.sqrt(dx * dx + dy * dy);
}

function minDistanceToRouteKm(vehicle, coords) {
  if (!coords || coords.length < 2) return Infinity;
  const point = { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) };
  let minDist = Infinity;
  for (let i = 0; i < coords.length - 1; i += 1) {
    const d = pointToSegmentDistanceKm(point, coords[i], coords[i + 1]);
    if (d < minDist) minDist = d;
  }
  return minDist;
}

function graphNodes() {
  return (state.graph?.vertices || []).filter(
    (v) => typeof v?.latitude === "number" && typeof v?.longitude === "number"
  );
}

function nearestWarehouseNode(vehicle) {
  if (!vehicle) return null;
  const nodes = graphNodes();
  if (!nodes.length) return null;
  const point = { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) };
  let best = null;
  let bestDist = Infinity;
  nodes.forEach((node) => {
    const d = distanceKm(point, { lat: Number(node.latitude), lng: Number(node.longitude) });
    if (d < bestDist) {
      bestDist = d;
      best = { ...node, dist: d };
    }
  });
  return best;
}

function graphNodeById(id) {
  if (!id) return null;
  const nodes = graphNodes();
  return nodes.find((n) => n.id === id) || null;
}

function plannedRouteForVehicle(vehicle) {
  if (!vehicle) return null;
  const fromId = vehicle.planned_origin || null;
  const toId = vehicle.planned_destination || null;
  if (!fromId || !toId) return null;
  const fromNode = graphNodeById(fromId);
  const toNode = graphNodeById(toId);
  if (!fromNode || !toNode) return null;
  return { from: fromNode, to: toNode, label: `${fromNode.id} -> ${toNode.id}` };
}

function inferVehicleRoute(vehicle) {
  if (!vehicle) return { from: null, to: null, label: "-" };
  const planned = plannedRouteForVehicle(vehicle);
  if (planned) {
    const fromNode = planned.from;
    const toNode = planned.to;
    if (fromNode?.id && toNode?.id && fromNode.id !== toNode.id) {
      return { from: fromNode, to: toNode, label: `${fromNode.id} -> ${toNode.id}` };
    }
    return planned;
  }
  const fromNode = nearestWarehouseNode(vehicle);
  const bearing = state.truckHeadings.get(vehicle.vehicle_id) ?? getTrailBearing(vehicle.vehicle_id);
  const toNode = estimateNextWarehouse(
    { ...vehicle, warehouse_id: fromNode?.id || vehicle.warehouse_id },
    bearing
  );
  if (fromNode?.id && toNode?.id) return { from: fromNode, to: toNode, label: `${fromNode.id} -> ${toNode.id}` };
  if (fromNode?.id) return { from: fromNode, to: null, label: `${fromNode.id} -> ?` };
  if (toNode?.id) return { from: null, to: toNode, label: `? -> ${toNode.id}` };
  return { from: null, to: null, label: vehicle.route_id || "-" };
}

function selectedRealtimeCorridorNodes() {
  // Devuelve corredor RT solo con filtros de Tiempo Real (izquierda).
  if (!state.graph?.vertices?.length) return [];
  const source = document.getElementById("rt-source-select")?.value;
  const target = document.getElementById("rt-target-select")?.value;
  if (!source || !target) return [];
  const byId = Object.fromEntries(state.graph.vertices.map((v) => [v.id, v]));
  const s = byId[source];
  const t = byId[target];
  if (
    typeof s?.latitude !== "number" ||
    typeof s?.longitude !== "number" ||
    typeof t?.latitude !== "number" ||
    typeof t?.longitude !== "number"
  ) {
    return [];
  }
  return [
    { id: source, latitude: Number(s.latitude), longitude: Number(s.longitude) },
    { id: target, latitude: Number(t.latitude), longitude: Number(t.longitude) }
  ];
}

function selectedRealtimeRouteFilter() {
  // Snapshot de filtros RT para manejar casos parciales (TODOS->X o X->TODOS).
  return {
    source: document.getElementById("rt-source-select")?.value || "",
    target: document.getElementById("rt-target-select")?.value || "",
  };
}

function estimateNextNodeOnActiveRoute(vehicle) {
  // Si hay corredor RT activo, fuerza la navegacion hacia ese corredor.
  const nodes = selectedRealtimeCorridorNodes();
  if (!vehicle || nodes.length < 2) return null;
  const point = { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) };
  const a = nodes[0];
  const b = nodes[1];
  const distA = distanceKm(point, { lat: a.latitude, lng: a.longitude });
  const distB = distanceKm(point, { lat: b.latitude, lng: b.longitude });
  const nearest = nearestWarehouseNode(vehicle);
  if (nearest?.id === a.id) return { ...b, dist: distB };
  if (nearest?.id === b.id) return { ...a, dist: distA };
  const bearing = state.truckHeadings.get(vehicle.vehicle_id) ?? getTrailBearing(vehicle.vehicle_id);
  if (bearing === null || bearing === undefined) {
    const winner = distA <= distB ? b : a;
    return { ...winner, dist: winner.id === a.id ? distA : distB };
  }
  const bearingA = computeBearing(point, { lat: a.latitude, lng: a.longitude });
  const bearingB = computeBearing(point, { lat: b.latitude, lng: b.longitude });
  const turnA = angleDiff(bearing, bearingA);
  const turnB = angleDiff(bearing, bearingB);
  const winner = turnA <= turnB ? a : b;
  return { ...winner, dist: winner.id === a.id ? distA : distB };
}

function estimateNextNode(vehicle, bearing = null) {
  const strict = estimateNextNodeOnActiveRoute(vehicle);
  if (strict) return strict;
  const inferred = inferVehicleRoute(vehicle);
  if (inferred?.to?.id) {
    return {
      ...inferred.to,
      dist: distanceKm(
        { lat: Number(vehicle.latitude), lng: Number(vehicle.longitude) },
        { lat: Number(inferred.to.latitude), lng: Number(inferred.to.longitude) }
      ),
    };
  }
  return estimateNextWarehouse(vehicle, bearing);
}

function resolveVisibleVehicles(items) {
  // Filtro maestro de visibilidad de flota en Tiempo Real:
  // - soporta filtros parciales y completos
  // - limpia seleccion actual si queda fuera del filtro
  const { source, target } = selectedRealtimeRouteFilter();
  if (!source && !target) return items;

  const corridorNodes = selectedRealtimeCorridorNodes();
  const pathSet = new Set(corridorNodes.map((n) => n.id));
  const coords = corridorNodes.map((n) => ({ lat: n.latitude, lng: n.longitude }));

  const compatible = items.filter((vehicle) => {
    const routeInfo = inferVehicleRoute(vehicle);
    const fromId = routeInfo.from?.id || vehicle.warehouse_id || "";
    const toId = routeInfo.to?.id || "";

    if (source && target) {
      const distToRoute = coords.length >= 2 ? minDistanceToRouteKm(vehicle, coords) : Infinity;
      const strictNext = estimateNextNodeOnActiveRoute(vehicle);
      const strictNextOnPath = !!strictNext?.id && pathSet.has(strictNext.id);
      const fromOnPath = !!fromId && pathSet.has(fromId);
      return distToRoute <= 40 && fromOnPath && strictNextOnPath && toId === target;
    }

    if (source && !target) {
      return fromId === source;
    }

    if (!source && target) {
      return toId === target;
    }

    return true;
  });

  if (state.selectedVehicle && !compatible.some((v) => v.vehicle_id === state.selectedVehicle)) {
    state.selectedVehicle = "";
    const select = document.getElementById("vehicle-select");
    if (select) select.value = "";
  }

  return compatible;
}

function computeHeadingFromMovement(prev, next) {
  if (!prev || !next) return null;
  if (distanceKm(prev, next) < 0.02) return null;
  return computeBearing(prev, next);
}

function displayRouteForVehicle(vehicle) {
  const inferred = inferVehicleRoute(vehicle);
  const routeNext = estimateNextNodeOnActiveRoute(vehicle);
  if (inferred.from?.id && routeNext?.id) {
    if (inferred.from.id !== routeNext.id) return `${inferred.from.id} -> ${routeNext.id}`;
    const corridor = selectedRealtimeCorridorNodes();
    const alt = corridor.find((n) => n.id !== inferred.from.id);
    if (alt?.id) return `${inferred.from.id} -> ${alt.id}`;
  }
  return inferred.label;
}

function syncRealtimeViewFromFilters() {
  const base = state.allVehicles?.length ? state.allVehicles : state.vehicles;
  const visibleItems = resolveVisibleVehicles(base || []);
  state.vehicles = visibleItems;
  renderVehicleTable(visibleItems);
  updateMarkers(visibleItems);
  hydrateVehicleSelect(visibleItems);
  applyVehicleFocus();
}

function updateMarkers(items) {
  // Render incremental de flota:
  // - elimina marcadores fuera del set visible
  // - crea/actualiza posicion, icono orientado y traza corta
  // - mantiene popup con datos operativos
  const keep = new Set(items.map((v) => v.vehicle_id));
  for (const [key, marker] of state.markers.entries()) {
    if (keep.has(key)) continue;
    state.map.removeLayer(marker);
    state.markers.delete(key);
    const trail = state.trails.get(key);
    if (trail) {
      state.map.removeLayer(trail);
      state.trails.delete(key);
    }
    const arrow = state.trailArrows.get(key);
    if (arrow) {
      state.map.removeLayer(arrow);
      state.trailArrows.delete(key);
    }
    state.truckHeadings.delete(key);
    state.etaByVehicle.delete(key);
  }

  items.forEach((v) => {
    const key = v.vehicle_id;
    const latlng = [v.latitude, v.longitude];
    const selected = !state.selectedVehicle || state.selectedVehicle === key;

    if (!state.markers.has(key)) {
      const nextNode = estimateNextNode(v, null);
      const initialHeading = nextNode
        ? computeBearing(
            { lat: Number(v.latitude), lng: Number(v.longitude) },
            { lat: Number(nextNode.latitude), lng: Number(nextNode.longitude) }
          )
        : state.truckHeadings.get(key) || 0;
      state.truckHeadings.set(key, initialHeading);
      const marker = L.marker(latlng, { icon: buildTruckIcon(v, selected, initialHeading) }).addTo(state.map);
      marker.on("click", async () => {
        state.selectedVehicle = key;
        const select = document.getElementById("vehicle-select");
        if (select) select.value = key;
        await refreshVehicleHistory();
      });
      state.markers.set(key, marker);

      const trail = L.polyline([latlng], { color: "#f18f01", weight: 2, opacity: 0.0 }).addTo(state.map);
      state.trails.set(key, trail);
    } else if (state.playback) {
      const marker = state.markers.get(key);
      const prev = marker.getLatLng();
      const smoothedLatLng = smoothTargetPosition(prev, latlng, v.speed_kmh);
      const orientedVehicle = { ...v, latitude: smoothedLatLng[0], longitude: smoothedLatLng[1] };
      const movementHeading = computeHeadingFromMovement(prev, normalizeLatLng(smoothedLatLng));
      const nextNode = estimateNextNode(orientedVehicle, movementHeading);
      const fallbackHeading = nextNode
        ? computeBearing(
            { lat: Number(orientedVehicle.latitude), lng: Number(orientedVehicle.longitude) },
            { lat: Number(nextNode.latitude), lng: Number(nextNode.longitude) }
          )
        : state.truckHeadings.get(key) ?? getTrailBearing(key) ?? 0;
      const heading = movementHeading ?? fallbackHeading;
      state.truckHeadings.set(key, heading);
      marker.setLatLng(smoothedLatLng);
      marker.setIcon(buildTruckIcon(v, selected, heading));

      const trail = state.trails.get(key);
      const points = trail.getLatLngs();
      points.push(smoothedLatLng);
      while (points.length > 8) points.shift();
      trail.setLatLngs(points);
    }

    updateTrailArrow(key, selected);

    const marker = state.markers.get(key);
    marker.bindPopup(
      `<strong>${v.vehicle_id}</strong><br/>Almacen: ${v.warehouse_id}<br/>Ruta: ${displayRouteForVehicle(v)}<br/>Delay: ${v.delay_minutes} min<br/>Velocidad: ${fmt.n(v.speed_kmh, 1)} km/h<br/>${fmt.dt(v.event_time)}<br/><em>Clic para fijar seleccion</em>`
    );
  });
}

function renderWeather(items) {
  const tbody = document.querySelector("#weather-table tbody");
  tbody.innerHTML = items
    .map(
      (w) => `
      <tr>
        <td>${fmt.dt(w.observed_at)}</td>
        <td>${fmt.n(w.temperature_c, 1)}</td>
        <td>${fmt.n(w.precipitation_mm, 2)}</td>
        <td>${fmt.n(w.wind_kmh, 1)}</td>
      </tr>
    `
    )
    .join("");
  applyTableSortState("weather-table");
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function routeAdjustedWeather(items, route) {
  if (!route || !Array.isArray(items) || !items.length) return items;
  const globalFactor = Number(route.weather_factor || 0);
  const routeFactor = Number(route.route_weather_factor || globalFactor || 0);
  if (globalFactor <= 0 || routeFactor <= 0) return items;

  const ratio = clamp(routeFactor / globalFactor, 0.6, 3.0);
  const avgDelay =
    Array.isArray(route.edges) && route.edges.length
      ? route.edges.reduce((acc, e) => acc + Number(e.avg_delay_minutes || 0), 0) / route.edges.length
      : 0;
  const delayBoost = clamp(1 + avgDelay / 55.0, 1.0, 1.5);
  const rainScale = clamp(ratio * delayBoost, 0.7, 3.2);
  const windScale = clamp(0.92 + (ratio - 1) * 0.65, 0.75, 2.4);

  return items.map((w) => ({
    ...w,
    precipitation_mm: Number(w.precipitation_mm || 0) * rainScale,
    wind_kmh: Number(w.wind_kmh || 0) * windScale
  }));
}

function hydrateVehicleSelect(items) {
  const select = document.getElementById("vehicle-select");
  const prev = state.selectedVehicle || select.value || "";
  const values = [...new Set(items.map((v) => v.vehicle_id))].sort();
  select.innerHTML = `<option value=\"\">Todos</option>${values.map((v) => `<option value=\"${v}\">${v}</option>`).join("")}`;
  if (prev && values.includes(prev)) {
    select.value = prev;
    state.selectedVehicle = prev;
  } else {
    state.selectedVehicle = "";
    select.value = "";
  }
}

function hydrateGraphControls(vertices) {
  const sortedVertices = [...(vertices || [])].sort((a, b) => {
    const left = `${a?.id || ""} - ${a?.name || ""}`.toUpperCase();
    const right = `${b?.id || ""} - ${b?.name || ""}`.toUpperCase();
    return left.localeCompare(right, "es");
  });
  const nodeOptions = sortedVertices.map((v) => `<option value=\"${v.id}\">${v.id} - ${v.name}</option>`).join("");
  const options = `<option value=\"\">TODOS</option>${nodeOptions}`;
  document.getElementById("source-select").innerHTML = options;
  document.getElementById("target-select").innerHTML = options;
  document.getElementById("rt-source-select").innerHTML = options;
  document.getElementById("rt-target-select").innerHTML = options;
  document.getElementById("source-select").value = "";
  document.getElementById("target-select").value = "";
  document.getElementById("rt-source-select").value = "";
  document.getElementById("rt-target-select").value = "";
}

function enforceDistinctRouteEndpoints() {
  const sourceEl = document.getElementById("source-select");
  const targetEl = document.getElementById("target-select");
  if (!sourceEl || !targetEl) return;
  const source = sourceEl.value;
  const target = targetEl.value;
  if (!source || !target) return;
  if (source !== target) return;

  const candidate = Array.from(targetEl.options)
    .map((o) => o.value)
    .find((v) => v && v !== source);
  targetEl.value = candidate || "";
}

function routeEdgeKey(src, dst) {
  return `${src}|${dst}`;
}

function normalizeRouteEdgeSet(route) {
  const set = new Set();
  (route?.edges || []).forEach((e) => {
    set.add(routeEdgeKey(e.src, e.dst));
    set.add(routeEdgeKey(e.dst, e.src));
  });
  return set;
}

function weatherStrokeColor(level) {
  if (level === "high") return "#ff4f6d";
  if (level === "medium") return "#ffaf2f";
  return "#7aa2c7";
}

function edgeDelayMinutes(edge) {
  if (!edge) return 0;
  const effective = Number(edge.effective_avg_delay_minutes);
  if (Number.isFinite(effective)) return effective;
  return Number(edge.avg_delay_minutes || 0);
}

function edgeDelayLabel(edge) {
  const liveSamples = Number(edge?.live_sample_count || 0);
  if (liveSamples > 0) {
    const live = Number(edge?.live_avg_delay_minutes ?? edgeDelayMinutes(edge));
    return `${fmt.n(edgeDelayMinutes(edge), 1)} (live ${fmt.n(live, 1)})`;
  }
  return fmt.n(edgeDelayMinutes(edge), 1);
}

function edgeCongestionLevel(edge) {
  return edge?.congestion_level || "unknown";
}

function buildGraphApiUrl() {
  const params = new URLSearchParams({
    insights_profile: state.insightsProfile || "balanced",
    insights_min_congestion: state.insightsMinCongestion || "all"
  });
  return `/api/network/graph?${params.toString()}`;
}

function buildInsightsHistoryApiUrl() {
  const params = new URLSearchParams({
    insights_profile: state.insightsProfile || "balanced",
    insights_min_congestion: state.insightsMinCongestion || "all",
    snapshots: "12"
  });
  return `/api/network/insights/history?${params.toString()}`;
}

function renderNetworkInsights(insights) {
  const bottlenecksBody = document.querySelector("#bottlenecks-table tbody");
  const nodesBody = document.querySelector("#critical-nodes-table tbody");
  if (!bottlenecksBody || !nodesBody) return;
  const topBottlenecks = insights?.top_bottlenecks || [];
  const topNodes = insights?.top_critical_nodes || [];

  if (!topBottlenecks.length) {
    bottlenecksBody.innerHTML = `<tr><td colspan="5">Sin datos live suficientes</td></tr>`;
  } else {
    bottlenecksBody.innerHTML = topBottlenecks
      .map(
        (item) => `
      <tr>
        <td>${item.src} -> ${item.dst}</td>
        <td>${fmt.n(item.effective_avg_delay_minutes, 1)}</td>
        <td>${String(item.congestion_level || "unknown").toUpperCase()}</td>
        <td>${item.live_sample_count || 0}</td>
        <td>${fmt.n(item.impact_score, 2)}</td>
      </tr>
    `
      )
      .join("");
  }
  applyTableSortState("bottlenecks-table");

  if (!topNodes.length) {
    nodesBody.innerHTML = `<tr><td colspan="5">Sin datos de criticidad</td></tr>`;
  } else {
    nodesBody.innerHTML = topNodes
      .map(
        (item) => `
      <tr>
        <td>${item.id}</td>
        <td>${String(item.criticality || "unknown").toUpperCase()}</td>
        <td>${item.degree || 0}</td>
        <td>${fmt.n(item.avg_incident_delay_minutes, 1)}</td>
        <td>${fmt.n(item.criticality_score, 2)}</td>
      </tr>
    `
      )
      .join("");
  }
  applyTableSortState("critical-nodes-table");
}

function renderInsightsHistory(history) {
  const tbody = document.querySelector("#insights-history-table tbody");
  if (!tbody) return;
  const items = history?.items || [];
  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="5">Sin historico disponible en Cassandra</td></tr>`;
    applyTableSortState("insights-history-table");
    return;
  }
  tbody.innerHTML = items
    .map((item) => {
      const topEdge = item.top_bottleneck?.entity_id || "-";
      const edgeImpact = item.top_bottleneck ? fmt.n(item.top_bottleneck.impact_score, 2) : "-";
      const topNode = item.top_node?.entity_id || "-";
      const nodeScore = item.top_node ? fmt.n(item.top_node.criticality_score, 2) : "-";
      return `
      <tr>
        <td>${fmt.dt(item.snapshot_time)}</td>
        <td>${topEdge}</td>
        <td>${edgeImpact}</td>
        <td>${topNode}</td>
        <td>${nodeScore}</td>
      </tr>
    `;
    })
    .join("");
  applyTableSortState("insights-history-table");
}

function profileBaseMinutes(profile, edge) {
  const distance = Number(edge.distance_km || 0);
  const delay = edgeDelayMinutes(edge);
  if (profile === "fastest") return (distance / 88.0) * 60.0 + delay * 0.9;
  if (profile === "resilient") return (distance / 70.0) * 60.0 + delay * 1.15;
  return (distance / 75.0) * 60.0 + delay;
}

function computeEdgeEstimate(edge, profile) {
  const delay = edgeDelayMinutes(edge);
  const weatherFactor = Number(state.weatherFactor || 0);
  const base = profileBaseMinutes(profile, edge);
  let penalty = delay * weatherFactor * 1.9;
  if (profile === "fastest") penalty = delay * weatherFactor * 1.4;
  if (profile === "resilient") penalty = delay * weatherFactor * 0.7;
  return {
    base,
    penalty,
    total: base + penalty
  };
}

function renderAllRoutesTable(graph, route = null) {
  const tbody = document.querySelector("#all-routes-table tbody");
  if (!tbody) return;
  if (!graph?.edges?.length) {
    tbody.innerHTML = `<tr><td colspan="7">No hay rutas disponibles</td></tr>`;
    applyTableSortState("all-routes-table");
    return;
  }
  const profile = document.getElementById("profile-select")?.value || "balanced";
  const selectedSource = document.getElementById("source-select")?.value || "";
  const selectedTarget = document.getElementById("target-select")?.value || "";
  const routeEdges = normalizeRouteEdgeSet(route);
  let rows = [];

  const hasConcreteSelection = !!selectedSource && !!selectedTarget && selectedSource !== selectedTarget;
  if (hasConcreteSelection) {
    const routeRows =
      route?.edges?.map((edge) => ({
        src: edge.src,
        dst: edge.dst,
        edge
      })) || [];

    if (routeRows.length) {
      rows = routeRows;
    } else {
      const directEdge = graph.edges.find(
        (edge) =>
          (edge.src === selectedSource && edge.dst === selectedTarget) ||
          (edge.src === selectedTarget && edge.dst === selectedSource)
      );
      if (directEdge) {
        const directed =
          directEdge.src === selectedSource && directEdge.dst === selectedTarget
            ? { src: directEdge.src, dst: directEdge.dst, edge: directEdge }
            : { src: selectedSource, dst: selectedTarget, edge: directEdge };
        rows = [directed];
      }
    }
  } else {
    graph.edges.forEach((edge) => {
      const forward = { src: edge.src, dst: edge.dst, edge };
      const backward = { src: edge.dst, dst: edge.src, edge };
      rows.push(forward, backward);
    });
  }

  if (!rows.length) {
    if (hasConcreteSelection) {
      tbody.innerHTML = `<tr><td colspan="7">Sin tramos para la seleccion ${selectedSource} -> ${selectedTarget}</td></tr>`;
    } else {
      tbody.innerHTML = `<tr><td colspan="7">No hay datos de rutas para mostrar</td></tr>`;
    }
    applyTableSortState("all-routes-table");
    return;
  }

  tbody.innerHTML = rows
    .map((row) => {
      const estimate = computeEdgeEstimate(row.edge, profile);
      const active = routeEdges.has(routeEdgeKey(row.src, row.dst));
      return `
      <tr class=\"${active ? "route-hit" : ""}\">
        <td>${row.src} -> ${row.dst}</td>
        <td>${fmt.n(row.edge.distance_km, 1)}</td>
        <td>${edgeDelayLabel(row.edge)}</td>
        <td>${fmt.hm(estimate.base)}</td>
        <td>+${fmt.hm(estimate.penalty)}</td>
        <td>${fmt.hm(estimate.total)}</td>
        <td>${profile}</td>
      </tr>
    `;
    })
    .join("");
  applyTableSortState("all-routes-table");
}

function renderRouteSummary(route) {
  const tbody = document.querySelector("#route-table tbody");
  if (!tbody) return;
  if (!route) {
    tbody.innerHTML = `<tr><td>Estado</td><td>Selecciona origen y destino para calcular ruta</td></tr>`;
    applyTableSortState("route-table");
    return;
  }
  const base = Number(route.base_travel_minutes || 0);
  const penalty = Number(route.weather_penalty_minutes || 0);
  const climateImpactPct = base > 0 ? (penalty / base) * 100 : 0;
  const effectiveWeatherFactor = Number(route.route_weather_factor ?? route.weather_factor ?? 0);
  const effectiveImpactLevel = route.route_weather_impact_level || route.weather_impact_level || "low";
  tbody.innerHTML = `
    <tr><td>Perfil</td><td>${route.profile}</td></tr>
    <tr><td>Ultimo recalculo</td><td>${state.lastRouteCalcAt ? fmt.dt(state.lastRouteCalcAt) : "-"}</td></tr>
    <tr><td>Camino</td><td>${route.path.join(" -> ")}</td></tr>
    <tr><td>Distancia total</td><td>${fmt.n(route.total_distance_km, 1)} km</td></tr>
    <tr><td>Tiempo base</td><td>${fmt.hm(route.base_travel_minutes)}</td></tr>
    <tr><td>Penalizacion meteo</td><td>+${fmt.hm(route.weather_penalty_minutes)} (${effectiveImpactLevel.toUpperCase()})</td></tr>
    <tr><td>Impacto meteo en ruta</td><td>${fmt.n(climateImpactPct, 1)}%</td></tr>
    <tr><td>Delay esperado</td><td>${fmt.hm(route.expected_delay_minutes)}</td></tr>
    <tr><td>Tiempo estimado</td><td>${fmt.hm(route.estimated_travel_minutes)}</td></tr>
    <tr><td>Aristas con telemetria live</td><td>${(route.edges || []).filter((e) => Number(e.live_sample_count || 0) > 0).length}</td></tr>
    <tr><td>Factor meteo global</td><td>${fmt.n(route.weather_factor, 2)}</td></tr>
    <tr><td>Factor meteo aplicado</td><td>${fmt.n(effectiveWeatherFactor, 2)} (${effectiveImpactLevel.toUpperCase()})</td></tr>
  `;
  applyTableSortState("route-table");
}

function setRouteStatus(text, level = "") {
  const status = document.getElementById("route-status");
  if (!status) return;
  status.textContent = text;
  status.classList.remove("loading", "ok", "error");
  if (level) status.classList.add(level);
}

function renderWarehousesOnMap(vertices) {
  if (!state.map) return;
  if (state.warehouseLayer) state.map.removeLayer(state.warehouseLayer);
  state.warehouseLayer = L.layerGroup().addTo(state.map);

  const bounds = [];
  vertices.forEach((v) => {
    if (typeof v.latitude !== "number" || typeof v.longitude !== "number") return;
    const color = v.criticality === "high" ? "#ffaf2f" : "#7aa2c7";
    const marker = L.circleMarker([v.latitude, v.longitude], {
      radius: 6,
      color,
      weight: 2,
      fillColor: color,
      fillOpacity: 0.85
    });
    marker.bindTooltip(v.id, {
      direction: "top",
      permanent: true,
      className: "warehouse-label"
    });
    marker.bindPopup(`<strong>${v.id}</strong> - ${v.name}<br/>Criticidad: ${v.criticality}`);
    marker.addTo(state.warehouseLayer);
    bounds.push([v.latitude, v.longitude]);
  });

  if (!state.didFitToWarehouses && bounds.length > 1) {
    state.map.fitBounds(bounds, { padding: [40, 40] });
    state.didFitToWarehouses = true;
  }
}

function renderNetworkMap(graph, route = null) {
  // Render de red logistica (vista derecha) independiente de filtros RT.
  if (!state.networkMap) return;
  if (state.networkLayer) state.networkMap.removeLayer(state.networkLayer);
  state.networkLayer = L.layerGroup().addTo(state.networkMap);

  const byId = {};
  graph.vertices.forEach((v) => {
    if (typeof v.latitude === "number" && typeof v.longitude === "number") byId[v.id] = v;
  });

  const routeEdges = normalizeRouteEdgeSet(route);
  const weatherColor = weatherStrokeColor(state.weatherImpactLevel);
  const bounds = [];

  graph.edges.forEach((e) => {
    const s = byId[e.src];
    const d = byId[e.dst];
    if (!s || !d) return;
    const highlighted = routeEdges.has(routeEdgeKey(e.src, e.dst));
    const congestion = edgeCongestionLevel(e);
    let baseColor = weatherColor;
    if (congestion === "high") baseColor = "#d7263d";
    if (congestion === "medium") baseColor = "#ffaf2f";
    const liveSampleCount = Number(e.live_sample_count || 0);
    const poly = L.polyline(
      [
        [s.latitude, s.longitude],
        [d.latitude, d.longitude]
      ],
      {
        color: highlighted ? "#d7263d" : baseColor,
        weight: highlighted ? 5 : Math.max(2, 2 + state.weatherFactor + Math.min(2, liveSampleCount)),
        opacity: highlighted ? 0.95 : 0.70
      }
    ).addTo(state.networkLayer);
    poly.bindTooltip(
      `${e.src} -> ${e.dst} | ${e.distance_km} km | delay ${fmt.n(edgeDelayMinutes(e), 1)} min | congestion ${congestion}`,
      {
      sticky: true
      }
    );
  });

  graph.vertices.forEach((v) => {
    if (typeof v.latitude !== "number" || typeof v.longitude !== "number") return;
    const inRoute = route?.path?.includes(v.id);
    const color = v.criticality === "high" ? "#ffaf2f" : "#7aa2c7";
    const marker = L.circleMarker([v.latitude, v.longitude], {
      radius: inRoute ? 9 : 7,
      color,
      weight: inRoute ? 3 : 2,
      fillColor: inRoute ? "#ffd166" : color,
      fillOpacity: 0.9
    }).addTo(state.networkLayer);
    marker.bindTooltip(v.id, { permanent: true, direction: "top", className: "network-label" });
    marker.bindPopup(`<strong>${v.id}</strong> - ${v.name}<br/>Criticidad: ${v.criticality}`);
    bounds.push([v.latitude, v.longitude]);
  });

  if (bounds.length > 1) state.networkMap.fitBounds(bounds, { padding: [30, 30] });
}

function renderSelectedVehiclePanel() {
  const panel = document.getElementById("selected-vehicle-panel");
  if (!panel) return;
  if (!state.selectedVehicle) {
    panel.innerHTML = "Selecciona un vehiculo para ver su ruta reciente, rumbo y siguiente destino estimado.";
    return;
  }
  const vehicle = state.vehicles.find((v) => v.vehicle_id === state.selectedVehicle);
  if (!vehicle) {
    panel.innerHTML = `No hay datos recientes para <strong>${state.selectedVehicle}</strong>.`;
    return;
  }
  const bearing = getTrailBearing(vehicle.vehicle_id);
  const nextNode = estimateNextNode(vehicle, bearing);
  const inferred = inferVehicleRoute(vehicle);
  const eta = computeVehicleEta(vehicle);
  const displayBearing = state.truckHeadings.get(vehicle.vehicle_id) ?? bearing;
  const heading = displayBearing === null || displayBearing === undefined ? "-" : `${Math.round(displayBearing)}° (${bearingToCompass(displayBearing)})`;
  const nextLabel = nextNode ? `${nextNode.id} (${fmt.n(nextNode.dist, 1)} km)` : "-";

  panel.innerHTML = `
    <table>
      <tbody>
        <tr><td><strong>Vehiculo</strong></td><td>${vehicle.vehicle_id}</td></tr>
        <tr><td><strong>Almacen actual</strong></td><td>${inferred.from?.id || vehicle.warehouse_id || "-"}</td></tr>
        <tr><td><strong>Ruta reportada</strong></td><td>${displayRouteForVehicle(vehicle)}</td></tr>
        <tr><td><strong>Rumbo actual</strong></td><td>${heading}</td></tr>
        <tr><td><strong>Siguiente nodo estimado</strong></td><td>${nextLabel}</td></tr>
        <tr><td><strong>Hora estimada llegada</strong></td><td>${eta.etaLabel}</td></tr>
        <tr><td><strong>Delay</strong></td><td>${vehicle.delay_minutes} min</td></tr>
        <tr><td><strong>Velocidad</strong></td><td>${fmt.n(vehicle.speed_kmh, 1)} km/h</td></tr>
      </tbody>
    </table>
  `;
}

function renderProjectionLine() {
  if (state.projectionLine) {
    state.map.removeLayer(state.projectionLine);
    state.projectionLine = null;
  }
  if (!state.selectedVehicle) return;
  const vehicle = state.vehicles.find((v) => v.vehicle_id === state.selectedVehicle);
  if (!vehicle) return;
  const bearing = getTrailBearing(vehicle.vehicle_id);
  const nextNode = estimateNextNode(vehicle, bearing);
  if (!nextNode) return;
  state.projectionLine = L.polyline(
    [
      [vehicle.latitude, vehicle.longitude],
      [nextNode.latitude, nextNode.longitude]
    ],
    {
      color: "#ffd166",
      weight: 2,
      dashArray: "8 6",
      opacity: 0.85
    }
  ).addTo(state.map);
  state.projectionLine.bindTooltip(`Destino estimado: ${nextNode.id}`, { sticky: true });
}

function applyVehicleFocus() {
  const selected = state.selectedVehicle;
  state.vehicles.forEach((vehicle) => {
    const marker = state.markers.get(vehicle.vehicle_id);
    const trail = state.trails.get(vehicle.vehicle_id);
    const arrow = state.trailArrows.get(vehicle.vehicle_id);
    if (!marker || !trail) return;

    const show = !!selected && selected === vehicle.vehicle_id;
    const markerPos = marker.getLatLng();
    const orientedVehicle = { ...vehicle, latitude: markerPos.lat, longitude: markerPos.lng };
    const heading =
      state.truckHeadings.get(vehicle.vehicle_id) ??
      headingTowardRouteDestination(orientedVehicle) ??
      getTrailBearing(vehicle.vehicle_id) ??
      0;
    state.truckHeadings.set(vehicle.vehicle_id, heading);
    marker.setIcon(buildTruckIcon(vehicle, show, heading));
    trail.setStyle({
      color: "#f18f01",
      opacity: show ? 0.42 : 0.0,
      weight: show ? 2.2 : 1.2
    });
    if (arrow && arrow.getElement()) {
      arrow.getElement().style.opacity = show ? "1" : "0";
    }
  });

  if (selected) {
    const points = state.vehicles.filter((v) => v.vehicle_id === selected).map((v) => [v.latitude, v.longitude]);
    if (points.length > 0) state.map.panTo(points[points.length - 1]);
  }
  renderProjectionLine();
  renderSelectedVehiclePanel();
}

async function refreshFleet() {
  // Refresco periodico principal de Tiempo Real.
  // Carga overview + flota + clima, aplica filtros RT y sincroniza UI.
  const [overviewRes, latestRes, weatherRes] = await Promise.all([
    fetchJson("/api/overview"),
    fetchJson("/api/vehicles/latest?limit=80"),
    fetchJson("/api/weather/latest?limit=12")
  ]);

  const visibleItems = resolveVisibleVehicles(latestRes.items);

  state.liveEdgeSummary = overviewRes.live_edge_summary || null;
  renderOverview(overviewRes.overview, state.liveEdgeSummary);
  renderDataSources(overviewRes.vehicle_source, overviewRes.weather_source);
  renderFleetFreshness(overviewRes.overview.latest_event_time);
  renderVehicleTable(visibleItems);
  renderWeather(state.route ? routeAdjustedWeather(weatherRes.items, state.route) : weatherRes.items);
  updateMarkers(visibleItems);

  state.allVehicles = latestRes.items;
  state.vehicles = visibleItems;
  state.weatherFactor = Number(overviewRes.overview.weather_factor || 0);
  state.weatherImpactLevel = overviewRes.overview.weather_impact_level || "low";
  hydrateVehicleSelect(visibleItems);
  applyVehicleFocus();

  if (state.graph) {
    renderAllRoutesTable(state.graph, state.route);
    renderNetworkMap(state.graph, state.route);
  }
}

async function refreshVehicleHistory() {
  const vehicleId = document.getElementById("vehicle-select").value;
  if (!vehicleId) {
    applyVehicleFocus();
    return;
  }
  const history = await fetchJson(`/api/vehicles/history?vehicle_id=${encodeURIComponent(vehicleId)}&points=80`);
  const sampled = history.items
    .map((p) => [p.latitude, p.longitude])
    .filter((p, idx, arr) => arr.length <= 16 || idx % Math.ceil(arr.length / 16) === 0 || idx === arr.length - 1);
  const trail = state.trails.get(vehicleId);
  if (trail && sampled.length) {
    trail.setLatLngs(sampled);
    updateTrailArrow(vehicleId, true);
  }

  applyVehicleFocus();
}

async function loadGraph() {
  const [graph, history] = await Promise.all([fetchJson(buildGraphApiUrl()), fetchJson(buildInsightsHistoryApiUrl())]);
  state.graph = graph;
  state.liveEdgeSummary = graph.live_edge_summary || state.liveEdgeSummary;
  state.networkInsights = graph.network_insights || null;
  state.insightsHistory = history || null;
  state.weatherFactor = Number(graph.weather_factor || state.weatherFactor || 0);
  state.weatherImpactLevel = graph.weather_impact_level || state.weatherImpactLevel || "low";
  hydrateGraphControls(graph.vertices);
  renderWarehousesOnMap(graph.vertices);
  renderAllRoutesTable(graph, null);
  renderNetworkMap(graph, null);
  renderRouteSummary(null);
  renderNetworkInsights(state.networkInsights);
  renderInsightsHistory(state.insightsHistory);
}

async function refreshNetworkInsightsOnly() {
  if (!state.graph) return;
  const [graph, history] = await Promise.all([fetchJson(buildGraphApiUrl()), fetchJson(buildInsightsHistoryApiUrl())]);
  state.networkInsights = graph.network_insights || null;
  state.insightsHistory = history || null;
  renderNetworkInsights(state.networkInsights);
  renderInsightsHistory(state.insightsHistory);
}

async function calculateRoute() {
  // Recalculo de mejor ruta para bloque de Red Logistica (vista derecha).
  // No debe alterar filtros/estado de la vista Tiempo Real.
  if (!state.graph) return;
  if (state.routeRequestInFlight) {
    state.routeRecalcPending = true;
    return;
  }
  state.routeRequestInFlight = true;
  const routeBtn = document.getElementById("route-btn");
  const previousLabel = routeBtn ? routeBtn.textContent : "";
  if (routeBtn) {
    routeBtn.disabled = true;
    routeBtn.textContent = "Calculando...";
  }
  setRouteStatus("Recalculando ruta...", "loading");
  enforceDistinctRouteEndpoints();
  const source = document.getElementById("source-select").value;
  const target = document.getElementById("target-select").value;
  const profile = document.getElementById("profile-select").value;

  if (!source || !target) {
    state.route = null;
    const visibleItems = resolveVisibleVehicles(state.allVehicles?.length ? state.allVehicles : state.vehicles);
    state.vehicles = visibleItems;
    renderVehicleTable(visibleItems);
    updateMarkers(visibleItems);
    hydrateVehicleSelect(visibleItems);
    applyVehicleFocus();
    renderAllRoutesTable(state.graph, null);
    renderNetworkMap(state.graph, null);
    renderRouteSummary(null);
    setRouteStatus("Vista global (TODOS): sin ruta concreta seleccionada", "ok");
    state.routeRequestInFlight = false;
    if (routeBtn) {
      routeBtn.disabled = false;
      routeBtn.textContent = previousLabel || "Calcular mejor ruta";
    }
    return;
  }

  try {
    const [result, weatherRes, overviewRes, graphRes, historyRes] = await Promise.all([
      fetchJson(
        `/api/network/best-route?source=${encodeURIComponent(source)}&target=${encodeURIComponent(target)}&profile=${encodeURIComponent(profile)}`
      ),
      fetchJson("/api/weather/latest?limit=12"),
      fetchJson("/api/overview"),
      fetchJson(buildGraphApiUrl()),
      fetchJson(buildInsightsHistoryApiUrl())
    ]);
    state.graph = graphRes;
    state.route = result.route;
    state.lastRouteCalcAt = new Date().toISOString();
    state.weatherFactor = Number(result.route.route_weather_factor || result.route.weather_factor || overviewRes.overview.weather_factor || 0);
    state.weatherImpactLevel =
      result.route.route_weather_impact_level || result.route.weather_impact_level || overviewRes.overview.weather_impact_level || "low";
    const visibleItems = resolveVisibleVehicles(state.allVehicles?.length ? state.allVehicles : state.vehicles);
    state.vehicles = visibleItems;
    renderWeather(routeAdjustedWeather(weatherRes.items, state.route));
    state.liveEdgeSummary = result.live_edge_summary || overviewRes.live_edge_summary || state.liveEdgeSummary;
    state.networkInsights = graphRes.network_insights || state.networkInsights;
    state.insightsHistory = historyRes || state.insightsHistory;
    renderOverview(overviewRes.overview, state.liveEdgeSummary);
    renderDataSources(overviewRes.vehicle_source, overviewRes.weather_source);
    renderFleetFreshness(overviewRes.overview.latest_event_time);
    renderVehicleTable(visibleItems);
    updateMarkers(visibleItems);
    hydrateVehicleSelect(visibleItems);
    applyVehicleFocus();
    renderAllRoutesTable(state.graph, state.route);
    renderNetworkMap(state.graph, state.route);
    renderRouteSummary(state.route);
    renderNetworkInsights(state.networkInsights);
    renderInsightsHistory(state.insightsHistory);
    setRouteStatus(
      `Ruta actualizada (${profile}) ${new Date().toLocaleTimeString("es-ES", { hour12: false })}`,
      "ok"
    );
  } catch (err) {
    state.route = null;
    const visibleItems = resolveVisibleVehicles(state.allVehicles?.length ? state.allVehicles : state.vehicles);
    state.vehicles = visibleItems;
    renderVehicleTable(visibleItems);
    updateMarkers(visibleItems);
    hydrateVehicleSelect(visibleItems);
    applyVehicleFocus();
    renderAllRoutesTable(state.graph, null);
    renderNetworkMap(state.graph, null);
    const tbody = document.querySelector("#route-table tbody");
    if (tbody) tbody.innerHTML = `<tr><td>Error</td><td>No se pudo calcular la ruta: ${err.message}</td></tr>`;
    setRouteStatus(`Error calculando ruta: ${err.message}`, "error");
  } finally {
    state.routeRequestInFlight = false;
    if (routeBtn) {
      routeBtn.disabled = false;
      routeBtn.textContent = previousLabel || "Calcular mejor ruta";
    }
    if (state.routeRecalcPending) {
      state.routeRecalcPending = false;
      calculateRoute();
    }
  }
}

function bindEvents() {
  // Registro centralizado de eventos UI y desacople de vistas.
  // - source/target (logistica) -> calculateRoute
  // - rt-source/rt-target (tiempo real) -> syncRealtimeViewFromFilters
  document.getElementById("refresh-btn").addEventListener("click", refreshFleet);
  document.getElementById("vehicle-select").addEventListener("change", async () => {
    state.selectedVehicle = document.getElementById("vehicle-select").value || "";
    await refreshVehicleHistory();
  });
  document.getElementById("route-btn").addEventListener("click", calculateRoute);
  document.getElementById("source-select").addEventListener("change", calculateRoute);
  document.getElementById("target-select").addEventListener("change", calculateRoute);
  document.getElementById("profile-select").addEventListener("change", () => {
    const profile = document.getElementById("profile-select").value || "balanced";
    state.insightsProfile = profile;
    const insightsProfileSelect = document.getElementById("insights-profile-select");
    if (insightsProfileSelect) insightsProfileSelect.value = profile;
    calculateRoute();
  });
  document.getElementById("insights-profile-select").addEventListener("change", async () => {
    state.insightsProfile = document.getElementById("insights-profile-select").value || "balanced";
    await refreshNetworkInsightsOnly();
  });
  document.getElementById("insights-congestion-select").addEventListener("change", async () => {
    state.insightsMinCongestion = document.getElementById("insights-congestion-select").value || "all";
    await refreshNetworkInsightsOnly();
  });
  document.getElementById("rt-source-select").addEventListener("change", syncRealtimeViewFromFilters);
  document.getElementById("rt-target-select").addEventListener("change", syncRealtimeViewFromFilters);
  document.getElementById("play-btn").addEventListener("click", () => {
    state.playback = !state.playback;
    document.getElementById("play-btn").textContent = state.playback ? "Pausar animacion" : "Reanudar animacion";
  });
  document.getElementById("theme-btn").addEventListener("click", () => {
    applyTheme(state.theme === "dark" ? "light" : "dark");
    applyAllTableSortStates();
  });
}

async function bootstrap() {
  // Secuencia de inicializacion de la aplicacion.
  initMap();
  initNetworkMap();
  initTheme();
  state.insightsProfile = document.getElementById("insights-profile-select")?.value || "balanced";
  state.insightsMinCongestion = document.getElementById("insights-congestion-select")?.value || "all";
  const routeProfile = document.getElementById("profile-select");
  if (routeProfile) routeProfile.value = state.insightsProfile;
  bindSortableTables();
  bindEvents();
  await Promise.all([refreshFleet(), loadGraph()]);
  await calculateRoute();

  if (state.animationTimer) clearInterval(state.animationTimer);
  state.animationTimer = setInterval(async () => {
    await refreshFleet();
    if (state.playback) await refreshVehicleHistory();
  }, 12000);
}

bootstrap().catch((err) => {
  console.error(err);
  const tbody = document.querySelector("#route-table tbody");
  if (tbody) tbody.innerHTML = `<tr><td>Error</td><td>Error inicializando dashboard: ${err.message}</td></tr>`;
});
