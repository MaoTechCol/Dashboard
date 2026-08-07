import "chart.js/auto";

import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import type { ChartOptions } from "chart.js";
import {
  AlertTriangle,
  Brain,
  Building2,
  CameraOff,
  CarFront,
  Cigarette,
  Coffee,
  Download,
  EyeOff,
  Gauge,
  LogOut,
  MoonStar,
  RefreshCw,
  Shield,
  Smartphone,
  Upload,
  UserCircle2,
} from "lucide-react";
import { memo, startTransition, useCallback, useEffect, useState } from "react";
import type { CSSProperties, FormEvent, ReactNode } from "react";
import { Bar, Doughnut, Line } from "react-chartjs-2";

import { ApiError, FEED_REFRESH_MS, apiFetch, apiJson, buildApiUrl } from "./api";
import { useDashboardStream } from "./hooks/useDashboardStream";
import { useFeedStatus } from "./hooks/useFeedStatus";
import { buildTimeline, formatCategory } from "./lib/alerts";
import type {
  AdminAudit,
  AdminIngestionStatus,
  AdminLiveSetup,
  AdminOverview,
  AdminVehicle,
  AuthMeResponse,
  CompanySummary,
  DashboardSnapshot,
  FeedState,
  IngestionAnomaly,
  MockDataPurgeResult,
  RecentEvent,
  ReportFile,
  TimelineFilter,
  VehicleTableRow,
} from "./types";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(relativeTime);

type DashboardTab = "24h" | "semana" | "mes" | "vehiculos" | "patrones" | "informes";
type PortalModule = "dashboard" | "reportes" | "administracion" | "auditoria";

const DASHBOARD_TABS: Array<{ id: DashboardTab; label: string }> = [
  { id: "24h", label: "Últimas 24h" },
  { id: "semana", label: "Semana" },
  { id: "mes", label: "Mes (30 días)" },
  { id: "vehiculos", label: "Comparativa Vehículos" },
  { id: "patrones", label: "Patrones" },
  { id: "informes", label: "Informes mensuales" },
];

const CLIENT_MODULES: Array<{ id: PortalModule; label: string }> = [
  { id: "dashboard", label: "Dashboard cliente" },
];

const ADMIN_MODULES: Array<{ id: PortalModule; label: string }> = [
  { id: "administracion", label: "Administracion" },
  { id: "auditoria", label: "Diagnostico" },
  { id: "dashboard", label: "Dashboard cliente" },
  { id: "reportes", label: "Reportes" },
];

const FILTERS: Array<{ id: TimelineFilter; label: string }> = [
  { id: "todas", label: "Todas" },
  { id: "critico", label: "Critico" },
  { id: "alto", label: "Alto" },
  { id: "noche", label: "Noche" },
];

const CATEGORY_COLORS: Record<string, string> = {
  "Uso de celular": "#ef4444",
  "Fatiga en progresion": "#f97316",
  "Ojos cerrados": "#fb7185",
  "Riesgo de colision": "#38bdf8",
  Bostezo: "#f59e0b",
  "Camara cubierta": "#94a3b8",
  Fumando: "#a855f7",
  Distraccion: "#10b981",
};

const GRID_COLOR = "rgba(138, 144, 168, 0.18)";
const TICK_COLOR = "#8a90a8";

const STACKED_BAR_OPTIONS: ChartOptions<"bar"> = {
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  plugins: {
    legend: {
      position: "bottom",
      labels: { color: "#eef2ff", boxWidth: 14, boxHeight: 14 },
    },
  },
  scales: {
    x: { stacked: true, ticks: { color: TICK_COLOR }, grid: { color: GRID_COLOR } },
    y: { stacked: true, ticks: { color: TICK_COLOR }, grid: { color: GRID_COLOR } },
  },
};

const HORIZONTAL_BAR_OPTIONS: ChartOptions<"bar"> = {
  ...STACKED_BAR_OPTIONS,
  indexAxis: "y",
};

const LINE_OPTIONS: ChartOptions<"line"> = {
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  interaction: { mode: "index", intersect: false },
  plugins: {
    legend: {
      position: "bottom",
      labels: { color: "#eef2ff", boxWidth: 14, boxHeight: 14 },
    },
  },
  scales: {
    x: { ticks: { color: TICK_COLOR }, grid: { color: GRID_COLOR } },
    y: { ticks: { color: TICK_COLOR }, grid: { color: GRID_COLOR } },
  },
};

const DOUGHNUT_OPTIONS: ChartOptions<"doughnut"> = {
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  plugins: {
    legend: {
      position: "bottom",
      labels: { color: "#eef2ff", boxWidth: 12, boxHeight: 12 },
    },
  },
};

function App() {
  const [session, setSession] = useState<AuthMeResponse | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [authError, setAuthError] = useState<string | null>(null);
  const [loginBusy, setLoginBusy] = useState(false);
  const [loginForm, setLoginForm] = useState({ username: "admin", password: "Admin123!" });
  const [selectedCompany, setSelectedCompany] = useState<string | null>(null);
  const [activeModule, setActiveModule] = useState<PortalModule>("dashboard");
  const [activeTab, setActiveTab] = useState<DashboardTab>("24h");
  const [timelineFilter, setTimelineFilter] = useState<TimelineFilter>(() => {
    if (typeof window === "undefined") return "todas";
    const saved = window.localStorage.getItem("dms.timeline.filter");
    return saved === "critico" || saved === "alto" || saved === "noche" ? saved : "todas";
  });

  const applySession = useCallback((payload: AuthMeResponse, currentCompany: string | null = null) => {
    setSession(payload);
    setSelectedCompany(pickCompanySlug(payload, currentCompany));
    setActiveModule(defaultModuleForRole(payload.user.role));
    setAuthError(null);
  }, []);

  useEffect(() => {
    let cancelled = false;

    const loadSession = async () => {
      try {
        const payload = await apiJson<AuthMeResponse>("/auth/me");
        if (!cancelled) {
          applySession(payload);
        }
      } catch (error) {
        if (cancelled) return;
        if (error instanceof ApiError && error.status === 401) {
          setSession(null);
          setSelectedCompany(null);
          setAuthError(null);
        } else {
          setAuthError(error instanceof Error ? error.message : "No se pudo validar la sesion");
        }
      } finally {
        if (!cancelled) {
          setAuthLoading(false);
        }
      }
    };

    void loadSession();

    return () => {
      cancelled = true;
    };
  }, [applySession]);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("dms.timeline.filter", timelineFilter);
    }
  }, [timelineFilter]);

  const activeCompany = session?.companies.find((company) => company.slug === selectedCompany) ?? session?.companies[0] ?? null;

  useEffect(() => {
    if (!activeCompany) return;
    const root = document.documentElement;
    root.style.setProperty("--accent", activeCompany.brand.accent);
    root.style.setProperty("--warning", activeCompany.brand.warning);
    root.style.setProperty("--danger", activeCompany.brand.danger);
    root.style.setProperty("--muted", activeCompany.brand.muted);
  }, [activeCompany]);

  const dashboard = useDashboardStream(selectedCompany);
  const feed = useFeedStatus(selectedCompany, dashboard.snapshot?.feed.last_cycle_received_at ?? null);
  const isAdmin = session?.user.role === "admin";

  const effectiveFeed = feed.payload
    ? {
        status: feed.payload.feed_status,
        label: feed.payload.feed_label,
        connection_state: feed.payload.connection_state,
        last_cycle_received_at: feed.payload.last_cycle_received_at,
        last_event_observed_at: feed.payload.last_event_observed_at,
        last_error: feed.payload.last_error,
      }
    : dashboard.snapshot?.feed ?? null;

  const feedCycleAt = feed.payload?.last_cycle_received_at ?? null;
  const snapshotCycleAt = dashboard.snapshot?.feed.last_cycle_received_at ?? null;
  const ingestionCycleMinutes = dashboard.snapshot?.rules.ingestion_cycle_minutes ?? 15;
  const feedCycleBucket = floorCycleBucket(feedCycleAt, ingestionCycleMinutes);
  const snapshotCycleBucket = floorCycleBucket(snapshotCycleAt, ingestionCycleMinutes);
  const newCycleAvailable = Boolean(
    feed.payload?.new_cycle_available &&
      feedCycleBucket &&
      (!snapshotCycleBucket || feedCycleBucket > snapshotCycleBucket),
  );
  const modules = isAdmin ? ADMIN_MODULES : CLIENT_MODULES;
  const showFeedDiagnosticBanner = Boolean(effectiveFeed?.last_error && effectiveFeed.status !== "al_dia");

  const handleLogin = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setLoginBusy(true);
    setAuthError(null);
    try {
      const payload = await apiJson<AuthMeResponse>("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(loginForm),
      });
      applySession(payload);
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : "No se pudo iniciar sesion");
    } finally {
      setLoginBusy(false);
    }
  };

  const handleLogout = async () => {
    await apiJson("/auth/logout", { method: "POST" });
    setSession(null);
    setSelectedCompany(null);
    setActiveModule("dashboard");
  };

  if (authLoading) {
    return (
      <div className="shell centered">
        <div className="empty-card">
          <div className="empty-title">Validando sesion local</div>
          <div className="empty-copy">Conectando el portal del dashboard con la API local.</div>
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <LoginScreen
        authError={authError}
        busy={loginBusy}
        form={loginForm}
        onChange={(field, value) => setLoginForm((current) => ({ ...current, [field]: value }))}
        onSubmit={handleLogin}
      />
    );
  }

  const headerTitle = isAdmin ? "Consola local DMS" : activeCompany?.brand.title ?? "Panel de seguridad de flota";
  const headerSubtitle = isAdmin
    ? "Operacion live, reportes y auditoria antes del despliegue"
    : dashboard.snapshot
      ? formatDashboardHeaderSummary(dashboard.snapshot)
      : activeCompany?.brand.subtitle ?? "Monitoreo local";

  return (
    <div className="shell">
      <header className="hero">
        <div>
          <div className="eyebrow">{isAdmin ? "Portal Operativo DMS" : activeCompany?.brand.eyebrow ?? "Monitoreo de Conduccion · DMS"}</div>
          <h1>{headerTitle}</h1>
          <p className="subhead">{headerSubtitle}</p>
        </div>

        <div className="hero-actions">
          {isAdmin ? (
            <>
              <div className="toolbar">
                <select
                  value={selectedCompany ?? ""}
                  onChange={(event) =>
                    startTransition(() => {
                      setSelectedCompany(event.target.value || null);
                    })
                  }
                >
                  {session.companies.map((company) => (
                    <option key={company.slug} value={company.slug}>
                      {company.name}
                    </option>
                  ))}
                </select>

                <button className="ghost-btn" type="button" onClick={dashboard.refresh}>
                  <RefreshCw size={16} />
                  Refrescar snapshot
                </button>

                <button className="ghost-btn" type="button" onClick={handleLogout}>
                  <LogOut size={16} />
                  Salir
                </button>
              </div>

              {modules.length > 1 ? (
                <div className="toolbar">
                  {modules.map((module) => (
                    <button
                      key={module.id}
                      className={`tab ${activeModule === module.id ? "active" : ""}`}
                      type="button"
                      onClick={() =>
                        startTransition(() => {
                          setActiveModule(module.id);
                        })
                      }
                    >
                      {module.label}
                    </button>
                  ))}
                </div>
              ) : null}

              <div className="toolbar">
                <div className="feed-pill">
                  <Gauge size={16} />
                  {(dashboard.snapshot?.meta.ingestMode ?? "live").toUpperCase()}
                </div>
                <div className={`feed-pill ${effectiveFeed?.status ?? ""}`}>
                  <span className="feed-dot" />
                  {effectiveFeed?.label ?? "sin estado"}
                </div>
                <div className="feed-pill">
                  <UserCircle2 size={16} />
                  {session.user.username}
                </div>
              </div>
            </>
          ) : (
            <div className="toolbar">
              <div className={`feed-pill ${effectiveFeed?.status ?? ""}`}>
                <span className="feed-dot" />
                {effectiveFeed?.label ?? "sin estado"}
              </div>
              <button className="ghost-btn" type="button" onClick={dashboard.refresh}>
                <RefreshCw size={16} />
                Refrescar
              </button>
              <button className="ghost-btn" type="button" onClick={handleLogout}>
                <LogOut size={16} />
                Salir
              </button>
            </div>
          )}
        </div>
      </header>

      {isAdmin && feed.error ? <div className="banner error">Estado del feed no disponible: {feed.error}</div> : null}
      {isAdmin && showFeedDiagnosticBanner ? <div className="banner error">Diagnostico de ingesta: {effectiveFeed?.last_error}</div> : null}
      {isAdmin && newCycleAvailable ? (
        <div className="banner success">
          Ya entro un corte live mas nuevo que el snapshot visible. El dashboard lo tomara en su siguiente refresh de 15 minutos, o puedes refrescarlo ahora.
        </div>
      ) : null}

      {activeModule === "dashboard" && selectedCompany ? (
        <>
          <nav className="tabs">
            {DASHBOARD_TABS.map((tab) => (
              <button
                key={tab.id}
                className={`tab ${activeTab === tab.id ? "active" : ""}`}
                type="button"
                onClick={() => startTransition(() => setActiveTab(tab.id))}
              >
                {tab.label}
              </button>
            ))}
          </nav>

          <MemoizedDashboardBody
            activeTab={activeTab}
            clientView={activeModule === "dashboard"}
            company={activeCompany}
            error={dashboard.error}
            loading={dashboard.loading}
            setTimelineFilter={setTimelineFilter}
            snapshot={dashboard.snapshot}
            timelineFilter={timelineFilter}
          />
        </>
      ) : null}

      {activeModule === "reportes" && selectedCompany ? (
        <ReportsModule
          company={activeCompany}
          companySlug={selectedCompany}
          error={dashboard.error}
          loading={dashboard.loading}
          snapshot={dashboard.snapshot}
        />
      ) : null}

      {activeModule === "administracion" && session.user.role === "admin" && selectedCompany ? (
        <AdminOperationsModule
          company={activeCompany}
          enabled={activeModule === "administracion"}
          onRefreshDashboard={dashboard.refresh}
          selectedCompany={selectedCompany}
        />
      ) : null}

      {activeModule === "auditoria" && session.user.role === "admin" && selectedCompany ? (
        <AdminAuditModule
          company={activeCompany}
          enabled={activeModule === "auditoria"}
          selectedCompany={selectedCompany}
        />
      ) : null}
    </div>
  );
}

interface LoginScreenProps {
  authError: string | null;
  busy: boolean;
  form: { username: string; password: string };
  onChange: (field: "username" | "password", value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => Promise<void>;
}

function LoginScreen({ authError, busy, form, onChange, onSubmit }: LoginScreenProps) {
  return (
    <div className="shell centered auth-screen">
      <div className="auth-card">
        <div className="eyebrow">Portal Local DMS</div>
        <h1>Acceso autenticado</h1>
        <p className="subhead">
          Login real en FastAPI con sesion por cookie. Cliente y administracion comparten la misma arquitectura local.
        </p>

        <form className="auth-form" onSubmit={onSubmit}>
          <label>
            Usuario
            <input value={form.username} onChange={(event) => onChange("username", event.target.value)} />
          </label>
          <label>
            Contrasena
            <input
              type="password"
              value={form.password}
              onChange={(event) => onChange("password", event.target.value)}
            />
          </label>
          <button className="primary-btn" type="submit" disabled={busy}>
            {busy ? "Entrando..." : "Ingresar"}
          </button>
        </form>

        {authError ? <div className="banner error">{authError}</div> : null}

        <div className="auth-hints">
          <div className="panel compact">
            <div className="panel-kicker">Semillas locales</div>
            <strong>Admin:</strong> `admin / Admin123!`
            <br />
            <strong>Cliente:</strong> `ismocol / Cliente123!`
          </div>
        </div>
      </div>
    </div>
  );
}

interface DashboardBodyProps {
  activeTab: DashboardTab;
  clientView: boolean;
  company: CompanySummary | null;
  error: string | null;
  loading: boolean;
  setTimelineFilter: (value: TimelineFilter) => void;
  snapshot: DashboardSnapshot | null;
  timelineFilter: TimelineFilter;
}

const MemoizedDashboardBody = memo(function DashboardBody({
  activeTab,
  clientView,
  company,
  error,
  loading,
  setTimelineFilter,
  snapshot,
  timelineFilter,
}: DashboardBodyProps) {
  if (loading && !snapshot) {
    return (
      <div className="page-grid">
        <div className="empty-card">
          <div className="empty-title">Cargando snapshot operativo</div>
          <div className="empty-copy">Estamos armando el dashboard local con el ultimo corte disponible.</div>
        </div>
      </div>
    );
  }

  if (error && !snapshot) {
    return (
      <div className="page-grid">
        <div className="empty-card">
          <div className="empty-title">No se pudo abrir el dashboard</div>
          <div className="empty-copy">{error}</div>
        </div>
      </div>
    );
  }

  if (!snapshot || !company) {
    return null;
  }

  const visibleNotes = filterNotesForTab(snapshot.dataQuality.active_notes, activeTab, snapshot.meta.rangeEnd);

  return (
    <main className="page-grid">
      {!clientView ? (
        <section className="metric-grid four">
          <MetricCard label="Vehiculos visibles" value={String(snapshot.meta.vehicleCount)} detail={company.customer} />
          <MetricCard label="KM ventana cerrada" value={formatKm(snapshot.meta.kmTotalClosedWindow)} detail={snapshot.dms.kpis.rango} />
          <MetricCard
            label="Hoy provisional"
            tone="amber"
            value={formatKm(snapshot.meta.currentDayKmProvisional)}
            detail={`Actualiza cada ${snapshot.rules.ingestion_cycle_minutes} min`}
          />
          <MetricCard
            label="Snapshot live"
            value={formatDateTime(snapshot.meta.generatedAt, snapshot.meta.timezone)}
            detail={`Ciclo: ${snapshot.feed.last_cycle_received_at ? formatDateTime(snapshot.feed.last_cycle_received_at, snapshot.meta.timezone) : "sin ciclo"} · Evento: ${
              snapshot.feed.last_event_observed_at ? formatDateTime(snapshot.feed.last_event_observed_at, snapshot.meta.timezone) : "sin evento"
            } · Conexion: ${snapshot.feed.connection_state}`}
          />
        </section>
      ) : null}

      {!clientView && (visibleNotes.length > 0 || snapshot.dataQuality.anomaly_count_24h > 0) ? (
        <section className="stack">
          {visibleNotes.map((note) => (
            <div key={`${note.title}-${note.start_date}`} className={`banner ${note.severity === "critical" ? "error" : ""}`}>
              <strong>{note.title}.</strong> {note.message}
            </div>
          ))}
          {snapshot.dataQuality.anomaly_count_24h > 0 ? (
            <div className="banner error">
              Se detectaron {snapshot.dataQuality.anomaly_count_24h} anomalias de ingesta en las ultimas 24 horas.
            </div>
          ) : null}
        </section>
      ) : null}

      {activeTab === "24h" ? (
        <Last24Tab
          snapshot={snapshot}
          timelineFilter={timelineFilter}
          onTimelineFilterChange={setTimelineFilter}
        />
      ) : null}

      {activeTab === "semana" ? <WeekTab snapshot={snapshot} /> : null}
      {activeTab === "mes" ? <MonthTab snapshot={snapshot} /> : null}
      {activeTab === "vehiculos" ? <VehiclesTab snapshot={snapshot} /> : null}
      {activeTab === "patrones" ? <PatternsTab snapshot={snapshot} /> : null}
      {activeTab === "informes" ? <ReportsTab companySlug={company.slug} reports={snapshot.reports} /> : null}
    </main>
  );
});

interface Last24TabProps {
  onTimelineFilterChange: (value: TimelineFilter) => void;
  snapshot: DashboardSnapshot;
  timelineFilter: TimelineFilter;
}

function Last24Tab({ onTimelineFilterChange, snapshot, timelineFilter }: Last24TabProps) {
  const timeline = buildTimeline(snapshot, timelineFilter);
  const topVehicles = snapshot.dms.ultimo.por_vehiculo;
  const activeVehicles24h = new Set(snapshot.recentEvents.map((event) => event.plate || event.deviceId)).size;
  const nightEventCount = snapshot.recentEvents.filter((event) =>
    isNightHour(
      dayjs(event.occurredAt).tz(snapshot.meta.timezone).hour(),
      snapshot.rules.night_window_start,
      snapshot.rules.night_window_end,
    ),
  ).length;
  const criticalWindow = pickDominantCriticalWindow(snapshot.recentEvents, snapshot.meta.timezone);
  const maxVehicleTotal = Math.max(...topVehicles.map((vehicle) => vehicle.total), 1);

  return (
    <section className="layout-two">
      <div className="stack">
        <div className="timeline-header">
          <div className="filter-row">
            <span className="filter-label">Filtrar lista</span>
            {FILTERS.map((filter) => (
              <button
                key={filter.id}
                className={`filter-chip ${timelineFilter === filter.id ? "active" : ""}`}
                type="button"
                onClick={() => startTransition(() => onTimelineFilterChange(filter.id))}
              >
                {filter.label} · {timeline.counts[filter.id]}
              </button>
            ))}
            <div className="filter-summary">{timeline.summary}</div>
          </div>
        </div>

        {timeline.visible.length === 0 ? (
          <div className="empty-card">
            <div className="empty-title">No hay alertas visibles para este filtro</div>
            <div className="empty-copy">{timeline.emptyHint}</div>
          </div>
        ) : (
          <div className="panel">
            <div className="panel-head">
              <div className="panel-dot" />
              <strong>Ahora {formatClockTime(snapshot.meta.generatedAt, snapshot.meta.timezone)}</strong>
              <span className="muted">hacia atras 24 h</span>
            </div>

            {timeline.dayAlerts.length > 0 ? (
              <div>
                {timeline.dayAlerts.map((alert) => (
                  <AlertCard key={alert.id} alert={alert} />
                ))}
              </div>
            ) : null}

            {timeline.nightAlerts.length > 0 ? (
              <>
                <div className="night-divider">
                  <MoonStar size={16} />
                  Tramo nocturno
                </div>
                <div>
                  {timeline.nightAlerts.map((alert) => (
                    <AlertCard key={alert.id} alert={alert} />
                  ))}
                </div>
              </>
            ) : null}
          </div>
        )}
      </div>

      <aside className="stack">
        <div className="panel summary-hero-card">
          <div className="panel-kicker">Comportamiento 24 h · {formatDayBadge(snapshot.meta.generatedAt, snapshot.meta.timezone)}</div>
          <div className="summary-hero-line">
            <div className="panel-metric">{snapshot.dms.ultimo.total}</div>
            <div className={`delta-badge ${snapshot.dms.ultimo.delta_pct !== null && snapshot.dms.ultimo.delta_pct > 0 ? "up" : ""}`}>
              {formatDeltaBadge(snapshot.dms.ultimo.delta_pct)}
            </div>
          </div>
          <div className="panel-copy">
            alertas · promedio 30 d: {formatNumber(snapshot.dms.ultimo.baseline_promedio)} · {activeVehicles24h} vehiculos
          </div>
          <div className="progress-list summary-breakdown">
            {snapshot.dms.cat_order.map((category) => {
              const value = snapshot.dms.ultimo.por_cat[category] ?? 0;
              const total = snapshot.dms.ultimo.total || 1;
              return (
                <div key={category} className="progress-row">
                  <div className="progress-label">{formatCategory(category)}</div>
                  <div className="progress-track">
                    <div
                      className="progress-fill"
                      style={{
                        width: `${Math.max((value / total) * 100, value > 0 ? 8 : 0)}%`,
                        background: CATEGORY_COLORS[category] ?? "var(--accent)",
                      }}
                    />
                  </div>
                  <div className="progress-value">{value}</div>
                </div>
              );
            })}
          </div>
          <div className="tail-note">
            {nightEventCount} alarmas en franja nocturna.
            {criticalWindow ? ` El ${formatNumber(criticalWindow.sharePct)}% de los eventos criticos cayo entre ${criticalWindow.label}.` : ""}
          </div>
        </div>

        <div className="panel vehicle-day-card">
          <div className="panel-kicker">Vehiculos del dia</div>
          <div className="vehicle-day-list">
            {topVehicles.length === 0 ? (
              <div className="empty-copy">Aun no hay vehiculos destacados en la jornada visible.</div>
            ) : (
              topVehicles.map((vehicle) => {
                const multiple = vehicle.baseline > 0 ? vehicle.total / vehicle.baseline : null;
                return (
                  <div key={vehicle.placa} className="vehicle-day-row">
                    <div className="vehicle-day-head">
                      <strong>{vehicle.placa}</strong>
                      <span className={`vehicle-day-metric ${vehicle.spike ? "spike" : ""}`}>
                        {vehicle.total} {multiple ? `${formatNumber(multiple)}x` : ""}
                        {vehicle.spike ? " ▲" : ""}
                      </span>
                    </div>
                    <div className="progress-track vehicle-day-track">
                      <div
                        className="progress-fill"
                        style={{
                          width: `${Math.max((vehicle.total / maxVehicleTotal) * 100, vehicle.total > 0 ? 12 : 0)}%`,
                          background: vehicle.spike ? "#ef4444" : "#f97316",
                        }}
                      />
                    </div>
                    <div className="panel-copy">Baseline {formatNumber(vehicle.baseline)} · {multiple ? `${formatNumber(multiple)}x` : "sin baseline"}</div>
                  </div>
                );
              })
            )}
          </div>
          <div className="tail-note">El multiplo compara contra el promedio diario del propio vehiculo. ▲ marca desvio superior a 1.5x.</div>
        </div>
      </aside>
    </section>
  );
}

function WeekTab({ snapshot }: { snapshot: DashboardSnapshot }) {
  const labels = snapshot.dms.semana.veh;
  const trendEntries = Object.entries(snapshot.dms.semana.linea_veh);
  const categoryData = {
    labels,
    datasets: snapshot.dms.cat_order.map((category) => ({
      label: formatCategory(category),
      data: snapshot.dms.semana.cat_veh[category] ?? [],
      backgroundColor: CATEGORY_COLORS[category] ?? "#10b981",
      borderRadius: 8,
    })),
  };
  const lineData = {
    labels: snapshot.dms.semana.fechas.map(formatShortDate),
    datasets: trendEntries.map(([plate, values], index) => ({
      label: plate,
      data: values,
      borderColor: vehicleColor(index),
      backgroundColor: vehicleColor(index),
      borderWidth: 2,
      pointRadius: 2,
      pointHoverRadius: 4,
      tension: 0.35,
      fill: false,
    })),
  };
  const trendOptions: ChartOptions<"line"> = {
    ...LINE_OPTIONS,
    plugins: {
      ...LINE_OPTIONS.plugins,
      legend: {
        display: false,
      },
    },
  };

  return (
    <section className="stack">
      <div className="metric-grid three">
        <MetricCard label="Alarmas 7 dias" value={String(snapshot.dms.semana.total)} />
        <MetricCard label="Promedio diario" value={formatNumber(snapshot.dms.semana.total / Math.max(snapshot.dms.semana.fechas.length, 1))} />
        <MetricCard label="Vehiculos activos" value={String(snapshot.dms.semana.veh.length)} />
      </div>

      <ChartPanel title="Que alarmas y en que vehiculos - ultima semana">
        <Bar data={categoryData} options={STACKED_BAR_OPTIONS} />
      </ChartPanel>

      <ChartPanel title="Tendencia diaria por vehiculo">
        <Line data={lineData} options={trendOptions} />
      </ChartPanel>

      <ChartPanel title={`Placas incluidas en la tendencia (${trendEntries.length})`}>
        <div className="chip-row trend-chip-row">
          {trendEntries.map(([plate], index) => (
            <span
              key={plate}
              className="chip trend-chip"
              style={{
                borderColor: `${vehicleColor(index)}66`,
                backgroundColor: `${vehicleColor(index)}1a`,
              }}
            >
              <span className="trend-chip-dot" style={{ backgroundColor: vehicleColor(index) }} />
              {plate}
            </span>
          ))}
        </div>
      </ChartPanel>

      <div className="panel">
        <p className="panel-copy">
          Cada barra es un vehiculo; los colores muestran que tipo de alarma acumulo en la semana. Prioriza los vehiculos con mas rojo y rosa.
        </p>
      </div>
    </section>
  );
}

function MonthTab({ snapshot }: { snapshot: DashboardSnapshot }) {
  const categorySeries = {
    labels: snapshot.dms.fechas.map(formatShortDate),
    datasets: snapshot.dms.cat_order.map((category) => ({
      label: formatCategory(category),
      data: snapshot.dms.serie_cat[category] ?? [],
      backgroundColor: CATEGORY_COLORS[category] ?? "#10b981",
      borderRadius: 6,
      stack: "cats",
    })),
  };
  const kmSeries = {
    labels: snapshot.dms.fechas.map(formatShortDate),
    datasets: [
      {
        label: "KM diarios",
        data: snapshot.dms.km_dia,
        borderColor: "#38bdf8",
        backgroundColor: "rgba(56, 189, 248, 0.22)",
        fill: true,
        tension: 0.3,
      },
    ],
  };
  const distribution = {
    labels: snapshot.dms.dist_tipo.slice(0, 8).map((row) => row.tipo),
    datasets: [
      {
        label: "Eventos",
        data: snapshot.dms.dist_tipo.slice(0, 8).map((row) => row.n),
        backgroundColor: snapshot.dms.dist_tipo.slice(0, 8).map((row) => CATEGORY_COLORS[row.cat] ?? "#10b981"),
      },
    ],
  };
  const composition = {
    labels: snapshot.dms.cat_order.map(formatCategory),
    datasets: [
      {
        label: "Composicion por categoria",
        data: snapshot.dms.cat_order.map((category) =>
          (snapshot.dms.serie_cat[category] ?? []).reduce((sum, value) => sum + value, 0),
        ),
        backgroundColor: snapshot.dms.cat_order.map((category) => CATEGORY_COLORS[category] ?? "#10b981"),
      },
    ],
  };

  return (
    <section className="stack">
      <div className="metric-grid four">
        <MetricCard label="Alarmas totales" value={String(snapshot.dms.kpis.total)} detail={snapshot.dms.kpis.rango} />
        <MetricCard label="Eventos criticos" tone="danger" value={String(snapshot.dms.kpis.critico)} detail="Ojos cerrados + celular" />
        <MetricCard label="Eventos altos" tone="warning" value={String(snapshot.dms.kpis.alto)} detail="Bostezo + colision" />
        <MetricCard label="Eventos medios" tone="amber" value={String(snapshot.dms.kpis.medio)} detail="Fumando + distraccion + camara" />
      </div>

      <div className="metric-grid three">
        <MetricCard label="Km recorridos (flota)" value={formatNumber(snapshot.dms.kpis.km)} />
        <MetricCard label="Alarmas / 100 km" value={formatRate(snapshot.dms.kpis.por100km)} />
        <MetricCard label="Alarmas nocturnas" value={`${snapshot.dms.kpis.nocturno_pct}%`} />
      </div>

      <ChartPanel title="Evolucion diaria de alarmas por categoria">
        <Bar data={categorySeries} options={STACKED_BAR_OPTIONS} />
      </ChartPanel>

      <ChartPanel title="Km recorridos por dia (flota)">
        <Line data={kmSeries} options={LINE_OPTIONS} />
      </ChartPanel>

      <div className="double-panel">
        <ChartPanel title="Distribucion por tipo de alarma">
          <Bar data={distribution} options={HORIZONTAL_BAR_OPTIONS} />
        </ChartPanel>
        <ChartPanel title="Composicion por categoria">
          <Doughnut data={composition} options={DOUGHNUT_OPTIONS} />
        </ChartPanel>
      </div>
    </section>
  );
}

function VehiclesTab({ snapshot }: { snapshot: DashboardSnapshot }) {
  const rows = snapshot.dms.tabla;
  const chartHeight = Math.max(rows.length * 14 + 160, 360);
  const riskData = {
    labels: rows.map((row) => row.placa),
    datasets: [
      {
        label: "Riesgo 100km",
        data: rows.map((row) => row.riesgo100km ?? 0),
        backgroundColor: "#38bdf8",
        borderRadius: 8,
      },
      {
        label: "Spike vs baseline",
        data: rows.map((row) => (row.spike ? row.riesgo100km ?? 0 : null)),
        backgroundColor: "#ef4444",
        borderRadius: 8,
        barThickness: 6,
      },
    ],
  };

  return (
    <section className="stack">
      <div className="panel">
        <p className="panel-copy">
          Alarmas/100 km normaliza por distancia real recorrida y riesgo/100 km pondera por gravedad, con ojos cerrados, uso de celular y colision pesando mas.
        </p>
      </div>

      <div className="panel table-wrap">
        <h3>Ranking de vehiculos</h3>
        <table>
          <thead>
            <tr>
              <th>Placa</th>
              <th>Total</th>
              <th>KM</th>
              <th>Alertas 100km</th>
              <th>Riesgo 100km</th>
              <th>Nocturno</th>
              <th>Baseline</th>
              <th>Categorias</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <VehicleRow key={row.placa} row={row} />
            ))}
          </tbody>
        </table>
      </div>

      <ChartPanel title="Alarmas por 100 km - comparativa" frameStyle={{ height: `${chartHeight}px` }}>
        <Bar data={riskData} options={HORIZONTAL_BAR_OPTIONS} />
      </ChartPanel>

      <div className="panel">
        <p className="panel-copy">
          La longitud de cada barra muestra el riesgo por 100 km. El marcador rojo identifica vehiculos con desvio superior a 1.5x frente a su baseline diario.
        </p>
      </div>
    </section>
  );
}

function PatternsTab({ snapshot }: { snapshot: DashboardSnapshot }) {
  const fatigueProfiles = snapshot.dms.tabla
    .map((row) => ({
      row,
      total: (row.cats["Fatiga en progresion"] ?? 0) + (row.cats["Ojos cerrados"] ?? 0) + (row.cats.Bostezo ?? 0),
    }))
    .filter((entry) => entry.total >= snapshot.rules.fatigue_profile_min_alarms)
    .sort((left, right) => right.total - left.total)
    .slice(0, 5);

  const nightProfiles = snapshot.dms.tabla
    .filter((row) => row.nocturno >= snapshot.rules.night_profile_min_alarms)
    .sort((left, right) => right.nocturno - left.nocturno)
    .slice(0, 5);
  const cellphoneProfiles = snapshot.dms.tabla
    .filter((row) => (row.cats["Uso de celular"] ?? 0) > 0)
    .sort((left, right) => (right.cats["Uso de celular"] ?? 0) - (left.cats["Uso de celular"] ?? 0))
    .slice(0, 5);
  const fatigueVehicleCount = snapshot.dms.tabla.filter((row) => {
    const fatigueTotal = (row.cats["Fatiga en progresion"] ?? 0) + (row.cats["Ojos cerrados"] ?? 0) + (row.cats.Bostezo ?? 0);
    return row.total >= snapshot.rules.fatigue_profile_min_alarms && fatigueTotal / Math.max(row.total, 1) >= 0.7;
  }).length;
  const cellphoneVehicleCount = snapshot.dms.tabla.filter((row) => (row.cats["Uso de celular"] ?? 0) > 0).length;
  const hourlyData = {
    labels: Array.from({ length: 24 }, (_, hour) => `${hour}h`),
    datasets: snapshot.dms.cat_order.map((category) => ({
      label: formatCategory(category),
      data: snapshot.dms.heat[category] ?? Array(24).fill(0),
      backgroundColor: CATEGORY_COLORS[category] ?? "#10b981",
      borderRadius: 8,
    })),
  };

  return (
    <section className="stack">
      <ChartPanel title="Patron horario de la flota">
        <Bar data={hourlyData} options={STACKED_BAR_OPTIONS} />
      </ChartPanel>

      <div className="patterns-heading">
        <h3>Huella de conducta por vehiculo</h3>
        <p className="panel-copy">La mezcla de categorias dice que hacer mejor que el total.</p>
      </div>

      <div className="triple-panel">
        <ProfileInsightCard
          accent="#ef4444"
          action="Accion: revisar jornada, turno y descansos."
          summary={`${fatigueVehicleCount} vehiculos`}
          subtitle="Mas del 70% de sus alarmas son ojos cerrados o bostezo."
          title="Perfil fatiga"
          rows={fatigueProfiles.map((entry) => ({
            label: entry.row.placa,
            metric: `${entry.total} de ${entry.row.total}`,
            value: entry.total,
          }))}
          emptyText="Aun no hay vehiculos que superen el minimo configurado para fatiga."
        />
        <ProfileInsightCard
          accent="#ec4899"
          action="Accion: es conducta, no sensor. Campana de flota."
          summary={`${cellphoneVehicleCount} de ${snapshot.meta.vehicleCount}`}
          subtitle="Vehiculos con al menos un uso de celular en 30 dias. No es asunto de unos pocos."
          title="Perfil celular"
          rows={cellphoneProfiles.map((row) => {
            const cellphoneCount = row.cats["Uso de celular"] ?? 0;
            return {
              label: row.placa,
              metric: `${cellphoneCount} · ${formatRate(rateByKm(cellphoneCount, row.km))}/100km`,
              value: cellphoneCount,
            };
          })}
          emptyText="Aun no hay vehiculos con uso de celular visible en 30 dias."
        />
        <ProfileInsightCard
          accent="#818cf8"
          action="Accion: contrastar con la programacion de turnos."
          summary={`${snapshot.dms.kpis.nocturno_pct}% flota`}
          subtitle="Media de alarmas nocturnas, pero muy desigual entre vehiculos."
          title="Perfil nocturno"
          rows={nightProfiles.map((row) => ({
            label: row.placa,
            metric: `${formatNumber((row.nocturno / Math.max(row.total, 1)) * 100)}% · ${row.nocturno}`,
            value: row.nocturno,
          }))}
          emptyText="Aun no hay vehiculos que entren al perfil nocturno."
        />
      </div>
    </section>
  );
}

function ReportsModule({
  company,
  companySlug,
  error,
  loading,
  snapshot,
}: {
  company: CompanySummary | null;
  companySlug: string;
  error: string | null;
  loading: boolean;
  snapshot: DashboardSnapshot | null;
}) {
  if (loading && !snapshot) {
    return (
      <main className="page-grid">
        <div className="empty-card">
          <div className="empty-title">Cargando reportes protegidos</div>
          <div className="empty-copy">Estamos consultando los PDFs cerrados disponibles para la empresa seleccionada.</div>
        </div>
      </main>
    );
  }

  if (error && !snapshot) {
    return (
      <main className="page-grid">
        <div className="empty-card">
          <div className="empty-title">No se pudieron cargar los reportes</div>
          <div className="empty-copy">{error}</div>
        </div>
      </main>
    );
  }

  if (!snapshot || !company) {
    return null;
  }

  return (
    <main className="page-grid">
      <section className="metric-grid three">
        <MetricCard label="Empresa visible" value={company.name} detail={company.customer} />
        <MetricCard label="Reportes disponibles" value={String(snapshot.reports.length)} detail="Meses cerrados y protegidos por sesion" />
        <MetricCard
          label="Ultimo snapshot live"
          value={formatDateTime(snapshot.meta.generatedAt, snapshot.meta.timezone)}
          detail={`Hoy provisional: ${formatKm(snapshot.meta.currentDayKmProvisional)}`}
        />
      </section>

      <ReportsTab companySlug={companySlug} reports={snapshot.reports} />
    </main>
  );
}

function ReportsTab({ companySlug, reports }: { companySlug: string; reports: ReportFile[] }) {
  const grouped = groupReports(reports);

  return (
    <section className="stack">
      <div className="panel">
        <h3>Informes mensuales cerrados</h3>
        <p className="panel-copy">
          Solo se muestran meses cerrados. El mes en curso no aparece y los meses sin archivo simplemente se omiten.
        </p>
      </div>

      {grouped.length === 0 ? (
        <div className="empty-card">
          <div className="empty-title">Aun no hay informes cargados</div>
          <div className="empty-copy">La carga de PDFs se hace desde Administracion y la descarga queda protegida por la sesion.</div>
        </div>
      ) : (
        grouped.map(([year, yearReports]) => (
          <div key={year} className="report-year">
            <div className="report-year-title">{year}</div>
            <div className="report-grid">
              {yearReports.map((report) => (
                <a
                  key={`${report.year}-${report.month}`}
                  className="report-card"
                  href={buildApiUrl(`${report.download_url}?company=${encodeURIComponent(companySlug)}`)}
                >
                  <div className="report-month">{formatReportMonth(report.year, report.month)}</div>
                  <div className="report-meta">
                    {humanBytes(report.size_bytes)} · subido {dayjs(report.uploaded_at).fromNow()}
                  </div>
                  <div className="report-meta" style={{ marginTop: "0.85rem" }}>
                    <Download size={14} style={{ verticalAlign: "middle", marginRight: "0.35rem" }} />
                    Descargar PDF
                  </div>
                </a>
              ))}
            </div>
          </div>
        ))
      )}
    </section>
  );
}

interface AdminOperationsModuleProps {
  company: CompanySummary | null;
  enabled: boolean;
  onRefreshDashboard: () => void;
  selectedCompany: string;
}

function AdminOperationsModule({ company, enabled, onRefreshDashboard, selectedCompany }: AdminOperationsModuleProps) {
  const [status, setStatus] = useState<AdminIngestionStatus | null>(null);
  const [overview, setOverview] = useState<AdminOverview | null>(null);
  const [liveSetup, setLiveSetup] = useState<AdminLiveSetup | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [adminToken, setAdminToken] = useState("");
  const [selectedFleetIds, setSelectedFleetIds] = useState<string[]>([]);
  const [selectionTouched, setSelectionTouched] = useState(false);
  const [reportForm, setReportForm] = useState({
    year: String(dayjs().subtract(1, "month").year()),
    month: String(dayjs().subtract(1, "month").month() + 1),
    file: null as File | null,
  });
  const [backfillForm, setBackfillForm] = useState({
    device_id: "",
    start_at: `${dayjs().subtract(7, "day").format("YYYY-MM-DD")}T00:00`,
    end_at: `${dayjs().format("YYYY-MM-DD")}T23:59`,
  });
  const showAdminLastError = Boolean(status?.last_error && overview?.feed.status !== "al_dia");
  const hasVisibleMockData = Boolean(
    (liveSetup?.assignment.visible_mock_devices ?? 0) > 0 ||
      (liveSetup?.assignment.visible_mock_snapshots ?? 0) > 0 ||
      (liveSetup?.assignment.visible_mock_alarms ?? 0) > 0,
  );
  const hasMockContamination = Boolean(
    (liveSetup?.mock_data.devices_mock ?? 0) > 0 ||
      (liveSetup?.mock_data.snapshots_mock ?? 0) > 0 ||
      (liveSetup?.mock_data.alarms_mock ?? 0) > 0,
  );
  const assignedFleet = liveSetup?.fleet_candidates.find((fleet) => fleet.selected) ?? null;

  useEffect(() => {
    setSelectedFleetIds([]);
    setSelectionTouched(false);
  }, [selectedCompany]);

  const loadAdmin = useCallback(async () => {
    try {
      const [nextStatus, nextOverview, nextLiveSetup] = await Promise.all([
        apiJson<AdminIngestionStatus>(`/admin/ingestion/status?company=${encodeURIComponent(selectedCompany)}`),
        apiJson<AdminOverview>(`/admin/overview?company=${encodeURIComponent(selectedCompany)}`),
        apiJson<AdminLiveSetup>(`/admin/live-setup?company=${encodeURIComponent(selectedCompany)}`),
      ]);
      setStatus(nextStatus);
      setOverview(nextOverview);
      setLiveSetup(nextLiveSetup);
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo cargar administracion");
    } finally {
      setLoading(false);
    }
  }, [selectedCompany]);

  useEffect(() => {
    if (!liveSetup || selectionTouched) return;
    setSelectedFleetIds(liveSetup.assignment.fleet_ids);
  }, [liveSetup, selectionTouched]);

  useEffect(() => {
    if (!enabled) return;
    setLoading(true);
    void loadAdmin();
    const timer = window.setInterval(() => {
      void loadAdmin();
    }, FEED_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [enabled, loadAdmin]);

  const handleUpload = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!reportForm.file) {
      setError("Selecciona un PDF para cargar");
      return;
    }

    setError(null);
    setSuccess(null);
    const formData = new FormData();
    formData.append("company_slug", selectedCompany);
    formData.append("year", reportForm.year);
    formData.append("month", reportForm.month);
    formData.append("file", reportForm.file);

    const response = await apiFetch("/admin/reports", {
      method: "POST",
      body: formData,
      headers: adminToken ? { "X-Admin-Token": adminToken } : undefined,
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || `Upload fallido (${response.status})`);
    }
    setSuccess("Reporte cargado correctamente. Si el mes ya existia, fue reemplazado.");
    setReportForm((current) => ({ ...current, file: null }));
    await loadAdmin();
    onRefreshDashboard();
  };

  const handleBackfill = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    setSuccess(null);
    const payload = {
      company_slug: selectedCompany,
      device_id: backfillForm.device_id.trim() || null,
      start_at: new Date(backfillForm.start_at).toISOString(),
      end_at: new Date(backfillForm.end_at).toISOString(),
    };

    const response = await apiJson<{ inserted: number; anomalies: number; devices: number }>("/admin/backfill", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setSuccess(
      `Backfill completado: ${response.inserted} eventos insertados, ${response.anomalies} anomalias, ${response.devices} dispositivos procesados.`,
    );
    await loadAdmin();
    onRefreshDashboard();
  };

  const safeHandleUpload = async (event: FormEvent<HTMLFormElement>) => {
    try {
      await handleUpload(event);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo subir el reporte");
    }
  };

  const safeHandleBackfill = async (event: FormEvent<HTMLFormElement>) => {
    try {
      await handleBackfill(event);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo ejecutar el backfill");
    }
  };

  const applySelectedFleets = async () => {
    if (selectedFleetIds.length === 0) {
      setError("Selecciona al menos una flota real antes de aplicar la asignacion");
      return;
    }
    setError(null);
    setSuccess(null);
    await apiJson<AdminLiveSetup>("/admin/company-assignment", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        company_slug: selectedCompany,
        fleet_ids: selectedFleetIds,
        device_ids: [],
      }),
    });
    setSelectionTouched(false);
    setSuccess(`Asignacion live actualizada para ${company?.name ?? selectedCompany}.`);
    await loadAdmin();
    onRefreshDashboard();
  };

  const purgeMockLegacy = async () => {
    if (typeof window !== "undefined") {
      const confirmed = window.confirm(
        "Esto borrara de la base local los device_id DEV-* y el fleet cotaba-main. Sirve para limpiar el legado mock antes de reconciliar la flota real. Deseas continuar?",
      );
      if (!confirmed) return;
    }
    setError(null);
    setSuccess(null);
    const result = await apiJson<MockDataPurgeResult>("/admin/purge-mock", { method: "POST" });
    setSuccess(
      `Limpieza mock completada: ${result.deleted_devices} devices, ${result.deleted_snapshots} snapshots, ${result.deleted_alarms} alarmas y ${result.deleted_mileage_readings} lecturas eliminadas.`,
    );
    await loadAdmin();
    onRefreshDashboard();
  };

  return (
    <main className="page-grid">
      <section className="panel">
        <div className="panel-kicker">Administracion</div>
        <h3>Consola operativa de {company?.name ?? selectedCompany}</h3>
        <p className="panel-copy">
          Esta vista concentra estado live, cobertura, kilometraje provisional del dia, carga de PDFs y backfill manual.
        </p>
      </section>

      {error ? <div className="banner error">{error}</div> : null}
      {success ? <div className="banner success">{success}</div> : null}
      {overview?.active_notes.map((note) => (
        <div key={`${note.title}-${note.start_date}`} className={`banner ${note.severity === "critical" ? "error" : ""}`}>
          <strong>{note.title}.</strong> {note.message}
        </div>
      ))}
      {liveSetup?.assignment.points_to_mock ? (
        <div className="banner error">
          La empresa sigue asignada a datos mock. Cambia la asignacion a una o mas flotas reales y luego limpia el legado DEV-* para validar el dashboard con ISMOCOL live.
        </div>
      ) : null}

      <section className="metric-grid three">
        <MetricCard
          label="Modo de ingesta"
          value={status?.mode ?? (loading ? "..." : "sin datos")}
          detail={status?.connection_state ?? "estado"}
        />
        <MetricCard
          label="Ultimo ciclo recibido"
          value={status?.last_cycle_received_at ? formatDateTime(status.last_cycle_received_at) : "sin datos"}
          detail={status?.last_event_observed_at ? `Evento: ${formatDateTime(status.last_event_observed_at)}` : "sin evento"}
        />
        <MetricCard
          label="Cobertura 24h"
          value={
            overview
              ? `${overview.coverage.reporting_vehicles_24h}/${overview.coverage.total_vehicles}`
              : loading
                ? "..."
                : "sin datos"
          }
          detail={overview ? `${overview.coverage.stale_vehicles} vehiculos atrasados` : "Vehiculos con recepcion reciente"}
        />
        <MetricCard
          label="Ultimo sync de vehiculos"
          value={status?.last_device_sync_at ? formatDateTime(status.last_device_sync_at) : "pendiente"}
          detail={status?.last_alarm_at ? `Ultima alarma: ${formatDateTime(status.last_alarm_at)}` : "sin alarma"}
        />
        <MetricCard
          label="Hoy provisional"
          tone="amber"
          value={overview ? formatKm(overview.km.current_day_km_provisional) : loading ? "..." : "-"}
          detail={overview ? `Ventana cerrada: ${formatKm(overview.km.closed_window_km)}` : "KM del dia visible en dashboard"}
        />
        <MetricCard
          label="Anomalias 24h"
          tone={status?.anomaly_count_24h ? "danger" : "white"}
          value={String(status?.anomaly_count_24h ?? 0)}
          detail={overview ? `${overview.reports.available_reports} reportes cerrados` : "Eventos futuros o inconsistentes"}
        />
      </section>

      {showAdminLastError ? <div className="banner error">Ultimo error de la ingesta: {status?.last_error}</div> : null}
      {!showAdminLastError && status?.connection_state && status.connection_state !== "connected" && overview?.feed.status === "al_dia" ? (
        <div className="banner">La ultima captura sigue disponible, pero la sesion live aun esta en proceso de reconexion.</div>
      ) : null}

      <section className="double-panel">
        <div className="panel">
          <h3>Asignacion live</h3>
          <div className="key-value-list">
            <div className="key-value-row">
              <span>Fleet IDs asignados</span>
              <strong>{liveSetup?.assignment.fleet_ids.join(", ") || "ninguno"}</strong>
            </div>
            <div className="key-value-row">
              <span>Devices visibles</span>
              <strong>{liveSetup?.assignment.visible_devices ?? 0}</strong>
            </div>
            {hasVisibleMockData ? (
              <>
                <div className="key-value-row">
                  <span>Visibles mock vs real</span>
                  <strong>
                    {liveSetup?.assignment.visible_mock_devices ?? 0} / {liveSetup?.assignment.visible_real_devices ?? 0}
                  </strong>
                </div>
                <div className="key-value-row">
                  <span>Snapshots visibles mock vs real</span>
                  <strong>
                    {liveSetup?.assignment.visible_mock_snapshots ?? 0} / {liveSetup?.assignment.visible_real_snapshots ?? 0}
                  </strong>
                </div>
                <div className="key-value-row">
                  <span>Alarmas visibles mock vs real</span>
                  <strong>
                    {liveSetup?.assignment.visible_mock_alarms ?? 0} / {liveSetup?.assignment.visible_real_alarms ?? 0}
                  </strong>
                </div>
              </>
            ) : (
              <>
                <div className="key-value-row">
                  <span>Devices reales visibles</span>
                  <strong>{liveSetup?.assignment.visible_real_devices ?? liveSetup?.assignment.visible_devices ?? 0}</strong>
                </div>
                <div className="key-value-row">
                  <span>Snapshots reales visibles</span>
                  <strong>{liveSetup?.assignment.visible_real_snapshots ?? 0}</strong>
                </div>
                <div className="key-value-row">
                  <span>Alarmas reales visibles</span>
                  <strong>{liveSetup?.assignment.visible_real_alarms ?? 0}</strong>
                </div>
              </>
            )}
          </div>
        </div>

        <div className="panel">
          <h3>{hasMockContamination ? "Saneamiento de base local" : "Base local real"}</h3>
          <div className="key-value-list">
            {hasMockContamination ? (
              <>
                <div className="key-value-row">
                  <span>Devices mock / real</span>
                  <strong>
                    {liveSetup?.mock_data.devices_mock ?? 0} / {liveSetup?.mock_data.devices_real ?? 0}
                  </strong>
                </div>
                <div className="key-value-row">
                  <span>Snapshots mock / real</span>
                  <strong>
                    {liveSetup?.mock_data.snapshots_mock ?? 0} / {liveSetup?.mock_data.snapshots_real ?? 0}
                  </strong>
                </div>
                <div className="key-value-row">
                  <span>Alarmas mock / real</span>
                  <strong>
                    {liveSetup?.mock_data.alarms_mock ?? 0} / {liveSetup?.mock_data.alarms_real ?? 0}
                  </strong>
                </div>
              </>
            ) : (
              <>
                <div className="key-value-row">
                  <span>Devices reales</span>
                  <strong>{liveSetup?.mock_data.devices_real ?? 0}</strong>
                </div>
                <div className="key-value-row">
                  <span>Snapshots reales</span>
                  <strong>{liveSetup?.mock_data.snapshots_real ?? 0}</strong>
                </div>
                <div className="key-value-row">
                  <span>Alarmas reales</span>
                  <strong>{liveSetup?.mock_data.alarms_real ?? 0}</strong>
                </div>
              </>
            )}
          </div>
          <div className="toolbar admin-toolbar" style={{ marginTop: "1rem" }}>
            <button className="primary-btn" type="button" onClick={() => void applySelectedFleets()} disabled={selectedFleetIds.length === 0}>
              <Building2 size={16} />
              Aplicar flotas seleccionadas
            </button>
            {hasMockContamination ? (
              <button className="ghost-btn" type="button" onClick={() => void purgeMockLegacy()}>
                <AlertTriangle size={16} />
                Limpiar legado mock
              </button>
            ) : null}
          </div>
        </div>
      </section>

      <section className="panel">
        <h3>Flotas y empresas visibles en el servidor</h3>
        <p className="panel-copy">
          {assignedFleet
            ? `${company?.name ?? selectedCompany} esta asignada hoy a ${assignedFleet.fleet_name ?? assignedFleet.fleet_id}, pero puedes cambiarla aqui para validar otra flota real.`
            : `Selecciona una o mas flotas reales para ${company?.name ?? selectedCompany}.`}
        </p>
        {liveSetup?.fleet_candidates.length ? (
          <div className="stack">
            {liveSetup.fleet_candidates.map((fleet) => {
              const checked = selectedFleetIds.includes(fleet.fleet_id);
              return (
                <label key={fleet.fleet_id} className="panel compact" style={{ cursor: "pointer" }}>
                  <div className="panel-head">
                    <strong>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(event) => {
                          const enabledFleet = event.target.checked;
                          setSelectionTouched(true);
                          setSelectedFleetIds((current) =>
                            enabledFleet ? [...new Set([...current, fleet.fleet_id])] : current.filter((value) => value !== fleet.fleet_id),
                          );
                        }}
                        style={{ marginRight: "0.65rem" }}
                      />
                      {fleet.fleet_name ?? fleet.fleet_id}
                    </strong>
                    {fleet.selected ? <span className="tone-pill success">Asignada</span> : null}
                  </div>
                  <div className="panel-copy">ID real: {fleet.fleet_id}</div>
                  <div className="chip-row">
                    <span className="chip">Devices {fleet.total_devices}</span>
                    <span className="chip">Status {fleet.devices_with_status}</span>
                    <span className="chip">Seen 24h {fleet.devices_seen_24h}</span>
                    <span className="chip">Alarmas 7d {fleet.alarm_events_7d}</span>
                  </div>
                  <div className="panel-copy" style={{ marginTop: "0.65rem" }}>
                    Placas muestra: {fleet.sample_plates.join(", ") || "sin placas"} · Ultimo seen{" "}
                    {fleet.latest_seen_at ? formatDateTime(fleet.latest_seen_at, company?.timezone) : "sin dato"} · Ultima alarma{" "}
                    {fleet.latest_alarm_at ? formatDateTime(fleet.latest_alarm_at, company?.timezone) : "sin dato"}
                  </div>
                </label>
              );
            })}
          </div>
        ) : (
          <div className="empty-card">
            <div className="empty-title">Aun no aparecen flotas reales candidatas</div>
            <div className="empty-copy">Necesitamos mas sincronizacion live o validar que `vehicle/findAll.action` siga trayendo el catalogo esperado.</div>
          </div>
        )}
      </section>

      <section className="panel">
        <h3>Codigos reales aun sin clasificar</h3>
        {liveSetup?.unclassified_codes.length ? (
          <div className="key-value-list">
            {liveSetup.unclassified_codes.map((row) => (
              <div key={`${row.subtype ?? "null"}-${row.event_code ?? "null"}`} className="key-value-row">
                <span>
                  tp {row.subtype ?? "-"} · ec {row.event_code ?? "-"} · sample {row.sample_plate ?? row.sample_device_id ?? "-"}
                </span>
                <strong>{row.count}</strong>
              </div>
            ))}
          </div>
        ) : (
          <div className="empty-copy">No hay codigos unclassified recientes para revisar.</div>
        )}
      </section>

      <section className="double-panel">
        <div className="panel">
          <h3>Cobertura y conciliacion rapida</h3>
          <div className="key-value-list">
            <div className="key-value-row">
              <span>Total de vehiculos live</span>
              <strong>{overview?.coverage.total_vehicles ?? 0}</strong>
            </div>
            <div className="key-value-row">
              <span>Con snapshot hoy</span>
              <strong>{overview?.coverage.vehicles_with_snapshot_today ?? 0}</strong>
            </div>
            <div className="key-value-row">
              <span>Con alarmas 24h</span>
              <strong>{overview?.coverage.vehicles_with_alarm_24h ?? 0}</strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo estado feed</span>
              <strong>{overview?.feed.label ?? "sin datos"}</strong>
            </div>
          </div>
        </div>

        <div className="panel">
          <h3>KM y reportes</h3>
          <div className="key-value-list">
            <div className="key-value-row">
              <span>KM ventana completa</span>
              <strong>{overview ? formatKm(overview.km.total_window_km) : "-"}</strong>
            </div>
            <div className="key-value-row">
              <span>Dia provisional</span>
              <strong>
                {overview ? `${formatDateTime(`${overview.km.current_day_label}T12:00:00Z`, company?.timezone)} · ${formatKm(overview.km.current_day_km_provisional)}` : "-"}
              </strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo reporte disponible</span>
              <strong>
                {overview?.reports.latest_report_year && overview.reports.latest_report_month
                  ? formatReportMonth(overview.reports.latest_report_year, overview.reports.latest_report_month)
                  : "sin reportes"}
              </strong>
            </div>
            <div className="key-value-row">
              <span>Total reportes</span>
              <strong>{overview?.reports.available_reports ?? 0}</strong>
            </div>
          </div>
        </div>
      </section>

      <section className="double-panel">
        <div className="panel">
          <h3>Subir o reemplazar informe PDF</h3>
          <form className="form-grid" onSubmit={safeHandleUpload}>
            <label>
              Empresa
              <input value={company?.name ?? selectedCompany} readOnly />
            </label>
            <label>
              Token admin opcional
              <input value={adminToken} onChange={(event) => setAdminToken(event.target.value)} />
            </label>
            <label>
              Ano
              <input value={reportForm.year} onChange={(event) => setReportForm((current) => ({ ...current, year: event.target.value }))} />
            </label>
            <label>
              Mes
              <input value={reportForm.month} onChange={(event) => setReportForm((current) => ({ ...current, month: event.target.value }))} />
            </label>
            <label className="wide">
              Archivo PDF
              <input type="file" accept="application/pdf" onChange={(event) => setReportForm((current) => ({ ...current, file: event.target.files?.[0] ?? null }))} />
            </label>
            <button className="primary-btn wide" type="submit">
              <Upload size={16} />
              Cargar informe
            </button>
          </form>
        </div>

        <div className="panel">
          <h3>Backfill manual por rango</h3>
          <form className="form-grid" onSubmit={safeHandleBackfill}>
            <label>
              Empresa
              <input value={company?.name ?? selectedCompany} readOnly />
            </label>
            <label>
              Device ID opcional
              <input value={backfillForm.device_id} onChange={(event) => setBackfillForm((current) => ({ ...current, device_id: event.target.value }))} />
            </label>
            <label>
              Inicio
              <input
                type="datetime-local"
                value={backfillForm.start_at}
                onChange={(event) => setBackfillForm((current) => ({ ...current, start_at: event.target.value }))}
              />
            </label>
            <label>
              Fin
              <input
                type="datetime-local"
                value={backfillForm.end_at}
                onChange={(event) => setBackfillForm((current) => ({ ...current, end_at: event.target.value }))}
              />
            </label>
            <button className="primary-btn wide" type="submit">
              <RefreshCw size={16} />
              Ejecutar backfill
            </button>
          </form>
        </div>
      </section>
    </main>
  );
}

function AdminAuditModule({
  company,
  enabled,
  selectedCompany,
}: {
  company: CompanySummary | null;
  enabled: boolean;
  selectedCompany: string;
}) {
  const [audit, setAudit] = useState<AdminAudit | null>(null);
  const [anomalies, setAnomalies] = useState<IngestionAnomaly[]>([]);
  const [vehicles, setVehicles] = useState<AdminVehicle[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [range, setRange] = useState({
    from: `${dayjs().subtract(7, "day").format("YYYY-MM-DD")}T00:00`,
    to: `${dayjs().format("YYYY-MM-DD")}T23:59`,
  });

  const loadAudit = useCallback(async () => {
    const params = new URLSearchParams({
      company: selectedCompany,
      from: new Date(range.from).toISOString(),
      to: new Date(range.to).toISOString(),
    });

    try {
      const [nextAudit, nextAnomalies, nextVehicles] = await Promise.all([
        apiJson<AdminAudit>(`/admin/audit?${params.toString()}`),
        apiJson<IngestionAnomaly[]>(`/admin/anomalies?company=${encodeURIComponent(selectedCompany)}&limit=100`),
        apiJson<AdminVehicle[]>(`/admin/vehicles?company=${encodeURIComponent(selectedCompany)}`),
      ]);
      setAudit(nextAudit);
      setAnomalies(nextAnomalies);
      setVehicles(nextVehicles);
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo cargar la auditoria");
    } finally {
      setLoading(false);
    }
  }, [range.from, range.to, selectedCompany]);

  useEffect(() => {
    if (!enabled) return;
    setLoading(true);
    void loadAudit();
    const timer = window.setInterval(() => {
      void loadAudit();
    }, FEED_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [enabled, loadAudit]);

  return (
    <main className="page-grid">
      <section className="panel">
        <div className="panel-kicker">Diagnostico y Auditoria</div>
        <h3>Conciliacion live de {company?.name ?? selectedCompany}</h3>
        <p className="panel-copy">
          Aqui validamos lo recibido, lo aceptado por reglas y lo que finalmente queda visible en el dashboard operativo.
        </p>
      </section>

      <section className="panel">
        <div className="toolbar admin-toolbar">
          <label>
            Desde
            <input
              type="datetime-local"
              value={range.from}
              onChange={(event) => setRange((current) => ({ ...current, from: event.target.value }))}
            />
          </label>
          <label>
            Hasta
            <input
              type="datetime-local"
              value={range.to}
              onChange={(event) => setRange((current) => ({ ...current, to: event.target.value }))}
            />
          </label>
          <button className="ghost-btn" type="button" onClick={() => void loadAudit()}>
            <RefreshCw size={16} />
            Refrescar auditoria
          </button>
        </div>
      </section>

      {error ? <div className="banner error">{error}</div> : null}

      <section className="metric-grid four">
        <MetricCard label="Alarmas aceptadas" value={String(audit?.alarms.accepted_total ?? (loading ? "..." : 0))} detail="Entraron al almacenamiento analitico" />
        <MetricCard label="Visibles por reglas" value={String(audit?.alarms.visible_total ?? (loading ? "..." : 0))} detail="Despues de filtros y agrupacion" />
        <MetricCard label="Alertas 24h visibles" value={String(audit?.recent_24h.visible_alerts ?? (loading ? "..." : 0))} detail={`Descartadas: ${audit?.recent_24h.dismissed_alerts ?? 0}`} />
        <MetricCard label="Vehiculos live" value={String(vehicles.length)} detail="Ultimo estado reconciliado por vehiculo" />
      </section>

      <section className="double-panel">
        <div className="panel">
          <h3>Mapa de clasificacion</h3>
          <div className="key-value-list">
            {Object.entries(audit?.alarms.mapping_sources ?? {}).length === 0 ? (
              <div className="empty-copy">Aun no hay datos suficientes para el mapa de clasificacion.</div>
            ) : (
              Object.entries(audit?.alarms.mapping_sources ?? {}).map(([source, count]) => (
                <div key={source} className="key-value-row">
                  <span>{source}</span>
                  <strong>{count}</strong>
                </div>
              ))
            )}
          </div>
          <div className="panel-copy">Sin clasificar: {audit?.alarms.unclassified_total ?? 0}</div>
        </div>

        <div className="panel">
          <h3>Conteo por categoria</h3>
          <div className="chip-row">
            {Object.entries(audit?.alarms.by_category ?? {}).map(([category, count]) => (
              <span key={category} className="chip" style={{ borderColor: CATEGORY_COLORS[category] ?? "var(--line)" }}>
                {formatCategory(category)} {count}
              </span>
            ))}
          </div>
          <div className="panel-copy" style={{ marginTop: "1rem" }}>
            Episodios 24h agrupados: {audit?.recent_24h.grouped_episodes ?? 0} · crudos 24h: {audit?.recent_24h.raw_events ?? 0}
          </div>
        </div>
      </section>

      <section className="double-panel">
        <div className="panel">
          <h3>Subtipos mas frecuentes</h3>
          <div className="key-value-list">
            {(audit?.alarms.by_subtype ?? []).slice(0, 10).map((row) => (
              <div key={row.subtype} className="key-value-row">
                <span>{row.subtype}</span>
                <strong>{row.count}</strong>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <h3>Anomalias por motivo</h3>
          <div className="key-value-list">
            {Object.entries(audit?.anomalies.by_reason ?? {}).length === 0 ? (
              <div className="empty-copy">No se registran anomalias para este rango.</div>
            ) : (
              Object.entries(audit?.anomalies.by_reason ?? {}).map(([reason, count]) => (
                <div key={reason} className="key-value-row">
                  <span>{reason}</span>
                  <strong>{count}</strong>
                </div>
              ))
            )}
          </div>
          <div className="panel-copy">Total de anomalias en rango: {audit?.anomalies.total ?? 0}</div>
        </div>
      </section>

      <section className="panel table-wrap">
        <h3>Vehiculos y kilometraje reconciliado</h3>
        {vehicles.length === 0 ? (
          <div className="empty-copy">No hay vehiculos live visibles para la empresa seleccionada.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Placa</th>
                <th>Device</th>
                <th>Feed</th>
                <th>Ultima recepcion</th>
                <th>Ultima alarma</th>
                <th>KM total</th>
                <th>KM dia</th>
                <th>Snapshot</th>
              </tr>
            </thead>
            <tbody>
              {vehicles.map((vehicle) => (
                <tr key={vehicle.device_id}>
                  <td>{vehicle.plate_no ?? "-"}</td>
                  <td>{vehicle.device_id}</td>
                  <td>
                    <span className={`tone-pill ${feedTone(vehicle.feed_status)}`}>{vehicle.feed_status}</span>
                  </td>
                  <td>{vehicle.last_received_at ? formatDateTime(vehicle.last_received_at, company?.timezone) : "-"}</td>
                  <td>{vehicle.last_alarm_at ? formatDateTime(vehicle.last_alarm_at, company?.timezone) : "-"}</td>
                  <td>{formatKm(vehicle.last_total_km)}</td>
                  <td>{formatKm(vehicle.last_day_km)}</td>
                  <td>
                    {vehicle.last_snapshot_at ? formatDateTime(vehicle.last_snapshot_at, company?.timezone) : "-"}
                    <br />
                    <span className="muted">
                      {formatKm(vehicle.last_snapshot_total_km)} · {formatKm(vehicle.last_snapshot_day_km)}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="panel table-wrap">
        <h3>Anomalias recientes</h3>
        {anomalies.length === 0 ? (
          <div className="empty-copy">No hay anomalias recientes para la empresa seleccionada.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Recibido</th>
                <th>Device</th>
                <th>Tipo</th>
                <th>Hora cruda</th>
                <th>Motivo</th>
              </tr>
            </thead>
            <tbody>
              {anomalies.map((anomaly) => (
                <tr key={anomaly.id}>
                  <td>{formatDateTime(anomaly.received_at, company?.timezone)}</td>
                  <td>{anomaly.device_id ?? "-"}</td>
                  <td>{anomaly.source_type}</td>
                  <td>{anomaly.raw_event_time ?? "-"}</td>
                  <td>{anomaly.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}

function AlertCard({
  alert,
}: {
  alert: {
    id: string;
    plate: string;
    category: string;
    level: "critico" | "alto" | "medio";
    label: string;
    title: string;
    detail: string;
    timeLabel: string;
    isNight: boolean;
    isNew: boolean;
    rawCount: number;
  };
}) {
  return (
    <div className={`alert-card ${alert.isNight ? "night" : ""}`}>
      <div className="alert-time">{alert.timeLabel}</div>
      <div className="alert-body">
        <div className="alert-marker" style={{ background: CATEGORY_COLORS[alert.category] ?? "var(--accent)" }} />
        <div className="alert-content">
          <div className="alert-topline">
            <span className={`tag ${alert.level}`}>{alert.label}</span>
            {alert.isNight ? <span className="tag night">Noche</span> : null}
            {alert.isNew ? <span className="tag new">Nuevo</span> : null}
            <span className="plate-pill">{alert.plate}</span>
          </div>
          <div className="alert-title-row">
            <div className="alert-icon">{pickAlertIcon(alert.category)}</div>
            <div>
              <div className="alert-title">{alert.title}</div>
              <div className="alert-copy">{alert.detail}</div>
              <div className="priority">{alert.rawCount} eventos crudos en el episodio.</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function VehicleRow({ row }: { row: VehicleTableRow }) {
  return (
    <tr>
      <td>
        <strong>{row.placa}</strong>
      </td>
      <td>{row.total}</td>
      <td>{formatKm(row.km)}</td>
      <td>{formatRate(row.por100km)}</td>
      <td>{formatRate(row.riesgo100km)}</td>
      <td>{row.nocturno}</td>
      <td>
        {formatNumber(row.baseline)}
        {row.spike ? " ▲" : ""}
      </td>
      <td>
        <div className="chip-row">
          {Object.entries(row.cats)
            .filter(([, value]) => value > 0)
            .map(([category, value]) => (
              <span key={category} className="chip" style={{ borderColor: CATEGORY_COLORS[category] ?? "var(--line)" }}>
                {formatCategory(category)} {value}
              </span>
            ))}
        </div>
      </td>
    </tr>
  );
}

function ChartPanel({
  children,
  frameStyle,
  title,
}: {
  children: ReactNode;
  frameStyle?: CSSProperties;
  title: string;
}) {
  return (
    <div className="panel chart-panel">
      <h3>{title}</h3>
      <div className="chart-frame" style={frameStyle}>
        {children}
      </div>
    </div>
  );
}

function MetricCard({
  detail,
  label,
  tone = "white",
  value,
}: {
  detail?: string;
  label: string;
  tone?: "white" | "danger" | "warning" | "amber";
  value: string;
}) {
  return (
    <div className={`metric-card ${tone}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      {detail ? <div className="muted">{detail}</div> : null}
    </div>
  );
}

function ProfileInsightCard({
  accent,
  action,
  emptyText,
  rows,
  subtitle,
  summary,
  title,
}: {
  accent: string;
  action: string;
  emptyText: string;
  rows: Array<{ label: string; metric: string; value: number }>;
  subtitle: string;
  summary: string;
  title: string;
}) {
  const maxValue = Math.max(...rows.map((row) => row.value), 1);
  return (
    <div className="panel profile-board-card" style={{ borderColor: `${accent}66` }}>
      <div className="profile-board-eyebrow" style={{ color: accent }}>
        {title}
      </div>
      <div className="profile-board-summary">{summary}</div>
      <div className="panel-copy">{subtitle}</div>
      <div className="profile-board-list">
        {rows.length === 0 ? (
          <div className="empty-copy">{emptyText}</div>
        ) : (
          rows.map((row) => (
            <div key={`${row.label}-${row.metric}`} className="profile-board-row">
              <div className="profile-board-row-top">
                <strong>{row.label}</strong>
                <span style={{ color: accent }}>{row.metric}</span>
              </div>
              <div className="progress-track profile-board-track">
                <div
                  className="progress-fill"
                  style={{
                    width: `${Math.max((row.value / maxValue) * 100, row.value > 0 ? 10 : 0)}%`,
                    background: accent,
                  }}
                />
              </div>
            </div>
          ))
        )}
      </div>
      <div className="profile-board-action">{action}</div>
    </div>
  );
}

function pickCompanySlug(session: AuthMeResponse, current: string | null) {
  if (current && session.companies.some((company) => company.slug === current)) {
    return current;
  }
  return session.selected_company_slug ?? session.companies[0]?.slug ?? null;
}

function filterNotesForTab(
  notes: DashboardSnapshot["dataQuality"]["active_notes"],
  activeTab: DashboardTab,
  rangeEndIso: string,
) {
  const rangeEnd = dayjs(`${rangeEndIso}T12:00:00`);
  const days =
    activeTab === "24h"
      ? 1
      : activeTab === "semana"
        ? 7
        : 30;
  const rangeStart = rangeEnd.subtract(days - 1, "day");

  return notes.filter((note) => {
    const noteStart = dayjs(note.start_date);
    const noteEnd = note.end_date ? dayjs(note.end_date) : rangeEnd;
    return !noteStart.isAfter(rangeEnd, "day") && !noteEnd.isBefore(rangeStart, "day");
  });
}

function defaultModuleForRole(role: AuthMeResponse["user"]["role"]): PortalModule {
  return role === "admin" ? "administracion" : "dashboard";
}

function feedTone(status: FeedState["status"]) {
  if (status === "al_dia") return "success";
  if (status === "atrasado") return "warning";
  return "danger";
}

function formatDateTime(value: string, timezoneName = "America/Bogota") {
  return dayjs(value).tz(timezoneName).format("DD/MM/YYYY HH:mm");
}

function formatClockTime(value: string, timezoneName = "America/Bogota") {
  return dayjs(value).tz(timezoneName).format("HH:mm");
}

function formatIsoDate(value: string) {
  return dayjs(value).format("YYYY-MM-DD");
}

function formatDashboardHeaderSummary(snapshot: DashboardSnapshot) {
  return `${formatIsoDate(snapshot.meta.rangeStart)} a ${formatIsoDate(snapshot.meta.rangeEnd)} · ${snapshot.meta.vehicleCount} vehiculos · ${formatKm(snapshot.meta.kmTotal)}`;
}

function formatShortDate(value: string) {
  return dayjs(value).format("DD/MM");
}

function formatReportMonth(year: number, month: number) {
  return dayjs(`${year}-${String(month).padStart(2, "0")}-01`).format("MMMM YYYY");
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return new Intl.NumberFormat("es-CO", { maximumFractionDigits: 1 }).format(value);
}

function formatKm(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return `${formatNumber(value)} km`;
}

function formatRate(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return `${formatNumber(value)}`;
}

function formatDeltaBadge(value: number | null) {
  if (value === null) return "sin baseline";
  const arrow = value > 0 ? "▲" : value < 0 ? "▼" : "•";
  return `${arrow} ${formatNumber(Math.abs(value))}%`;
}

function floorCycleBucket(value: string | null, cycleMinutes: number) {
  if (!value) return null;
  const date = dayjs(value);
  if (!date.isValid()) return null;
  const minutes = Math.max(cycleMinutes, 1);
  const bucketMinute = Math.floor(date.minute() / minutes) * minutes;
  return date.second(0).millisecond(0).minute(bucketMinute).toISOString();
}

function humanBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function vehicleColor(index: number) {
  const palette = ["#10b981", "#38bdf8", "#f59e0b", "#ef4444", "#a855f7", "#22c55e"];
  return palette[index % palette.length];
}

function formatDayBadge(value: string, timezoneName = "America/Bogota") {
  const date = dayjs(value).tz(timezoneName);
  const months = ["ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"];
  return `${date.format("DD")} ${months[date.month()]}`;
}

function rateByKm(count: number, km: number | null | undefined) {
  if (!km || km <= 0) return null;
  return (count / km) * 100;
}

function isNightHour(hour: number, start: number, end: number) {
  if (start === end) return true;
  if (start < end) return hour >= start && hour < end;
  return hour >= start || hour < end;
}

function pickDominantCriticalWindow(events: RecentEvent[], timezoneName = "America/Bogota") {
  const criticalCategories = new Set(["Ojos cerrados", "Uso de celular"]);
  const critical = events.filter((event) => criticalCategories.has(event.category));
  if (critical.length === 0) return null;
  const hourCounts = Array.from({ length: 24 }, () => 0);
  critical.forEach((event) => {
    const hour = dayjs(event.occurredAt).tz(timezoneName).hour();
    hourCounts[hour] += 1;
  });
  const windowSize = 6;
  let bestStart = 0;
  let bestCount = 0;
  for (let start = 0; start <= 24 - windowSize; start += 1) {
    const total = hourCounts.slice(start, start + windowSize).reduce((sum, value) => sum + value, 0);
    if (total > bestCount) {
      bestCount = total;
      bestStart = start;
    }
  }
  return {
    label: `${String(bestStart).padStart(2, "0")}:00 y ${String(bestStart + windowSize).padStart(2, "0")}:00`,
    sharePct: (bestCount / critical.length) * 100,
  };
}

function groupReports(reports: ReportFile[]) {
  const byYear = new Map<number, ReportFile[]>();
  for (const report of reports) {
    const current = byYear.get(report.year) ?? [];
    current.push(report);
    byYear.set(report.year, current);
  }
  return [...byYear.entries()].sort((left, right) => right[0] - left[0]);
}

function pickAlertIcon(category: string) {
  switch (category) {
    case "Uso de celular":
      return <Smartphone size={18} />;
    case "Fatiga en progresion":
      return <Brain size={18} />;
    case "Ojos cerrados":
      return <EyeOff size={18} />;
    case "Riesgo de colision":
      return <CarFront size={18} />;
    case "Bostezo":
      return <Coffee size={18} />;
    case "Camara cubierta":
      return <CameraOff size={18} />;
    case "Fumando":
      return <Cigarette size={18} />;
    case "Distraccion":
      return <AlertTriangle size={18} />;
    default:
      return <Shield size={18} />;
  }
}

export default App;
