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
  Filter,
  Gauge,
  LogOut,
  MoonStar,
  RefreshCw,
  Shield,
  Smartphone,
  Upload,
  UserCircle2,
} from "lucide-react";
import { memo, startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties, FormEvent, ReactNode } from "react";
import { Bar, Doughnut, Line } from "react-chartjs-2";

import { ApiError, apiFetch, apiJson, buildApiUrl, waitForBackgroundJob } from "./api";
import { useDashboardStream } from "./hooks/useDashboardStream";
import { buildTimeline, formatCategory } from "./lib/alerts";
import type {
  AdminAudit,
  AdminCompanyCatalog,
  AdminCompanyCatalogItem,
  AdminIngestionStatus,
  AdminLiveSetup,
  AdminOverview,
  AdminVehicle,
  AuthMeResponse,
  BackgroundJobStatus,
  CompanySummary,
  DashboardSnapshot,
  FeedState,
  FleetCandidate,
  HistoricalRebuildResult,
  IngestionAnomaly,
  KmQualitySummary,
  ReconciliationReviewItem,
  ReconciliationReviewBulkDecisionResult,
  ReconciliationReviewList,
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
type DiagnosticWindowMode = "24h" | "7d" | "month";

const DIAGNOSTIC_WINDOW_OPTIONS: Array<{ key: DiagnosticWindowMode; label: string }> = [
  { key: "24h", label: "24 h" },
  { key: "7d", label: "7 dias" },
  { key: "month", label: "30 dias" },
];
const DIAGNOSTIC_CACHE_TTL_MS = 30_000;
const DIAGNOSTIC_REVIEW_PAGE_SIZE = 12;

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
const N2_MONTH_CATEGORIES = [
  "Ojos cerrados",
  "Bostezo",
  "Riesgo de colision",
  "Distraccion",
  "Uso de celular",
  "Fumando",
] as const;

const GRID_COLOR = "rgba(138, 144, 168, 0.18)";
const TICK_COLOR = "#8a90a8";

function buildAuditMonthValue(timezoneName: string, referenceIso: string | null = null) {
  const base = referenceIso ? dayjs(referenceIso).tz(timezoneName) : dayjs().tz(timezoneName);
  return base.format("YYYY-MM");
}

function buildAuditMonthRange(monthValue: string, timezoneName: string, referenceIso: string | null = null) {
  const base = dayjs.tz(`${monthValue}-01T00:00`, timezoneName);
  const referenceLocal = referenceIso ? dayjs(referenceIso).tz(timezoneName) : dayjs().tz(timezoneName);
  const end = referenceLocal.isSame(base, "month") ? referenceLocal : base.endOf("month");
  return {
    from: base.format("YYYY-MM-DDTHH:mm"),
    to: end.format("YYYY-MM-DDTHH:mm"),
  };
}

type DiagnosticSnapshotMeta = Pick<DashboardSnapshot["meta"], "rangeStart" | "weekWindowStart">;

function buildDiagnosticRange(
  windowMode: DiagnosticWindowMode,
  timezoneName: string,
  referenceIso: string | null = null,
  snapshotMeta: DiagnosticSnapshotMeta | null = null,
) {
  const nowLocal = referenceIso ? dayjs(referenceIso).tz(timezoneName) : dayjs().tz(timezoneName);
  if (windowMode === "24h") {
    return {
      from: nowLocal.subtract(24, "hour").format("YYYY-MM-DDTHH:mm"),
      to: nowLocal.format("YYYY-MM-DDTHH:mm"),
      label: "Ultimas 24 h",
      subtitle: "Operacion reciente y alertas que no entraron en el ultimo dia",
    };
  }
  if (windowMode === "7d") {
    const weekStart = snapshotMeta?.weekWindowStart
      ? dayjs.tz(`${snapshotMeta.weekWindowStart}T00:00`, timezoneName)
      : nowLocal.startOf("day").subtract(6, "day");
    return {
      from: weekStart.format("YYYY-MM-DDTHH:mm"),
      to: nowLocal.format("YYYY-MM-DDTHH:mm"),
      label: "Ultimos 7 dias",
      subtitle: "Misma ventana de la pestaña Semana del dashboard cliente",
    };
  }
  const thirtyDayStart = snapshotMeta?.rangeStart
    ? dayjs.tz(`${snapshotMeta.rangeStart}T00:00`, timezoneName)
    : nowLocal.startOf("day").subtract(29, "day");
  return {
    from: thirtyDayStart.format("YYYY-MM-DDTHH:mm"),
    to: nowLocal.format("YYYY-MM-DDTHH:mm"),
    label: "Ultimos 30 dias",
    subtitle: "Misma ventana de la pestaña Mes (30 dias) del dashboard cliente",
  };
}

function settledError(result: PromiseSettledResult<unknown>): string | null {
  if (result.status === "fulfilled") return null;
  return result.reason instanceof Error ? result.reason.message : "Request failed";
}

interface DiagnosticAuditCacheEntry {
  loadedAt: number;
  audit: AdminAudit;
}

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

const COMPACT_HORIZONTAL_BAR_OPTIONS: ChartOptions<"bar"> = {
  ...HORIZONTAL_BAR_OPTIONS,
  plugins: {
    legend: {
      display: false,
    },
  },
};

const COMPACT_CHART_FRAME_STYLE: CSSProperties = {
  height: "15rem",
  minHeight: "15rem",
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
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
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
  const [clockNowMs, setClockNowMs] = useState(() => Date.now());

  const applySession = useCallback((payload: AuthMeResponse, currentCompany: string | null = null) => {
    setSession(payload);
    setSelectedCompany(pickCompanySlug(payload, currentCompany));
    setActiveModule(defaultModuleForRole(payload.user.role));
    setAuthError(null);
  }, []);

  const reloadSession = useCallback(
    async (preferredCompany: string | null = null) => {
      const payload = await apiJson<AuthMeResponse>("/auth/me", { timeoutMs: 10_000 });
      applySession(payload, preferredCompany ?? selectedCompany);
    },
    [applySession, selectedCompany],
  );

  const bootstrapSession = useCallback(async () => {
    setAuthLoading(true);
    setBootstrapError(null);
    try {
      const payload = await apiJson<AuthMeResponse>("/auth/me", { timeoutMs: 10_000 });
      applySession(payload);
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        setSession(null);
        setSelectedCompany(null);
        setAuthError(null);
      } else {
        setBootstrapError(error instanceof Error ? error.message : "No se pudo validar la sesion");
      }
    } finally {
      setAuthLoading(false);
    }
  }, [applySession]);

  useEffect(() => {
    void bootstrapSession();
  }, [bootstrapSession]);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("dms.timeline.filter", timelineFilter);
    }
  }, [timelineFilter]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setClockNowMs(Date.now());
    }, 1_000);
    return () => window.clearInterval(timer);
  }, []);

  const activeCompany = session?.companies.find((company) => company.slug === selectedCompany) ?? session?.companies[0] ?? null;

  useEffect(() => {
    if (!session) return;
    const nextSelectedCompany = pickCompanySlug(session, selectedCompany);
    if (nextSelectedCompany !== selectedCompany) {
      setSelectedCompany(nextSelectedCompany);
    }
  }, [selectedCompany, session]);

  useEffect(() => {
    if (!activeCompany) return;
    const root = document.documentElement;
    root.style.setProperty("--accent", activeCompany.brand.accent);
    root.style.setProperty("--warning", activeCompany.brand.warning);
    root.style.setProperty("--danger", activeCompany.brand.danger);
    root.style.setProperty("--muted", activeCompany.brand.muted);
  }, [activeCompany]);

  const dashboard = useDashboardStream(selectedCompany);
  const isAdmin = session?.user.role === "admin";
  const effectiveFeed = dashboard.snapshot?.feed ?? null;
  const snapshotScheduleLabel =
    activeCompany && dashboard.nextRefreshAt
      ? buildSnapshotScheduleLabel({
          timezoneName: activeCompany.timezone,
          nextRefreshAt: dashboard.nextRefreshAt,
          nowMs: clockNowMs,
        })
      : null;
  const modules = isAdmin ? ADMIN_MODULES : CLIENT_MODULES;
  const showFeedDiagnosticBanner = !isAdmin && Boolean(effectiveFeed?.last_error && effectiveFeed.status !== "al_dia");
  const handleGlobalRefresh = useCallback(async () => {
    await dashboard.refresh();
    if (!isAdmin) {
      return;
    }
    try {
      await reloadSession(selectedCompany);
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : "No se pudo actualizar la sesion");
    }
  }, [dashboard, isAdmin, reloadSession, selectedCompany]);

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
          <div className="empty-title">Validando sesion</div>
          <div className="empty-copy">Conectando el portal con el servicio DMS.</div>
        </div>
      </div>
    );
  }

  if (bootstrapError && !session) {
    return (
      <div className="shell centered">
        <div className="empty-card connection-error-card">
          <div className="empty-title">El servicio DMS no esta disponible</div>
          <div className="empty-copy">{bootstrapError}</div>
          <button className="primary-btn" type="button" onClick={() => void bootstrapSession()}>
            Reintentar conexion
          </button>
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
                  onChange={(event) => {
                    setSelectedCompany(event.target.value || null);
                  }}
                >
                  {session.companies.length ? (
                    session.companies.map((company) => (
                      <option key={company.slug} value={company.slug}>
                        {company.name}
                      </option>
                    ))
                  ) : (
                    <option value="">Sin empresas activas</option>
                  )}
                </select>

                <button className="ghost-btn" type="button" onClick={() => void handleGlobalRefresh()}>
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
                      onClick={() => {
                        setActiveModule(module.id);
                      }}
                    >
                      {module.label}
                    </button>
                  ))}
                </div>
              ) : null}

              <div className="toolbar">
                <div className="feed-pill">
                  <Gauge size={16} />
                  LIVE
                </div>
                {snapshotScheduleLabel ? (
                  <div className={`feed-pill ${effectiveFeed?.status ?? ""}`}>
                    <span className="feed-dot" />
                    {snapshotScheduleLabel}
                  </div>
                ) : null}
                <div className="feed-pill">
                  <UserCircle2 size={16} />
                  {session.user.username}
                </div>
              </div>
            </>
          ) : (
            <div className="toolbar">
              {snapshotScheduleLabel ? (
                <div className={`feed-pill ${effectiveFeed?.status ?? ""}`}>
                  <span className="feed-dot" />
                  {snapshotScheduleLabel}
                </div>
              ) : null}
              <button className="ghost-btn" type="button" onClick={() => void handleGlobalRefresh()}>
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

      {isAdmin && showFeedDiagnosticBanner ? <div className="banner error">Diagnostico de ingesta: {effectiveFeed?.last_error}</div> : null}

      {activeModule === "dashboard" && selectedCompany ? (
        <>
          <nav className="tabs">
            {DASHBOARD_TABS.map((tab) => (
              <button
                key={tab.id}
                className={`tab ${activeTab === tab.id ? "active" : ""}`}
                type="button"
                onClick={() => setActiveTab(tab.id)}
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
          isAdmin={isAdmin}
          loading={dashboard.loading}
          onRefreshDashboard={dashboard.refresh}
          snapshot={dashboard.snapshot}
        />
      ) : null}

      {session.user.role === "admin" ? (
        <>
          <div style={{ display: activeModule === "administracion" ? "block" : "none" }}>
            <AdminOperationsModule
              adminUsername={session.user.username}
              company={activeCompany}
              enabled={activeModule === "administracion"}
              onReloadSession={reloadSession}
              selectedCompany={selectedCompany}
              snapshotVersion={dashboard.snapshot?.meta.generatedAt ?? null}
              visibleCompanySlugs={session.companies.map((company) => company.slug)}
            />
          </div>

          {selectedCompany ? (
            <div style={{ display: activeModule === "auditoria" ? "block" : "none" }}>
              <AdminAuditModule
                company={activeCompany}
                enabled={activeModule === "auditoria"}
                selectedCompany={selectedCompany}
                snapshot={dashboard.snapshot}
                snapshotVersion={dashboard.snapshot?.meta.generatedAt ?? null}
              />
            </div>
          ) : activeModule !== "administracion" ? (
            <section className="panel">
              <h3>Activa una empresa para continuar</h3>
              <p className="panel-copy">
                Diagnostico, Dashboard cliente y Reportes usan la empresa elegida en el selector superior. Primero activa una
                empresa desde Administracion y luego aparecera arriba para operarla.
              </p>
            </section>
          ) : null}
        </>
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
            <div className="empty-title">
              {timelineFilter === "todas" && snapshot.recentEvents.length === 0
                ? "No hubo detecciones DMS en las ultimas 24 h"
                : "No hay alertas visibles para este filtro"}
            </div>
            <div className="empty-copy">
              {timelineFilter === "todas" && snapshot.recentEvents.length === 0
                ? `Ultimo DMS visible: ${snapshot.meta.lastDmsEventAt ? formatDateTime(snapshot.meta.lastDmsEventAt, snapshot.meta.timezone) : "sin detecciones DMS recientes"} · ultimo mensaje live: ${
                    snapshot.feed.last_live_alarm_message_at
                      ? formatDateTime(snapshot.feed.last_live_alarm_message_at, snapshot.meta.timezone)
                      : "sin mensaje live reciente"
                  } · ultimo ciclo: ${
                    snapshot.feed.last_cycle_received_at
                      ? formatDateTime(snapshot.feed.last_cycle_received_at, snapshot.meta.timezone)
                      : "sin ciclo reciente"
                  }`
                : timeline.emptyHint}
            </div>
          </div>
        ) : (
          <div className="panel">
            <div className="panel-head">
              <div className="panel-dot" />
              <strong>Ahora {formatClockTime(snapshot.meta.generatedAt, snapshot.meta.timezone)}</strong>
              <span className="muted">hacia atras 24 h</span>
            </div>

            <div>
              {timeline.entries.map((entry) =>
                entry.kind === "marker" ? (
                  <TimelineMarker key={entry.id} label={entry.label} timeLabel={entry.timeLabel} />
                ) : (
                  <AlertCard key={entry.id} alert={entry.alert} />
                ),
              )}
            </div>
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
    datasets: N2_MONTH_CATEGORIES.map((category) => ({
      label: formatCategory(category),
      data: snapshot.dms.semana.cat_veh[category] ?? [],
      backgroundColor: CATEGORY_COLORS[category] ?? "#10b981",
      borderRadius: 8,
    })),
  };
  const trendDates = snapshot.dms.semana.fechas.map(formatShortDate);
  const trendData = {
    labels: trendDates,
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
        position: "bottom",
        labels: { color: "#eef2ff", boxWidth: 14, boxHeight: 14 },
      },
    },
  };

  return (
    <section className="stack">
      <div className="metric-grid three">
        <MetricCard
          label="Alarmas 7 dias"
          value={String(snapshot.dms.semana.total)}
          detail={`${formatShortDate(snapshot.meta.weekWindowStart)} -> ${formatShortDate(snapshot.meta.weekWindowEnd)} · semana calendario local`}
        />
        <MetricCard label="Promedio diario" value={formatNumber(snapshot.dms.semana.total / Math.max(snapshot.dms.semana.fechas.length, 1))} />
        <MetricCard label="Vehiculos activos" value={String(snapshot.dms.semana.veh.length)} />
      </div>

      <ChartPanel title="Que alarmas y en que vehiculos - ultima semana">
        <Bar data={categoryData} options={STACKED_BAR_OPTIONS} />
      </ChartPanel>

      <ChartPanel title="Tendencia diaria por vehiculo">
        <Line data={trendData} options={trendOptions} />
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
  const kmCoverageDays = snapshot.meta.kmCoverageDays ?? snapshot.dms.km_dia.filter((value) => value !== null).length;
  const kmWindowDays = snapshot.meta.kmWindowDays ?? snapshot.dms.fechas.length;
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
    labels: N2_MONTH_CATEGORIES.map(formatCategory),
    datasets: [
      {
        label: "Eventos",
        data: N2_MONTH_CATEGORIES.map((category) =>
          (snapshot.dms.serie_cat[category] ?? []).reduce((sum, value) => sum + value, 0),
        ),
        backgroundColor: N2_MONTH_CATEGORIES.map((category) => CATEGORY_COLORS[category]),
      },
    ],
  };
  const composition = {
    labels: N2_MONTH_CATEGORIES.map(formatCategory),
    datasets: [
      {
        label: "Composición por categoría",
        data: N2_MONTH_CATEGORIES.map((category) =>
          (snapshot.dms.serie_cat[category] ?? []).reduce((sum, value) => sum + value, 0),
        ),
        backgroundColor: N2_MONTH_CATEGORIES.map((category) => CATEGORY_COLORS[category]),
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
        <MetricCard
          label="Km recorridos (dias con cobertura)"
          value={formatNumber(snapshot.dms.kpis.km)}
          detail={
            snapshot.meta.kmDataComplete
              ? "Cobertura completa de 30 dias"
              : `Cobertura confiable ${kmCoverageDays}/${kmWindowDays} dias`
          }
        />
        <MetricCard
          label="Alarmas / 100 km"
          value={formatRate(snapshot.dms.kpis.por100km)}
          detail={snapshot.meta.kmDataComplete ? undefined : "No calculado: cobertura de km incompleta"}
        />
        <MetricCard label="Alarmas nocturnas" value={`${snapshot.dms.kpis.nocturno_pct}%`} />
      </div>

      <ChartPanel title="Evolución diaria de alarmas por categoría">
        <Bar data={categorySeries} options={STACKED_BAR_OPTIONS} />
      </ChartPanel>

      <ChartPanel title="Km recorridos por día (flota)">
        <Line data={kmSeries} options={LINE_OPTIONS} />
      </ChartPanel>

      <div className="double-panel">
        <ChartPanel title="Distribución por categoría DMS">
          <Bar data={distribution} options={HORIZONTAL_BAR_OPTIONS} />
        </ChartPanel>
        <ChartPanel title="Composición por categoría">
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
        <h3>Huella de conducta por vehículo</h3>
        <p className="panel-copy">La mezcla de categorías orienta acciones específicas por vehículo.</p>
      </div>

      <div className="triple-panel">
        <ProfileInsightCard
          accent="#ef4444"
          action="Acción sugerida: revisar jornada, turno y descansos."
          summary={`${fatigueVehicleCount} vehículos`}
          subtitle="Vehículos donde más del 70% de las alertas corresponde a ojos cerrados o bostezo."
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
          action="Acción sugerida: revisar o descargar en Howen los videos de uso de celular de estas placas y definir una intervención de conducta."
          summary={`${cellphoneVehicleCount} de ${snapshot.meta.vehicleCount}`}
          subtitle="Vehículos con más alertas de uso de celular y promedio de alertas por cada 100 km."
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
          action="Acción sugerida: contrastar las placas con mayor actividad nocturna frente a la programación de turnos."
          summary={`${snapshot.dms.kpis.nocturno_pct}% de las alertas`}
          subtitle="Porcentaje de alertas nocturnas del total y vehículos con más alertas durante la noche."
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
  isAdmin,
  loading,
  onRefreshDashboard,
  snapshot,
}: {
  company: CompanySummary | null;
  companySlug: string;
  error: string | null;
  isAdmin: boolean;
  loading: boolean;
  onRefreshDashboard: () => void;
  snapshot: DashboardSnapshot | null;
}) {
  const [reportActionError, setReportActionError] = useState<string | null>(null);
  const [reportActionSuccess, setReportActionSuccess] = useState<string | null>(null);
  const [adminToken, setAdminToken] = useState("");
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

  const handleUpload = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!reportForm.file) {
      setReportActionError("Selecciona un PDF para cargar");
      return;
    }

    setReportActionError(null);
    setReportActionSuccess(null);
    const formData = new FormData();
    formData.append("company_slug", companySlug);
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

    setReportActionSuccess("Reporte cargado correctamente. Si el mes ya existia, fue reemplazado.");
    setReportForm((current) => ({ ...current, file: null }));
    onRefreshDashboard();
  };

  const handleBackfill = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setReportActionError(null);
    setReportActionSuccess(null);
    const payload = {
      company_slug: companySlug,
      device_id: backfillForm.device_id.trim() || null,
      start_at: new Date(backfillForm.start_at).toISOString(),
      end_at: new Date(backfillForm.end_at).toISOString(),
      publish_snapshot: true,
    };

    const response = await apiJson<{ job_id: string; status: string }>("/admin/backfill", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setReportActionSuccess(
      `Backfill encolado (${response.job_id.slice(0, 8)}). El worker lo procesara sin bloquear el portal y publicara el resultado al terminar.`,
    );
  };

  const safeHandleUpload = async (event: FormEvent<HTMLFormElement>) => {
    try {
      await handleUpload(event);
    } catch (nextError) {
      setReportActionError(nextError instanceof Error ? nextError.message : "No se pudo subir el reporte");
    }
  };

  const safeHandleBackfill = async (event: FormEvent<HTMLFormElement>) => {
    try {
      await handleBackfill(event);
    } catch (nextError) {
      setReportActionError(nextError instanceof Error ? nextError.message : "No se pudo ejecutar el backfill");
    }
  };

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

      {reportActionError ? <div className="banner error">{reportActionError}</div> : null}
      {reportActionSuccess ? <div className="banner success">{reportActionSuccess}</div> : null}

      <ReportsTab companySlug={companySlug} reports={snapshot.reports} />

      {isAdmin ? (
        <section className="double-panel">
          <div className="panel">
            <h3>Subir o reemplazar informe PDF</h3>
            <p className="panel-copy">
              Esta accion ya vive en Reportes porque afecta a la empresa seleccionada, no al estado global del servidor.
            </p>
            <form className="form-grid" onSubmit={safeHandleUpload}>
              <label>
                Empresa
                <input value={company?.name ?? companySlug} readOnly />
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
            <p className="panel-copy">
              Usa esta recuperacion solo para la empresa seleccionada cuando haga falta traer historico puntual o reparar un hueco operativo.
            </p>
            <form className="form-grid" onSubmit={safeHandleBackfill}>
              <label>
                Empresa
                <input value={company?.name ?? companySlug} readOnly />
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
      ) : null}
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
  adminUsername: string;
  company: CompanySummary | null;
  enabled: boolean;
  onReloadSession: (preferredCompany?: string | null) => Promise<void>;
  selectedCompany: string | null;
  snapshotVersion: string | null;
  visibleCompanySlugs: string[];
}

type ActivationNoticeStatus = "submitting" | "running" | "ready" | "failed";

interface ActivationNotice {
  slug: string;
  name: string;
  status: ActivationNoticeStatus;
  message?: string;
}

function slugifyCompanyValue(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48);
}

function buildUniqueCompanySlug(
  fleet: FleetCandidate,
  usedSlugs: Set<string>,
) {
  const baseSlug =
    slugifyCompanyValue(fleet.fleet_name?.trim() || "") ||
    slugifyCompanyValue(fleet.fleet_id) ||
    fleet.fleet_id.toLowerCase();
  if (!usedSlugs.has(baseSlug)) {
    usedSlugs.add(baseSlug);
    return baseSlug;
  }
  let attempt = 2;
  while (usedSlugs.has(`${baseSlug}-${attempt}`)) {
    attempt += 1;
  }
  const candidate = `${baseSlug}-${attempt}`;
  usedSlugs.add(candidate);
  return candidate;
}

function AdminOperationsModule({
  adminUsername,
  company,
  enabled,
  onReloadSession,
  selectedCompany,
  snapshotVersion,
  visibleCompanySlugs,
}: AdminOperationsModuleProps) {
  const [status, setStatus] = useState<AdminIngestionStatus | null>(null);
  const [overview, setOverview] = useState<AdminOverview | null>(null);
  const [companyCatalog, setCompanyCatalog] = useState<AdminCompanyCatalog | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [companySaving, setCompanySaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [selectedFleetIds, setSelectedFleetIds] = useState<string[]>([]);
  const [candidatePasswords, setCandidatePasswords] = useState<Record<string, string>>({});
  const [activationNotices, setActivationNotices] = useState<ActivationNotice[]>([]);
  const [companyPasswordDrafts, setCompanyPasswordDrafts] = useState<Record<string, string>>({});
  const [adminPasswordDraft, setAdminPasswordDraft] = useState("");
  const [passwordSavingTarget, setPasswordSavingTarget] = useState<string | null>(null);
  const adminBootstrappedRef = useRef(false);
  const lastAdminSnapshotVersionRef = useRef<string | null>(null);
  const lastSessionSyncSignatureRef = useRef<string>("");
  const showAdminLastError = Boolean(status?.last_error && status.connection_state !== "connected");
  const catchupCooldownActive = Boolean(
    status?.last_catchup_error?.toLowerCase().includes("requests too frequent"),
  );
  const operationalRecency = overview?.operational_recency ?? status?.operational_recency ?? null;
  const activeCompanies = companyCatalog?.companies.filter((item) => item.operational) ?? [];
  const companyCatalogItems = companyCatalog?.companies ?? [];
  const readyCompanies = useMemo(
    () => activeCompanies.filter((item) => item.ready_in_selector),
    [activeCompanies],
  );
  const companiesPendingBootstrap = useMemo(
    () => activeCompanies.filter((item) => !item.ready_in_selector),
    [activeCompanies],
  );
  const readyCompanySignature = useMemo(
    () => readyCompanies.map((item) => item.slug).sort().join("|"),
    [readyCompanies],
  );
  const visibleCompanySignature = useMemo(
    () => [...visibleCompanySlugs].sort().join("|"),
    [visibleCompanySlugs],
  );
  const companyItemsBySlug = useMemo(
    () => new Map(companyCatalogItems.map((item) => [item.slug, item])),
    [companyCatalogItems],
  );
  const activationJobs = useMemo(
    () => companyCatalog?.activation_jobs ?? [],
    [companyCatalog?.activation_jobs],
  );
  const selectedActiveCompany = useMemo(
    () => activeCompanies.find((item) => item.slug === selectedCompany) ?? null,
    [activeCompanies, selectedCompany],
  );
  const selectedFleets = useMemo(
    () =>
      (companyCatalog?.fleet_candidates ?? []).filter(
        (item) => selectedFleetIds.includes(item.fleet_id) && !item.assigned_company_slug,
      ),
    [companyCatalog?.fleet_candidates, selectedFleetIds],
  );
  const selectableFleets = useMemo(
    () => (companyCatalog?.fleet_candidates ?? []).filter((item) => !item.assigned_company_slug),
    [companyCatalog?.fleet_candidates],
  );
  const busyActivationSlugs = useMemo(
    () => new Set(activationJobs.map((item) => item.slug)),
    [activationJobs],
  );
  const activationPlans = useMemo(() => {
    const usedSlugs = new Set((companyCatalog?.companies ?? []).map((item) => item.slug));
    return selectedFleets.map((fleet) => {
      const displayName = fleet.fleet_name?.trim() || fleet.fleet_id;
      const slug = buildUniqueCompanySlug(fleet, usedSlugs);
      return {
        fleet,
        displayName,
        slug,
        username: slug,
        password: candidatePasswords[fleet.fleet_id] ?? "",
      };
    });
  }, [candidatePasswords, companyCatalog?.companies, selectedFleets]);
  const activationPlanByFleetId = useMemo(
    () => new Map(activationPlans.map((plan) => [plan.fleet.fleet_id, plan])),
    [activationPlans],
  );
  const activationReady = activationPlans.length > 0 && activationPlans.every((plan) => plan.password.trim().length > 0);
  const hasActivationWork =
    activationNotices.some((item) => item.status === "submitting" || item.status === "running") ||
    activationJobs.some((item) => item.rebuild_status === "queued" || item.rebuild_status === "running");
  const formatRebuildStatus = useCallback((item: AdminCompanyCatalogItem) => {
    if (item.ready_in_selector) {
      return "Lista para selector";
    }
    if (item.rebuild_status === "queued" && item.rebuild_next_retry_at) {
      return "Esperando proveedor";
    }
    if (item.rebuild_status === "failed") {
      return "Reconstruccion con fallo";
    }
    if (item.rebuild_status === "running") {
      return "Reconstruyendo historico";
    }
    if (item.rebuild_status === "queued") {
      return "En cola de reconstruccion";
    }
    return "Pendiente de publicacion";
  }, []);

  const loadAdmin = useCallback(async (background = false) => {
    if (background) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    setError(null);
    try {
      const results = await Promise.allSettled([
        apiJson<AdminIngestionStatus>("/admin/ingestion/status").then((value) => {
          setStatus(value);
          return value;
        }),
        apiJson<AdminOverview>("/admin/overview").then((value) => {
          setOverview(value);
          return value;
        }),
        apiJson<AdminCompanyCatalog>("/admin/companies").then((value) => {
          setCompanyCatalog(value);
          return value;
        }),
      ]);
      const errors = results.map(settledError).filter(Boolean);
      setError(errors.length === results.length ? "No se pudo cargar administracion" : errors[0] ?? null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo cargar administracion");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    if (!enabled) return;
    if (!adminBootstrappedRef.current) {
      adminBootstrappedRef.current = true;
      lastAdminSnapshotVersionRef.current = snapshotVersion;
      void loadAdmin(false);
      return;
    }
    if (!snapshotVersion || snapshotVersion === lastAdminSnapshotVersionRef.current) {
      return;
    }
    lastAdminSnapshotVersionRef.current = snapshotVersion;
    void loadAdmin(true);
  }, [enabled, loadAdmin, snapshotVersion]);

  useEffect(() => {
    if (!enabled || !companyCatalog) return;
    if (readyCompanySignature === visibleCompanySignature) {
      lastSessionSyncSignatureRef.current = readyCompanySignature;
      return;
    }
    if (lastSessionSyncSignatureRef.current === readyCompanySignature) {
      return;
    }
    lastSessionSyncSignatureRef.current = readyCompanySignature;
    void onReloadSession(selectedCompany).catch(() => undefined);
  }, [
    companyCatalog,
    enabled,
    onReloadSession,
    readyCompanySignature,
    selectedCompany,
    visibleCompanySignature,
  ]);

  useEffect(() => {
    const candidates = companyCatalog?.fleet_candidates ?? [];
    setSelectedFleetIds((current) => current.filter((fleetId) => candidates.some((item) => item.fleet_id === fleetId && !item.assigned_company_slug)));
  }, [companyCatalog]);

  useEffect(() => {
    const selected = new Set(selectedFleetIds);
    setCandidatePasswords((current) =>
      Object.fromEntries(Object.entries(current).filter(([fleetId]) => selected.has(fleetId))),
    );
  }, [selectedFleetIds]);

  useEffect(() => {
    const activeSlugs = new Set(activeCompanies.map((item) => item.slug));
    setCompanyPasswordDrafts((current) =>
      Object.fromEntries(Object.entries(current).filter(([slug]) => activeSlugs.has(slug))),
    );
  }, [activeCompanies]);

  useEffect(() => {
    if (!enabled || !hasActivationWork) return;
    let cancelled = false;
    let timerId: number | null = null;

    const pollActivation = async () => {
      try {
        const nextCatalog = await apiJson<AdminCompanyCatalog>("/admin/companies");
        if (cancelled) return;
        setCompanyCatalog(nextCatalog);
        setActivationNotices((current) =>
          current.map((notice) => {
            const companyItem = nextCatalog.companies.find((item) => item.slug === notice.slug);
            const jobItem = nextCatalog.activation_jobs.find((item) => item.slug === notice.slug);
            const item = companyItem ?? jobItem;
            if (companyItem?.ready_in_selector) {
              return { ...notice, status: "ready", message: "Empresa lista y disponible en el selector." };
            }
            if (item?.rebuild_status === "failed") {
              return {
                ...notice,
                status: "failed",
                message: item.rebuild_error_message ?? "La reconstruccion historica termino con error.",
              };
            }
            return {
              ...notice,
              status: "running",
              message: "Reconstruyendo y validando el historico inicial.",
            };
          }),
        );
      } catch {
        // El seguimiento es auxiliar; el job continua aunque falle una consulta de progreso.
      } finally {
        if (!cancelled) {
          timerId = window.setTimeout(pollActivation, 1_000);
        }
      }
    };

    timerId = window.setTimeout(pollActivation, 350);
    return () => {
      cancelled = true;
      if (timerId !== null) window.clearTimeout(timerId);
    };
  }, [enabled, hasActivationWork]);

  useEffect(() => {
    if (!activationNotices.some((item) => item.status === "ready")) return;
    const timerId = window.setTimeout(() => {
      setActivationNotices((current) => current.filter((item) => item.status !== "ready"));
    }, 15_000);
    return () => window.clearTimeout(timerId);
  }, [activationNotices]);

  const toggleFleetSelection = (fleet: FleetCandidate) => {
    if (fleet.assigned_company_slug) {
      return;
    }
    setSelectedFleetIds((current) =>
      current.includes(fleet.fleet_id)
        ? current.filter((value) => value !== fleet.fleet_id)
        : [...current, fleet.fleet_id],
    );
  };

  const updateCandidatePassword = (fleetId: string, value: string) => {
    setCandidatePasswords((current) => ({
      ...current,
      [fleetId]: value,
    }));
  };

  const saveCompany = async () => {
    if (!activationPlans.length) {
      setError("Selecciona al menos una flota candidata para activarla.");
      return;
    }
    const missingPassword = activationPlans.find((plan) => !plan.password.trim());
    if (missingPassword) {
      setError(`Debes asignar una contrasena al usuario ${missingPassword.username} antes de activarlo.`);
      return;
    }
    const plansToActivate = [...activationPlans];
    setActivationNotices(
      plansToActivate.map((plan) => ({
        slug: plan.slug,
        name: plan.displayName,
        status: "submitting",
        message: "Registrando empresa y preparando la reconstruccion.",
      })),
    );
    try {
      setCompanySaving(true);
      setError(null);
      setSuccess(null);
      let nextCatalog = companyCatalog;
      for (const plan of plansToActivate) {
        nextCatalog = await apiJson<AdminCompanyCatalog>("/admin/companies", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            slug: plan.slug,
            name: plan.displayName,
            customer: plan.displayName,
            timezone: "America/Bogota",
            subdomain: null,
            fleet_ids: [plan.fleet.fleet_id],
            device_ids: [],
            notes: `Activada desde catalogo live (${plan.fleet.fleet_id})`,
            client_password: plan.password.trim(),
          }),
        });
        setCompanyCatalog(nextCatalog);
        const createdCompany = nextCatalog.companies.find((item) => item.slug === plan.slug);
        setActivationNotices((current) =>
          current.map((notice) =>
            notice.slug === plan.slug
              ? {
                  ...notice,
                  status: createdCompany?.ready_in_selector ? "ready" : "running",
                  message: createdCompany?.ready_in_selector
                    ? "Empresa lista y disponible en el selector."
                    : "Reconstruyendo y validando el historico inicial.",
                }
              : notice,
          ),
        );
      }
      if (nextCatalog) {
        setCompanyCatalog(nextCatalog);
      }
      setSelectedFleetIds([]);
      setCandidatePasswords({});
    } catch (nextError) {
      const message = nextError instanceof Error ? nextError.message : "No se pudo activar la empresa";
      setError(message);
      setActivationNotices((current) =>
        current.map((notice) =>
          notice.status === "ready" ? notice : { ...notice, status: "failed", message },
        ),
      );
    } finally {
      setCompanySaving(false);
    }
  };

  const deactivateCompany = async (targetCompany?: AdminCompanyCatalogItem | null) => {
    const companyToDeactivate = targetCompany ?? selectedActiveCompany;
    if (!companyToDeactivate) {
      setError("Selecciona o identifica una empresa activa antes de desactivarla.");
      return;
    }
    if (typeof window !== "undefined") {
      const confirmed = window.confirm(
        `Vas a desactivar ${companyToDeactivate.name}. Se eliminara su usuario cliente, su historial operativo local y dejara de aparecer en el selector superior hasta volverla a activar. ¿Continuar?`,
      );
      if (!confirmed) {
        return;
      }
    }
    try {
      setCompanySaving(true);
      setError(null);
      setSuccess(null);
      const nextCatalog = await apiJson<AdminCompanyCatalog>(
        `/admin/companies/${encodeURIComponent(companyToDeactivate.slug)}/deactivate`,
        {
          method: "POST",
        },
      );
      setCompanyCatalog(nextCatalog);
      setSuccess(
        `La desactivacion de ${companyToDeactivate.name} quedo encolada. El worker retirara datos, usuario y selector de forma auditada sin bloquear la consola.`,
      );
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo desactivar la empresa");
    } finally {
      setCompanySaving(false);
    }
  };

  const changeAdminPassword = async () => {
    if (!adminPasswordDraft.trim()) {
      setError("Escribe la nueva contrasena del administrador antes de guardarla.");
      return;
    }
    try {
      setPasswordSavingTarget("admin");
      setError(null);
      setSuccess(null);
      await apiJson("/admin/users/admin/password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ new_password: adminPasswordDraft.trim() }),
      });
      setAdminPasswordDraft("");
      setSuccess("La contrasena del administrador quedo actualizada.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo actualizar la contrasena del administrador");
    } finally {
      setPasswordSavingTarget(null);
    }
  };

  const changeCompanyPassword = async (companySlug: string) => {
    const nextPassword = companyPasswordDrafts[companySlug]?.trim() ?? "";
    if (!nextPassword) {
      setError("Debes escribir la nueva contrasena de la empresa antes de guardarla.");
      return;
    }
    try {
      setPasswordSavingTarget(`company:${companySlug}`);
      setError(null);
      setSuccess(null);
      await apiJson("/admin/users/company/password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ company_slug: companySlug, new_password: nextPassword }),
      });
      setCompanyPasswordDrafts((current) => ({
        ...current,
        [companySlug]: "",
      }));
      const companyName = activeCompanies.find((item) => item.slug === companySlug)?.name ?? companySlug;
      setSuccess(`La contrasena de ${companyName} quedo actualizada.`);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo actualizar la contrasena de la empresa");
    } finally {
      setPasswordSavingTarget(null);
    }
  };

  return (
    <main className="page-grid">
      <section className="panel">
        <h3>Centro de control operativo</h3>
        <p className="panel-copy">
          Aqui supervisas la salud general del servicio, la publicacion y la cobertura live. El selector no cambia
          este resumen global: solo aplica a Diagnostico, Dashboard cliente, Reportes y a las acciones manuales de la
          empresa elegida.
        </p>
        {refreshing ? <p className="panel-copy">Actualizando administracion en segundo plano...</p> : null}
      </section>

      {error ? <div className="banner error">{error}</div> : null}
      {success ? <div className="banner success">{success}</div> : null}
      {overview?.active_notes.map((note) => (
        <div key={`${note.title}-${note.start_date}`} className={`banner ${note.severity === "critical" ? "error" : ""}`}>
          <strong>{note.title}.</strong> {note.message}
        </div>
      ))}

      <section className="double-panel">
        <div className="panel">
          <h3>Estado de publicacion</h3>
          <div className="key-value-list">
            <div className="key-value-row">
              <span>Host publico</span>
              <strong>{overview?.publication.dashboard_host ?? "sin configurar"}</strong>
            </div>
            <div className="key-value-row">
              <span>DNS</span>
              <strong>{overview?.publication.dns_status ?? "sin datos"}</strong>
            </div>
            <div className="key-value-row">
              <span>Dashboard URL</span>
              <strong>{overview?.publication.dashboard_url ?? "solo local"}</strong>
            </div>
            <div className="key-value-row">
              <span>API URL</span>
              <strong>{overview?.publication.api_url ?? "solo local"}</strong>
            </div>
          </div>
          <div className="panel-copy" style={{ marginTop: "1rem" }}>
            {overview?.publication.message ?? "Sin diagnostico de publicacion disponible."}
          </div>
          {overview?.publication.resolved_targets.length ? (
            <div className="chip-row" style={{ marginTop: "0.8rem" }}>
              {overview.publication.resolved_targets.map((target) => (
                <span key={target} className="chip">
                  {target}
                </span>
              ))}
            </div>
          ) : null}
        </div>

        <div className="panel">
          <h3>Estado de datos live</h3>
          <div className="key-value-list">
            <div className="key-value-row">
              <span>Conexion Howen</span>
              <strong>{status?.connection_state ?? "sin datos"}</strong>
            </div>
            <div className="key-value-row">
              <span>Semaforo operativo</span>
              <strong>{formatFeedStatusLabel(overview?.feed.status ?? "sin_datos")}</strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo ciclo</span>
              <strong>{status?.last_cycle_received_at ? formatDateTime(status.last_cycle_received_at, company?.timezone) : "sin datos"}</strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo DMS raw</span>
              <strong>{operationalRecency?.last_raw_dms_at ? formatDateTime(operationalRecency.last_raw_dms_at, company?.timezone) : "sin DMS raw reciente"}</strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo DMS aceptado</span>
              <strong>{operationalRecency?.last_accepted_dms_at ? formatDateTime(operationalRecency.last_accepted_dms_at, company?.timezone) : "sin DMS aceptado reciente"}</strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo DMS visible</span>
              <strong>{operationalRecency?.last_visible_dms_at ? formatDateTime(operationalRecency.last_visible_dms_at, company?.timezone) : "sin DMS visible reciente"}</strong>
            </div>
            <div className="key-value-row">
              <span>Pendientes manuales</span>
              <strong>{operationalRecency?.pending_review_count ?? 0}</strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo caso pendiente</span>
              <strong>
                {operationalRecency?.last_pending_review_at
                  ? `${formatDateTime(operationalRecency.last_pending_review_at, company?.timezone)} · ${operationalRecency.latest_pending_plate ?? operationalRecency.latest_pending_reason ?? "revision"}`
                  : "sin pendientes manuales"}
              </strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo catchup exitoso</span>
              <strong>
                {status?.last_successful_catchup_cursor_at
                  ? formatDateTime(status.last_successful_catchup_cursor_at, company?.timezone)
                  : "sin catchup exitoso aun"}
              </strong>
            </div>
            <div className="key-value-row">
              <span>Hueco pendiente</span>
              <strong>
                {status?.pending_range_start_at && status?.pending_range_end_at
                  ? `${formatDateTime(status.pending_range_start_at, company?.timezone)} -> ${formatDateTime(status.pending_range_end_at, company?.timezone)}`
                  : "sin backlog pendiente"}
              </strong>
            </div>
            <div className="key-value-row">
              <span>Ultimo intento de catchup</span>
              <strong>{status?.last_catchup_attempt_at ? formatDateTime(status.last_catchup_attempt_at, company?.timezone) : "sin intento"}</strong>
            </div>
            <div className="key-value-row">
              <span>Proximo retry catchup</span>
              <strong>{status?.next_catchup_retry_at ? formatDateTime(status.next_catchup_retry_at, company?.timezone) : "sin cooldown activo"}</strong>
            </div>
            <div className="key-value-row">
              <span>Racha rate limit</span>
              <strong>{status?.catchup_rate_limit_streak ?? 0}</strong>
            </div>
          </div>
          <div className="panel-copy" style={{ marginTop: "1rem" }}>
            Esta seccion separa la salud de la ingesta live del estado del sitio publico para no confundir una caida del VPS con un problema de datos.
          </div>
          <div className="panel-copy" style={{ marginTop: "0.65rem" }}>
            La conciliacion exacta sigue siendo manual y supervisada: solo conviene correrla cuando aparezca un hueco pendiente, un alias de placa, una diferencia contra Howen o un error que quieras explicar antes de tocar el dashboard.
          </div>
          {catchupCooldownActive && status?.next_catchup_retry_at ? (
            <div className="panel-copy" style={{ marginTop: "0.65rem" }}>
              Catchup historico en cooldown por rate limit del proveedor. Proximo retry: {formatDateTime(status.next_catchup_retry_at, company?.timezone)}.
            </div>
          ) : null}
        </div>
      </section>

      <section className="metric-grid three">
        <MetricCard
          label="Modo operativo"
          value={overview?.ingest_mode ?? "live"}
          detail="cortes historicos de 15 min + status websocket"
        />
        <MetricCard
          label="Ultimo ciclo recibido"
          value={status?.last_cycle_received_at ? formatDateTime(status.last_cycle_received_at) : "sin datos"}
          detail={
            operationalRecency?.last_visible_dms_at
              ? `Ultimo DMS visible: ${formatDateTime(operationalRecency.last_visible_dms_at)}`
              : operationalRecency?.last_raw_dms_at
                ? `Ultimo DMS raw: ${formatDateTime(operationalRecency.last_raw_dms_at)}`
                : status?.last_event_observed_at
                  ? `Ultimo evento Howen: ${formatDateTime(status.last_event_observed_at)}`
                : "sin evento"
          }
        />
        <MetricCard
          label="Cobertura 24h"
          value={
            overview
              ? `${overview.coverage.vehicles_reporting_status_24h}/${overview.coverage.total_vehicles}`
              : loading
                ? "..."
                : "sin datos"
          }
          detail={
            overview
              ? `${overview.coverage.vehicles_with_dms_alarm_24h} con DMS en 24h · ${overview.coverage.stale_vehicles} atrasados`
              : "Vehiculos con recepcion reciente"
          }
        />
        <MetricCard
          label="Ultimo sync de vehiculos"
          value={status?.last_device_sync_at ? formatDateTime(status.last_device_sync_at) : "pendiente"}
          detail={status?.last_live_alarm_message_at ? `Ultimo mensaje 80004: ${formatDateTime(status.last_live_alarm_message_at)}` : "sin mensaje 80004"}
        />
        <MetricCard
          label="Hoy provisional"
          tone="amber"
          value={overview ? formatKm(overview.km.current_day_km_provisional) : loading ? "..." : "-"}
          detail={
            overview
              ? `Ventana cerrada: ${formatKm(overview.km.closed_window_km)} · validos: ${overview.coverage.vehicles_with_valid_day_km_today}/${overview.coverage.total_vehicles}`
              : "KM del dia visible en dashboard"
          }
        />
        <MetricCard
          label="DMS cosechados 24h"
          tone={(status?.future_rejected_count_24h ?? 0) || (status?.catchup_failures_24h ?? 0) ? "danger" : "white"}
          value={`${status?.raw_dms_count_24h ?? 0} DMS`}
          detail={`harvest ${status?.raw_dms_count_24h ?? 0} · no DMS ${status?.non_dms_count_24h ?? 0} · futuros ${status?.future_rejected_count_24h ?? 0}`}
        />
        <MetricCard
          label="Corte historico actual"
          value={overview?.alarmHarvest?.currentCutAt ? formatDateTime(overview.alarmHarvest.currentCutAt, company?.timezone) : (loading ? "..." : "sin corte")}
          detail={
            overview?.alarmHarvest
              ? `${overview.alarmHarvest.completedCompanies} empresas al dia · ${overview.alarmHarvest.delayedCompanies} atrasadas`
              : "Estado global de cortes de 15 minutos"
          }
        />
        <MetricCard
          label="Cortes 15 min activos"
          value={
            overview?.alarmHarvest
              ? `${overview.alarmHarvest.runningCuts + overview.alarmHarvest.queuedCuts}`
              : (loading ? "..." : "0")
          }
          detail={
            overview?.alarmHarvest
              ? `${overview.alarmHarvest.runningCuts} corriendo · ${overview.alarmHarvest.queuedCuts} esperando proveedor`
              : "Cola operativa de cortes"
          }
        />
        <MetricCard
          label="Reconstrucciones activas"
          value={
            overview?.alarmHarvest
              ? `${overview.alarmHarvest.activeRebuilds + overview.alarmHarvest.queuedRebuilds}`
              : (loading ? "..." : "0")
          }
          detail={
            overview?.alarmHarvest
              ? `${overview.alarmHarvest.activeRebuilds} corriendo · ${overview.alarmHarvest.queuedRebuilds} en espera`
              : "Bootstrap historico de empresas"
          }
        />
        <MetricCard
          label="Cola durable del worker"
          tone={(overview?.backgroundJobs?.failed ?? 0) || (overview?.backgroundJobs?.stale_running ?? 0) ? "danger" : "white"}
          value={
            overview?.backgroundJobs
              ? `${overview.backgroundJobs.running + overview.backgroundJobs.queued}`
              : (loading ? "..." : "0")
          }
          detail={
            overview?.backgroundJobs
              ? `${overview.backgroundJobs.running} ejecutando · ${overview.backgroundJobs.queued} en espera · ${overview.backgroundJobs.failed} fallidos históricos${overview.backgroundJobs.stale_running ? ` · ${overview.backgroundJobs.stale_running} sin heartbeat` : ""}`
              : "Jobs persistidos con lease y reintentos"
          }
        />
      </section>

      {showAdminLastError ? <div className="banner error">Ultimo error de la ingesta: {status?.last_error}</div> : null}
      {!showAdminLastError && status?.connection_state && status.connection_state !== "connected" && overview?.feed.status === "al_dia" ? (
        <div className="banner">La ultima captura sigue disponible, pero la sesion live aun esta en proceso de reconexion.</div>
      ) : null}

      <section className="panel">
        <div className="panel-headline">
          <div>
            <div className="panel-kicker">Empresas operativas</div>
            <h3>Activacion desde flotas detectadas</h3>
            <p className="panel-subtitle">
              Este bloque usa la lista que llega del proveedor. Puedes marcar varias candidatas y activarlas juntas. Solo apareceran en el selector superior cuando termine su reconstruccion historica inicial.
            </p>
          </div>
          <span className="chip">
            {readyCompanies.length} listas · {companiesPendingBootstrap.length} en activacion · {companyCatalog?.total_companies ?? 0} registradas
          </span>
        </div>

        {readyCompanies.length ? (
          <div className="chip-row" style={{ marginTop: "1rem" }}>
            {readyCompanies.map((item) => (
              <span key={item.slug} className={`chip ${item.slug === selectedCompany ? "active" : ""}`}>
                {item.name}
              </span>
            ))}
          </div>
        ) : null}

        {activationNotices.length ? (
          <div className="stack" style={{ marginTop: "1rem" }}>
            {activationNotices.map((notice) => (
              <div
                key={notice.slug}
                className={`banner ${notice.status === "failed" ? "error" : notice.status === "ready" ? "success" : ""}`}
              >
                <strong>{notice.name}</strong>
                {` · ${notice.message ?? "Preparando activacion."}`}
              </div>
            ))}
          </div>
        ) : null}

        {activationJobs.length ? (
          <section className="panel compact" style={{ marginTop: "1rem" }}>
            <div className="panel-head">
              <strong>Activaciones en progreso</strong>
              <span className="chip">{activationJobs.length}</span>
            </div>
            <div className="stack" style={{ marginTop: "0.85rem" }}>
              {activationJobs.map((item) => {
                const hasRowProgress = (item.rebuild_rows_total ?? 0) > 0;
                const progressLabel = hasRowProgress
                  ? `${item.rebuild_rows_processed ?? 0}/${item.rebuild_rows_total ?? 0} eventos`
                  : item.rebuild_days_total > 0
                    ? `${item.rebuild_days_done}/${item.rebuild_days_total} dias`
                    : "preparando reconstruccion";
                return (
                  <div key={item.slug} className="panel compact" style={{ padding: "0.95rem 1rem" }}>
                    <div className="panel-head">
                      <strong>{item.name}</strong>
                      <div className="chip-row">
                        <span className={`tone-pill ${item.rebuild_status === "failed" ? "danger" : "warning"}`}>
                          {formatRebuildStatus(item)}
                        </span>
                        {item.can_deactivate ? (
                          <button
                            className="ghost-btn"
                            type="button"
                            onClick={() => void deactivateCompany(item)}
                            disabled={companySaving}
                          >
                            <Building2 size={16} />
                            Desactivar
                          </button>
                        ) : null}
                      </div>
                    </div>
                    <div className="panel-copy" style={{ marginTop: "0.7rem" }}>
                      {progressLabel}
                      {item.rebuild_progress_pct !== null ? ` · ${item.rebuild_progress_pct}%` : ""} · aun no aparece en el selector superior.
                    </div>
                    {item.rebuild_progress_pct !== null ? (
                      <div style={{ marginTop: "0.7rem", height: "0.5rem", borderRadius: "999px", background: "rgba(148, 163, 184, 0.16)", overflow: "hidden" }}>
                        <div
                          style={{
                            width: `${Math.min(100, Math.max(0, item.rebuild_progress_pct))}%`,
                            height: "100%",
                            borderRadius: "999px",
                            background: item.rebuild_status === "failed" ? "var(--danger)" : "var(--accent)",
                          }}
                        />
                      </div>
                    ) : null}
                    <div className="panel-copy" style={{ marginTop: "0.7rem" }}>
                      {item.rebuild_next_retry_at
                        ? `Reintento programado: ${formatDateTime(item.rebuild_next_retry_at, item.timezone)}`
                        : `Inicio ${item.rebuild_started_at ? formatDateTime(item.rebuild_started_at, item.timezone) : "pendiente"} · Publicacion final ${
                            item.rebuild_published_cut_at ? formatDateTime(item.rebuild_published_cut_at, item.timezone) : "aun no disponible"
                          }`}
                    </div>
                    {item.rebuild_error_message ? (
                      <div className="panel-copy" style={{ marginTop: "0.7rem", color: "var(--danger)" }}>
                        {item.rebuild_error_message}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </section>
        ) : null}

        <section className="panel" style={{ marginTop: "1rem" }}>
          <div className="panel-headline">
            <div>
              <div className="panel-head">
                <strong>Flotas candidatas detectadas</strong>
                <span className="chip">{selectableFleets.length} disponibles</span>
              </div>
              <p className="panel-copy" style={{ marginTop: "0.75rem" }}>
                Haz click sobre las tarjetas para marcar una o varias candidatas. Las ya activadas quedan solo como referencia y no se vuelven a seleccionar.
              </p>
            </div>
            <div style={{ minWidth: "20rem", maxWidth: "100%" }}>
              <div className="chip-row" style={{ justifyContent: "flex-end" }}>
                <span className="chip">{selectedFleets.length} marcadas</span>
                <span className="chip">{selectableFleets.length} disponibles</span>
              </div>
              <div className="panel-copy" style={{ marginTop: "0.75rem", textAlign: "right" }}>
                Marca las flotas que quieras activar. La contrasena del cliente se define dentro de cada tarjeta marcada.
              </div>
              <div className="toolbar admin-toolbar" style={{ marginTop: "0.9rem", justifyContent: "flex-end" }}>
                <button className="primary-btn" type="button" onClick={() => void saveCompany()} disabled={companySaving || !activationReady}>
                  <Building2 size={16} />
                  {companySaving
                    ? "Activando empresas..."
                    : activationPlans.length <= 1
                      ? "Activar empresa"
                      : `Activar ${activationPlans.length} empresas`}
                </button>
              </div>
            </div>
          </div>

          {companyCatalog?.fleet_candidates.length ? (
            <div className="stack" style={{ marginTop: "1rem" }}>
              {companyCatalog.fleet_candidates.map((fleet) => {
                const isSelected = selectedFleetIds.includes(fleet.fleet_id);
                const isAssigned = Boolean(fleet.assigned_company_slug);
                const candidateSlug =
                  slugifyCompanyValue(fleet.fleet_name?.trim() || "") ||
                  slugifyCompanyValue(fleet.fleet_id) ||
                  fleet.fleet_id.toLowerCase();
                const isBusy = busyActivationSlugs.has(candidateSlug);
                const assignedCompanyItem = fleet.assigned_company_slug
                  ? companyItemsBySlug.get(fleet.assigned_company_slug)
                  : null;
                const plan = activationPlanByFleetId.get(fleet.fleet_id);
                return (
                  <div
                    key={fleet.fleet_id}
                    className="panel compact company-candidate-card"
                    role={isAssigned ? undefined : "button"}
                    tabIndex={isAssigned ? -1 : 0}
                    aria-pressed={isAssigned ? undefined : isSelected}
                    onClick={() => toggleFleetSelection(fleet)}
                    onKeyDown={(event) => {
                      if (isAssigned) return;
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        toggleFleetSelection(fleet);
                      }
                    }}
                    style={{
                      opacity: isAssigned ? 0.7 : 1,
                      borderColor: isSelected ? "rgba(16, 185, 129, 0.95)" : undefined,
                      boxShadow: isSelected ? "0 0 0 1px rgba(16, 185, 129, 0.35)" : undefined,
                      cursor: isAssigned ? "default" : "pointer",
                    }}
                  >
                    <div className="company-candidate-main">
                      <div className="panel-head">
                        <strong>{fleet.fleet_name ?? fleet.fleet_id}</strong>
                        <div className="chip-row">
                          {isSelected ? <span className="chip active">Marcada</span> : null}
                          {fleet.assigned_company_name ? (
                            assignedCompanyItem?.ready_in_selector ? (
                              <span className="tone-pill success">Lista en {fleet.assigned_company_name}</span>
                            ) : assignedCompanyItem?.rebuild_status === "failed" ? (
                              <span className="tone-pill danger">Requiere reconstruccion</span>
                            ) : (
                              <span className="tone-pill warning">En activacion en {fleet.assigned_company_name}</span>
                            )
                          ) : isBusy ? (
                            <span className="tone-pill warning">Reconstruyendo historico</span>
                          ) : (
                            <span className="tone-pill">Disponible</span>
                          )}
                        </div>
                      </div>
                      <div className="chip-row" style={{ marginTop: "0.7rem" }}>
                        <span className="chip">Fleet ID {fleet.fleet_id}</span>
                        <span className="chip">{fleet.total_devices} devices</span>
                        <span className="chip">{fleet.devices_seen_24h} vistos 24h</span>
                        <span className="chip">{fleet.alarm_events_7d} alarmas 7d</span>
                      </div>
                      <div className="panel-copy" style={{ marginTop: "0.7rem" }}>
                        Placas: {fleet.sample_plates.join(", ") || "sin muestra"} · Ultimo seen{" "}
                        {fleet.latest_seen_at ? formatDateTime(fleet.latest_seen_at, company?.timezone) : "sin dato"} · Ultima alarma{" "}
                        {fleet.latest_alarm_at ? formatDateTime(fleet.latest_alarm_at, company?.timezone) : "sin dato"}
                      </div>
                      {isSelected && plan && !isAssigned ? (
                        <div
                          className="company-candidate-inline-credentials"
                          onClick={(event) => event.stopPropagation()}
                          onKeyDown={(event) => event.stopPropagation()}
                        >
                          <div className="panel-kicker">Credenciales del cliente</div>
                          <div className="panel-copy">
                            Esta empresa se crea con su usuario cliente y no se puede activar sin contrasena.
                          </div>
                          <div className="form-grid" style={{ marginTop: "0.85rem" }}>
                            <label>
                              Usuario
                              <input value={plan.username} readOnly />
                            </label>
                            <label>
                              Contrasena obligatoria
                              <input
                                type="password"
                                value={plan.password}
                                placeholder="Define la contrasena del cliente"
                                onChange={(event) => updateCandidatePassword(plan.fleet.fleet_id, event.target.value)}
                              />
                            </label>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="empty-card" style={{ marginTop: "1rem" }}>
              <div className="empty-title">Aun no hay candidatas detectadas</div>
              <div className="empty-copy">
                Cuando Howen y la base local sincronicen mas flotas reales, apareceran aqui listas para activar.
              </div>
            </div>
          )}
        </section>

        <section className="panel" style={{ marginTop: "1rem" }}>
          <div className="panel-headline">
            <div>
              <div className="panel-kicker">Credenciales activas</div>
              <h3>Gestion de contrasenas</h3>
              <p className="panel-subtitle">
                Desde aqui el administrador cambia su propia contrasena y tambien la de cada empresa registrada, aunque siga terminando su bootstrap historico.
              </p>
            </div>
            <span className="chip">{activeCompanies.length} empresas registradas</span>
          </div>

          <div className="double-panel" style={{ marginTop: "1rem" }}>
            <div className="panel compact">
              <div className="panel-head">
                <strong>Administrador</strong>
                <span className="chip active">{adminUsername}</span>
              </div>
              <div className="form-grid" style={{ marginTop: "0.85rem" }}>
                <label>
                  Usuario
                  <input value={adminUsername} readOnly />
                </label>
                <label>
                  Nueva contrasena
                  <input
                    type="password"
                    value={adminPasswordDraft}
                    placeholder="Nueva contrasena del admin"
                    onChange={(event) => setAdminPasswordDraft(event.target.value)}
                  />
                </label>
              </div>
              <div className="toolbar admin-toolbar" style={{ marginTop: "1rem", justifyContent: "flex-end" }}>
                <button
                  className="primary-btn"
                  type="button"
                  onClick={() => void changeAdminPassword()}
                  disabled={passwordSavingTarget === "admin" || !adminPasswordDraft.trim()}
                >
                  <Shield size={16} />
                  {passwordSavingTarget === "admin" ? "Guardando..." : "Guardar contrasena admin"}
                </button>
              </div>
            </div>

            <div className="panel compact">
              <div className="panel-head">
                <strong>Empresas registradas</strong>
                <span className="chip">{activeCompanies.length}</span>
              </div>
              {activeCompanies.length ? (
                <div className="stack" style={{ marginTop: "0.85rem" }}>
                  {activeCompanies.map((item) => (
                    <div key={item.slug} className="panel compact" style={{ padding: "0.95rem 1rem" }}>
                      <div className="panel-head">
                        <strong>{item.name}</strong>
                        <div className="chip-row">
                          <span className="chip active">{item.slug}</span>
                          <span className={`tone-pill ${item.ready_in_selector ? "success" : item.rebuild_status === "failed" ? "danger" : "warning"}`}>
                            {item.ready_in_selector ? "Lista en selector" : formatRebuildStatus(item)}
                          </span>
                        </div>
                      </div>
                      <div className="form-grid" style={{ marginTop: "0.8rem" }}>
                        <label>
                          Usuario cliente
                          <input value={item.slug} readOnly />
                        </label>
                        <label>
                          Nueva contrasena
                          <input
                            type="password"
                            value={companyPasswordDrafts[item.slug] ?? ""}
                            placeholder={`Nueva contrasena para ${item.name}`}
                            onChange={(event) =>
                              setCompanyPasswordDrafts((current) => ({
                                ...current,
                                [item.slug]: event.target.value,
                              }))
                            }
                          />
                        </label>
                      </div>
                      <div className="toolbar admin-toolbar" style={{ marginTop: "0.9rem", justifyContent: "flex-end" }}>
                        <button
                          className="ghost-btn"
                          type="button"
                          onClick={() => void deactivateCompany(item)}
                          disabled={companySaving || passwordSavingTarget === `company:${item.slug}`}
                        >
                          <Building2 size={16} />
                          Desactivar empresa
                        </button>
                        <button
                          className="ghost-btn"
                          type="button"
                          onClick={() => void changeCompanyPassword(item.slug)}
                          disabled={
                            companySaving ||
                            passwordSavingTarget === `company:${item.slug}` ||
                            !(companyPasswordDrafts[item.slug] ?? "").trim()
                          }
                        >
                          <Shield size={16} />
                          {passwordSavingTarget === `company:${item.slug}` ? "Guardando..." : "Actualizar contrasena"}
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-card" style={{ marginTop: "0.85rem" }}>
                  <div className="empty-title">Todavia no hay empresas activas</div>
                  <div className="empty-copy">
                    Cuando actives una flota candidata, aparecera aqui para que puedas cambiar su contrasena despues.
                  </div>
                </div>
              )}
            </div>
          </div>
        </section>
      </section>
    </main>
  );
}

interface AdminAuditModuleProps {
  company: CompanySummary | null;
  enabled: boolean;
  selectedCompany: string;
  snapshot: DashboardSnapshot | null;
  snapshotVersion: string | null;
}

function AdminAuditModule({
  company,
  enabled,
  selectedCompany,
  snapshot,
  snapshotVersion,
}: AdminAuditModuleProps) {
  const [selectedWindowMode, setSelectedWindowMode] = useState<DiagnosticWindowMode>("24h");
  const [audit, setAudit] = useState<AdminAudit | null>(null);
  const [anomalies, setAnomalies] = useState<IngestionAnomaly[]>([]);
  const [vehicles, setVehicles] = useState<AdminVehicle[]>([]);
  const [detailLiveSetup, setDetailLiveSetup] = useState<AdminLiveSetup | null>(null);
  const [kmQuality, setKmQuality] = useState<KmQualitySummary | null>(null);
  const [reviewQueue, setReviewQueue] = useState<ReconciliationReviewItem[]>([]);
  const [reviewQueueTotal, setReviewQueueTotal] = useState(0);
  const [reviewFilteredTotal, setReviewFilteredTotal] = useState(0);
  const [reviewPage, setReviewPage] = useState(1);
  const [reviewTotalPages, setReviewTotalPages] = useState(0);
  const [reviewLoading, setReviewLoading] = useState(false);
  const [reviewCountsByAction, setReviewCountsByAction] = useState<Record<string, number>>({});
  const [reviewCountsByReason, setReviewCountsByReason] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [rebuildLoading, setRebuildLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [loadedDetailKey, setLoadedDetailKey] = useState<string | null>(null);
  const [selectedReviewBuckets, setSelectedReviewBuckets] = useState<string[]>([]);
  const [selectedReviewReasons, setSelectedReviewReasons] = useState<string[]>([]);
  const [selectedReviewIds, setSelectedReviewIds] = useState<number[]>([]);
  const auditWindowCacheRef = useRef<Map<string, DiagnosticAuditCacheEntry>>(new Map());
  const kmQualityCacheRef = useRef<Map<string, { loadedAt: number; snapshotVersion: string | null; value: KmQualitySummary }>>(
    new Map(),
  );
  const activeAuditRequestRef = useRef(0);
  const activeReviewRequestRef = useRef(0);
  const auditBootstrappedRef = useRef(false);
  const lastAuditSnapshotVersionRef = useRef<string | null>(null);
  const timezoneName = company?.timezone ?? "America/Bogota";
  const publishedReferenceIso =
    snapshot?.meta.companySlug === selectedCompany
      ? snapshot.meta.publishedCutAt ?? snapshot.meta.generatedAt
      : null;
  const currentAuditMonth = useMemo(
    () => buildAuditMonthValue(timezoneName, publishedReferenceIso),
    [publishedReferenceIso, timezoneName],
  );
  const monthRange = useMemo(
    () => buildAuditMonthRange(currentAuditMonth, timezoneName, publishedReferenceIso),
    [currentAuditMonth, publishedReferenceIso, timezoneName],
  );
  const windowRanges = useMemo(
    () => ({
      "24h": buildDiagnosticRange(
        "24h",
        timezoneName,
        publishedReferenceIso,
        snapshot?.meta.companySlug === selectedCompany ? snapshot.meta : null,
      ),
      "7d": buildDiagnosticRange(
        "7d",
        timezoneName,
        publishedReferenceIso,
        snapshot?.meta.companySlug === selectedCompany ? snapshot.meta : null,
      ),
      month: buildDiagnosticRange(
        "month",
        timezoneName,
        publishedReferenceIso,
        snapshot?.meta.companySlug === selectedCompany ? snapshot.meta : null,
      ),
    }),
    [publishedReferenceIso, selectedCompany, snapshot, timezoneName],
  );
  const range = windowRanges[selectedWindowMode];
  const monthStartDate = useMemo(() => dayjs.tz(monthRange.from, timezoneName).format("YYYY-MM-DD"), [monthRange.from, timezoneName]);
  const monthEndDate = useMemo(() => dayjs.tz(monthRange.to, timezoneName).format("YYYY-MM-DD"), [monthRange.to, timezoneName]);
  const detailCacheKey = useMemo(
    () => `${selectedCompany}:${selectedWindowMode}:${range.from}:${range.to}`,
    [range.from, range.to, selectedCompany, selectedWindowMode],
  );
  const buildWindowCacheKey = useCallback(
    (windowMode: DiagnosticWindowMode) => {
      const windowRange = windowRanges[windowMode];
      return `${selectedCompany}:${windowMode}:${windowRange.from}:${windowRange.to}`;
    },
    [selectedCompany, windowRanges],
  );
  const pendingVisibilityCount = useMemo(
    () => reviewCountsByAction.review_visibility ?? 0,
    [reviewCountsByAction],
  );
  const pendingRawCount = useMemo(
    () => reviewCountsByAction.review_raw ?? 0,
    [reviewCountsByAction],
  );
  const pendingAnomalyCount = useMemo(
    () => reviewCountsByAction.review_anomaly ?? 0,
    [reviewCountsByAction],
  );
  const pendingKmCount = useMemo(
    () => reviewCountsByAction.review_km ?? 0,
    [reviewCountsByAction],
  );
  const kmExceptionVehicles = useMemo(() => {
    const flagged = new Set([
      ...(kmQuality?.sample_invalid_vehicles ?? []),
      ...(kmQuality?.sample_total_regression_vehicles ?? []),
      ...(kmQuality?.sample_missing_day_km_vehicles ?? []),
    ]);
    return vehicles.filter((vehicle) => flagged.has(vehicle.plate_no ?? vehicle.device_id));
  }, [kmQuality, vehicles]);
  const missingDayKmCount = useMemo(() => {
    if (!kmQuality) return 0;
    return Math.max(
      0,
      kmQuality.total_vehicles - kmQuality.vehicles_with_valid_day_km - kmQuality.vehicles_with_invalid_day_km,
    );
  }, [kmQuality]);
  const activeWindowMetrics = audit?.requested_window ?? null;
  const reviewFilterOptions = useMemo(
    () => [
      { key: "review_visibility", label: "Reglas cliente", count: pendingVisibilityCount },
      { key: "review_anomaly", label: "Anomalias", count: pendingAnomalyCount },
      { key: "review_raw", label: "Flujo DMS", count: pendingRawCount },
      { key: "review_km", label: "Km", count: pendingKmCount },
    ],
    [pendingAnomalyCount, pendingKmCount, pendingRawCount, pendingVisibilityCount],
  );
  const reviewReasonOptions = useMemo(
    () =>
      Object.entries(reviewCountsByReason)
        .sort((left, right) => right[1] - left[1])
        .map(([reason, count]) => ({
          key: reason,
          label: formatAuditReason(reason),
          count,
        })),
    [reviewCountsByReason],
  );
  const filteredReviewQueue = reviewQueue;
  const activeReviewFilterLabel = useMemo(() => {
    const actionLabel =
      selectedReviewBuckets.length === 0
        ? "Todas las categorias"
        : reviewFilterOptions
            .filter((option) => selectedReviewBuckets.includes(option.key))
            .map((option) => option.label)
            .join(" · ");
    const reasonLabel =
      selectedReviewReasons.length === 0
        ? "Todos los motivos"
        : reviewReasonOptions
            .filter((option) => selectedReviewReasons.includes(option.key))
            .map((option) => option.label)
            .join(" · ");
    return `${actionLabel} · ${reasonLabel}`;
  }, [reviewFilterOptions, reviewReasonOptions, selectedReviewBuckets, selectedReviewReasons]);
  const supportFlowChart = useMemo(() => {
    const requested = audit?.requested_window;
    const labels = ["DMS recibidos", "Analiticos", "Episodios visibles", "Fusionados", "Retenidos", "Descartados"];
    const values = requested
      ? [
          requested.received_dms,
          requested.analytic_dms,
          requested.visible_episodes,
          requested.fused_detections,
          requested.retained_for_review,
          requested.discarded_by_admin,
        ]
      : [0, 0, 0, 0, 0, 0];
    return {
      hasData: values.some((value) => value > 0),
      data: {
        labels,
        datasets: [
          {
            label: "Eventos de la ventana",
            data: values,
            backgroundColor: ["#10b981", "#38bdf8", "#f59e0b", "#64748b", "#a855f7", "#ef4444"],
            borderRadius: 10,
          },
        ],
      },
    };
  }, [audit]);
  const reviewReasonChart = useMemo(() => {
    const entries = Object.entries(reviewCountsByReason)
      .sort((left, right) => right[1] - left[1])
      .slice(0, 6);
    return {
      hasData: entries.length > 0,
      data: {
        labels: entries.map(([reason]) => formatAuditReason(reason)),
        datasets: [
          {
            label: "Casos pendientes",
            data: entries.map(([, count]) => count),
            backgroundColor: ["#fb7185", "#f59e0b", "#38bdf8", "#10b981", "#a855f7", "#94a3b8"],
            borderRadius: 10,
          },
        ],
      },
    };
  }, [reviewCountsByReason]);
  const kmExceptionChart = useMemo(() => {
    const values = [
      kmQuality?.vehicles_with_invalid_day_km ?? 0,
      kmQuality?.vehicles_with_total_regression ?? 0,
      missingDayKmCount,
    ];
    return {
      hasData: values.some((value) => value > 0),
      data: {
        labels: ["day_km invalido", "Regresion de odometro", "Sin km del dia"],
        datasets: [
          {
            label: "Excepciones km",
            data: values,
            backgroundColor: ["#fb7185", "#f59e0b", "#38bdf8"],
            borderRadius: 10,
          },
        ],
      },
    };
  }, [kmQuality, missingDayKmCount]);
  const unclassifiedCodesChart = useMemo(() => {
    const entries = [...(detailLiveSetup?.unclassified_codes ?? [])]
      .sort((left, right) => right.count - left.count)
      .slice(0, 5);
    return {
      hasData: entries.length > 0,
      data: {
        labels: entries.map((row) => `subtipo ${row.subtype ?? "-"} · ec ${row.event_code ?? "-"}`),
        datasets: [
          {
            label: "Codigos sin mapa",
            data: entries.map((row) => row.count),
            backgroundColor: ["#a855f7", "#8b5cf6", "#c084fc", "#6366f1", "#94a3b8"],
            borderRadius: 10,
          },
        ],
      },
    };
  }, [detailLiveSetup]);
  const rawTechnicalChart = useMemo(() => {
    const diagnostics = detailLiveSetup?.recent_raw_diagnostics ?? [];
    const counts = new Map<string, number>();
    diagnostics.forEach((row) => {
      const key = formatDiagnosticResult(row.ingest_result) || "sin resultado";
      counts.set(key, (counts.get(key) ?? 0) + 1);
    });
    const entries = Array.from(counts.entries()).sort((left, right) => right[1] - left[1]).slice(0, 6);
    return {
      hasData: entries.length > 0,
      data: {
        labels: entries.map(([label]) => label),
        datasets: [
          {
            label: "Eventos tecnicos",
            data: entries.map(([, count]) => count),
            backgroundColor: ["#94a3b8", "#64748b", "#a855f7", "#f59e0b", "#38bdf8", "#fb7185"],
            borderRadius: 10,
          },
        ],
      },
    };
  }, [detailLiveSetup]);
  const kmWatchVehiclesChart = useMemo(() => {
    const counts = new Map<string, number>();
    const registerPlate = (plates: string[]) => {
      plates.forEach((plate) => {
        if (!plate) return;
        counts.set(plate, (counts.get(plate) ?? 0) + 1);
      });
    };
    registerPlate(kmQuality?.sample_invalid_vehicles ?? []);
    registerPlate(kmQuality?.sample_total_regression_vehicles ?? []);
    registerPlate(kmQuality?.sample_missing_day_km_vehicles ?? []);
    const entries = Array.from(counts.entries()).sort((left, right) => right[1] - left[1]).slice(0, 6);
    return {
      hasData: entries.length > 0,
      data: {
        labels: entries.map(([plate]) => plate),
        datasets: [
          {
            label: "Tipos de excepcion",
            data: entries.map(([, count]) => count),
            backgroundColor: ["#f59e0b", "#fb7185", "#38bdf8", "#10b981", "#a855f7", "#94a3b8"],
            borderRadius: 10,
          },
        ],
      },
    };
  }, [kmQuality]);

  const applyAuditCacheEntry = useCallback((entry: DiagnosticAuditCacheEntry) => {
    setAudit(entry.audit);
  }, []);

  useEffect(() => {
    setError(null);
    setSuccess(null);
    setRefreshing(false);
    setRebuildLoading(false);
    setDetailOpen(false);
    setDetailLoading(false);
    setLoadedDetailKey(null);
    setSelectedReviewBuckets([]);
    setSelectedReviewReasons([]);
    setSelectedReviewIds([]);
    setReviewQueue([]);
    setReviewQueueTotal(0);
    setReviewFilteredTotal(0);
    setReviewTotalPages(0);
    setReviewCountsByAction({});
    setReviewCountsByReason({});
    setReviewPage(1);
  }, [selectedCompany, selectedWindowMode, timezoneName]);

  const loadAudit = useCallback(
    async ({
      windowMode,
      background = false,
      includeKm = false,
      applyCurrent = true,
    }: {
      windowMode: DiagnosticWindowMode;
      background?: boolean;
      includeKm?: boolean;
      applyCurrent?: boolean;
    }) => {
      const windowRange = windowRanges[windowMode];
      const cacheKey = buildWindowCacheKey(windowMode);
      const requestId = applyCurrent ? activeAuditRequestRef.current + 1 : activeAuditRequestRef.current;
      const hasWindowCache = auditWindowCacheRef.current.has(cacheKey);

      if (applyCurrent) {
        activeAuditRequestRef.current = requestId;
        if (!background && !hasWindowCache) {
          setLoading(true);
          setRefreshing(false);
        } else if (background || audit !== null || kmQuality !== null) {
          setRefreshing(true);
        } else {
          setLoading(true);
        }
      }

      try {
        const params = new URLSearchParams({
          company: selectedCompany,
          from: toCompanyIso(windowRange.from, timezoneName),
          to: toCompanyIso(windowRange.to, timezoneName),
        });
        const requestBatch = await Promise.allSettled([
          includeKm
            ? apiJson<KmQualitySummary>(`/admin/km/quality?company=${encodeURIComponent(selectedCompany)}`)
            : Promise.resolve<KmQualitySummary | null>(null),
          apiJson<AdminAudit>(`/admin/audit?${params.toString()}`),
        ]);

        const [nextKmQuality, nextAudit] = requestBatch;
        if (nextAudit.status === "fulfilled") {
          const nextCacheEntry: DiagnosticAuditCacheEntry = {
            loadedAt: Date.now(),
            audit: nextAudit.value,
          };
          auditWindowCacheRef.current.set(cacheKey, nextCacheEntry);
          if (applyCurrent && activeAuditRequestRef.current === requestId && cacheKey === detailCacheKey) {
            applyAuditCacheEntry(nextCacheEntry);
          }
        }

        if (nextKmQuality.status === "fulfilled" && nextKmQuality.value) {
          kmQualityCacheRef.current.set(selectedCompany, {
            loadedAt: Date.now(),
            snapshotVersion,
            value: nextKmQuality.value,
          });
          if (applyCurrent && activeAuditRequestRef.current === requestId) {
            setKmQuality(nextKmQuality.value);
          }
        }

        if (applyCurrent) {
          const relevantResults = includeKm ? requestBatch : requestBatch.slice(1);
          const errors = relevantResults.map(settledError).filter(Boolean);
          setError(errors.length === relevantResults.length ? "No se pudo cargar el diagnostico" : errors[0] ?? null);
        }
      } catch (nextError) {
        if (applyCurrent) {
          setError(nextError instanceof Error ? nextError.message : "No se pudo cargar el diagnostico");
        }
      } finally {
        if (applyCurrent && activeAuditRequestRef.current === requestId) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    },
    [
      applyAuditCacheEntry,
      audit,
      buildWindowCacheKey,
      detailCacheKey,
      kmQuality,
      selectedCompany,
      snapshotVersion,
      timezoneName,
      windowRanges,
    ],
  );

  const loadReviewPage = useCallback(
    async ({ syncQueue = false }: { syncQueue?: boolean } = {}) => {
      const requestId = activeReviewRequestRef.current + 1;
      activeReviewRequestRef.current = requestId;
      setReviewLoading(true);
      try {
        const params = new URLSearchParams({
          company: selectedCompany,
          from: toCompanyIso(range.from, timezoneName),
          to: toCompanyIso(range.to, timezoneName),
          status: "pending",
          page: String(reviewPage),
          page_size: String(DIAGNOSTIC_REVIEW_PAGE_SIZE),
          sync: syncQueue ? "1" : "0",
        });
        if (selectedReviewBuckets.length > 0) {
          params.set("suggested", selectedReviewBuckets.join(","));
        }
        if (selectedReviewReasons.length > 0) {
          params.set("reason", selectedReviewReasons.join(","));
        }
        const response = await apiJson<ReconciliationReviewList>(`/admin/reconciliation/reviews?${params.toString()}`);
        if (activeReviewRequestRef.current !== requestId) return;
        setReviewQueue(response.items);
        setReviewQueueTotal(response.total_items);
        setReviewFilteredTotal(response.filtered_items);
        setReviewPage(response.page);
        setReviewTotalPages(response.total_pages);
        setReviewCountsByAction(response.counts_by_action);
        setReviewCountsByReason(response.counts_by_reason);
        setSelectedReviewIds((current) => current.filter((id) => response.items.some((review) => review.id === id)));
      } catch (nextError) {
        if (activeReviewRequestRef.current === requestId) {
          setError(nextError instanceof Error ? nextError.message : "No se pudo cargar la bandeja de decisiones");
        }
      } finally {
        if (activeReviewRequestRef.current === requestId) {
          setReviewLoading(false);
        }
      }
    }, [range.from, range.to, reviewPage, selectedCompany, selectedReviewBuckets, selectedReviewReasons, timezoneName],
  );

  const loadAuditDetails = useCallback(async () => {
    setDetailLoading(true);
    try {
      const [nextAnomalies, nextVehicles, nextLiveSetup] = await Promise.allSettled([
        apiJson<IngestionAnomaly[]>(
          `/admin/anomalies?company=${encodeURIComponent(selectedCompany)}&from=${encodeURIComponent(
            toCompanyIso(range.from, timezoneName),
          )}&to=${encodeURIComponent(toCompanyIso(range.to, timezoneName))}&limit=100`,
        ),
        apiJson<AdminVehicle[]>(`/admin/vehicles?company=${encodeURIComponent(selectedCompany)}`),
        apiJson<AdminLiveSetup>(
          `/admin/live-setup?company=${encodeURIComponent(selectedCompany)}&from=${encodeURIComponent(
            toCompanyIso(range.from, timezoneName),
          )}&to=${encodeURIComponent(toCompanyIso(range.to, timezoneName))}`,
        ),
      ]);
      if (nextAnomalies.status === "fulfilled") {
        setAnomalies(nextAnomalies.value);
      }
      if (nextVehicles.status === "fulfilled") {
        setVehicles(nextVehicles.value);
      }
      if (nextLiveSetup.status === "fulfilled") {
        setDetailLiveSetup(nextLiveSetup.value);
      }
      setLoadedDetailKey(detailCacheKey);
      const errors = [nextAnomalies, nextVehicles, nextLiveSetup].map(settledError).filter(Boolean);
      if (errors.length === 3) {
        setError("No se pudo cargar el detalle tecnico");
      } else if (errors.length > 0) {
        setError(errors[0] ?? null);
      }
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo cargar el detalle tecnico");
    } finally {
      setDetailLoading(false);
    }
  }, [detailCacheKey, range.from, range.to, selectedCompany, timezoneName]);

  const refreshDiagnostic = useCallback(
    async (syncQueue = true) => {
      await Promise.all([
        loadAudit({
          windowMode: selectedWindowMode,
          background: false,
          includeKm: true,
          applyCurrent: true,
        }),
        loadReviewPage({ syncQueue }),
      ]);
      if (detailOpen) {
        setLoadedDetailKey(null);
        await loadAuditDetails();
      }
    },
    [detailOpen, loadAudit, loadAuditDetails, loadReviewPage, selectedWindowMode],
  );

  useEffect(() => {
    if (!enabled) return;
    const cachedAudit = auditWindowCacheRef.current.get(detailCacheKey);
    const cachedKm = kmQualityCacheRef.current.get(selectedCompany);
    if (cachedAudit) {
      applyAuditCacheEntry(cachedAudit);
      setLoading(false);
    } else {
      setAudit(null);
      setReviewQueue([]);
      setReviewQueueTotal(0);
      setReviewCountsByAction({});
      setReviewCountsByReason({});
      setLoading(true);
    }
    if (cachedKm) {
      setKmQuality(cachedKm.value);
    }
    const shouldFetchAudit =
      !cachedAudit || Date.now() - cachedAudit.loadedAt >= DIAGNOSTIC_CACHE_TTL_MS;
    const shouldFetchKm =
      !cachedKm || cachedKm.snapshotVersion !== snapshotVersion || Date.now() - cachedKm.loadedAt >= DIAGNOSTIC_CACHE_TTL_MS;
    if (shouldFetchAudit || shouldFetchKm) {
      void loadAudit({
        windowMode: selectedWindowMode,
        background: Boolean(cachedAudit || cachedKm),
        includeKm: shouldFetchKm,
        applyCurrent: true,
      });
    }
    auditBootstrappedRef.current = true;
    lastAuditSnapshotVersionRef.current = snapshotVersion;
  }, [
    applyAuditCacheEntry,
    detailCacheKey,
    enabled,
    loadAudit,
    selectedCompany,
    selectedWindowMode,
    snapshotVersion,
  ]);

  useEffect(() => {
    if (!enabled) return;
    void loadReviewPage();
  }, [enabled, loadReviewPage, snapshotVersion]);

  useEffect(() => {
    if (!enabled || !snapshotVersion) return;
    if (!auditBootstrappedRef.current) return;
    if (snapshotVersion === lastAuditSnapshotVersionRef.current) return;
    lastAuditSnapshotVersionRef.current = snapshotVersion;
    void loadAudit({
      windowMode: selectedWindowMode,
      background: true,
      includeKm: true,
      applyCurrent: true,
    });
  }, [enabled, loadAudit, selectedWindowMode, snapshotVersion]);

  useEffect(() => {
    if (!enabled || !detailOpen || loadedDetailKey === detailCacheKey) return;
    void loadAuditDetails();
  }, [detailCacheKey, detailOpen, enabled, loadAuditDetails, loadedDetailKey]);

  const rebuildCurrentMonth = async () => {
    try {
      setRebuildLoading(true);
      setError(null);
      setSuccess(null);
      const response = await apiJson<HistoricalRebuildResult>("/admin/harvest/rebuild-history", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          company_slug: selectedCompany,
          start_date: monthStartDate,
          end_date: monthEndDate,
          publish_snapshot: true,
          maintenance: true,
        }),
      });
      setSuccess(
        `Repoblado del mes encolado para ${company?.name ?? selectedCompany} (${response.job_id?.slice(0, 8) ?? "job"}). El worker conserva el progreso y publicara el snapshot solo al completar toda la ventana.`,
      );
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo repoblar el historico del mes actual");
    } finally {
      setRebuildLoading(false);
    }
  };

  const decideReviews = async (reviewIds: number[], action: "approve" | "discard") => {
    if (reviewIds.length === 0) return;
    try {
      setActionLoading(true);
      setError(null);
      setSuccess(null);
      const queuedJob = await apiJson<BackgroundJobStatus>(`/admin/reconciliation/reviews/bulk/${action}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ids: reviewIds, note: null }),
      });
      const completedJob = await waitForBackgroundJob<BackgroundJobStatus>(queuedJob.job_id, { timeoutMs: 120_000 });
      const response = completedJob?.result as unknown as ReconciliationReviewBulkDecisionResult | null;
      if (!response) {
        setSuccess("La decision quedo en proceso y se reflejara al terminar el worker.");
        setSelectedReviewIds([]);
        return;
      }
      const affected = response.updated;
      const label = affected === 1 ? "1 caso" : `${affected} casos`;
      setSuccess(
        action === "approve"
          ? `${label} aprobado${affected === 1 ? "" : "s"}. Si afecta el dashboard operativo, se reflejara en el siguiente corte de 15 minutos o cuando refresques snapshot manualmente.`
          : `${label} descartado${affected === 1 ? "" : "s"} por administracion. Ya no seguira${affected === 1 ? "" : "n"} pendiente${affected === 1 ? "" : "s"} en la bandeja mensual.`,
      );
      setSelectedReviewIds([]);
      await refreshDiagnostic(true);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "No se pudo registrar la decision manual");
    } finally {
      setActionLoading(false);
    }
  };

  const visibleReviewIds = useMemo(() => filteredReviewQueue.map((review) => review.id), [filteredReviewQueue]);
  const selectedVisibleCount = useMemo(
    () => selectedReviewIds.filter((id) => visibleReviewIds.includes(id)).length,
    [selectedReviewIds, visibleReviewIds],
  );

  return (
    <main className="page-grid">
      <section className="panel">
        <div className="panel-kicker">Diagnostico y Auditoria</div>
        <h3>{company?.name ?? selectedCompany} · {range.label}</h3>
        <p className="panel-copy">{range.subtitle}</p>
        {refreshing ? <p className="panel-copy">Actualizando diagnostico en segundo plano...</p> : null}
      </section>

      <section className="panel">
        <div className="panel-headline">
          <div>
            <div className="panel-kicker">Ventana activa</div>
            <h3>Una sola ventana gobierna toda la vista</h3>
            <p className="panel-subtitle">
              Todo lo que ves abajo usa este mismo rango: resumen, alertas retenidas, anomalías y soporte.
            </p>
          </div>
          <div className="toolbar admin-toolbar">
            <div className="chip-row">
              {DIAGNOSTIC_WINDOW_OPTIONS.map((option) => (
                <button
                  key={option.key}
                  type="button"
                  className={`chip chip-toggle ${selectedWindowMode === option.key ? "active" : ""}`}
                  onClick={() => setSelectedWindowMode(option.key)}
                  disabled={refreshing || rebuildLoading || actionLoading}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <button
              className="ghost-btn"
              type="button"
              onClick={() => void refreshDiagnostic(true)}
              disabled={refreshing || rebuildLoading || actionLoading}
            >
              <RefreshCw size={16} />
              Refrescar diagnostico
            </button>
            <button
              className="primary-btn"
              type="button"
              onClick={() => void rebuildCurrentMonth()}
              disabled={rebuildLoading || refreshing || actionLoading}
            >
              <RefreshCw size={16} />
              {rebuildLoading ? "Repoblando mes actual..." : "Repoblar mes actual"}
            </button>
          </div>
        </div>
        <div className="chip-row" style={{ marginTop: "1rem" }}>
          <span className="chip">Empresa: {company?.name ?? selectedCompany}</span>
          <span className="chip">Ventana: {range.label}</span>
          <span className="chip">Rango activo: {formatAuditMonthRangeLabel(range.from, range.to, timezoneName)}</span>
          <span className="chip">Pendientes: {reviewQueueTotal}</span>
        </div>
      </section>

      {error ? <div className="banner error">{error}</div> : null}
      {success ? <div className="banner success">{success}</div> : null}

      <section className="metric-grid audit-kpi-grid">
        <MetricCard
          label="DMS recibidos"
          value={activeWindowMetrics ? String(activeWindowMetrics.received_dms) : loading ? "..." : "0"}
          detail={`${range.label} · identificados en la fuente`}
        />
        <MetricCard
          label="DMS analiticos"
          value={activeWindowMetrics ? String(activeWindowMetrics.analytic_dms) : loading ? "..." : "0"}
          detail="Persistidos para reglas y agregados"
        />
        <MetricCard
          label="Episodios visibles"
          value={activeWindowMetrics ? String(activeWindowMetrics.visible_episodes) : loading ? "..." : "0"}
          detail="Tarjetas que recibe el dashboard cliente"
        />
        <MetricCard
          label="Detecciones fusionadas"
          value={activeWindowMetrics ? String(activeWindowMetrics.fused_detections) : loading ? "..." : "0"}
          detail="Incluidas dentro de episodios visibles"
        />
        <MetricCard
          label="DMS retenidos"
          value={activeWindowMetrics ? String(activeWindowMetrics.retained_for_review) : loading ? "..." : "0"}
          detail={`${reviewQueueTotal} casos totales en bandeja, incluidos km`}
        />
        <MetricCard
          label="Descartados por admin"
          value={activeWindowMetrics ? String(activeWindowMetrics.discarded_by_admin) : loading ? "..." : "0"}
          detail="Decisiones humanas registradas en la ventana"
        />
        <MetricCard
          label="Diferencia inexplicada"
          value={activeWindowMetrics ? String(activeWindowMetrics.unexplained_difference) : loading ? "..." : "0"}
          detail="Debe permanecer en cero"
          tone={(activeWindowMetrics?.unexplained_difference ?? 0) > 0 ? "danger" : "white"}
        />
      </section>

      <section className="panel">
        <div className="panel-headline">
          <div>
            <div className="panel-kicker">Supervision humana obligatoria</div>
            <h3>Bandeja de decisiones · {range.label}</h3>
            <p className="panel-subtitle">
              Cada fila representa un caso que puedes aprobar o descartar manualmente. Si lo apruebas, quedara listo
              para el siguiente corte de 15 minutos o para un refresh manual del snapshot.
            </p>
          </div>
          <span className="tone-pill warning">{reviewQueueTotal} pendientes</span>
        </div>
        <div className="chip-row" style={{ marginBottom: "1rem" }}>
          <button
            type="button"
            className={`chip chip-toggle ${selectedReviewBuckets.length === 0 ? "active" : ""}`}
            onClick={() => {
              setSelectedReviewBuckets([]);
              setReviewPage(1);
            }}
          >
            Todas {reviewQueueTotal}
          </button>
          {reviewFilterOptions.map((option) => (
            <button
              key={option.key}
              type="button"
              className={`chip chip-toggle ${selectedReviewBuckets.includes(option.key) ? "active" : ""}`}
              onClick={() => {
                setReviewPage(1);
                setSelectedReviewBuckets((current) =>
                  current.includes(option.key)
                    ? current.filter((value) => value !== option.key)
                    : [...current, option.key],
                );
              }}
            >
              {option.label} {option.count}
            </button>
          ))}
        </div>
        <div className="chip-row" style={{ marginBottom: "1rem" }}>
          <button
            type="button"
            className={`chip chip-toggle ${selectedReviewReasons.length === 0 ? "active" : ""}`}
            onClick={() => {
              setSelectedReviewReasons([]);
              setReviewPage(1);
            }}
          >
            Todos los motivos
          </button>
          {reviewReasonOptions.map((option) => (
            <button
              key={option.key}
              type="button"
              className={`chip chip-toggle ${selectedReviewReasons.includes(option.key) ? "active" : ""}`}
              onClick={() => {
                setReviewPage(1);
                setSelectedReviewReasons((current) =>
                  current.includes(option.key)
                    ? current.filter((value) => value !== option.key)
                    : [...current, option.key],
                );
              }}
            >
              {option.label} {option.count}
            </button>
          ))}
        </div>
        <div className="panel-copy" style={{ marginBottom: "1rem" }}>
          Filtro activo: {activeReviewFilterLabel}. Toda la bandeja respeta {range.label.toLowerCase()}.
        </div>
        <div className="panel-copy" style={{ marginBottom: "1rem" }}>
          Si apruebas un caso, quedara listo para entrar en el siguiente corte o con refresh manual. Si lo descartas, saldra de la bandeja y seguira fuera del dashboard cliente.
        </div>
        <div className="toolbar admin-toolbar" style={{ marginBottom: "1rem" }}>
          <span className="chip">{selectedVisibleCount} seleccionados en el filtro actual</span>
          <button
            className="ghost-btn"
            type="button"
            onClick={() => setSelectedReviewIds(visibleReviewIds)}
            disabled={visibleReviewIds.length === 0 || actionLoading}
          >
            <Shield size={16} />
            Seleccionar visibles
          </button>
          <button
            className="ghost-btn"
            type="button"
            onClick={() => setSelectedReviewIds([])}
            disabled={selectedReviewIds.length === 0 || actionLoading}
          >
            <Filter size={16} />
            Limpiar seleccion
          </button>
          <button
            className="primary-btn"
            type="button"
            onClick={() => void decideReviews(selectedReviewIds, "approve")}
            disabled={selectedReviewIds.length === 0 || actionLoading}
          >
            <Shield size={16} />
            Aprobar seleccionados
          </button>
          <button
            className="ghost-btn review-discard-btn"
            type="button"
            onClick={() => void decideReviews(selectedReviewIds, "discard")}
            disabled={selectedReviewIds.length === 0 || actionLoading}
          >
            <AlertTriangle size={16} />
            Descartar seleccionados
          </button>
        </div>
        {reviewFilteredTotal > reviewQueue.length ? (
          <p className="panel-copy">
            Mostrando {reviewQueue.length > 0 ? (reviewPage - 1) * DIAGNOSTIC_REVIEW_PAGE_SIZE + 1 : 0}–
            {Math.min(reviewPage * DIAGNOSTIC_REVIEW_PAGE_SIZE, reviewFilteredTotal)} de {reviewFilteredTotal} casos que coinciden con el filtro.
          </p>
        ) : null}
        {(loading || reviewLoading) && reviewQueueTotal === 0 ? (
          <div className="reconciliation-empty">
            <div className="reconciliation-empty-icon">
              <RefreshCw size={18} />
            </div>
            <div>
              <strong className="reconciliation-empty-title">Cargando la ventana seleccionada</strong>
              <div className="empty-copy">
                Estamos recalculando resumen, pendientes y soporte para {range.label.toLowerCase()}.
              </div>
            </div>
          </div>
        ) : reviewQueueTotal === 0 ? (
          <div className="reconciliation-empty">
            <div className="reconciliation-empty-icon">
              <Shield size={18} />
            </div>
            <div>
              <strong className="reconciliation-empty-title">
                No hay pendientes manuales para esta ventana
              </strong>
              <div className="empty-copy">
                Cuando aparezca una alerta retenida, una anomalia temporal o un caso de kilometraje dudoso dentro de
                este rango, se mostrara aqui para aprobarlo o descartarlo manualmente.
              </div>
            </div>
            <div className="panel-copy">
              Nada pasa al dashboard del cliente por esta via sin una decision humana explicita.
            </div>
          </div>
        ) : filteredReviewQueue.length === 0 ? (
          <div className="reconciliation-empty">
            <div className="reconciliation-empty-icon">
              <Filter size={18} />
            </div>
            <div>
              <strong className="reconciliation-empty-title">No hay casos en las categorias seleccionadas</strong>
              <div className="empty-copy">
                Cambia los filtros de arriba para ver otra parte de la bandeja mensual.
              </div>
            </div>
          </div>
        ) : (
          <div className="review-queue-grid">
            {filteredReviewQueue.map((review) => (
              <article key={review.id} className={`review-card ${selectedReviewIds.includes(review.id) ? "selected" : ""}`}>
                <div className="review-card-top">
                  <div className="chip-row">
                    <label className="chip chip-toggle active" style={{ display: "inline-flex", alignItems: "center", gap: "0.45rem" }}>
                      <input
                        type="checkbox"
                        checked={selectedReviewIds.includes(review.id)}
                        onChange={(event) =>
                          setSelectedReviewIds((current) =>
                            event.target.checked
                              ? [...new Set([...current, review.id])]
                              : current.filter((item) => item !== review.id),
                          )
                        }
                      />
                      Marcar
                    </label>
                    <span className="tone-pill warning">Pendiente</span>
                    <span
                      className={`tone-pill ${
                        review.suggested_action === "review_anomaly"
                          ? "danger"
                          : review.suggested_action === "review_raw"
                            ? "warning"
                            : review.suggested_action === "review_km"
                              ? "warning"
                            : "success"
                      }`}
                    >
                      {formatReviewSource(review.suggested_action)}
                    </span>
                    {review.category ? <span className="chip">{formatCategory(review.category)}</span> : null}
                    <span className="chip">{review.plate_no ?? review.device_id ?? "Sin placa"}</span>
                  </div>
                  <div className="review-card-time">
                    {review.observed_at ? formatDateTime(review.observed_at, timezoneName) : review.portal_begin_time ?? "-"}
                  </div>
                </div>
                <div className="review-card-title">
                  {review.raw_alarm_type ?? `tp ${review.raw_tp ?? "-"} / ec ${review.raw_event_code ?? "-"}`}
                </div>
                <div className="review-card-copy">{formatAuditReason(review.reason)}</div>
                <div className="review-card-meta">
                  <div className="review-meta-row">
                    <span>Begin</span>
                    <strong>{review.portal_begin_time ?? "-"}</strong>
                  </div>
                  <div className="review-meta-row">
                    <span>Reporting</span>
                    <strong>{review.portal_reporting_time ?? "-"}</strong>
                  </div>
                  <div className="review-meta-row">
                    <span>Clasificacion</span>
                    <strong>{review.classification_status ?? "-"}</strong>
                  </div>
                  <div className="review-meta-row">
                    <span>Visibilidad</span>
                    <strong>{review.visibility_status ?? "-"}</strong>
                  </div>
                </div>
                {review.diagnostic_note ? <div className="panel-copy review-note">{review.diagnostic_note}</div> : null}
                <div className="review-card-actions">
                  <button className="primary-btn" type="button" onClick={() => void decideReviews([review.id], "approve")} disabled={actionLoading}>
                    <Shield size={16} />
                    Aprobar
                  </button>
                  <button className="ghost-btn review-discard-btn" type="button" onClick={() => void decideReviews([review.id], "discard")} disabled={actionLoading}>
                    <AlertTriangle size={16} />
                    Descartar
                  </button>
                </div>
              </article>
            ))}
          </div>
        )}
        {reviewTotalPages > 1 ? (
          <div className="review-pagination" aria-label="Paginacion de decisiones">
            <button
              className="ghost-btn"
              type="button"
              onClick={() => setReviewPage((current) => Math.max(1, current - 1))}
              disabled={reviewPage <= 1 || reviewLoading || actionLoading}
            >
              Anterior
            </button>
            <span className="chip">Pagina {reviewPage} de {reviewTotalPages}</span>
            <button
              className="ghost-btn"
              type="button"
              onClick={() => setReviewPage((current) => Math.min(reviewTotalPages, current + 1))}
              disabled={reviewPage >= reviewTotalPages || reviewLoading || actionLoading}
            >
              Siguiente
            </button>
          </div>
        ) : null}
      </section>

      <details
        className="panel audit-details-panel"
        open={detailOpen}
        onToggle={(event) => setDetailOpen((event.currentTarget as HTMLDetailsElement).open)}
      >
        <summary className="audit-details-summary">
          <div>
            <div className="panel-kicker">Soporte compacto</div>
            <strong>Resumen visual del soporte</strong>
          </div>
          <span className="chip">
            {detailLoading
              ? "cargando..."
              : `${range.label} · ${reviewQueueTotal} pendientes · ${anomalies.length} anomalias · ${kmExceptionVehicles.length} excepciones km`}
          </span>
        </summary>

        <div className="audit-details-content">
          <section className="audit-mini-grid">
            <div className="mini-stat">
              <span className="mini-stat-label">Alertas visibles</span>
              <span className="mini-stat-value">{audit?.requested_window.visible_alerts ?? 0}</span>
            </div>
            <div className="mini-stat">
              <span className="mini-stat-label">Suprimidas por regla</span>
              <span className="mini-stat-value">{audit?.requested_window.suppressed_by_rule ?? 0}</span>
            </div>
            <div className="mini-stat">
              <span className="mini-stat-label">Anomalias temporales</span>
              <span className="mini-stat-value">{audit?.requested_window.future_rejected ?? 0}</span>
            </div>
            <div className="mini-stat">
              <span className="mini-stat-label">Pendientes manuales</span>
              <span className="mini-stat-value">{reviewQueueTotal}</span>
            </div>
          </section>

          <section className="double-panel">
            <ChartPanel title={`Que paso con los eventos en ${range.label.toLowerCase()}`} frameStyle={COMPACT_CHART_FRAME_STYLE}>
              {supportFlowChart.hasData ? (
                <Bar data={supportFlowChart.data} options={COMPACT_HORIZONTAL_BAR_OPTIONS} />
              ) : (
                <div className="empty-copy">Aun no hay suficientes eventos en esta ventana para resumir el flujo.</div>
              )}
            </ChartPanel>
            <ChartPanel title="Motivos que hoy si requieren decision" frameStyle={COMPACT_CHART_FRAME_STYLE}>
              {reviewReasonChart.hasData ? (
                <Bar data={reviewReasonChart.data} options={COMPACT_HORIZONTAL_BAR_OPTIONS} />
              ) : (
                <div className="empty-copy">No hay motivos pendientes que resumir en la bandeja mensual.</div>
              )}
            </ChartPanel>
          </section>

          <section className="double-panel">
            <ChartPanel title="Excepciones de kilometraje" frameStyle={COMPACT_CHART_FRAME_STYLE}>
              {kmExceptionChart.hasData ? (
                <Bar data={kmExceptionChart.data} options={COMPACT_HORIZONTAL_BAR_OPTIONS} />
              ) : (
                <div className="empty-copy">No hay excepciones de kilometraje activas en esta ventana.</div>
              )}
            </ChartPanel>
            <ChartPanel title="Codigos por mapear" frameStyle={COMPACT_CHART_FRAME_STYLE}>
              {unclassifiedCodesChart.hasData ? (
                <Bar data={unclassifiedCodesChart.data} options={COMPACT_HORIZONTAL_BAR_OPTIONS} />
              ) : (
                <div className="empty-copy">No hay codigos DMS pendientes de mapeo en esta ventana.</div>
              )}
            </ChartPanel>
          </section>

          <details className="panel">
            <summary className="audit-details-summary">
              <div>
                <div className="panel-kicker">Apoyo opcional</div>
                <strong>Detalle tecnico de soporte</strong>
              </div>
              <span className="chip">
                {(detailLiveSetup?.recent_raw_diagnostics.length ?? 0)} eventos no limpios · {kmExceptionVehicles.length} placas a vigilar
              </span>
            </summary>

            <div className="audit-details-content">
              <section className="double-panel">
                <ChartPanel title="Eventos fuera del dashboard por resultado" frameStyle={COMPACT_CHART_FRAME_STYLE}>
                  {rawTechnicalChart.hasData ? (
                    <Bar data={rawTechnicalChart.data} options={COMPACT_HORIZONTAL_BAR_OPTIONS} />
                  ) : (
                    <div className="empty-copy">No hay eventos raw tecnicos recientes para esta empresa.</div>
                  )}
                </ChartPanel>

                <ChartPanel title="Placas con kilometraje por revisar" frameStyle={COMPACT_CHART_FRAME_STYLE}>
                  {kmWatchVehiclesChart.hasData ? (
                    <Bar data={kmWatchVehiclesChart.data} options={COMPACT_HORIZONTAL_BAR_OPTIONS} />
                  ) : (
                    <div className="empty-copy">No hay placas con excepciones activas de kilometraje.</div>
                  )}
                </ChartPanel>
              </section>

            </div>
          </details>
        </div>
      </details>
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
    note?: string;
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
              <div className="priority">{alert.rawCount} detecciones en el episodio.</div>
              {alert.note ? <div className="priority">{alert.note}</div> : null}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function TimelineMarker({ label, timeLabel }: { label: string; timeLabel: string }) {
  return (
    <div className="timeline-marker">
      <div className="alert-time">{timeLabel}</div>
      <div className="timeline-marker-body">
        <MoonStar size={15} />
        {label}
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

function formatFeedStatusLabel(status: FeedState["status"] | "sin_datos") {
  switch (status) {
    case "al_dia":
      return "al dia";
    case "atrasado":
      return "atrasado";
    case "detenido":
      return "detenido";
    default:
      return "sin datos";
  }
}

function formatDateTime(value: string, timezoneName = "America/Bogota") {
  return dayjs(value).tz(timezoneName).format("DD/MM/YYYY HH:mm");
}

function toCompanyIso(value: string, timezoneName = "America/Bogota") {
  return dayjs.tz(value, timezoneName).toISOString();
}

function formatClockTime(value: string, timezoneName = "America/Bogota") {
  return dayjs(value).tz(timezoneName).format("HH:mm");
}

function formatClockTimeFromMs(value: number, timezoneName = "America/Bogota") {
  return dayjs(value).tz(timezoneName).format("hh:mm A");
}

function formatIsoDate(value: string) {
  return dayjs(value).format("YYYY-MM-DD");
}

function formatAuditMonthRangeLabel(startValue: string, endValue: string, timezoneName = "America/Bogota") {
  return `${dayjs.tz(startValue, timezoneName).format("DD MMM YYYY")} -> ${dayjs.tz(endValue, timezoneName).format("DD MMM YYYY")}`;
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

function formatCountdownMs(value: number) {
  const safeValue = Math.max(value, 0);
  const totalSeconds = Math.floor(safeValue / 1_000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function buildSnapshotScheduleLabel({
  timezoneName,
  nextRefreshAt,
  nowMs,
}: {
  timezoneName: string;
  nextRefreshAt: number | null;
  nowMs: number;
}) {
  if (!nextRefreshAt) return "";
  return `Siguiente corte ${formatClockTimeFromMs(nextRefreshAt, timezoneName)} · en ${formatCountdownMs(nextRefreshAt - nowMs)}`;
}

function humanBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatAuditReason(value: string) {
  return value
    .replaceAll("_", " ")
    .replace("classified non dms", "clasificada como no DMS")
    .replace("non dms text", "clasificada como no DMS por texto")
    .replace("dms like unmapped", "parece DMS pero quedo sin mapa")
    .replace("missing local", "no existe en local")
    .replace("stored local unmapped", "guardada local sin mapa")
    .replace("stored local classified non dms", "guardada local como no DMS")
    .replace("suppressed by rule", "suprimida por regla")
    .replace("rejected temporal", "rechazada por temporalidad")
    .replace("fused in episode", "fusionada en episodio")
    .replace("visible episode", "abre episodio")
    .replace("single eye closed", "ojo cerrado aislado")
    .replace("distraction below 3x", "distraccion bajo umbral 3x")
    .replace("merged yawn into fatigue", "bostezo fusionado a fatiga")
    .replace("missing dashboard mapping", "oculta por mapeo local")
    .replace("normalization failed", "fallo de normalizacion")
    .replace("missing day km", "sin kilometraje diario confiable")
    .replace("day gt total", "km del dia mayor que el total")
    .replace("total regression", "regresion de odometro");
}

function formatReviewSource(value: string) {
  switch (value) {
    case "review_visibility":
      return "Regla del cliente";
    case "review_raw":
      return "Flujo DMS";
    case "review_anomaly":
      return "Anomalia";
    case "review_km":
      return "Kilometraje";
    default:
      return "Revision";
  }
}

function formatDiagnosticResult(value: string | null | undefined) {
  switch (value) {
    case "kept_raw_only_non_dms":
      return "solo soporte";
    case "inserted_alarm_event":
      return "paso a dashboard";
    case "updated_alarm_event":
      return "actualizo dashboard";
    case "inserted_from_portal":
      return "insertado desde portal";
    case "updated_from_portal":
      return "actualizado desde portal";
    case "future_timestamp":
      return "rechazado por tiempo";
    case "normalization_failed":
      return "fallo normalizacion";
    default:
      return value ?? "sin resultado";
  }
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
