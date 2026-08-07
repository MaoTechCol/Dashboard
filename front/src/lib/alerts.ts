import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

import type { DashboardSnapshot, RecentEvent, TimelineFilter } from "../types";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(relativeTime);

type AlertLevel = "critico" | "alto" | "medio";

interface GroupedAlert {
  id: string;
  plate: string;
  category: string;
  level: AlertLevel;
  label: string;
  title: string;
  detail: string;
  priority: number;
  occurredAt: string;
  timeLabel: string;
  isNight: boolean;
  isNew: boolean;
  rawCount: number;
}

const DISPLAY_LABELS: Record<string, string> = {
  "Uso de celular": "Uso de celular",
  "Fatiga en progresion": "Fatiga en progresion",
  "Ojos cerrados": "Ojos cerrados",
  "Riesgo de colision": "Riesgo de colision",
  Bostezo: "Bostezo",
  "Camara cubierta": "Camara cubierta",
  Fumando: "Fumando",
  Distraccion: "Distraccion",
};

const WEIGHTS: Record<string, number> = {
  "Uso de celular": 10,
  "Fatiga en progresion": 8,
  "Ojos cerrados": 6,
  "Riesgo de colision": 5,
  Bostezo: 3,
  "Camara cubierta": 3,
  Fumando: 2,
  Distraccion: 1,
};

const BASE_CONFIDENCE: Record<string, number> = {
  "Uso de celular": 1,
  "Fatiga en progresion": 0.9,
  "Ojos cerrados": 0.5,
  "Riesgo de colision": 0.8,
  Bostezo: 0.8,
  "Camara cubierta": 0.9,
  Fumando: 0.7,
  Distraccion: 0.4,
};

const CRITICAL_CATEGORIES = new Set(["Uso de celular", "Ojos cerrados", "Fatiga en progresion"]);
const HIGH_CATEGORIES = new Set(["Riesgo de colision", "Bostezo"]);

function isNight(dateValue: dayjs.Dayjs, startHour: number, endHour: number) {
  const hour = dateValue.hour();
  if (startHour < endHour) {
    return hour >= startHour && hour < endHour;
  }
  return hour >= startHour || hour < endHour;
}

function timeLabel(occurredAt: dayjs.Dayjs, now: dayjs.Dayjs) {
  const diffMinutes = now.diff(occurredAt, "minute");
  if (diffMinutes <= 60) {
    return `hace ${Math.max(diffMinutes, 0)} min`;
  }
  if (occurredAt.isSame(now, "day")) {
    return occurredAt.format("HH:mm");
  }
  if (occurredAt.add(1, "day").isSame(now, "day")) {
    return `ayer ${occurredAt.format("HH:mm")}`;
  }
  return occurredAt.format("DD/MM HH:mm");
}

export function formatCategory(category: string) {
  return DISPLAY_LABELS[category] ?? category;
}

export function buildTimeline(snapshot: DashboardSnapshot, filter: TimelineFilter) {
  const timezoneName = snapshot.meta.timezone;
  const now = dayjs(snapshot.meta.generatedAt).tz(timezoneName);
  const rules = snapshot.rules;
  const cycleMark = snapshot.feed.last_cycle_received_at
    ? dayjs(snapshot.feed.last_cycle_received_at).tz(timezoneName)
    : now;
  const rawEvents = snapshot.recentEvents
    .map((event) => ({
      ...event,
      occurredAtDayjs: dayjs(event.occurredAt).tz(timezoneName),
    }))
    .sort((left, right) => left.occurredAtDayjs.valueOf() - right.occurredAtDayjs.valueOf());

  const grouped: Array<{
    plate: string;
    category: string;
    events: Array<(RecentEvent & { occurredAtDayjs: dayjs.Dayjs })>;
  }> = [];
  const openGroups = new Map<string, { plate: string; category: string; events: Array<(RecentEvent & { occurredAtDayjs: dayjs.Dayjs })> }>();

  for (const event of rawEvents) {
    const key = `${event.plate}|${event.category}`;
    const current = openGroups.get(key);
    const gapSeconds = current
      ? event.occurredAtDayjs.diff(current.events[current.events.length - 1].occurredAtDayjs, "second")
      : null;
    const gapMinutes = current
      ? event.occurredAtDayjs.diff(current.events[current.events.length - 1].occurredAtDayjs, "minute", true)
      : null;
    const windowMinutes =
      event.category === "Riesgo de colision"
        ? rules.collision_window_minutes
        : event.category === "Bostezo"
          ? rules.yawn_window_minutes
          : rules.streak_window_minutes;

    if (current && gapMinutes !== null && gapMinutes <= windowMinutes) {
      if (gapSeconds !== null && gapSeconds <= rules.echo_window_seconds) {
        current.events[current.events.length - 1] = event;
      } else {
        current.events.push(event);
      }
      continue;
    }

    const nextGroup = { plate: event.plate, category: event.category, events: [event] };
    openGroups.set(key, nextGroup);
    grouped.push(nextGroup);
  }

  const consumed = new Set<string>();
  const alerts: GroupedAlert[] = [];
  let dismissed = 0;

  for (const group of grouped) {
    if (consumed.has(`${group.plate}|${group.category}|${group.events[0]?.guid}`)) {
      continue;
    }

    const first = group.events[0];
    const last = group.events[group.events.length - 1];
    const sameDayCount = rawEvents.filter(
      (event) =>
        event.plate === group.plate &&
        event.category === group.category &&
        event.occurredAtDayjs.isSame(last.occurredAtDayjs, "day"),
    ).length;
    const vehicleDeviation = snapshot.deviationByVehicle[group.plate] ?? 1;
    let category = group.category;
    let level: AlertLevel = "medio";
    let confidence = BASE_CONFIDENCE[group.category] ?? 0.5;
    let title = formatCategory(group.category);
    let detail = last.occurredAtDayjs.format("HH:mm");
    let label = "Medio";

    if (group.category === "Ojos cerrados") {
      const matchingYawn = grouped.find((candidate) => {
        if (candidate.plate !== group.plate || candidate.category !== "Bostezo") return false;
        const tail = candidate.events[candidate.events.length - 1];
        return (
          tail.occurredAtDayjs.isBefore(first.occurredAtDayjs) &&
          first.occurredAtDayjs.diff(tail.occurredAtDayjs, "minute") <= rules.fatigue_merge_window_minutes
        );
      });
      if (matchingYawn) {
        category = "Fatiga en progresion";
        confidence = BASE_CONFIDENCE["Fatiga en progresion"];
        level = "critico";
        label = "Critico · fatiga en progresion";
        title = `${matchingYawn.events.length} bostezos y ${group.events.length} ojos cerrados`;
        detail = `Bostezos ${matchingYawn.events[0].occurredAtDayjs.format("HH:mm")} -> ${matchingYawn.events[matchingYawn.events.length - 1].occurredAtDayjs.format("HH:mm")}, luego ojos cerrados ${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
        consumed.add(`${matchingYawn.plate}|${matchingYawn.category}|${matchingYawn.events[0]?.guid}`);
      } else if (group.events.length >= rules.eyes_closed_critical_threshold) {
        level = "critico";
        confidence = 0.9;
        label = "Critico · racha confirmada";
        title = `${group.events.length} eventos de ojos cerrados`;
        detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
      } else if (group.events.length === 2) {
        level = "alto";
        label = "Alto · por verificar";
        title = "2 eventos de ojos cerrados";
        detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
      } else {
        dismissed += 1;
        continue;
      }
    } else if (group.category === "Uso de celular") {
      level = "critico";
      label = "Critico · deteccion confiable";
      title = "Manipulacion de celular al conducir";
      detail = `${last.occurredAtDayjs.format("HH:mm")}. Un solo evento ya abre alerta.`;
    } else if (group.category === "Riesgo de colision") {
      if (group.events.length >= rules.collision_pattern_threshold) {
        level = "alto";
        label = "Alto · conduccion";
      }
      title = `${group.events.length} riesgos de colision`;
      detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
    } else if (group.category === "Bostezo") {
      if (group.events.length >= rules.yawn_fatigue_threshold) {
        level = "alto";
        label = "Alto · fatiga temprana";
      }
      title = `${group.events.length} bostezos`;
      detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
    } else if (group.category === "Distraccion") {
      if (vehicleDeviation < 3) {
        dismissed += 1;
        continue;
      }
      title = `${group.events.length} distracciones`;
      detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
    } else if (group.category === "Camara cubierta") {
      const activeDays = new Set(
        rawEvents
          .filter((event) => event.plate === group.plate && event.category === group.category)
          .map((event) => event.occurredAtDayjs.format("YYYY-MM-DD")),
      );
      if (activeDays.size >= 2) {
        level = "alto";
        label = "Alto · recurrencia";
      }
      title = "Camara cubierta";
      detail = `${last.occurredAtDayjs.format("HH:mm")}.`;
    } else {
      title = formatCategory(group.category);
      detail = `${last.occurredAtDayjs.format("HH:mm")}.`;
    }

    if (sameDayCount > rules.anti_noise_daily_cap) {
      level = "medio";
      label = "Medio · posible falla de calibracion";
      title = `${formatCategory(group.category)} fuera de patron`;
      detail = `${sameDayCount} eventos del mismo tipo en el dia para ${group.plate}.`;
    }

    const streakFactor = group.events.length >= 4 ? 1.8 : group.events.length >= 2 ? 1.4 : 1;
    const nightFactor = isNight(last.occurredAtDayjs, rules.night_window_start, rules.night_window_end) ? 1.3 : 1;
    const deviationFactor = vehicleDeviation > rules.spike_threshold_multiplier ? 1.25 : 1;
    const priority =
      (WEIGHTS[category] ?? WEIGHTS[group.category] ?? 1) *
      confidence *
      streakFactor *
      nightFactor *
      deviationFactor;

    const cycleGap = cycleMark.diff(last.occurredAtDayjs, "minute", true);

    alerts.push({
      id: group.events.map((event) => event.guid).join("-"),
      plate: group.plate,
      category,
      level,
      label,
      title,
      detail,
      priority,
      occurredAt: last.occurredAtDayjs.toISOString(),
      timeLabel: timeLabel(last.occurredAtDayjs, now),
      isNight: isNight(last.occurredAtDayjs, rules.night_window_start, rules.night_window_end),
      isNew: cycleGap >= 0 && cycleGap <= rules.ingestion_cycle_minutes,
      rawCount: group.events.length,
    });
  }

  alerts.sort((left, right) => dayjs(right.occurredAt).valueOf() - dayjs(left.occurredAt).valueOf());

  const counts = {
    todas: alerts.length,
    critico: alerts.filter((alert) => alert.level === "critico").length,
    alto: alerts.filter((alert) => alert.level === "alto").length,
    noche: alerts.filter((alert) => alert.isNight).length,
  };

  const visible = alerts.filter((alert) => {
    if (filter === "todas") return true;
    if (filter === "noche") return alert.isNight;
    return alert.level === filter;
  });

  const emptyHint = buildEmptyHint(rawEvents, filter, now, rules);

  return {
    all: alerts,
    visible,
    dayAlerts: visible.filter((alert) => !alert.isNight),
    nightAlerts: visible.filter((alert) => alert.isNight),
    counts,
    dismissed,
    emptyHint,
    summary: `${visible.length} de ${alerts.length} alertas · ventana base ${rules.streak_window_minutes} min · colision ${rules.collision_window_minutes} · bostezo ${rules.yawn_window_minutes}`,
  };
}

function buildEmptyHint(
  rawEvents: Array<RecentEvent & { occurredAtDayjs: dayjs.Dayjs }>,
  filter: TimelineFilter,
  now: dayjs.Dayjs,
  rules: DashboardSnapshot["rules"],
) {
  const relevant = rawEvents.filter((event) => {
    if (filter === "todas") return true;
    if (filter === "noche") return isNight(event.occurredAtDayjs, rules.night_window_start, rules.night_window_end);
    if (filter === "critico") return CRITICAL_CATEGORIES.has(event.category);
    return HIGH_CATEGORIES.has(event.category);
  });
  const latest = relevant.at(-1);
  if (!latest) {
    if (filter === "noche") return "No hubo eventos en franja nocturna dentro de las ultimas 24 horas.";
    if (filter === "critico") return "No hubo eventos criticos relacionados dentro de las ultimas 24 horas.";
    if (filter === "alto") return "No hubo eventos altos relacionados dentro de las ultimas 24 horas.";
    return "No hay alertas abiertas en las ultimas 24 horas.";
  }
  const label = filter === "noche" ? "de esa franja" : "de ese grupo";
  return `No hay alertas visibles para este filtro. Ultimo evento ${label}: ${timeLabel(latest.occurredAtDayjs, now)}.`;
}
