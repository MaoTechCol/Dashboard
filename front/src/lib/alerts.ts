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
  note?: string;
}

interface TimelineMarker {
  id: string;
  kind: "marker";
  label: string;
  timeLabel: string;
}

interface TimelineAlertEntry {
  id: string;
  kind: "alert";
  alert: GroupedAlert;
}

const DISPLAY_LABELS: Record<string, string> = {
  "Uso de celular": "Uso de celular",
  "Fatiga en progresion": "Fatiga en progresión",
  "Ojos cerrados": "Ojos cerrados",
  "Riesgo de colision": "Riesgo de colisión",
  Bostezo: "Bostezo",
  "Camara cubierta": "Cámara cubierta",
  Fumando: "Fumando",
  Distraccion: "Distracción",
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
  const snapshotCut = snapshot.meta.publishedCutAt ?? snapshot.meta.generatedAt;
  const now = dayjs(snapshotCut).tz(timezoneName);
  const windowStart = now.subtract(24, "hour");
  const rules = snapshot.rules;
  const cycleMark = dayjs(snapshotCut).tz(timezoneName);
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
    const episodeCategory = event.episodeTitle ?? event.category;
    const key = event.episodeGuid
      ? `episode|${event.episodeGuid}`
      : `${event.plate}|${event.category}`;
    const current = openGroups.get(key);
    const gapMinutes = current
      ? event.occurredAtDayjs.diff(current.events[current.events.length - 1].occurredAtDayjs, "minute", true)
      : null;
    const windowMinutes =
      event.category === "Riesgo de colision"
        ? rules.collision_window_minutes
        : event.category === "Bostezo"
          ? rules.yawn_window_minutes
          : rules.streak_window_minutes;

    if (current && (event.episodeGuid || (gapMinutes !== null && gapMinutes <= windowMinutes))) {
      current.events.push(event);
      continue;
    }

    const nextGroup = { plate: event.plate, category: episodeCategory, events: [event] };
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
    let note: string | undefined;

    if (group.category === "Fatiga en progresion") {
      const yawns = group.events.filter((event) => event.category === "Bostezo");
      const eyes = group.events.filter((event) => event.category === "Ojos cerrados");
      category = "Fatiga en progresion";
      confidence = BASE_CONFIDENCE["Fatiga en progresion"];
      level = "critico";
      label = "Crítico · fatiga en progresión";
      title = `${yawns.length} bostezos y ${eyes.length} ojos cerrados`;
      detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
    } else if (group.category === "Ojos cerrados") {
      const matchingYawn = grouped.find((candidate) => {
        if (group.events.some((event) => event.episodeGuid)) return false;
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
        label = "Crítico · fatiga en progresión";
        title = `${matchingYawn.events.length} bostezos y ${group.events.length} ojos cerrados`;
        detail = `Bostezos ${matchingYawn.events[0].occurredAtDayjs.format("HH:mm")} -> ${matchingYawn.events[matchingYawn.events.length - 1].occurredAtDayjs.format("HH:mm")}, luego ojos cerrados ${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
        consumed.add(`${matchingYawn.plate}|${matchingYawn.category}|${matchingYawn.events[0]?.guid}`);
      } else if (group.events.length >= rules.eyes_closed_critical_threshold) {
        level = "critico";
        confidence = 0.9;
        label = "Crítico · racha confirmada";
        title = `${group.events.length} eventos de ojos cerrados`;
        detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
      } else {
        level = "alto";
        label = "Alto · por verificar";
        title = "1 evento de ojos cerrados";
        detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
      }
      if (sameDayCount > 12) {
        note = "Puede ser por uso de gafas oscuras. Conviene revisar configuracion o calibracion del sensor.";
      }
    } else if (group.category === "Uso de celular") {
      level = "critico";
      label = "Crítico · detección confiable";
      title = "Manipulación de celular al conducir";
      detail = `${last.occurredAtDayjs.format("HH:mm")}. Un solo evento ya abre alerta.`;
    } else if (group.category === "Riesgo de colision") {
      if (group.events.length >= rules.collision_pattern_threshold) {
        level = "alto";
        label = "Alto · conducción";
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
      title = `${group.events.length} distracciones`;
      detail = `${first.occurredAtDayjs.format("HH:mm")} -> ${last.occurredAtDayjs.format("HH:mm")}.`;
    } else if (group.category === "Camara cubierta") {
      if (last.ruleLevel === "alto") {
        level = "alto";
        label = "Alto · recurrencia";
      }
      title = "Cámara cubierta";
      detail = `${last.occurredAtDayjs.format("HH:mm")}.`;
    } else {
      title = formatCategory(group.category);
      detail = `${last.occurredAtDayjs.format("HH:mm")}.`;
    }

    const backendLevel = group.events.find((event) => event.ruleLevel)?.ruleLevel;
    if (backendLevel) {
      level = backendLevel;
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
      note,
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
  const entries = buildTimelineEntries({
    alerts: visible,
    now,
    windowStart,
    nightStartHour: rules.night_window_start,
    nightEndHour: rules.night_window_end,
  });

  return {
    all: alerts,
    visible,
    entries,
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

function buildTimelineEntries({
  alerts,
  now,
  windowStart,
  nightStartHour,
  nightEndHour,
}: {
  alerts: GroupedAlert[];
  now: dayjs.Dayjs;
  windowStart: dayjs.Dayjs;
  nightStartHour: number;
  nightEndHour: number;
}) {
  const markers = buildNightMarkers({ alerts, now, windowStart, nightStartHour, nightEndHour });
  const entries: Array<TimelineAlertEntry | TimelineMarker> = alerts.map((alert) => ({
    id: alert.id,
    kind: "alert",
    alert,
  }));

  for (const marker of markers) {
    const index = entries.findIndex((entry) => {
      if (entry.kind !== "alert") return false;
      return dayjs(entry.alert.occurredAt).valueOf() <= marker.at.valueOf();
    });
    const payload: TimelineMarker = {
      id: `marker-${marker.label}-${marker.at.toISOString()}`,
      kind: "marker",
      label: marker.label,
      timeLabel: formatBoundaryTimeLabel(marker.at, now),
    };
    if (index === -1) {
      entries.push(payload);
    } else {
      entries.splice(index, 0, payload);
    }
  }

  return entries;
}

function buildNightMarkers({
  alerts,
  now,
  windowStart,
  nightStartHour,
  nightEndHour,
}: {
  alerts: GroupedAlert[];
  now: dayjs.Dayjs;
  windowStart: dayjs.Dayjs;
  nightStartHour: number;
  nightEndHour: number;
}) {
  if (alerts.length < 2) return [];
  const newestAlertAt = dayjs(alerts[0].occurredAt);
  const oldestAlertAt = dayjs(alerts[alerts.length - 1].occurredAt);
  const markers: Array<{ label: string; at: dayjs.Dayjs }> = [];
  let cursor = windowStart.startOf("day").subtract(1, "day");
  const lastCursor = now.startOf("day").add(1, "day");

  while (cursor.isBefore(lastCursor) || cursor.isSame(lastCursor, "day")) {
    const start = cursor.hour(nightStartHour).minute(0).second(0).millisecond(0);
    const end = (nightStartHour < nightEndHour ? cursor : cursor.add(1, "day"))
      .hour(nightEndHour)
      .minute(0)
      .second(0)
      .millisecond(0);

    if (start.isAfter(windowStart) && start.isBefore(now) && start.isBefore(newestAlertAt) && start.isAfter(oldestAlertAt)) {
      markers.push({ label: "Comienzo de franja nocturna", at: start });
    }
    if (end.isAfter(windowStart) && end.isBefore(now) && end.isBefore(newestAlertAt) && end.isAfter(oldestAlertAt)) {
      markers.push({ label: "Fin de franja nocturna", at: end });
    }

    cursor = cursor.add(1, "day");
  }

  return markers.sort((left, right) => right.at.valueOf() - left.at.valueOf());
}

function formatBoundaryTimeLabel(value: dayjs.Dayjs, now: dayjs.Dayjs) {
  if (value.isSame(now, "day")) return value.format("HH:mm");
  if (value.add(1, "day").isSame(now, "day")) return `ayer ${value.format("HH:mm")}`;
  return value.format("DD/MM HH:mm");
}
