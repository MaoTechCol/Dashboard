from __future__ import annotations

CATEGORY_ORDER = [
    "Uso de celular",
    "Fatiga en progresion",
    "Ojos cerrados",
    "Riesgo de colision",
    "Bostezo",
    "Camara cubierta",
    "Fumando",
    "Distraccion",
]

CATEGORY_META = {
    "Uso de celular": {
        "weight": 10,
        "severity": "critico",
        "color": "#ff5c8a",
        "icon": "smartphone",
    },
    "Fatiga en progresion": {
        "weight": 8,
        "severity": "critico",
        "color": "#c084fc",
        "icon": "moon",
    },
    "Ojos cerrados": {
        "weight": 6,
        "severity": "critico",
        "color": "#ef4444",
        "icon": "eye-off",
    },
    "Riesgo de colision": {
        "weight": 5,
        "severity": "alto",
        "color": "#fb7185",
        "icon": "triangle-alert",
    },
    "Bostezo": {
        "weight": 3,
        "severity": "alto",
        "color": "#f97316",
        "icon": "circle-alert",
    },
    "Camara cubierta": {
        "weight": 3,
        "severity": "medio",
        "color": "#818cf8",
        "icon": "camera-off",
    },
    "Fumando": {
        "weight": 2,
        "severity": "medio",
        "color": "#f59e0b",
        "icon": "cigarette",
    },
    "Distraccion": {
        "weight": 1,
        "severity": "medio",
        "color": "#10b981",
        "icon": "activity",
    },
}

DEFAULT_SUBTYPE_MAP = {
    "65": "Ojos cerrados",
    "66": "Bostezo",
    "67": "Distraccion",
    "68": "Uso de celular",
}
