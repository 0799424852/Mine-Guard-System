THRESHOLDS = {
    # metric: (warn, evacuate). For o2_pct, low is bad.
    "ch4_lel":     (1.0, 2.5),
    "co_ppm":      (25, 100),
    "o2_pct":      (19.5, 18.0),
    "h2s_ppm":     (5, 15),
    "body_temp_c": (38.0, 39.0),
    "heart_rate":  (120, 140),
}

LOW_IS_BAD = {"o2_pct"}


def evaluate(payload: dict) -> list[tuple[str, str, float]]:
    """Return a list of (level, metric, value) alerts for one reading."""
    out: list[tuple[str, str, float]] = []
    for metric, (warn, evac) in THRESHOLDS.items():
        value = payload.get(metric)
        if value is None:
            continue
        if metric in LOW_IS_BAD:
            if value <= evac:
                out.append(("EVACUATE", metric, value))
            elif value <= warn:
                out.append(("WARN", metric, value))
        else:
            if value >= evac:
                out.append(("EVACUATE", metric, value))
            elif value >= warn:
                out.append(("WARN", metric, value))
    return out