import asyncio
import json
import os
import time

import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlmodel import Session, select

from alert_engine import evaluate
from database import Alert, Evacuation, Telemetry, engine, init_db

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")

app = FastAPI(title="Miner Safety API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ws_clients: list[WebSocket] = []
main_loop: asyncio.AbstractEventLoop | None = None
mqtt_client: mqtt.Client | None = None


# ------------------------------------------------------------------ startup
@app.on_event("startup")
async def startup() -> None:
    global main_loop, mqtt_client
    init_db()
    main_loop = asyncio.get_running_loop()

    mqtt_client = mqtt.Client()
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_HOST, 1883)
    mqtt_client.subscribe("mine/miner/+/telemetry")
    mqtt_client.loop_start()
    print("[startup] MQTT subscribed and DB initialized")


# ------------------------------------------------------------------ mqtt -> db -> ws
def broadcast(event: dict) -> None:
    if main_loop is None:
        return
    for ws in list(ws_clients):
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(json.dumps(event)), main_loop)
        except Exception:
            pass


def on_message(client, userdata, msg) -> None:
    p = json.loads(msg.payload)
    with Session(engine) as session:
        session.add(
            Telemetry(
                ts=p["ts"],
                miner_id=p["miner_id"],
                zone=p["zone"],
                ch4_lel=p["ch4_lel"],
                co_ppm=p["co_ppm"],
                o2_pct=p["o2_pct"],
                h2s_ppm=p["h2s_ppm"],
                body_temp_c=p["body_temp_c"],
                heart_rate=p["heart_rate"],
                x=p["x"],
                y=p["y"],
            )
        )
        fired = evaluate(p)
        for level, metric, value in fired:
            session.add(
                Alert(
                    ts=time.time(),
                    miner_id=p["miner_id"],
                    zone=p["zone"],
                    level=level,
                    metric=metric,
                    value=value,
                )
            )
        session.commit()

    for level, metric, value in fired:
        event = {
            "type": "alert",
            "level": level,
            "miner_id": p["miner_id"],
            "zone": p["zone"],
            "metric": metric,
            "value": value,
            "ts": time.time(),
        }
        broadcast(event)
        client.publish("mine/alerts", json.dumps(event))

        if level == "EVACUATE":
            trigger_evacuation(p["zone"], f"{metric}={value}", p["miner_id"], client)


def trigger_evacuation(zone: str, reason: str, by: str, client=None) -> None:
    with Session(engine) as session:
        session.add(
            Evacuation(
                ts=time.time(),
                zone=zone,
                reason=reason,
                triggered_by=by,
                status="OPEN",
            )
        )
        session.commit()

    event = {
        "type": "EVACUATION",
        "zone": zone,
        "reason": reason,
        "triggered_by": by,
        "ts": time.time(),
    }
    broadcast(event)
    if client:
        client.publish("mine/evacuation", json.dumps(event))
    print(f"[EVACUATION] zone={zone} reason={reason} triggered_by={by}")


# ------------------------------------------------------------------ REST
@app.get("/alerts")
def get_alerts(limit: int = 50):
    with Session(engine) as session:
        rows = session.exec(
            select(Alert).order_by(Alert.id.desc()).limit(limit)  # type: ignore[attr-defined]
        ).all()
    return {"alerts": [r.model_dump() for r in rows]}


@app.get("/miners")
def get_miners():
    with Session(engine) as session:
        rows = session.exec(select(Telemetry)).all()
    # keep latest per miner
    latest: dict[str, Telemetry] = {}
    for r in rows:
        prev = latest.get(r.miner_id)
        if prev is None or r.ts > prev.ts:
            latest[r.miner_id] = r
    return {"miners": [m.model_dump() for m in latest.values()]}


@app.get("/stats")
def get_stats():
    with Session(engine) as session:
        tele = session.exec(select(Telemetry)).all()
        alerts = session.exec(select(Alert)).all()
        evacs = session.exec(select(Evacuation)).all()

    miners_online = len({t.miner_id for t in tele})
    warn_alerts = sum(1 for a in alerts if a.level == "WARN")
    open_evacs = sum(1 for e in evacs if e.status == "OPEN")
    last_seen = max((t.ts for t in tele), default=None)
    return {
        "miners_online": miners_online,
        "warn_alerts": warn_alerts,
        "open_evacuations": open_evacs,
        "last_seen": last_seen,
        "now": time.time(),
    }


class EvacRequest(BaseModel):
    zone: str
    reason: str = "manual admin trigger"
    triggered_by: str = "admin"


@app.post("/evacuate")
def evacuate(req: EvacRequest):
    trigger_evacuation(req.zone, req.reason, req.triggered_by, mqtt_client)
    return {"status": "evacuation_triggered", "zone": req.zone}


@app.post("/evacuate/{evac_id}/close")
def close_evacuation(evac_id: int):
    with Session(engine) as session:
        row = session.get(Evacuation, evac_id)
        if row:
            row.status = "CLOSED"
            session.add(row)
            session.commit()
    return {"status": "closed", "id": evac_id}


@app.get("/evacuations")
def get_evacuations():
    with Session(engine) as session:
        rows = session.exec(
            select(Evacuation).order_by(Evacuation.id.desc()).limit(20)  # type: ignore[attr-defined]
        ).all()
    return {"evacuations": [r.model_dump() for r in rows]}


# ------------------------------------------------------------------ WebSocket
@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    try:
        while True:
            await ws.receive_text()  # keepalive
    except WebSocketDisconnect:
        ws_clients.remove(ws)