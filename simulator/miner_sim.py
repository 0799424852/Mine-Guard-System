import json
import random
import time

import paho.mqtt.client as mqtt

BROKER = "localhost"
MINERS = ["M001", "M002", "M003"]
ZONES = ["tunnel_A", "tunnel_B", "shaft_1"]

client = mqtt.Client()
client.connect(BROKER, 1883)


def reading(miner_id: str) -> dict:
    spike = random.random() < 0.02  # 2% chance of hazard spike
    return {
        "miner_id": miner_id,
        "zone": random.choice(ZONES),
        "ts": time.time(),
        "ch4_lel": round(random.uniform(0, 3) + (45 if spike else 0), 2),
        "co_ppm": round(random.uniform(0, 15) + (120 if spike else 0), 2),
        "o2_pct": round(random.uniform(19.5, 20.9), 2),
        "h2s_ppm": round(random.uniform(0, 2), 2),
        "body_temp_c": round(random.uniform(36.2, 37.6), 2),
        "heart_rate": random.randint(65, 110),
        "x": random.randint(0, 200),
        "y": random.randint(0, 200),
    }


if __name__ == "__main__":
    print(f"Publishing simulated telemetry to MQTT broker at {BROKER}:1883")
    while True:
        for m in MINERS:
            payload = reading(m)
            client.publish(f"mine/miner/{m}/telemetry", json.dumps(payload))
        time.sleep(2)