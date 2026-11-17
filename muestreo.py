#!/usr/bin/env python3
"""Cada segundo, lo que el broker dice de las tres colas.

    python3 muestreo.py --salida muestras.csv      # hasta SIGTERM

Listos y sin confirmar los cuenta el broker; las tasas son las suyas,
promediadas por la API de administración (collect_statistics_interval =
1000 ms). La edad del más viejo sale de la marca de tiempo del mensaje en la
cabeza de la cola, con resolución de un segundo.
"""
from __future__ import annotations

import argparse
import csv
import signal
import time

from comun import COLA, DLQ, ESPERA, ahora_ms, api

seguir = True


def parar(*_):
    global seguir
    seguir = False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", required=True)
    ap.add_argument("--cada", type=float, default=1.0)
    a = ap.parse_args()
    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)

    with open(a.salida, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t", "listos", "sin_confirmar", "consumidores", "tasa_pub", "tasa_ack", "tasa_reentrega",
                    "edad_mas_viejo_s", "dlq", "espera", "memoria_broker_mb"])
        siguiente = time.monotonic()
        while seguir:
            t = ahora_ms()
            try:
                qs = {q["name"]: q for q in api("/queues/%2F")}
                n = api("/nodes")[0]
                p = qs.get(COLA, {})
                ms = p.get("message_stats", {})
                cabeza = p.get("head_message_timestamp")
                w.writerow([t, p.get("messages_ready", 0), p.get("messages_unacknowledged", 0),
                            p.get("consumers", 0),
                            round(ms.get("publish_details", {}).get("rate", 0.0), 1),
                            round(ms.get("ack_details", {}).get("rate", 0.0), 1),
                            round(ms.get("redeliver_details", {}).get("rate", 0.0), 1),
                            (t // 1000 - cabeza) if cabeza else "",
                            qs.get(DLQ, {}).get("messages", 0), qs.get(ESPERA, {}).get("messages", 0),
                            round(n.get("mem_used", 0) / 1e6, 1)])
                f.flush()
            except Exception as e:      # una muestra perdida no detiene la corrida
                w.writerow([t, "", "", "", "", "", "", "", "", "", f"error: {e}"])
            siguiente += a.cada
            time.sleep(max(0.0, siguiente - time.monotonic()))


if __name__ == "__main__":
    main()
