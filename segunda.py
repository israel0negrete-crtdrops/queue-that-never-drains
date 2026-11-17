#!/usr/bin/env python3
"""Segunda corrida: la capacidad que se sostiene, no la de diez segundos.

    python3 segunda.py --raiz ../resultados/20261029T1457/segunda

La primera corrida midió la capacidad vaciando 10,000 mensajes (unos 9 s).
El diagnóstico mostró que, tras ~3 minutos de un commit por mensaje, el NVMe
del N100 escribe tres veces más lento y el consumidor baja de ~1,150/s a
~300/s. Esta corrida mide eso de frente. Definiciones fijadas ANTES de correr:

1. sostenida   un consumidor (prefetch 10, un commit por mensaje) vacía una
               cola precargada durante 15 min. Capacidad sostenida = la media
               de mensajes por segundo de los últimos 5 minutos.
2. descanso    con el disco ya lento: pausa de 30, 120 y 300 s, y tras cada
               una, 40 s vaciando. Se anota la media de los primeros 20 s.
3. tasa        productor a 0.80, 0.95 y 1.10 de la capacidad sostenida,
               10 min cada uno, con un consumidor normal.
4. lote        como 1, pero confirmando por lote (10 por commit, prefetch 20),
               10 min.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

import escenarios as e
from comun import listos, vaciar
from diagnostico import disco, freq_mhz, temp_c

AQUI = Path(__file__).resolve().parent
PY = sys.executable


def precargar(n: int) -> None:
    subprocess.run([PY, AQUI / "productor.py", "--tasa", "0", "--cuantos", str(n)], cwd=AQUI, check=True,
                   stdout=subprocess.DEVNULL)


def vaciar_midiendo(d: Path, nombre: str, segundos: int, prefetch: int = 10, lote: int = 1) -> list[dict]:
    """Un consumidor vacía la cola precargada; cada segundo: salieron, frecuencia, temperatura, disco."""
    args = ["--prefetch", str(prefetch), "--db", d / f"{nombre}.db", "--registro", d / f"{nombre}-consumidor.csv"]
    if lote > 1:
        args += ["--lote", str(lote)]
    c = subprocess.Popen([PY, AQUI / "consumidor.py", *map(str, args)], cwd=AQUI)
    filas = []
    prev_l, prev_d = listos(), disco()
    t0 = time.monotonic()
    for s in range(segundos):
        time.sleep(max(0.0, t0 + s + 1 - time.monotonic()))
        l, dd = listos(), disco()
        n = dd[0] - prev_d[0]
        filas.append({"s": s + 1, "salieron": prev_l - l, "listos": l, "mhz": freq_mhz(), "temp_c": temp_c(),
                      "escrituras": n, "ms_por_escritura": round((dd[1] - prev_d[1]) / n, 3) if n else ""})
        prev_l, prev_d = l, dd
        if l == 0:
            break
    c.terminate()
    c.wait()
    with (d / f"{nombre}.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0]))
        w.writeheader()
        w.writerows(filas)
    return filas


def media(filas: list[dict], desde: int, hasta: int) -> float:
    return round(statistics.fmean(x["salieron"] for x in filas[desde:hasta]), 1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raiz", required=True)
    a = ap.parse_args()
    raiz = Path(a.raiz)
    raiz.mkdir(parents=True, exist_ok=True)
    res: dict = {}

    # 1. sostenida
    e.hora_permitida()
    d = raiz / "sostenida"
    d.mkdir(exist_ok=True)
    vaciar()
    precargar(1_100_000)
    time.sleep(20)
    f = vaciar_midiendo(d, "fila", 900)
    res["sostenida"] = {"primeros_60s": media(f, 0, 60), "ultimos_300s": media(f, 600, 900),
                        "segundos": len(f), "por_minuto": [media(f, i, i + 60) for i in range(0, len(f), 60)]}
    mu = res["sostenida"]["ultimos_300s"]
    print("sostenida", res["sostenida"], flush=True)

    # 2. descanso (la cola sigue cargada con lo que no salió)
    res["descanso"] = []
    for pausa in (30, 120, 300):
        e.hora_permitida()
        if listos() < 60_000:
            precargar(100_000)
        time.sleep(pausa)
        f = vaciar_midiendo(d, f"descanso-{pausa}", 40)
        res["descanso"].append({"pausa_s": pausa, "primeros_20s": media(f, 0, 20), "ultimos_20s": media(f, 20, 40)})
        print("descanso", res["descanso"][-1], flush=True)

    # 3. tasa contra la capacidad sostenida
    res["tasa"] = []
    for fac in (0.80, 0.95, 1.10):
        e.hora_permitida()
        dt = e.preparar(raiz / "tasa" / f"x{fac:.2f}")
        ev = e.correr_con_productor(dt, round(fac * mu, 1), 600, {}, cola_extra=10)
        ev["tasa"] = round(fac * mu, 1)
        (dt / "eventos.json").write_text(json.dumps(ev, indent=1))
        res["tasa"].append({"factor": fac, **ev})
        print("tasa", fac, ev, flush=True)

    # 4. lote
    e.hora_permitida()
    d4 = raiz / "lote"
    d4.mkdir(exist_ok=True)
    vaciar()
    precargar(1_000_000)
    time.sleep(20)
    f = vaciar_midiendo(d4, "lote10", 600, prefetch=20, lote=10)
    res["lote"] = {"primeros_60s": media(f, 0, 60), "ultimos_300s": media(f, 300, 600), "segundos": len(f),
                   "por_minuto": [media(f, i, i + 60) for i in range(0, len(f), 60)]}
    print("lote", res["lote"], flush=True)
    vaciar()

    (raiz / "segunda.json").write_text(json.dumps(res, indent=1))
    print("TERMINADO", flush=True)


if __name__ == "__main__":
    main()
