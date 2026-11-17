#!/usr/bin/env python3
"""Corre los escenarios en orden y deja todo en resultados/<fecha>/.

    python3 escenarios.py                     # todos
    python3 escenarios.py --solo capacidad

0. capacidad   cuántos mensajes por segundo guarda el consumidor, con
               prefetch 1, 10 y 100 y con 1, 2 y 4 consumidores.
1. tasa        el productor a 0.5, 0.8, 0.95, 1.05 y 1.2 veces esa capacidad.
2. caida       el consumidor se detiene 60 s mientras el productor sigue, a
               0.5, 0.7 y 0.9 de la capacidad; luego vuelve y vacía el atraso.
3. veneno      un mensaje inválido, uno con falla transitoria (2 fallas) y uno
               que nunca se recupera, con las tres políticas de fallas.
4. sin_ack     cinco mensajes se guardan y el consumidor muere antes del ACK;
               con la tabla ingenua y con la idempotente.

La configuración base de 1 a 4 es prefetch 10 y un consumidor, y su capacidad
es la que mida el escenario 0 en esta misma corrida: nunca un número fijo.
Se niega a empezar un escenario después de las 21:00.
"""
from __future__ import annotations

import argparse
import csv
import json
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from comun import COLA, DLQ, listos, vaciar

AQUI = Path(__file__).resolve().parent
PY = sys.executable
BASE = {"prefetch": 10, "consumidores": 1}


def hora_permitida() -> None:
    h = datetime.now()
    if h.hour * 100 + h.minute >= 2100 or h.hour < 7:
        sys.exit("fuera de horario: los escenarios corren de 7:00 a 21:00")


def lanzar(script: str, *args, salida: Path | None = None) -> subprocess.Popen:
    log = open(salida, "a") if salida else subprocess.DEVNULL
    return subprocess.Popen([PY, str(AQUI / script), *map(str, args)], cwd=AQUI, stdout=log, stderr=log)


def detener(procs) -> None:
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()


class Consumidores:
    """n consumidores; si uno muere (escenario 4) se reinicia al segundo."""

    def __init__(self, d: Path, n: int, prefetch: int, **extra):
        self.d, self.args = d, ["--prefetch", prefetch, "--db", d / "prueba.db"]
        for k, v in extra.items():
            self.args += [f"--{k.replace('_', '-')}", v]
        self.procs = [self._uno(i) for i in range(n)]
        self.reinicios = 0

    def _uno(self, i: int) -> subprocess.Popen:
        return lanzar("consumidor.py", *self.args, "--registro", self.d / f"consumidor{i}.csv",
                      salida=self.d / "consola.txt")

    def vigilar(self) -> None:
        for i, p in enumerate(self.procs):
            if p.poll() is not None:
                time.sleep(1)
                self.procs[i] = self._uno(i)
                self.reinicios += 1

    def detener(self) -> None:
        detener(self.procs)


def esperar(segundos: float, cons: Consumidores | None = None) -> None:
    fin = time.monotonic() + segundos
    while time.monotonic() < fin:
        if cons:
            cons.vigilar()
        time.sleep(0.2)


def filas(d: Path) -> list[dict]:
    out = []
    for f in sorted(d.glob("consumidor*.csv")):
        out += list(csv.DictReader(f.open()))
    return out


def preparar(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    vaciar()
    return d


# 0 ---------------------------------------------------------------------------
def capacidad(raiz: Path, n_msgs: int = 10_000) -> dict:
    tabla = []
    for pf in (1, 10, 100):
        for n in (1, 2, 4):
            hora_permitida()
            d = preparar(raiz / "capacidad" / f"pf{pf}-c{n}")
            subprocess.run([PY, AQUI / "productor.py", "--tasa", "0", "--cuantos", str(n_msgs)], cwd=AQUI,
                           check=True, stdout=subprocess.DEVNULL)
            m = lanzar("muestreo.py", "--salida", d / "muestras.csv")
            cons = Consumidores(d, n, pf)
            limite = time.monotonic() + 180
            while listos() > 0 and time.monotonic() < limite:
                time.sleep(0.2)
            esperar(2)
            cons.detener()
            detener([m])
            r = [x for x in filas(d) if x["resultado"] == "ok"]
            t0 = min(int(x["t_entrega"]) for x in r)
            t1 = max(int(x["t_fin"]) for x in r)
            fila = {"prefetch": pf, "consumidores": n, "guardados": len(r), "segundos": (t1 - t0) / 1000,
                    "por_segundo": round(len(r) / ((t1 - t0) / 1000), 1)}
            tabla.append(fila)
            print("capacidad", fila, flush=True)
    (raiz / "capacidad" / "capacidad.json").write_text(json.dumps(tabla, indent=1))
    return {(x["prefetch"], x["consumidores"]): x["por_segundo"] for x in tabla}


def mu_base(raiz: Path) -> float:
    t = json.loads((raiz / "capacidad" / "capacidad.json").read_text())
    return next(x["por_segundo"] for x in t if x["prefetch"] == BASE["prefetch"]
                and x["consumidores"] == BASE["consumidores"])


def correr_con_productor(d: Path, tasa: float, duracion: float, cons_kw: dict, cola_extra: float = 0,
                         caida: tuple[float, float] | None = None, especiales: str = "",
                         prefetch: int = BASE["prefetch"]) -> dict:
    """Productor a tasa fija; consumidores desde el inicio (salvo durante la caída)."""
    m = lanzar("muestreo.py", "--salida", d / "muestras.csv")
    time.sleep(1.5)
    cons = Consumidores(d, BASE["consumidores"], prefetch, **cons_kw)
    t0 = time.monotonic()
    p = lanzar("productor.py", "--tasa", tasa, "--duracion", duracion, "--salida", d / "pub.csv",
               *(["--especiales", especiales] if especiales else []), salida=d / "consola.txt")
    eventos = {"inicio_ms": int(time.time() * 1000)}
    if caida:
        desde, dura = caida
        esperar(desde, cons)
        cons.detener()
        eventos["caida_ms"] = int(time.time() * 1000)
        time.sleep(dura)
        cons = Consumidores(d, BASE["consumidores"], prefetch, **cons_kw)
        eventos["regreso_ms"] = int(time.time() * 1000)
    while p.poll() is None:
        esperar(0.5, cons)
    eventos["fin_productor_ms"] = int(time.time() * 1000)
    esperar(cola_extra, cons)
    eventos["listos_al_final"] = listos()
    eventos["dlq_al_final"] = listos(DLQ)
    eventos["reinicios"] = cons.reinicios
    cons.detener()
    time.sleep(1)
    eventos["listos_tras_detener"] = listos()   # lo que quedaba en vuelo vuelve a la cola
    detener([m])
    eventos["segundos"] = round(time.monotonic() - t0, 1)
    (d / "eventos.json").write_text(json.dumps(eventos, indent=1))
    return eventos


# 1 ---------------------------------------------------------------------------
def tasa(raiz: Path) -> None:
    mu = mu_base(raiz)
    for f in (0.5, 0.8, 0.95, 1.05, 1.2):
        hora_permitida()
        d = preparar(raiz / "tasa" / f"x{f:.2f}")
        ev = correr_con_productor(d, round(f * mu, 1), 120, {}, cola_extra=20)
        print("tasa", f, ev, flush=True)


# 2 ---------------------------------------------------------------------------
def caida(raiz: Path) -> None:
    mu = mu_base(raiz)
    for f in (0.5, 0.7, 0.9):
        hora_permitida()
        d = preparar(raiz / "caida" / f"x{f:.2f}")
        prevista = f * 60 / (1 - f)          # atraso acumulado / margen de capacidad
        dur = min(60 + 60 + 1.5 * prevista + 60, 900)
        ev = correr_con_productor(d, round(f * mu, 1), dur, {}, cola_extra=5, caida=(60, 60))
        ev["recuperacion_prevista_s"] = round(prevista, 1)
        (d / "eventos.json").write_text(json.dumps(ev, indent=1))
        print("caida", f, ev, flush=True)


# 3 ---------------------------------------------------------------------------
def veneno(raiz: Path) -> None:
    mu = mu_base(raiz)
    lam = round(0.5 * mu, 1)
    n10, n20, n30 = int(10 * lam), int(20 * lam), int(30 * lam)
    esp = f"{n10}:invalido,{n20}:transitoria,{n30}:transitoria5"
    for modo in ("requeue", "dlq", "reintentos"):
        hora_permitida()
        d = preparar(raiz / "veneno" / modo)
        ev = correr_con_productor(d, lam, 60, {"fallas": modo}, cola_extra=10, especiales=esp)
        ev["especiales"] = esp
        (d / "eventos.json").write_text(json.dumps(ev, indent=1))
        print("veneno", modo, ev, flush=True)


# 4 ---------------------------------------------------------------------------
def sin_ack(raiz: Path) -> None:
    mu = mu_base(raiz)
    lam = round(0.5 * mu, 1)
    caer = ",".join(str(int(s * lam)) for s in (8, 18, 28, 38, 48))
    for tabla in ("ingenua", "idempotente"):
        hora_permitida()
        d = preparar(raiz / "sin_ack" / tabla)
        ev = correr_con_productor(d, lam, 60, {"tabla": tabla, "caer_tras_guardar": caer}, cola_extra=10)
        db = sqlite3.connect(d / "prueba.db")
        t = "posiciones_ingenua" if tabla == "ingenua" else "posiciones"
        ev["filas"] = db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        ev["seq_distintos"] = db.execute(f"SELECT COUNT(DISTINCT seq) FROM {t}").fetchone()[0]
        ev["caer"] = caer
        (d / "eventos.json").write_text(json.dumps(ev, indent=1))
        print("sin_ack", tabla, ev, flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--solo", nargs="*", default=["capacidad", "tasa", "caida", "veneno", "sin_ack"])
    ap.add_argument("--raiz")
    a = ap.parse_args()
    raiz = Path(a.raiz) if a.raiz else AQUI.parent / "resultados" / datetime.now().strftime("%Y%m%dT%H%M")
    raiz.mkdir(parents=True, exist_ok=True)
    print("resultados en", raiz, flush=True)
    for nombre in a.solo:
        {"capacidad": capacidad, "tasa": tasa, "caida": caida, "veneno": veneno, "sin_ack": sin_ack}[nombre](raiz)
    print("listo", flush=True)


if __name__ == "__main__":
    main()
