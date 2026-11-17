#!/usr/bin/env python3
"""¿Por qué el consumidor baja de ~1,100/s a ~300/s después de unos minutos?

    python3 diagnostico.py --mensajes 400000 --segundos 420 --salida diag.csv

Precarga la cola, deja un consumidor (prefetch 10) vaciándola sin productor y
cada segundo anota: cuántos salieron, frecuencia media de los núcleos,
temperatura del paquete, y escrituras y latencia media de escritura del NVMe.
"""
from __future__ import annotations

import argparse
import csv
import glob
import subprocess
import sys
import time
from pathlib import Path

from comun import listos, vaciar

AQUI = Path(__file__).resolve().parent


def freq_mhz() -> float:
    fs = [int(open(f).read()) for f in glob.glob("/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_cur_freq")]
    return round(sum(fs) / len(fs) / 1000)


def temp_c() -> float:
    return int(open("/sys/class/thermal/thermal_zone0/temp").read()) / 1000


def disco() -> tuple[int, int]:
    for l in open("/proc/diskstats"):
        p = l.split()
        if p[2] == "nvme0n1":
            return int(p[7]), int(p[10])      # escrituras completadas, ms escribiendo
    return 0, 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mensajes", type=int, default=400_000)
    ap.add_argument("--segundos", type=int, default=420)
    ap.add_argument("--salida", required=True)
    a = ap.parse_args()
    d = Path(a.salida).parent
    d.mkdir(parents=True, exist_ok=True)
    vaciar()
    for f in d.glob("diag.db*"):
        f.unlink()
    subprocess.run([sys.executable, AQUI / "productor.py", "--tasa", "0", "--cuantos", str(a.mensajes)],
                   cwd=AQUI, check=True)
    time.sleep(20)   # que se asiente lo que dejó la precarga
    c = subprocess.Popen([sys.executable, AQUI / "consumidor.py", "--prefetch", "10", "--db", d / "diag.db",
                          "--registro", d / "diag-consumidor.csv"], cwd=AQUI)
    with open(a.salida, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["s", "salieron", "listos", "mhz", "temp_c", "escrituras", "ms_por_escritura"])
        prev_l, prev_d = listos(), disco()
        t0 = time.monotonic()
        for s in range(a.segundos):
            time.sleep(max(0.0, t0 + s + 1 - time.monotonic()))
            l, dd = listos(), disco()
            n = dd[0] - prev_d[0]
            w.writerow([s + 1, prev_l - l, l, freq_mhz(), temp_c(), n,
                        round((dd[1] - prev_d[1]) / n, 3) if n else ""])
            f.flush()
            prev_l, prev_d = l, dd
            if l == 0:
                break
    c.terminate()
    c.wait()


if __name__ == "__main__":
    main()
