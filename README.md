# The queue that never drains

A lab measurement of when a RabbitMQ consumer stops keeping up: a real broker,
a consumer that stores every message in SQLite before acknowledging it, and a
producer at a fixed rate. Synthetic messages shaped like a GPS position; no
production queues and no data from real devices.

Note: [The queue that never drains](https://crtdrops.mx/notas/the-queue-that-never-drains.html)
(Spanish: [La cola que no se vacía](https://crtdrops.mx/notas/la-cola-que-no-se-vacia.html)).

The code and its comments are in Spanish; this README and `results/2026-10-29/NOTES.md`
are in English.

## What was measured

All on October 29, 2026, on one Intel N100 (4 cores, 8 GB, a 128 GB Samsung
MZVLQ128 NVMe without DRAM), with the broker and the database on the same disk.

- **Short-test capacity:** draining 10,000 preloaded messages, one consumer with
  prefetch 10 stored 1,095 a second. More consumers gave less (SQLite writes one
  at a time).
- **Sustained capacity:** draining for 15 minutes, 1,159, 1,169 and 1,130 a
  second for the first three minutes, then 261–275. Average of the last five:
  266. Temperature stayed around 46 °C on average in both phases; the average
  CPU frequency went down (1,727 to 1,441 MHz, less work); the NVMe went from
  about 3,200 writes a second at a median of 0.5 ms to about 835 at 1.75 ms,
  with peaks of 11.5 ms (`results/2026-10-29/segunda/sostenida/fila.csv`). The
  cause inside the drive is **not verified**.
- **Where it breaks:** at 400 a second the queue stayed caught up for eight
  minutes and then grew. 213, 253 and 293 a second held for ten minutes (not
  tested longer).
- **Batching:** 10 positions per commit, acknowledged together after the
  commit: about 7,750 a second draining (a 129 s run), and about 2,000 a
  second after ten minutes with a producer at 3,000.
- **Outage of 60 s:** at 50 % and 70 % of capacity, catching up took 57 and
  134 s (predicted 60 and 140).
- **Poison message:** requeued, it was redelivered about 3,850 times a second
  while the broker showed 0 messages ready.
- **Stored but not acknowledged (5 crashes):** 5 duplicate rows without a key,
  0 with the key (device, event time).

The figures belong to this machine. Another server will have other numbers;
what carries over is that the capacity to measure is the sustained one.

## Setup (`lab/`)

RabbitMQ 3.12.1 and Erlang 25.3.2.8 from the Ubuntu 24.04 repositories,
unpacked in user space: no sudo, no systemd service, nothing starts on boot.

```bash
L=~/labs/06-cola; mkdir -p $L/debs $L/root && cd $L/debs
apt-get download rabbitmq-server erlang-base erlang-asn1 erlang-crypto erlang-eldap \
  erlang-ftp erlang-inets erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key \
  erlang-runtime-tools erlang-snmp erlang-ssl erlang-syntax-tools erlang-tftp erlang-tools \
  erlang-xmerl python3-pika
for f in *.deb; do dpkg -x $f $L/root; done
E=$L/root/usr/lib/erlang
sed -i "s#/usr/lib/erlang#$E#g" $E/bin/erl $E/bin/start $E/erts-*/bin/erl $E/erts-*/bin/start
```

The sha256 of the 19 packages used are in `lab/debs.sha256`. Then copy
`rabbitmq.conf`, `advanced.config` and `enabled_plugins` to `$L/etc/`, and
`entorno.sh`, `arrancar.sh` and `detener.sh` to `$L/` (edit the paths in
`entorno.sh`). The four ports (5672, 15672, 25672, 4369) listen on 127.0.0.1
only. `arrancar.sh` refuses to start between 21:30 and 7:00.

## Code

| File | What it does |
|---|---|
| `comun.py` | topology (queue, DLQ, 1 s wait queue for retries), synthetic message |
| `productor.py` | publishes at a fixed rate against the clock |
| `consumidor.py` | stores in SQLite (WAL, `synchronous=FULL`) and only then acknowledges; failure policies; batching; crash after storing |
| `muestreo.py` | what the broker reports, every second |
| `escenarios.py` | first run: capacity, rate, outage, poison, no ACK |
| `diagnostico.py` | why capacity drops: frequency, temperature, disk latency |
| `segunda.py`, `tercera.py` | sustained capacity, rest, break point, batching |
| `analizar.py`, `series.py` | summary and per-second series |

Each script documents at the top what was fixed before running it.

```bash
. ~/labs/06-cola/entorno.sh && ~/labs/06-cola/arrancar.sh
python3 escenarios.py                 # about an hour
python3 analizar.py ../resultados/<run>
```

## Results (`results/2026-10-29/`)

Per-second series, broker samples and events for every case; see `NOTES.md`.
The raw per-message logs (759 MB) are not included.

## License

MIT. Not affiliated with RabbitMQ, Broadcom or Samsung.
