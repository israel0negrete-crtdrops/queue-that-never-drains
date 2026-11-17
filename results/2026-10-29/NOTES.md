# Runs of October 29, 2026

Three runs, in order. Each was designed after seeing the previous one, and
what changed is written at the top of its script before it ran. (Spanish
version: `LEEME.md`.)

## 1. First run (14:57–15:58), `escenarios.py`

- **Capacity on 10,000 messages (~9 s):** prefetch 10 and one consumer,
  1,095/s. With 2 and 4 consumers, 1,001 and 925/s: the database writes one at
  a time. Prefetch 1: 723/s.
- **Rate, 2 min each (rates from the second minute):** at 0.95 of that
  capacity, 1,041/s in and out, it does not grow; at 1.05, 1,150/s in and
  1,122/s out, it grows 28/s; at 1.20, 1,314/s in, 1,103/s out, it grows
  211/s.
- **60 s outage:** at 0.5, 57 s to catch up (predicted 60); at 0.7, 134 s
  (predicted 140). At 0.9 it never recovered: 470,000 messages and 8 min of age
  at the end. See the second run.
- **Poison:** requeued, the invalid message was redelivered thousands of times
  a second and the broker showed 0 ready. With a straight DLQ, the transient
  one was lost to the DLQ. With retries: invalid to the DLQ on the first
  attempt, transient stored on the third, the exhausted one to the DLQ.
- **Stored without ACK (5 crashes):** naive table, 5 extra rows; key
  (device, event time), 0.

## Diagnosis (16:03), `diagnostico.py`

Draining 400,000 with no producer: ~1,150/s for the first 3 min, then ~300/s.
Its CSV is not included; the 15-minute run of the second run
(`segunda/sostenida/fila.csv`) records the same columns and shows the same
thing: temperature around 46 °C on average in both phases, average frequency
down from 1,727 to 1,441 MHz (less work), and the NVMe going from ~3,200
writes/s at a median of 0.5 ms to ~835/s at 1.75 ms, with peaks of 11.5 ms.
The cause inside the drive (cache, garbage collection) is **not verified**.

## 2. Second run (16:19–17:20), `segunda.py`

- **Sustained, 15 min:** 1,159, 1,169, 1,130 and then 261–275/s. Average of
  the last 5 min: 266/s.
- **Rest:** after 30, 120 and 300 s of pause, 1,043, 1,153 and 1,151/s over the
  first 20 s.
- **Rate, 10 min:** at 213, 253 and 293/s, no backlog.
- **Batch of 10 (prefetch 20):** drained 1,000,000 in 129 s, ~7,750/s (short run).
- One consumer ended with a pika error when stopped mid-acknowledgment (what
  was not acknowledged went back to the queue); fixed for the third run.

## 3. Third run (18:11–18:53), `tercera.py`

No rest between cases: each inherits the disk state of the previous one.

- **400/s:** caught up for 8 min; in min 9 it started falling behind, in min 10
  ~286/s went out; when the producer stopped, age was 22 s (31 s while
  draining afterward).
- **550/s:** ~300/s from min 2; 143,000 queued and 263 s of age at the end.
- **700/s:** 680/s the first minute, then ~295/s; 217,000 and 312 s.
- **3,000/s with batches of 10:** 2,995/s the first minute, then dropping from
  2,512 to ~1,950/s; 448,000 queued and 150 s of age at the end.

## What can and cannot be said

- Short-test capacity (1,100/s) is not the sustained one (~270–300/s on this
  machine). How long the fast phase lasts depends on the rate and on how long
  the disk rested before.
- 293/s held for 10 min; it was not tested longer.
- Batching raised the sustained capacity about seven times (~2,000/s against
  ~290/s), but it also degraded over time.
- The figures belong to this machine: a 128 GB drive without DRAM, with the
  broker and the database on the same disk.
