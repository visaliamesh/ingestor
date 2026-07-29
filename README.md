# Visalia Mesh Ingestor

Runs one per radio. It connects to a Meshtastic or MeshCore node over serial,
TCP, or BLE and forwards what the radio hears to the
[Visalia Mesh](https://visaliamesh.com) dashboard. Pure RF, no MQTT.

It uses the same ingest API as the potato-mesh ingestor, so you can point an
existing potato setup at it without reconfiguring. One difference worth knowing:
it reads traceroute source and destination correctly, so traces land as real
routes instead of anonymous relay chains.

## Docker

```bash
git clone https://github.com/visaliamesh/ingestor.git
cd ingestor
cp .env.example .env      # set API_TOKEN and CONNECTION
docker compose up -d
docker compose logs -f    # watch it connect
```

On a USB radio, uncomment the `devices:` line in `docker-compose.yml` and set
your port (`ls /dev/ttyUSB* /dev/ttyACM*` to find it). TCP and BLE radios don't
need that, just set `CONNECTION`.

## Plain script

```bash
pip install -r requirements.txt
INSTANCE_DOMAIN=https://map.visaliamesh.com API_TOKEN=xxxx CONNECTION=192.168.1.50 \
  python ingestor.py
```

CLI flags override the env vars (`--server`, `--token`, `--connection`,
`--protocol`).

## Configuration

Set these in `.env` (or the environment):

- `INSTANCE_DOMAIN`: dashboard URL, `https://map.visaliamesh.com`
- `API_TOKEN`: your ingestor token, ask a Visalia Mesh admin
- `CONNECTION`: serial port (`/dev/ttyUSB0`, `COM5`, or blank to auto-detect),
  a `host[:port]` for TCP, or a MAC for BLE
- `PROTOCOL`: `meshtastic` (default) or `meshcore`
- `DEBUG`: `1` for verbose logs
- `ALLOWED_CHANNELS`: whitelist of channel **names** (e.g. `LongFast,Visalia`);
  packets on any other channel are discarded. Blank = accept all. Matched by
  name, case-insensitive; an unnamed primary channel resolves to its modem-preset
  name (`LongFast`, `MediumFast`, …)
- `HIDDEN_CHANNELS`: channel names to drop when forwarding (blacklist)
- `MIN_SNR`: drop packets weaker than this SNR floor in dB (blank = keep all)
- `RX_ONLY`: accepted for potato compatibility but a no-op — this ingestor is
  receive-only and never transmits to the mesh

## Coming from potato-mesh

It reads potato's environment variables (`INSTANCE_DOMAIN`, `API_TOKEN`,
`CONNECTION`, `PROTOCOL`, `DEBUG`, `INGESTOR_NODE_ID`, `ALLOWED_CHANNELS`,
`HIDDEN_CHANNELS`, `MIN_SNR`, `RX_ONLY`), so an existing potato `.env` works
as-is. Set `INSTANCE_DOMAIN` to this server, swap the image for
`ghcr.io/visaliamesh/ingestor:latest`, and restart. Your node keeps reporting
the same data, plus traceroutes with real endpoints.

## Auto-update

If you want new versions to roll out on their own, uncomment the `watchtower`
service in `docker-compose.yml` once you're running the published image. It
checks for a new image hourly and restarts the container when one lands.

## Reading the logs

`docker compose logs -f` (or `docker logs -f <container>`). The lines to look
for:

- `[ok] ...connected, this node: <num>` means the radio is talking to it. Until
  you see this, it hasn't reached the radio.
- `[info] ingest <url> | token …abcd | node ...` prints at startup so you can
  check the URL and token at a glance. The `…abcd` is the last 4 of your token,
  which matches what the dashboard logs, so you can line the two up.
- `[ok] sent N events (M accepted)` is a normal upload.
- `[status] node=... queued=... sent=... failed=... dropped=...` prints every
  few minutes, even when quiet, so you can confirm it's alive. `queued` climbing
  and `failed` climbing together means it can't reach the dashboard.
- `[info] waiting for this node's id before sending` shows only in the first
  second or two after connecting, while it learns its own node number. If it
  stays stuck there, the radio isn't reporting its id; set `INGESTOR_NODE_ID`.

`[warn] send failed` lines carry a hint at the cause:

- `check API_TOKEN` (403/401): the token is wrong or not on the server.
- `check INSTANCE_DOMAIN` (404/405): the URL is wrong. It should point at the
  server root, e.g. `https://map.visaliamesh.com`, not a page under it.
- `dashboard unreachable`: DNS, network, or the server is down.

Set `DEBUG=1` for per-packet detail and the server's response body on failures.

## License

Apache 2.0, see [LICENSE](LICENSE). Same as
[potato-mesh](https://github.com/l5yth/potato-mesh), which this stays
compatible with.
