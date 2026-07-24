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

## Coming from potato-mesh

It reads potato's environment variables (`INSTANCE_DOMAIN`, `API_TOKEN`,
`CONNECTION`, `PROTOCOL`, `DEBUG`, `INGESTOR_NODE_ID`), so an existing potato
`.env` works as-is. Set `INSTANCE_DOMAIN` to this server, swap the image for
`ghcr.io/visaliamesh/ingestor:latest`, and restart. Your node keeps reporting
the same data, plus traceroutes with real endpoints.

## Auto-update

If you want new versions to roll out on their own, uncomment the `watchtower`
service in `docker-compose.yml` once you're running the published image. It
checks for a new image hourly and restarts the container when one lands.

## License

Apache 2.0, see [LICENSE](LICENSE). Same as
[potato-mesh](https://github.com/l5yth/potato-mesh), which this stays
compatible with.
