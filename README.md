<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo-dark.png">
    <img src="docs/logo-light.png" alt="Visalia Mesh" width="140">
  </picture>
</p>

<h1 align="center">Visalia Mesh Ingestor</h1>

<p align="center">
  Turn a radio into a listening post for <a href="https://visaliamesh.com">visaliamesh.com</a>.
</p>

You run one of these next to a Meshtastic or MeshCore radio. It connects to the
radio over serial, TCP, or BLE, watches everything the radio hears on the air,
and forwards it to the Visalia Mesh dashboard. It reads the radio only. It never
transmits, never reboots your node, and never touches your settings. There is no
MQTT anywhere in the path, just the RF your antenna actually picks up.

The more radios feeding the dashboard from different spots around town, the
better the coverage map and the message history get. If you have a node up, this
is how you put it on the map.

## Contents

- [What it captures](#what-it-captures)
- [Quick start with Docker](#quick-start-with-docker)
- [A full docker-compose example](#a-full-docker-compose-example)
- [Running it as a plain script](#running-it-as-a-plain-script)
- [Connecting to your radio](#connecting-to-your-radio)
- [Configuration](#configuration)
- [Packet filters](#packet-filters)
- [What it sends: the ingest API](#what-it-sends-the-ingest-api)
- [Reading the logs](#reading-the-logs)
- [Keeping and watching logs](#keeping-and-watching-logs)
- [Auto-updating](#auto-updating)
- [Building the image yourself](#building-the-image-yourself)
- [How it works](#how-it-works)
- [Requirements](#requirements)
- [License](#license)

## What it captures

Whatever your radio decodes off the air, batched up and sent to the dashboard:

- **Text messages** on the channels you allow, with signal strength and hop count
- **Node info**: names, hardware model, and role as nodes announce themselves
- **Positions** from nodes that share GPS
- **Telemetry**: battery, voltage, channel utilization, and environment sensors
  (temperature, humidity, pressure) when a node reports them
- **Receptions**: a record of every packet heard, which powers the coverage heat
  map and the signal and hop statistics
- **Traceroutes**, captured in both directions with per-hop signal strength
- **Neighbor info** when nodes broadcast their direct neighbors
- On **MeshCore**, the full relay path each packet took, so the dashboard can
  show the exact chain of repeaters that carried a message

It also reports its own radio's health (firmware, battery, uptime, channel load)
so the dashboard can show which listeners are online.

## Quick start with Docker

This is the easy path and the one most people should take.

```bash
git clone https://github.com/visaliamesh/ingestor.git
cd ingestor
cp .env.example .env      # then edit .env: set API_TOKEN and CONNECTION
docker compose up -d
docker compose logs -f    # watch it connect
```

You need an `API_TOKEN`. Ask a Visalia Mesh admin for one.

For a USB radio, open `docker-compose.yml`, find the `devices:` block, uncomment
it, and set your port. Run `ls /dev/ttyUSB* /dev/ttyACM*` to find the port name.
Radios reached over TCP or BLE don't need that block, just set `CONNECTION`.

## A full docker-compose example

A complete, commented file for a Meshtastic radio reached over TCP. Change
`CONNECTION`, `PROTOCOL`, and the filters to match your node.

```yaml
services:
  visalia-ingestor:                                    # name it whatever you like
    image: ghcr.io/visaliamesh/ingestor:latest
    container_name: visalia-ingestor
    restart: unless-stopped                            # REQUIRED: the radio watchdog restarts by exiting
    network_mode: bridge
    environment:
      INSTANCE_DOMAIN: "https://map.visaliamesh.com"   # dashboard to send to (bare host works too)
      API_TOKEN: "ask-an-admin-for-this"               # your ingestor token
      CONNECTION: "10.0.0.101:4403"                    # radio: host[:port] (TCP), /dev/ttyUSB0 (serial), or AA:BB:CC:DD:EE:FF (BLE)
      PROTOCOL: "meshtastic"                           # meshtastic (default) or meshcore
      ALLOWED_CHANNELS: "MediumFast"                   # only forward these channel NAMES (blank = all)
      RADIO_DOWN_EXIT_MIN: "5"                          # watchdog: restart if the radio is unreachable this long (0 = off)
      DEBUG: "0"                                        # 1 = verbose; the failure hints still show at 0
    logging:                                           # cap the on-disk log (optional, recommended)
      driver: json-file
      options:
        max-size: "10m"
        max-file: "5"

    # For a USB/serial radio instead of TCP, drop the host:port from CONNECTION
    # (or leave it blank to auto-detect) and pass the device through:
    # devices:
    #   - "/dev/ttyUSB0:/dev/ttyUSB0"
```

Then `docker compose up -d` and `docker compose logs -f`.

## Running it as a plain script

No Docker required. Python 3.10 or newer.

```bash
pip install -r requirements.txt

INSTANCE_DOMAIN=https://map.visaliamesh.com \
API_TOKEN=your-token \
CONNECTION=192.168.1.50 \
  python ingestor.py
```

Command-line flags override the environment if you'd rather pass them directly:

```bash
python ingestor.py \
  --server https://map.visaliamesh.com \
  --token your-token \
  --connection 192.168.1.50 \
  --protocol meshtastic
```

| Flag | Same as | Meaning |
| --- | --- | --- |
| `--server` | `INSTANCE_DOMAIN` | Dashboard base URL |
| `--token` | `API_TOKEN` | Your ingestor token |
| `--connection` | `CONNECTION` | Serial port, `host[:port]`, or BLE MAC |
| `--protocol` | `PROTOCOL` | `meshtastic` or `meshcore` |

## Connecting to your radio

The `CONNECTION` value tells the ingestor how to reach the radio. It figures out
the type from what you give it:

| You want | Set `CONNECTION` to | Notes |
| --- | --- | --- |
| USB / serial | blank, `/dev/ttyUSB0`, `/dev/ttyACM0`, or `COM5` | Blank auto-detects the first serial radio it finds |
| Network (TCP) | `192.168.1.50` or `192.168.1.50:4403` | Default port is 4403 for Meshtastic, 5000 for MeshCore |
| Bluetooth (BLE) | `AA:BB:CC:DD:EE:FF` | The radio's Bluetooth MAC address |

If it loses the connection, it waits a few seconds and reconnects on its own. You
don't need to babysit it.

## Configuration

Everything is set through environment variables, usually in your `.env` file.
Only the first three are required.

### Required

| Variable | Example | What it does |
| --- | --- | --- |
| `INSTANCE_DOMAIN` | `https://map.visaliamesh.com` | Dashboard to send to. A bare host like `map.visaliamesh.com` works too; the `https://` is added for you. |
| `API_TOKEN` | `a1b2c3...` | Your ingestor token. Ask an admin. Sent as a bearer token on every upload. |
| `CONNECTION` | `192.168.1.50` | How to reach the radio. See the table above. Leave blank to auto-detect a serial radio. |

### Common

| Variable | Default | What it does |
| --- | --- | --- |
| `PROTOCOL` | `meshtastic` | Which mesh this radio runs: `meshtastic` or `meshcore`. |
| `DEBUG` | `0` | Set to `1` for per-packet logging and the server's full response on any failure. Handy during setup, noisy for daily use. |

### Filters (optional)

| Variable | Default | What it does |
| --- | --- | --- |
| `ALLOWED_CHANNELS` | blank (all) | Comma-separated list of channel **names** to forward. Anything on another channel is dropped before it leaves your machine. Case-insensitive. Example: `LongFast,Visalia`. |
| `HIDDEN_CHANNELS` | blank (none) | Channel **names** to never forward, even if `ALLOWED_CHANNELS` would let them through. |
| `MIN_SNR` | blank (keep all) | Drop any packet received weaker than this signal-to-noise floor, in dB. |

### Overrides (rarely needed)

| Variable | Default | What it does |
| --- | --- | --- |
| `INGESTOR_NODE_ID` | auto | Pin this listener's own node id when the radio can't report it. Use the radio's id, for example `!cc384cc7`, or a plain number. Auto-detection handles this on its own in almost every case. |
| `RADIO_DOWN_EXIT_MIN` | `5` | Radio watchdog. If the radio can't be reconnected for this many minutes, the ingestor exits so the container restarts fresh (see below). Set `0` to turn it off. |
| `RX_ONLY` | unset | A no-op. The ingestor only ever listens. It is read only so an older config that still sets this keeps working. |
| `MESH_PROTOCOL` | unset | Older name for `PROTOCOL`. Still read if present. |

### Radio watchdog

The ingestor reconnects on its own whenever the radio drops, retrying every 15
seconds. But if the radio itself is down or unreachable for a long stretch (host
powered off, IP changed, the radio's TCP service wedged), reconnecting can never
succeed, and older versions would loop on that forever until someone restarted
the container by hand.

Now, after `RADIO_DOWN_EXIT_MIN` minutes (default 5) of failed reconnects, the
ingestor logs the reason and exits. Docker's `restart: unless-stopped` then
starts a fresh process, which re-resolves the radio's host and rebuilds the
connection from scratch: the same thing a manual restart does, done for you.

This only works if the container has a restart policy. The compose example
above sets `restart: unless-stopped`; keep it. To disable the watchdog, set
`RADIO_DOWN_EXIT_MIN: "0"`.

## Packet filters

The three filter variables run on your machine, before anything is uploaded, so
a packet you filter out never leaves your network.

Channel names are matched case-insensitively. A radio whose primary channel has
no explicit name resolves to its modem-preset name (`LongFast`, `MediumFast`,
and so on), so you can match it by that.

```bash
# Only forward LongFast and the Visalia channel, and only if heard at -10 dB SNR
# or better:
ALLOWED_CHANNELS=LongFast,Visalia
MIN_SNR=-10
```

## What it sends: the ingest API

If you just want data on the map, you can skip this section. It's here for anyone
curious about the wire format or building against the same endpoint.

Every few seconds the ingestor drains its queue and POSTs a batch of events to a
single endpoint on the dashboard.

### Request

```
POST {INSTANCE_DOMAIN}/api/ingest
Authorization: Bearer {API_TOKEN}
Content-Type: application/json
```

```json
{
  "events": [ { "type": "reception", "num": 3663953607, "ts": 1723900000, "...": "..." } ],
  "ingestor_node": 3663953607,
  "ingestor_version": "1.3.7"
}
```

- `events` is a list of up to 100 event objects (see below).
- `ingestor_node` is this listener's own node number, so the dashboard credits
  the batch to the right radio. It is `null` for the first moment after startup,
  before the radio has reported its own id, during which nothing is sent yet.
- `ingestor_version` is this software's version, shown on the dashboard so admins
  can see which listeners are up to date.

### Response

```json
{ "accepted": 42 }
```

`accepted` is how many of the events in the batch the dashboard stored. It can be
lower than the number sent when some are duplicates already on record (the same
packet heard by two listeners collapses into one), which is normal and healthy.

### Event types

Every event has a `type`, the originating node number `num`, and a unix
timestamp `ts`. The rest depends on the type. Fields are omitted when the radio
didn't report them.

| Type | Key fields |
| --- | --- |
| `reception` | `snr`, `rssi`, `hops`, `hop_start`, `hop_limit`, `portnum`, and `path` on MeshCore. One per packet heard; the backbone of the coverage and signal stats. |
| `message` | `text`, `channel`, `msg_id`, `to`, `reply_id`, `emoji`, `snr`, `rssi`, `hops`, and `path` on MeshCore. |
| `nodeinfo` | `node_id`, `long_name`, `short_name`, `hw_model`, `role`. |
| `position` | `lat`, `lon`, `alt`. Only sent when the fix is real, never the no-GPS `(0, 0)` sentinel. |
| `telemetry` | any of `battery`, `voltage`, `ch_util`, `air_util`, `temp`, `humidity`, `pressure`. |
| `traceroute` | `to`, `route`, `route_back`, `snr_towards`, `snr_back`. Both directions, with per-hop SNR in dB. |
| `neighbors` | `neighbors`, a list of `{ num, snr }` for each directly-heard neighbor. |

On MeshCore, `path` is the relay chain, one byte per hop, each byte being the
first byte of a relay's public key. The dashboard resolves those back to known
repeaters to draw the route a message took.

The ingestor is receive only. It never calls any endpoint that would change the
dashboard, and it never sends anything to the radio.

## Reading the logs

`docker compose logs -f`, or `docker logs -f visalia-ingestor`. The lines are
written to say what happened and, when something fails, whether the problem is on
the dashboard/network side or in your own config.

Healthy lines:

- `[ok] ...connected, this node: <num>`: the radio is talking to it. Until you
  see this, it hasn't reached the radio.
- `[info] ingest <url> | token ...abcd | node ...`: prints at startup so you can
  eyeball the URL and token. The `...abcd` is the last four of your token and
  matches what the dashboard logs.
- `[ok] sent 100 events (100 accepted) in 0.4s`: a normal upload. The **duration**
  is a live read on dashboard health; a line ending `<-- SLOW: dashboard is near
  the timeout` means the server is struggling, not you.
- `[ok] sent 100 events (62 accepted, 38 dupes ignored) in 0.5s`: `dupes` just
  means another listener already reported those packets. Normal and healthy.
- `[status] node=... queued=... pending=... sent=... accepted=... failed=...
  dropped=... consec_fail=... session_age=...s`: a heartbeat every five minutes,
  even when the mesh is quiet. `consec_fail=0` means all is well.
- `[info] waiting for this node's id before sending`: shows for a second or two
  after connecting while it learns its own node number. If it sticks there, the
  radio isn't reporting an id; set `INGESTOR_NODE_ID`.

When an upload fails, it **retries on its own with backoff**, and the warning says
what happened and whose side it's on:

| Failure hint | What it means |
| --- | --- |
| `no reply within the timeout -> DASHBOARD slow/overloaded` | The server didn't answer in time (often busy, or a redeploy). Server side; it backs off and retries. |
| `connection dropped mid-request -> DASHBOARD restarted or a Cloudflare hiccup` | Usually a dashboard redeploy. Rides out on its own. |
| `HTTP 502/503/504 ... -> DASHBOARD side` | Gateway/origin error; server side. |
| `auth rejected -> YOUR config: check API_TOKEN` | Your token is wrong or not registered. |
| `wrong path -> YOUR config: check INSTANCE_DOMAIN` | Your URL is wrong; point it at the server root, not a page under it. |
| `DNS can't resolve` / `connection refused` | Your network, or a typo in `INSTANCE_DOMAIN`. |

It recovers by itself. After a few failures in a row it rebuilds its connection
(`[warn] rebuilt the HTTP session ...`, the thing a container restart used to fix)
and prints `[ok] dashboard reachable again after N failure(s)` once it's back, so
you should not need to restart it manually.

Set `DEBUG=1` for per-packet detail and the server's full response body on a
failure. The failure hints above show at `DEBUG=0` too.

## Keeping and watching logs

Docker keeps each container's log on disk, but by default that file has **no size
limit and grows forever**. The `logging:` block in the compose example caps it:
`max-size: 10m` rotates the log at 10 MB and `max-file: 5` keeps five files
(~50 MB total), deleting the rest. It has no effect on ingestion; it just stops
the log filling the disk on a box that runs for months. Optional but recommended.
(If your Docker host already sets a global default in `/etc/docker/daemon.json`,
the per-container block is redundant.)

Watch or review:

- Live: `docker logs -f visalia-ingestor`
- Just the problems: `docker logs visalia-ingestor 2>&1 | grep -E "warn|failed|rebuilt|reachable"`

You don't have to watch logs to know a listener is up, either: the dashboard's
Stats page shows every ingestor's online/offline state, last report time, and
version.

## Auto-updating

To have new versions roll out on their own, switch to the published image and
turn on Watchtower. In `docker-compose.yml`, comment out `build: .`, uncomment
the `image:` line, and uncomment the `watchtower` service at the bottom. It
checks for a newer image once an hour and restarts the container when one lands.

```yaml
    # build: .
    image: ghcr.io/visaliamesh/ingestor:latest
```

## Building the image yourself

The published image on GHCR covers both `amd64` and `arm64`, so it runs on a
regular server or a Raspberry Pi without any changes. If you'd rather build from
source:

```bash
docker build -t visalia-ingestor .
```

The `docker-compose.yml` builds locally by default (`build: .`), which is what
the quick start above uses.

## How it works

One process, a couple of moving parts:

- A **listener** thread connected to your radio. For Meshtastic it uses the
  official `meshtastic` library and subscribes to its packet events. For MeshCore
  it uses the official `meshcore` library and its async event stream. Each
  decoded packet becomes one or more events.
- A bounded in-memory **queue** (up to 5000 events). If the dashboard is
  unreachable for a while, new events pile up here and the oldest are dropped
  once it fills, so a long outage never eats all your memory.
- A **sender** thread that wakes every five seconds, pulls up to 100 events off
  the queue, and POSTs them. It is built to ride out a flaky or briefly-down
  dashboard on its own: it retries gateway errors and dropped connections on a
  fresh socket, keeps the batch and backs off exponentially (up to a minute) on
  repeated failures instead of hammering, and **rebuilds its whole HTTP
  connection after a few failures in a row** so a wedged connection heals itself
  without a container restart. Retries are safe because the dashboard de-dupes
  every event, so a re-sent batch is a no-op.

It waits until the radio has reported its own node number before it sends
anything, so every packet is credited to the right listener from the very first
upload. Alongside the mesh traffic, it reports its own radio's health at startup
and every five minutes. On Meshtastic it also re-reads the radio's node database
once an hour, but only forwards the nodes whose name, role, or hardware actually
**changed** since last time, so the periodic refresh is a trickle rather than a
burst. On MeshCore it refreshes the contact roster the same way every 15 minutes,
so repeaters the radio learns later pick up their names and roles.

None of this ever writes to the radio. Grep the source if you like; there are no
send, reboot, or admin calls anywhere in it.

## Requirements

- Python 3.10 or newer (3.12 in the Docker image).
- A Meshtastic or MeshCore radio reachable over serial, TCP, or BLE.
- Network access from the machine to your dashboard.

Python dependencies are pinned in `requirements.txt`: `requests`, plus
`meshtastic` or `meshcore` depending on which mesh you run.

Tested against a range of hardware, from Heltec V3 and RAK WisBlock boards to the
Seeed Wio Tracker and fixed station nodes. Anything the official libraries
support should work.

## License

Apache 2.0. See [LICENSE](LICENSE).
