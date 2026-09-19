#!/usr/bin/env python3
"""Visalia Mesh ingestor for Meshtastic and MeshCore.

Listens to a radio over serial, TCP, or BLE and forwards what it hears (RF only,
no MQTT) to the Visalia Mesh dashboard ingest API.

Configured with environment variables; CLI flags override:

    INSTANCE_DOMAIN   dashboard base URL           (--server)
    API_TOKEN         ingestor bearer token        (--token)
    CONNECTION        how to reach the radio       (--connection)
                        serial:  COM5, /dev/ttyACM0   (blank = auto-detect)
                        tcp:     192.168.1.50 or 192.168.1.50:4403
                        ble:     AA:BB:CC:DD:EE:FF
    PROTOCOL          meshtastic (default) | meshcore   (--protocol)
    DEBUG             1 for verbose logging

Examples:
    MESH_PROTOCOL=meshtastic CONNECTION=192.168.1.50 \
      INSTANCE_DOMAIN=https://dash.visaliamesh.com API_TOKEN=... python ingestor.py

    python ingestor.py --server http://127.0.0.1:8080 --token ... \
      --protocol meshcore --connection 192.168.1.60

Requires:  pip install meshtastic requests        (Meshtastic)
           pip install meshcore requests          (MeshCore, Python 3.10+)
"""

import argparse
import asyncio
import collections
import hashlib
import os
import queue
import re
import sys
import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__version__ = "1.5.0"     # bump on each release; logged at startup

FLUSH_SECONDS = 2         # upload cadence: smaller/more-frequent batches so the
                          # dashboard's live SSE stream trickles instead of chunking
                          # every 5 s (still batches bursts up to MAX_BATCH per POST)
MAX_BATCH = 100
MAX_QUEUE = 5000
STATUS_SECONDS = 300      # print a health line at least this often, even when idle
SEND_TIMEOUT = 30         # per-POST timeout (s); the dashboard can be slow under load
MAX_BACKOFF = 60          # cap the exponential retry backoff at this many seconds
SESSION_REBUILD_AFTER = 3 # consecutive failures before we throw away the HTTP session
# radio watchdog: if the radio can't be (re)connected for this long, exit so the
# container's restart policy starts a FRESH process — which re-resolves the host
# and rebuilds the interface, exactly what a manual restart does. 0 disables it.
RADIO_DOWN_EXIT_S = int(os.environ.get("RADIO_DOWN_EXIT_MIN", "5") or "5") * 60
# feed watchdog: a socket can look perfectly alive while NO packets flow — a
# half-open TCP to a MeshMonitor proxy or a shared node that quietly dropped us, a
# wedged USB radio. The connection-error watchdog above never fires (there's no
# error), so the ingestor "stays online" but ingests nothing. We fix that by
# measuring ACTUAL packet flow: silent for this long → force a reconnect, and if
# reconnecting doesn't restore the feed, restart the container. 0 disables it.
# Keep it comfortably above the longest gap a healthy feed shows. A busy Meshtastic
# mesh is never silent this long; MeshCore is far sparser so it gets a wider window.
RX_IDLE_S = int(os.environ.get("RX_IDLE_RESTART_MIN", "10") or "10") * 60
# MeshCore traffic is far sparser (a quiet mesh can be minutes between frames) AND
# its library already auto-reconnects the transport, so its feed watchdog is a pure
# last-resort with a much wider window — only genuinely-dead silence trips it.
MC_RX_IDLE_S = RX_IDLE_S * 12 if RX_IDLE_S else 0

events: "queue.Queue[dict]" = queue.Queue(maxsize=MAX_QUEUE)
cfg = None
self_num = None
last_rx = 0.0            # wall time of the last packet actually received (feed watchdog)
_mt_conn_lost = False    # set by the meshtastic 'connection.lost' pubsub event
DEBUG = os.environ.get("DEBUG") == "1"

# running totals for the periodic [status] line and for debugging
STATS = {"sent": 0, "accepted": 0, "failed": 0, "dropped": 0}

# health the ingestor periodically reports to the dashboard so the admin panel
# can show it remotely (the operator's Docker logs aren't reachable from there)
START_TIME = time.time()
RECENT_LOG: "collections.deque" = collections.deque(maxlen=200)  # last warn/error lines
LAST_ERROR = None                                               # {"text":..,"ts":..} or None

# last identity (id, name, short, hw, role) we forwarded per node, so the hourly
# node-db re-seed only sends nodes that actually CHANGED instead of re-POSTing the
# whole phonebook (hundreds of nodes) as one burst every hour
_seeded_ids: dict = {}


def log(msg: str) -> None:
    print(msg, flush=True)


def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[debug] {msg}", flush=True)


_warned_keys: set = set()


def dbg_once(key: str, msg: str) -> None:
    """Debug-log a recurring, harmless condition only the FIRST time, so it does
    not repeat on every poll and bury the lines that matter."""
    if key not in _warned_keys:
        _warned_keys.add(key)
        dbg(msg)


def warn(msg: str) -> None:
    """An operational warning: print to stderr AND keep it in a small ring buffer
    the ingestor reports to the dashboard, so an operator can see recent trouble
    from the admin panel without shell access to the box."""
    global LAST_ERROR
    line = f"[warn] {msg}"
    print(line, file=sys.stderr, flush=True)
    ts = int(time.time())
    RECENT_LOG.append([ts, line[:400]])
    LAST_ERROR = {"text": msg[:400], "ts": ts}


def status_payload(consec_fail: int) -> dict:
    """Structured health + the recent-warning tail, sent to the dashboard on the
    ingest heartbeat so the admin panel can show how a listener is doing."""
    return {
        "uptime_s": int(time.time() - START_TIME),
        "sent": STATS["sent"], "accepted": STATS["accepted"],
        "failed": STATS["failed"], "dropped": STATS["dropped"],
        "queued": events.qsize(), "consec_fail": consec_fail,
        "last_error": LAST_ERROR,
        "recent_log": list(RECENT_LOG),
    }


def flush_and_exit(reason: str) -> None:
    """Radio watchdog last resort. Log why, give the background sender a few
    seconds to drain the queue and report a final status, then hard-exit so the
    container's restart policy (restart: unless-stopped) starts a fresh process.
    A clean process re-resolves the radio host and rebuilds the interface, which
    is exactly what manually restarting the container does."""
    warn(reason)
    deadline = time.time() + 8
    while not events.empty() and time.time() < deadline:
        time.sleep(0.5)
    log(f"[watchdog] restarting the container so the radio reconnects cleanly"
        f" (queued left: {events.qsize()})")
    os._exit(1)


def channel_allowed(name) -> bool:
    """Channel gate. ALLOWED_CHANNELS is a name whitelist,
    HIDDEN_CHANNELS a name blacklist, both case-insensitive. A None/unknown
    channel passes (fail-open) so a name-resolution miss can never silently
    discard every packet; only channels we can actually name get filtered."""
    allowed = getattr(cfg, "allowed_channels", None) or set()
    hidden = getattr(cfg, "hidden_channels", None) or set()
    if not allowed and not hidden:
        return True
    if name is None:
        return True
    n = str(name).strip().lower()
    if allowed and n not in allowed:
        return False
    if n in hidden:
        return False
    return True


def put(ev: dict) -> None:
    ev.setdefault("network", cfg.protocol)
    # MIN_SNR: drop packets weaker than the floor. Events with
    # no SNR (roster nodeinfo, self telemetry) carry None and always pass.
    mn = getattr(cfg, "min_snr", None)
    snr = ev.get("snr")
    if mn is not None and isinstance(snr, (int, float)) and snr < mn:
        return
    try:
        events.put_nowait(ev)
    except queue.Full:
        STATS["dropped"] += 1
        # the queue only fills when the dashboard has been unreachable for a
        # while, so rate-limit this or it floods the log
        if STATS["dropped"] % 100 == 1:
            warn(f"event queue full, dropping events (total dropped {STATS['dropped']})")


def send_hint(exc: Exception) -> str:
    """Turn a failed POST into a one-line pointer at the likely cause, so the log
    says WHAT went wrong and whether it's ours (dashboard/network) or yours."""
    resp = getattr(exc, "response", None)
    if resp is not None:
        code = resp.status_code
        if code in (401, 403):
            return "  (auth rejected -> YOUR config: check API_TOKEN)"
        if code in (404, 405):
            return "  (wrong path -> YOUR config: check INSTANCE_DOMAIN, e.g. https://map.visaliamesh.com)"
        if code in (502, 503, 504):
            return f"  (HTTP {code} gateway/origin error -> DASHBOARD side is down, restarting, or overloaded; backing off)"
        if code >= 500:
            return f"  (HTTP {code} server error -> DASHBOARD side; backing off)"
        return f"  (HTTP {code})"
    # no HTTP response == the request never completed. Classify by the exception.
    s = f"{type(exc).__name__}: {exc}".lower()
    if "timed out" in s or "timeout" in s:
        return "  (no reply within the timeout -> DASHBOARD slow/overloaded or a redeploy in progress; backing off)"
    if "remotedisconnected" in s or "connection aborted" in s or "reset" in s or "broken pipe" in s:
        return "  (connection dropped mid-request -> DASHBOARD restarted or a Cloudflare hiccup; retrying on a fresh connection)"
    if "name or service not known" in s or "getaddrinfo" in s or "nodename nor servname" in s:
        return "  (DNS can't resolve the host -> YOUR network/DNS, or a typo in INSTANCE_DOMAIN)"
    if "refused" in s:
        return "  (connection refused -> DASHBOARD down or wrong host/port in INSTANCE_DOMAIN)"
    return "  (dashboard unreachable -> check YOUR network and INSTANCE_DOMAIN)"


def make_session() -> requests.Session:
    """A requests session that retries transient gateway/origin errors on a FRESH
    connection. A dropped keep-alive or a 502 while the dashboard restarts should
    not fail the whole batch by itself."""
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {cfg.token}"
    retry = Retry(total=2, connect=2, read=2, backoff_factor=0.5,
                  status_forcelist=(502, 503, 504),
                  allowed_methods=frozenset(["POST"]), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def flush_loop() -> None:
    pending: list[dict] = []
    session = make_session()
    session_started = time.time()
    url = cfg.server.rstrip("/") + "/api/ingest"
    last_status = time.time()
    last_status_sent = 0.0            # when we last reported health to the dashboard
    holding = False
    consec_fail = 0
    fail_since = 0.0

    while True:
        time.sleep(FLUSH_SECONDS)

        # Hold everything until we know our own node id. The dashboard credits a
        # batch to whoever sent it; with no id it falls back to the shared token
        # bucket, so a node's early packets would land under "community" instead
        # of itself. The radio reports the id within a second of connecting, so
        # this only ever holds the first flush or two. Events wait safely in the
        # queue (bounded by MAX_QUEUE) until then.
        if self_num is None:
            if not holding and not events.empty():
                log("[info] waiting for this node's id before sending; events queued")
                holding = True
            continue
        if holding:
            log(f"[ok] node id known ({self_num}); sending")
            holding = False

        while len(pending) < MAX_BATCH:
            try:
                pending.append(events.get_nowait())
            except queue.Empty:
                break

        # report health to the dashboard on the same ~5 min cadence (rides on a
        # batch when there is one, else a status-only POST keeps the heartbeat)
        status_due = (time.time() - last_status_sent) >= STATUS_SECONDS
        if pending or status_due:
            t0 = time.time()
            body = {"events": pending, "ingestor_node": self_num,
                    "ingestor_version": __version__}
            if status_due:
                body["status"] = status_payload(consec_fail)
            try:
                r = session.post(url, json=body, timeout=SEND_TIMEOUT)
                r.raise_for_status()
                dur = time.time() - t0
                accepted = r.json().get("accepted")
                STATS["sent"] += len(pending)
                STATS["accepted"] += accepted or 0
                if consec_fail:   # we were failing, now we're back
                    log(f"[ok] dashboard reachable again after {consec_fail} failure(s)"
                        f" / {int(time.time() - fail_since)}s down")
                    consec_fail = 0
                if status_due:
                    last_status_sent = time.time()
                if pending:
                    dupes = len(pending) - (accepted or 0)
                    log(f"[ok] sent {len(pending)} events ({accepted} accepted"
                        + (f", {dupes} dupes ignored" if dupes > 0 else "")
                        + f") in {dur:.1f}s"
                        + ("  <-- SLOW: dashboard is near the timeout" if dur > SEND_TIMEOUT * 0.6 else ""))
                else:
                    dbg(f"status heartbeat sent in {dur:.1f}s")
                pending = []
            except Exception as exc:
                consec_fail += 1
                STATS["failed"] += 1
                if consec_fail == 1:
                    fail_since = t0
                backoff = min(FLUSH_SECONDS * (2 ** (consec_fail - 1)), MAX_BACKOFF)
                warn(f"send failed ({consec_fail} in a row, {events.qsize()} queued,"
                     f" {len(pending)} in this batch), retry in {backoff}s: {exc}{send_hint(exc)}")
                resp = getattr(exc, "response", None)
                if DEBUG and resp is not None:
                    dbg(f"response {resp.status_code}: {resp.text[:300]}")
                # a wedged connection pool is the classic "had to restart the
                # container" case — throw the session away and rebuild so we
                # self-heal instead of failing forever on a dead connection
                if consec_fail % SESSION_REBUILD_AFTER == 0:
                    session = make_session()
                    session_started = time.time()
                    warn(f"rebuilt the HTTP session after {consec_fail}"
                         " consecutive failures (self-heal, no restart needed)")
                pending = pending[-MAX_BATCH * 5:]  # cap retry backlog
                time.sleep(backoff)                 # exponential backoff, don't hammer

        # a heartbeat so operators can tell it is alive and healthy even when the
        # radio is quiet; also the quickest read on queue depth and error counts
        if time.time() - last_status >= STATUS_SECONDS:
            log(f"[status] node={self_num} queued={events.qsize()} pending={len(pending)}"
                f" sent={STATS['sent']} accepted={STATS['accepted']}"
                f" failed={STATS['failed']} dropped={STATS['dropped']}"
                f" consec_fail={consec_fail} session_age={int(time.time() - session_started)}s")
            last_status = time.time()


def real_position(lat, lon) -> bool:
    """(0,0) is the Meshtastic no-GPS-fix sentinel; single-axis zeros are real."""
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return False
    return not (abs(lat) < 1e-9 and abs(lon) < 1e-9)


# ====================================================================
# Meshtastic (official python lib, pubsub events)
# ====================================================================

def mt_on_receive(packet, interface):  # noqa: ANN001 - meshtastic pubsub signature
    # ANY packet from the radio proves the feed is alive — even our own echo or a
    # packet we filter out. The feed watchdog keys off this timestamp.
    global last_rx
    last_rx = time.time()
    try:
        mt_handle_packet(packet)
    except Exception as exc:
        warn(f"failed to handle packet: {exc}")


def mt_on_conn_lost(*_args, **_kwargs) -> None:
    """The meshtastic library fires 'meshtastic.connection.lost' from its reader
    thread when the link drops. Flag it so the main loop reconnects at once instead
    of spinning obliviously until something else notices."""
    global _mt_conn_lost
    _mt_conn_lost = True


def mt_hops(packet: dict):
    """Hops a packet travelled = hop_start - hop_limit; 0 means we heard the
    origin directly. hop_start is required (see below); hop_limit defaults to 0
    when absent. Accept snake_case too in case a non-standard client feeds us.
    Returns (hops|None, hop_start, hop_limit)."""
    hs = packet.get("hopStart")
    if hs is None:
        hs = packet.get("hop_start")
    hl = packet.get("hopLimit")
    if hl is None:
        hl = packet.get("hop_limit")
    # proto3 omits a field that equals 0, so an ABSENT hop_limit means 0 (the
    # packet used up all its hops), not "unknown"; otherwise every fully
    # relayed packet is dropped to null. hop_start is the originator's max hops
    # and is only meaningful when > 0; with it, hops = hop_start - hop_limit
    # (0 = heard directly). Without a real hop_start we genuinely can't tell.
    hops = max(hs - (hl or 0), 0) if hs else None
    return hops, hs, hl


def mt_handle_packet(packet: dict) -> None:
    num = packet.get("from")
    if num is None:
        return
    # ALLOWED/HIDDEN_CHANNELS: discard the WHOLE packet when
    # it arrived on a channel we're not accepting (by real name; falls open if
    # the name is unknown). chan_name is also used to label the message below.
    chan_idx = packet.get("channel", 0)
    chan_name = mt_channel_names.get(chan_idx)
    if not channel_allowed(chan_name):
        return
    ts = int(time.time())
    decoded = packet.get("decoded", {})
    portnum = decoded.get("portnum", "UNKNOWN")
    snr = packet.get("rxSnr")
    rssi = packet.get("rxRssi")
    hops, hop_start, hop_limit = mt_hops(packet)
    base = {"num": num, "ts": ts, "snr": snr, "rssi": rssi, "hops": hops}

    # A node can't hear itself over RF: its own packets are just the API echoing
    # back what it transmitted, so they aren't real receptions (their snr/rssi/
    # hops are meaningless). Skip the reception record for them to keep "nodes
    # heard" and the hop stats honest. The message/position/telemetry/nodeinfo
    # below are still recorded, so an active node's own mesh traffic is kept.
    if num != self_num:
        if DEBUG and hops is None:
            # diagnoses nodes stuck at "0 direct": show the hop fields that
            # arrived so we can see whether the firmware/build sends hop_start
            dbg(f"no hops from {num} port={portnum}: hopStart={packet.get('hopStart')!r}"
                f" hopLimit={packet.get('hopLimit')!r}"
                f" keys={sorted(k for k in packet if k != 'decoded')}")
        put({**base, "type": "reception", "hop_limit": hop_limit,
             "hop_start": hop_start, "portnum": str(portnum)})

    if portnum == "TEXT_MESSAGE_APP":
        # prefer the real channel name so a non-primary radio (e.g. a MediumFast
        # primary) isn't relabeled by the server's index->name map; fall back to
        # the index when the name is unknown (server maps it via DASH_CHANNEL_NAMES)
        put({**base, "type": "message", "text": decoded.get("text", ""),
             "to": packet.get("to"), "channel": chan_name if chan_name else chan_idx,
             "msg_id": packet.get("id"),
             "reply_id": decoded.get("replyId"), "emoji": decoded.get("emoji")})

    elif portnum == "POSITION_APP":
        pos = decoded.get("position", {})
        lat, lon = pos.get("latitude"), pos.get("longitude")
        pos_time = pos.get("time")
        if real_position(lat, lon):
            put({**base, "type": "position", "lat": lat, "lon": lon,
                 "ts": int(pos_time) if pos_time and pos_time > 0 else ts,
                 "alt": pos.get("altitude")})

    elif portnum == "NODEINFO_APP":
        user = decoded.get("user", {})
        put({**base, "type": "nodeinfo", "node_id": user.get("id"),
             "long_name": user.get("longName"), "short_name": user.get("shortName"),
             "hw_model": str(user.get("hwModel", "")) or None,
             "role": mt_role(user)})

    elif portnum == "TELEMETRY_APP":
        tel = decoded.get("telemetry", {})
        dev = tel.get("deviceMetrics")
        env = tel.get("environmentMetrics")
        if dev:
            put({**base, "type": "telemetry",
                 "battery": dev.get("batteryLevel"), "voltage": dev.get("voltage"),
                 "ch_util": dev.get("channelUtilization"),
                 "air_util": dev.get("airUtilTx")})
        if env:
            put({**base, "type": "telemetry",
                 "temp": env.get("temperature"),
                 "humidity": env.get("relativeHumidity"),
                 "pressure": env.get("barometricPressure")})

    elif portnum == "TRACEROUTE_APP":
        rd = decoded.get("traceroute", {})
        # capture BOTH directions: forward (route/snrTowards) AND the return
        # path (routeBack/snrBack) the reply carries. protobuf SNR is dB * 4.
        # request_id != 0 marks a traceroute REPLY (it references the original
        # request's packet id); a request carries request_id 0. This lets the
        # dashboard tell request from reply DEFINITIVELY (who traced whom) instead
        # of guessing from whether route_back is populated.
        put({**base, "type": "traceroute", "to": packet.get("to"),
             "route": list(rd.get("route", [])),
             "route_back": list(rd.get("routeBack", [])),
             "snr_towards": [s / 4 for s in rd.get("snrTowards", [])],
             "snr_back": [s / 4 for s in rd.get("snrBack", [])],
             "request_id": decoded.get("requestId") or 0})

    elif portnum == "NEIGHBORINFO_APP":
        info = decoded.get("neighborinfo", {})
        neighbors = [{"num": nb.get("nodeId"), "snr": nb.get("snr")}
                     for nb in info.get("neighbors", [])]
        if neighbors:
            put({**base, "type": "neighbors", "neighbors": neighbors})


def mt_radio_info(iface):
    """This node's LoRa config for its ingestor card: (modem_preset_name, freq_mhz).
    Preset name matches the firmware's DisplayFormatters output (LongFast, ...).
    Frequency is only reported when the node pins override_frequency; the channel
    frequency the firmware derives from region+preset+channel-hash isn't reliably
    reproducible here, so it's left null rather than guessed (cosmetic field)."""
    preset = freq = None
    try:
        lora = iface.localConfig.lora
        if getattr(lora, "use_preset", True):
            preset = MT_PRESET_NAMES.get(int(getattr(lora, "modem_preset", -1)))
        ov = float(getattr(lora, "override_frequency", 0) or 0)
        if ov > 0:
            freq = round(ov, 4)
    except Exception as exc:
        dbg_once("radio_info", f"localConfig.lora unavailable ({exc}); this is HARMLESS"
                 " on a TCP/PORTDUINO link — falling back to the primary channel name"
                 " for the modem preset")
    # Fallback: some builds (notably PORTDUINO) don't expose localConfig.lora, so
    # the preset reads back None. If the PRIMARY channel is named after a known
    # preset (an unnamed default primary shows as its preset name, e.g.
    # "MediumFast"), trust that. Gated to real preset names so a custom channel
    # name like "Visalia" or "Public" is never mistaken for a modem preset.
    if preset is None:
        try:
            names = set(MT_PRESET_NAMES.values())
            for ch in getattr(iface.localNode, "channels", None) or []:
                if int(getattr(ch, "role", 0)) == 1:   # 1 = PRIMARY
                    nm = getattr(getattr(ch, "settings", None), "name", "") or ""
                    if nm in names:
                        preset = nm
                    break
        except Exception as exc:
            dbg(f"preset channel-name fallback failed: {exc}")
    return preset, freq


def mt_self_report(iface) -> None:
    """Report this node's own health (firmware, battery, channel/air util,
    uptime) plus its radio config (modem preset, frequency) so the dashboard has
    context for what it does and doesn't hear. Sent at connect and every 5 min."""
    try:
        info = iface.getMyNodeInfo() or {}
        num = info.get("num")
        if num is None:
            return
        user = info.get("user", {})
        # firmware version lives in the device metadata, not the User record
        fw = None
        meta = getattr(iface, "metadata", None)
        if meta is not None:
            fw = getattr(meta, "firmware_version", None) or getattr(meta, "firmwareVersion", None)
        dev = info.get("deviceMetrics") or {}
        preset, lora_freq = mt_radio_info(iface)
        put({"type": "nodeinfo", "num": num, "ts": int(time.time()),
             "node_id": user.get("id"), "long_name": user.get("longName"),
             "short_name": user.get("shortName"),
             "hw_model": str(user.get("hwModel", "")) or None,
             "role": mt_role(user),
             "firmware": str(fw) if fw else None,
             "uptime_seconds": dev.get("uptimeSeconds"),
             "modem_preset": preset, "lora_freq": lora_freq})
        if dev:
            put({"type": "telemetry", "num": num, "ts": int(time.time()),
                 "battery": dev.get("batteryLevel"), "voltage": dev.get("voltage"),
                 "ch_util": dev.get("channelUtilization"),
                 "air_util": dev.get("airUtilTx")})
    except Exception as exc:
        dbg(f"self-report failed: {exc}")


def mt_is_stub(user: dict) -> bool:
    """True when the radio's node-db entry is just the auto-placeholder for a
    node it has only overheard (no real NodeInfo): the default name
    'Meshtastic <last4>' plus no hardware model. Seeding that name would clobber
    a real name another ingestor captured (node fields are last-write-wins), so
    for these we seed the node id only and let the real NodeInfo fill the name."""
    nid = (user.get("id") or "").lstrip("!")
    suffix = nid[-4:].lower()
    long_name = (user.get("longName") or "").strip().lower()
    hw = str(user.get("hwModel", "")).upper()
    return bool(suffix) and long_name == f"meshtastic {suffix}" and hw in ("", "UNSET", "0")


def mt_role(user: dict) -> str:
    """A decoded User with NO role means CLIENT. proto3 omits default enum values,
    and CLIENT is 0, so a node that switches TO client broadcasts NodeInfo with the
    role field dropped entirely. Reading that as 'unknown/unchanged' (the old
    behavior) left such nodes stuck on their previous role on the map, and left the
    many plain CLIENT nodes with no role at all. Treat absent/empty as CLIENT."""
    return str(user.get("role", "")) or "CLIENT"


def mt_seed_nodedb(interface, names_only: bool = False) -> None:
    """Send the radio's node database for map coverage. Run at startup, then
    periodically re-run with names_only=True: the radio keeps LEARNING names,
    roles and hardware for nodes over time (from NodeInfo it decodes), so a
    long-running ingestor that only seeded once at connect would never forward
    those later-learned identities — leaving nodes stuck as bare `!hexid` on the
    site even though the radio now knows them. The re-run forwards the current
    phonebook; positions/telemetry are skipped on re-runs (they arrive live)."""
    count = sent = 0
    for node in (interface.nodes or {}).values():
        num = node.get("num")
        if num is None:
            continue
        count += 1
        ts = node.get("lastHeard") or int(time.time())
        user = node.get("user", {})
        stub = mt_is_stub(user)   # placeholder-only entry: don't seed a fake name
        identity = (user.get("id"),
                    None if stub else user.get("longName"),
                    None if stub else user.get("shortName"),
                    None if stub else (str(user.get("hwModel", "")) or None),
                    None if stub else mt_role(user))
        # periodic re-seed: skip a node whose identity is unchanged since we last
        # sent it. The initial full seed always sends (and populates the cache).
        if names_only and _seeded_ids.get(num) == identity:
            continue
        _seeded_ids[num] = identity
        put({"type": "nodeinfo", "num": num, "ts": ts,
             "node_id": identity[0], "long_name": identity[1], "short_name": identity[2],
             "hw_model": identity[3], "role": identity[4], "snr": node.get("snr")})
        sent += 1
        if not names_only:
            pos = node.get("position", {})
            if real_position(pos.get("latitude"), pos.get("longitude")):
                put({"type": "position", "num": num, "ts": ts,
                     "lat": pos["latitude"], "lon": pos["longitude"],
                     "alt": pos.get("altitude")})
            dev = node.get("deviceMetrics", {})
            if dev:
                put({"type": "telemetry", "num": num, "ts": ts,
                     "battery": dev.get("batteryLevel"), "voltage": dev.get("voltage"),
                     "ch_util": dev.get("channelUtilization"),
                     "air_util": dev.get("airUtilTx")})
    if names_only:
        log(f"[ok] re-seeded {sent} changed of {count} node names from radio node db")
    else:
        log(f"[ok] seeded {count} nodes from radio node db")


def mt_connect(conn: str | None):
    kind, target = parse_connection(conn, default_tcp_port=4403)
    if kind == "tcp":
        import meshtastic.tcp_interface
        host, _, port = target.partition(":")
        return meshtastic.tcp_interface.TCPInterface(
            hostname=host, portNumber=int(port) if port else 4403)
    if kind == "ble":
        import meshtastic.ble_interface
        return meshtastic.ble_interface.BLEInterface(target)
    import meshtastic.serial_interface
    return meshtastic.serial_interface.SerialInterface(devPath=target or None)


def mt_enable_keepalive(iface) -> None:
    """Turn on OS-level TCP keepalive on the interface's socket so a vanished peer
    (a MeshMonitor proxy or shared node that dropped us, a NAT/idle timeout) surfaces
    as a real socket error in ~2 min — which makes the library fire 'connection.lost'
    — instead of a recv() that blocks forever. The meshtastic TCPInterface never sets
    this, which is the root of the 'still online but ingesting nothing' hangs.
    Best-effort and platform-guarded; a no-op for serial/BLE (no .socket)."""
    sock = getattr(iface, "socket", None)
    if sock is None:
        return
    try:
        import socket as _sock
        sock.setsockopt(_sock.SOL_SOCKET, _sock.SO_KEEPALIVE, 1)
        # Linux tuning: first probe after 60s idle, repeat every 15s, give up after 4
        # (≈2 min to detect a dead peer). Each knob is optional across platforms.
        for name, val in (("TCP_KEEPIDLE", 60), ("TCP_KEEPINTVL", 15), ("TCP_KEEPCNT", 4)):
            if hasattr(_sock, name):
                sock.setsockopt(_sock.IPPROTO_TCP, getattr(_sock, name), val)
    except Exception as exc:
        dbg(f"could not set TCP keepalive (harmless): {exc}")


# Meshtastic modem preset -> the name an UNNAMED primary channel shows in the
# app (an empty primary on LONG_FAST is "LongFast"). Lets ALLOWED_CHANNELS match
# by name even though the packet only carries the channel index.
MT_PRESET_NAMES = {0: "LongFast", 1: "LongSlow", 2: "VeryLongSlow", 3: "MediumSlow",
                   4: "MediumFast", 5: "ShortSlow", 6: "ShortFast", 7: "LongMod",
                   8: "ShortTurbo"}

mt_channel_names: dict[int, str] = {}


def mt_load_channels(iface) -> None:
    """Build channel index -> real name from the radio so ALLOWED/HIDDEN_CHANNELS
    can filter by name. Best-effort: any failure leaves the map empty, which
    makes channel_allowed() fail open (no filtering) rather than drop everything."""
    mt_channel_names.clear()
    try:
        preset = None
        try:
            preset = int(iface.localConfig.lora.modem_preset)
        except Exception:
            pass
        default_primary = MT_PRESET_NAMES.get(preset, "LongFast")
        for i, ch in enumerate(getattr(iface.localNode, "channels", None) or []):
            role = int(getattr(ch, "role", 0))   # 0 DISABLED, 1 PRIMARY, 2 SECONDARY
            if role == 0:
                continue
            name = getattr(getattr(ch, "settings", None), "name", "") or ""
            if not name and role == 1:            # unnamed primary -> preset name
                name = default_primary
            if name:
                mt_channel_names[i] = name
    except Exception as exc:
        dbg(f"meshtastic channel-name map unavailable: {exc}")
    if getattr(cfg, "allowed_channels", None) or getattr(cfg, "hidden_channels", None):
        log(f"[ok] meshtastic channels: {mt_channel_names or 'unknown (filter fails open)'}")


def run_meshtastic() -> None:
    global self_num, last_rx, _mt_conn_lost
    from pubsub import pub
    pub.subscribe(mt_on_receive, "meshtastic.receive")
    pub.subscribe(mt_on_conn_lost, "meshtastic.connection.lost")

    iface = None
    last_good = time.time()   # last time we had a WORKING connection (drives the exit watchdog)
    stalls = 0                # consecutive feed-idle reconnects that never got a packet
    while True:
        try:
            _mt_conn_lost = False
            iface = mt_connect(cfg.connection)
            mt_enable_keepalive(iface)     # OS detects a dead/half-open TCP peer
            info = iface.getMyNodeInfo() or {}
            self_num = info.get("num")
            log(f"[ok] meshtastic connected, this node: {self_num}")
            now0 = time.time()
            last_good = now0
            last_rx = now0                 # fresh link: start the silence clock now
            connected_at = now0
            mt_load_channels(iface)        # resolve channel names for filtering
            mt_seed_nodedb(iface)
            mt_self_report(iface)          # report our own health right away
            last_self = now0
            last_seed = now0
            while True:
                now_t = time.time()
                last_good = now_t          # inner loop only runs while connected
                # (1) the library reported the link dropped → reconnect immediately
                if _mt_conn_lost:
                    raise ConnectionError("connection lost (reported by the radio link)")
                # (2) feed watchdog: the socket looks up but no packets are arriving.
                # Silent past the window → tear down and reconnect. If we never heard a
                # single packet since connecting, the SOURCE is dead (not just this
                # socket); count those and restart the whole container after a few.
                idle = now_t - last_rx
                if RX_IDLE_S and idle > RX_IDLE_S:
                    # reconnect first (fixes a wedged/half-open socket). Escalate to a
                    # full container restart only after ~1 h of UNBROKEN silence across
                    # reconnects — clearly a dead feed, not just a momentarily quiet
                    # mesh, so the rare quiet-mesh operator never gets a restart loop.
                    stalls = stalls + 1 if last_rx <= connected_at else 1
                    if stalls * RX_IDLE_S >= 3600:
                        flush_and_exit(f"no packets for {int(idle)}s across {stalls} reconnects"
                                       f" (feed stalled); restarting container")
                    warn(f"no packets for {int(idle)}s though the link looks up;"
                         f" reconnecting (feed stall {stalls})")
                    break
                if now_t - last_self > 300:   # refresh listener health every 5 min
                    mt_self_report(iface)
                    last_self = now_t
                if now_t - last_seed > 3600:  # re-forward the radio's phonebook every
                    mt_seed_nodedb(iface, names_only=True)   # hour: fill in names/
                    last_seed = now_t                        # roles the radio has since learned
                time.sleep(5)              # pubsub callbacks do the packet work; poll often
            try:                           # tidy teardown before we loop back to reconnect
                iface.close()
            except Exception:
                pass
        except KeyboardInterrupt:
            try:
                if iface:
                    iface.close()
            except Exception:
                pass
            return
        except Exception as exc:
            down_s = int(time.time() - last_good)
            # watchdog: reconnect keeps failing (radio down / host unreachable) →
            # restart the container instead of looping forever like before
            if RADIO_DOWN_EXIT_S and down_s >= RADIO_DOWN_EXIT_S:
                flush_and_exit(f"radio unreachable for {down_s}s (>{RADIO_DOWN_EXIT_S}s): {exc}")
            warn(f"radio connection lost ({exc}); radio down {down_s}s, reconnecting in 15 s")
            time.sleep(15)


# ====================================================================
# MeshCore (official `meshcore` python package, asyncio events)
# ====================================================================

# MeshCore adv types -> role names
MC_ROLES = {1: "COMPANION", 2: "REPEATER", 3: "ROOM_SERVER", 4: "SENSOR"}

MC_DIRECT_PATH_LEN = 255   # MeshCore path_len sentinel: heard directly = 0 hops


def _mc_first(p: dict, *keys):
    """First non-None value among keys. The RX-log join surfaces SNR/RSSI in
    upper-case, but be tolerant of either casing across library versions."""
    for k in keys:
        v = p.get(k)
        if v is not None:
            return v
    return None


def mc_hops(path_len) -> int | None:
    """MeshCore path_len -> hops travelled. 255 (the direct sentinel) or 0 mean
    heard directly (0 hops); 1..254 is that many relay hops; else unknown."""
    if path_len is None:
        return None
    try:
        v = int(path_len)
    except (TypeError, ValueError):
        return None
    if v == MC_DIRECT_PATH_LEN:
        return 0
    return v if v >= 0 else None


def mc_rf(p: dict):
    """Pull (snr, rssi, hops, path) from a MeshCore event payload. Populated
    once decrypt_channels is on and channel secrets are registered so the
    library joins each message to its RX-log frame."""
    snr = _mc_first(p, "SNR", "snr")
    rssi = _mc_first(p, "RSSI", "rssi")
    hops = mc_hops(p.get("path_len"))
    path = p.get("path")
    return snr, rssi, hops, (path.lower() if isinstance(path, str) and path else None)


def mc_num(pubkey: str | None) -> int | None:
    """Map a MeshCore public key (hex) into 32-bit node-number space."""
    if not pubkey:
        return None
    s = str(pubkey).strip().lower()
    if len(s) < 8:
        return None
    try:
        return int(s[:8], 16) & 0xFFFFFFFF
    except ValueError:
        return None


def mc_pseudo_num(name: str) -> int:
    """Stable pseudo node number for a sender we only know by display name."""
    return int.from_bytes(hashlib.sha256(f"mc:{name}".encode()).digest()[:4], "big") & 0xFFFFFFFF


def mc_msg_id(sender_identity: str, sender_ts: int, discriminator: str, text: str) -> int:
    """Stable message fingerprint: sha256, first 7 bytes, masked to 53 bits.

    A content hash over sender, timestamp, channel, and text, so the same
    message heard by more than one ingestor collapses to a single row on the
    dashboard instead of showing up twice.
    """
    raw = f"v1:{sender_identity}:{sender_ts}:{discriminator}:{text}"
    digest = hashlib.sha256(raw.encode()).digest()
    return int.from_bytes(digest[:7], "big") & ((1 << 53) - 1)


def run_meshcore() -> None:
    asyncio.run(mc_main())


async def mc_main() -> None:
    global self_num, last_rx
    from meshcore import MeshCore, EventType

    kind, target = parse_connection(cfg.connection, default_tcp_port=5000)
    contacts_by_name: dict[str, dict] = {}
    contacts_by_prefix: dict[str, dict] = {}
    # channel_idx -> real name (e.g. 0 -> "Public"), filled at registration; used
    # to label messages by their true channel instead of the server's meshtastic
    # slot map turning index 0 into "LongFast"
    channel_names: dict[int, str] = {}
    # per-window activity, reset every heartbeat, so a quiet radio is obvious at
    # a glance instead of hiding behind a stream of self-echo adverts
    heard = {"self_adv": 0, "chan_msg": 0, "dm_msg": 0, "peers": set()}

    def upsert_contact(key: str, c: dict) -> None:
        num = mc_num(key)
        if num is None:
            return
        name = c.get("adv_name") or None
        ts = int(c.get("last_advert") or time.time())
        put({"type": "nodeinfo", "num": num, "ts": ts,
             "node_id": f"!{key[:8].lower()}",
             "long_name": name,
             "short_name": (name or "")[:4] or None,
             "role": MC_ROLES.get(c.get("type"), "COMPANION")})
        lat, lon = c.get("adv_lat"), c.get("adv_lon")
        if real_position(lat, lon):
            put({"type": "position", "num": num, "ts": ts, "lat": lat, "lon": lon})
        if name:
            contacts_by_name[name.strip().lower()] = {**c, "num": num}
        contacts_by_prefix[key[:12].lower()] = {**c, "num": num}

    async def seed_contacts(mc, tag: str) -> int:
        """Pull the radio's whole contact roster and upsert every contact as a
        full node record (name + role + position). Run at connect AND on a timer
        so repeaters the radio learns LATER get their name/role into the dashboard,
        instead of lingering as the bare !hex entry a plain advert leaves behind
        (that is what left relay hops showing as 'unknown repeater')."""
        try:
            res = await mc.commands.get_contacts()
            contacts = getattr(res, "payload", {}) or {}
            for key, c in contacts.items():
                if isinstance(c, dict):
                    upsert_contact(str(key), c)
            log(f"[ok] {tag} {len(contacts)} meshcore contacts")
            return len(contacts)
        except Exception as exc:
            dbg(f"meshcore contact seed failed: {exc}")
            return 0

    def sender_from_channel_text(text: str) -> tuple[int, str, str]:
        """MeshCore channel messages carry 'SenderName: text'. Returns
        (num, sender_identity_for_fingerprint, clean_text)."""
        name, sep, rest = text.partition(":")
        if sep and 0 < len(name.strip()) <= 40:
            ident = name.strip().lower()
            contact = contacts_by_name.get(ident)
            num = contact["num"] if contact else mc_pseudo_num(ident)
            if not contact:
                put({"type": "nodeinfo", "num": num, "ts": int(time.time()),
                     "long_name": name.strip()})
            return num, ident, text
        return mc_pseudo_num("unknown"), "", text

    last_good = time.time()   # last time the radio was healthily connected
    while True:
        try:
            if kind == "tcp":
                host, _, port = target.partition(":")
                mc = await MeshCore.create_tcp(host, int(port) if port else 5000,
                                               auto_reconnect=True)
            elif kind == "ble":
                mc = await MeshCore.create_ble(target)
            else:
                mc = await MeshCore.create_serial(target or "/dev/ttyUSB0", 115200)

            res = await mc.commands.send_appstart()
            info = getattr(res, "payload", {}) or {}
            self_key = info.get("public_key", "")
            self_num = mc_num(self_key)
            log(f"[ok] meshcore connected, this node: {self_num}")
            last_good = time.time()
            last_rx = time.time()          # fresh link: start the (wide) silence clock
            if real_position(info.get("adv_lat"), info.get("adv_lon")):
                put({"type": "position", "num": self_num, "ts": int(time.time()),
                     "lat": info["adv_lat"], "lon": info["adv_lon"]})

            # contact roster -> full node records (name, role, position)
            try:
                mc.auto_update_contacts = True
            except Exception:
                pass
            await seed_contacts(mc, "seeded")

            # Turn on the library's channel-log decryption so encrypted group
            # messages (GRP_TXT frames) get decrypted and delivered as
            # CHANNEL_MSG_RECV events, and the RX-log join adds SNR/RSSI/path.
            # NOTE: the API is a METHOD (set_decrypt_channel_logs), not a
            # `decrypt_channels` attribute; setting the attribute is a silent
            # no-op. It also needs each channel's secret registered, so fetch
            # every channel first. All best-effort so older libs degrade cleanly.
            try:
                res = await mc.commands.send_device_query()
                maxch = int((getattr(res, "payload", {}) or {}).get("max_channels") or 8)
                names = []
                for idx in range(max(1, min(maxch, 32))):
                    try:
                        r = await mc.commands.get_channel(idx)
                        pl = getattr(r, "payload", {}) or {}
                        cn = pl.get("channel_name") or pl.get("name")
                        if cn:
                            channel_names[idx] = cn
                            names.append(f"{idx}:{cn}")
                    except Exception:
                        break
                # names lets us confirm the "public" channel is registered; a
                # channel must be registered here for its group texts to decrypt
                log(f"[ok] meshcore channels registered: {names or maxch}")
            except Exception as exc:
                dbg(f"meshcore channel registration unavailable: {exc}")
            try:
                res = mc.set_decrypt_channel_logs(True)
                if asyncio.iscoroutine(res):
                    await res
                log("[ok] meshcore channel-log decryption enabled")
            except Exception as exc:
                dbg(f"meshcore set_decrypt_channel_logs unavailable: {exc}")

            # When the RX_LOG_DATA stream is available it carries the RF metrics
            # for adverts (snr/rssi/path_len), so it becomes the authoritative
            # "heard" record. The bare ADVERTISEMENT event then only does node
            # discovery. If RX_LOG_DATA isn't available, ADVERTISEMENT falls back
            # to a plain (no-RF) reception so "nodes heard" never regresses.
            rx_log_ok = False

            def on_advert(event):
                p = event.payload
                key = p.get("public_key") if isinstance(p, dict) else p
                num = mc_num(key)
                if num is None or num == self_num:
                    return   # our own advert, echoed back by a neighbor: not a peer
                ts = int(time.time())
                if str(key)[:12].lower() not in contacts_by_prefix:
                    put({"type": "nodeinfo", "num": num, "ts": ts,
                         "node_id": f"!{str(key)[:8].lower()}"})
                if not rx_log_ok:   # RX-log covers RF; this is the fallback
                    put({"type": "reception", "num": num, "ts": ts,
                         "portnum": "ADVERTISEMENT"})

            def on_rx_log(event):
                # raw received frames with RF metrics; advert frames carry adv_key
                p = event.payload or {}
                key = p.get("adv_key")
                if not key:
                    # not an advert; log it so we can see if channel messages
                    # arrive here (e.g. encrypted frames we couldn't decrypt)
                    if DEBUG:
                        dbg(f"meshcore rx-log non-advert: type={p.get('payload_typename')}"
                            f" snr={p.get('snr')} path_len={p.get('path_len')} keys={sorted(p.keys())}")
                    return
                num = mc_num(key)
                if num is None:
                    return
                ts = int(time.time())
                snr, rssi = p.get("snr"), p.get("rssi")
                hops = mc_hops(p.get("path_len"))
                rpath = p.get("path")
                rpath = rpath.lower() if isinstance(rpath, str) and rpath else None
                is_self = (num == self_num)
                if DEBUG:
                    dbg(f"meshcore rx-log advert: num={num} self={is_self} snr={snr}"
                        f" rssi={rssi} path_len={p.get('path_len')} hops={hops}"
                        f" key={str(key)[:8].lower()}")
                if is_self:
                    heard["self_adv"] += 1
                    return   # our own advert, echoed back by a neighbor: not a reception
                heard["peers"].add(num)
                if str(key)[:12].lower() not in contacts_by_prefix:
                    put({"type": "nodeinfo", "num": num, "ts": ts,
                         "node_id": f"!{str(key)[:8].lower()}"})
                lat, lon = p.get("adv_lat"), p.get("adv_lon")
                if real_position(lat, lon):
                    put({"type": "position", "num": num, "ts": ts, "lat": lat, "lon": lon})
                put({"type": "reception", "num": num, "ts": ts, "snr": snr,
                     "rssi": rssi, "hops": hops, "path": rpath,
                     "portnum": "ADVERTISEMENT"})

            def on_channel_msg(event):
                p = event.payload or {}
                text = p.get("text") or p.get("msg") or p.get("message") or ""
                num, ident, clean = (sender_from_channel_text(text) if text
                                     else (None, "", ""))
                snr, rssi, hops, path = mc_rf(p)
                if DEBUG:   # fire always so we can see events that arrive empty
                    dbg(f"meshcore channel-msg: text={text[:48]!r} sender={num}"
                        f" snr={snr} path_len={p.get('path_len')} keys={sorted(p.keys())}")
                if not text:
                    return
                heard["chan_msg"] += 1
                chan_idx = p.get("channel_idx", 0)
                # label by real channel name ("Public"); fingerprint stays keyed
                # on the index so it's stable regardless of naming
                chan = channel_names.get(chan_idx) or str(chan_idx)
                if not channel_allowed(chan):   # ALLOWED/HIDDEN_CHANNELS
                    return
                sender_ts = int(p.get("timestamp") or time.time())
                put({"type": "message", "num": num, "ts": int(time.time()),
                     "msg_id": mc_msg_id(ident, sender_ts, f"c{chan_idx}", text),
                     "channel": chan, "text": clean, "snr": snr, "rssi": rssi,
                     "hops": hops, "path": path})
                if num != self_num:   # keep our own message, but it's not a reception
                    put({"type": "reception", "num": num, "ts": int(time.time()),
                         "snr": snr, "rssi": rssi, "hops": hops, "path": path,
                         "portnum": "CHANNEL_MSG"})

            def on_contact_msg(event):
                p = event.payload or {}
                text = p.get("text") or ""
                prefix = str(p.get("pubkey_prefix") or "")
                num = mc_num(prefix)
                if not text or num is None:
                    return
                heard["dm_msg"] += 1
                sender_ts = int(p.get("timestamp") or time.time())
                snr, rssi, hops, path = mc_rf(p)
                put({"type": "message", "num": num, "ts": int(time.time()),
                     "msg_id": mc_msg_id(prefix, sender_ts, "dm", text),
                     "channel": "dm", "to": self_num, "text": text,
                     "snr": snr, "rssi": rssi, "hops": hops, "path": path})
                if num != self_num:
                    put({"type": "reception", "num": num, "ts": int(time.time()),
                         "snr": snr, "rssi": rssi, "hops": hops, "path": path,
                         "portnum": "CONTACT_MSG"})

            mc.subscribe(EventType.ADVERTISEMENT, on_advert)
            mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)
            mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)
            # RX_LOG_DATA is the raw-frame stream that carries advert RF metrics;
            # subscribing flips on_advert to node-discovery-only. Guarded so an
            # older meshcore lib without the event just uses the plain fallback.
            try:
                mc.subscribe(EventType.RX_LOG_DATA, on_rx_log)
                rx_log_ok = True
                log("[ok] meshcore RX-log subscribed (advert RF metrics enabled)")
            except Exception as exc:
                dbg(f"meshcore RX_LOG_DATA unavailable, advert RF limited: {exc}")

            # CRUCIAL: CHANNEL_MSG_RECV / CONTACT_MSG_RECV only fire for our
            # subscribers once the device's decrypted message queue is drained.
            # The device queues the messages it decrypted (Public channel + DMs to
            # us) and raises "messages_waiting"; without auto-fetch we'd only ever
            # see the raw, still-encrypted GRP_TXT/TEXT_MSG frames in the RX-log and
            # store nothing. This starts the drain loop that turns them into events.
            try:
                res = mc.start_auto_message_fetching()
                if asyncio.iscoroutine(res):
                    await res
                log("[ok] meshcore auto message fetching started")
            except Exception as exc:
                dbg(f"meshcore start_auto_message_fetching unavailable: {exc}")

            # heartbeat + periodic self battery. Tick every 60 s so a quiet radio
            # shows up quickly; poll battery only every 10th tick (~10 min).
            tick = 0
            while True:
                now_t = time.time()
                last_good = now_t          # inner loop only runs while connected
                # feed watchdog: any frame heard this window — including our own
                # advert reflected back (self_adv) — proves the link is alive. If the
                # radio goes truly silent past the (wide) MeshCore window, the feed is
                # wedged; restart the container for a clean reconnect. auto_reconnect
                # handles transport blips, so this is the last-resort safety net.
                if heard["peers"] or heard["chan_msg"] or heard["dm_msg"] or heard["self_adv"]:
                    last_rx = now_t
                if MC_RX_IDLE_S and now_t - last_rx > MC_RX_IDLE_S:
                    flush_and_exit(f"meshcore: no frames for {int(now_t - last_rx)}s"
                                   f" (feed stalled); restarting container")
                if tick % 10 == 0:
                    try:
                        res = await mc.commands.get_bat()
                        level = (getattr(res, "payload", {}) or {}).get("level")
                        if level is not None and self_num is not None:
                            put({"type": "telemetry", "num": self_num,
                                 "ts": int(time.time()), "battery": level})
                    except Exception as exc:
                        dbg(f"battery poll failed: {exc}")
                # re-seed the contact roster every ~15 min so repeaters the radio
                # learns while we're running get their name/role, not just a bare !hex
                if tick > 0 and tick % 15 == 0:
                    await seed_contacts(mc, "re-seeded")
                # one-line summary of what we actually HEARD this window: distinct
                # peers, channel/DM messages, and self-echo count. All zeros but a
                # high self_adv count = only hearing our own advert reflected back.
                log(f"[heard] peers={len(heard['peers'])} chan_msgs={heard['chan_msg']}"
                    f" dms={heard['dm_msg']} self_echo={heard['self_adv']}"
                    + (f" peer_nums={sorted(heard['peers'])}" if heard["peers"] else ""))
                heard["peers"].clear()
                heard["self_adv"] = heard["chan_msg"] = heard["dm_msg"] = 0
                tick += 1
                await asyncio.sleep(60)

        except KeyboardInterrupt:
            return
        except Exception as exc:
            down_s = int(time.time() - last_good)
            if RADIO_DOWN_EXIT_S and down_s >= RADIO_DOWN_EXIT_S:
                flush_and_exit(f"meshcore radio unreachable for {down_s}s (>{RADIO_DOWN_EXIT_S}s): {exc}")
            warn(f"meshcore radio connection lost ({exc}); radio down {down_s}s, reconnecting in 15 s")
            await asyncio.sleep(15)


# ====================================================================
# config + entry point
# ====================================================================

BLE_MAC = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")


def parse_connection(conn: str | None, default_tcp_port: int) -> tuple[str, str]:
    """CONNECTION string -> (kind, target). kind: serial | tcp | ble."""
    if not conn:
        return "serial", ""
    c = conn.strip()
    # some configs hand the radio over as a URL like http://host:port. Strip
    # the scheme (and any trailing /path) so we're left
    # with host[:port] for the TCP interface, otherwise it looks like a serial
    # device path and fails with "No such file or directory".
    m = re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://(.+)$", c)
    if m:
        return "tcp", m.group(1).split("/", 1)[0]
    if BLE_MAC.match(c):
        return "ble", c
    if c.upper().startswith("COM") or c.startswith("/dev/"):
        return "serial", c
    if re.match(r"^[\w.\-]+(:\d+)?$", c) and ("." in c or ":" in c):
        return "tcp", c
    return "serial", c


def main() -> None:
    global cfg
    parser = argparse.ArgumentParser(
        description="Visalia Mesh unified ingestor (Meshtastic + MeshCore)")
    parser.add_argument("--server", default=os.environ.get("INSTANCE_DOMAIN"),
                        help="dashboard base URL (env INSTANCE_DOMAIN)")
    parser.add_argument("--token", default=os.environ.get("API_TOKEN"),
                        help="ingestor bearer token (env API_TOKEN)")
    parser.add_argument("--connection", default=os.environ.get("CONNECTION"),
                        help="serial port, host[:port], or BLE MAC (env CONNECTION)")
    # current configs use PROTOCOL; older builds of this script used
    # MESH_PROTOCOL. Accept either so an existing config keeps working.
    parser.add_argument("--protocol",
                        default=(os.environ.get("PROTOCOL")
                                 or os.environ.get("MESH_PROTOCOL") or "meshtastic"),
                        choices=["meshtastic", "meshcore"],
                        help="mesh protocol (env PROTOCOL, or legacy MESH_PROTOCOL)")
    # legacy flags from the first version of this script
    parser.add_argument("--serial", nargs="?", const="", help=argparse.SUPPRESS)
    parser.add_argument("--tcp", help=argparse.SUPPRESS)
    parser.add_argument("--ble", help=argparse.SUPPRESS)
    cfg = parser.parse_args()

    if cfg.tcp:
        cfg.connection = cfg.tcp
    elif cfg.ble:
        cfg.connection = cfg.ble
    elif cfg.serial is not None:
        cfg.connection = cfg.serial

    if not cfg.server or not cfg.token:
        parser.error("--server/INSTANCE_DOMAIN and --token/API_TOKEN are required")

    # accept a bare host in INSTANCE_DOMAIN and add the scheme ourselves, so
    # "map.visaliamesh.com" works as well as a full URL.
    if not re.match(r"^https?://", cfg.server):
        cfg.server = "https://" + cfg.server

    # INGESTOR_NODE_ID overrides the host node number, for cases where
    # the radio can't report its own. Auto-detect takes over once connected.
    global self_num
    node_id_env = os.environ.get("INGESTOR_NODE_ID", "").strip()
    if node_id_env:
        try:
            self_num = (int(node_id_env[1:], 16) if node_id_env.startswith("!")
                        else int(node_id_env, 0)) & 0xFFFFFFFF
        except ValueError:
            log(f"[warn] INGESTOR_NODE_ID={node_id_env!r} is not a valid node id, ignoring")

    # packet filters (ingestor-side, per-listener):
    #   ALLOWED_CHANNELS - whitelist of channel NAMES; packets on any other
    #                      channel are discarded before sending (empty = accept all)
    #   HIDDEN_CHANNELS  - channel NAMES to drop when forwarding
    #   MIN_SNR          - drop packets whose SNR is below this floor (dB)
    #   RX_ONLY          - accepted for compat; this ingestor is receive-only and
    #                      never transmits to the mesh, so it is a no-op here
    def _chan_set(name):
        return {c.strip().lower() for c in os.environ.get(name, "").split(",") if c.strip()}
    cfg.allowed_channels = _chan_set("ALLOWED_CHANNELS")
    cfg.hidden_channels = _chan_set("HIDDEN_CHANNELS")
    mn = os.environ.get("MIN_SNR", "").strip()
    try:
        cfg.min_snr = float(mn) if mn else None
    except ValueError:
        cfg.min_snr = None
        log(f"[warn] MIN_SNR={mn!r} is not a number, ignoring")
    cfg.rx_only = os.environ.get("RX_ONLY", "").strip().lower() in ("1", "true", "yes")
    if cfg.allowed_channels or cfg.hidden_channels or cfg.min_snr is not None:
        log(f"[info] filters: allowed_channels={sorted(cfg.allowed_channels) or 'all'}"
            f" hidden_channels={sorted(cfg.hidden_channels) or 'none'} min_snr={cfg.min_snr}")
    if cfg.rx_only:
        log("[info] RX_ONLY set, noted; this ingestor never transmits to the mesh anyway")

    threading.Thread(target=flush_loop, daemon=True).start()
    log(f"[ok] Visalia Mesh ingestor v{__version__}: protocol={cfg.protocol}"
        f" connection={cfg.connection or 'auto serial'} server={cfg.server}")
    # one info line with everything worth checking when something is off: the
    # exact ingest URL, the token's last 4 (matches the dashboard's [auth] log),
    # and whether the node id is pinned or auto-detected
    log(f"[info] ingest {cfg.server.rstrip('/')}/api/ingest"
        f" | token …{cfg.token[-4:]}"
        f" | node {('pinned ' + str(self_num)) if self_num is not None else 'auto-detect'}"
        f"{' | DEBUG on' if DEBUG else ''}")
    if cfg.protocol == "meshcore":
        run_meshcore()
    else:
        run_meshtastic()


if __name__ == "__main__":
    main()
