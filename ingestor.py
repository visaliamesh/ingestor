#!/usr/bin/env python3
"""Visalia Mesh ingestor for Meshtastic and MeshCore.

Listens to a radio over serial, TCP, or BLE and forwards what it hears (RF only,
no MQTT) to the Visalia Mesh dashboard ingest API.

Configured with potato-mesh style environment variables; CLI flags override:

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
import hashlib
import os
import queue
import re
import sys
import threading
import time

import requests

__version__ = "1.0.0"     # bump on each release; logged at startup

FLUSH_SECONDS = 5
MAX_BATCH = 100
MAX_QUEUE = 5000

events: "queue.Queue[dict]" = queue.Queue(maxsize=MAX_QUEUE)
cfg = None
self_num = None
DEBUG = os.environ.get("DEBUG") == "1"


def log(msg: str) -> None:
    print(msg, flush=True)


def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[debug] {msg}", flush=True)


def put(ev: dict) -> None:
    ev.setdefault("network", cfg.protocol)
    try:
        events.put_nowait(ev)
    except queue.Full:
        print("[warn] event queue full, dropping event", file=sys.stderr)


def flush_loop() -> None:
    pending: list[dict] = []
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {cfg.token}"
    url = cfg.server.rstrip("/") + "/api/ingest"

    while True:
        time.sleep(FLUSH_SECONDS)
        while len(pending) < MAX_BATCH:
            try:
                pending.append(events.get_nowait())
            except queue.Empty:
                break
        if not pending:
            continue
        try:
            r = session.post(url, json={"events": pending, "ingestor_node": self_num},
                             timeout=15)
            r.raise_for_status()
            log(f"[ok] sent {len(pending)} events ({r.json().get('accepted')} accepted)")
            pending = []
        except Exception as exc:
            print(f"[warn] send failed, will retry: {exc}", file=sys.stderr)
            pending = pending[-MAX_BATCH * 5:]  # cap retry backlog


def real_position(lat, lon) -> bool:
    """(0,0) is the Meshtastic no-GPS-fix sentinel; single-axis zeros are real."""
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return False
    return not (abs(lat) < 1e-9 and abs(lon) < 1e-9)


# ====================================================================
# Meshtastic (official python lib, pubsub events)
# ====================================================================

def mt_on_receive(packet, interface):  # noqa: ANN001 - meshtastic pubsub signature
    try:
        mt_handle_packet(packet)
    except Exception as exc:
        print(f"[warn] failed to handle packet: {exc}", file=sys.stderr)


def mt_handle_packet(packet: dict) -> None:
    num = packet.get("from")
    if num is None:
        return
    ts = int(time.time())
    decoded = packet.get("decoded", {})
    portnum = decoded.get("portnum", "UNKNOWN")
    snr = packet.get("rxSnr")
    rssi = packet.get("rxRssi")
    hop_start = packet.get("hopStart")
    hop_limit = packet.get("hopLimit")
    hops = None
    if hop_start is not None and hop_limit is not None:
        hops = max(hop_start - hop_limit, 0)

    base = {"num": num, "ts": ts, "snr": snr, "rssi": rssi, "hops": hops}

    # every decoded packet -> reception record (RF analytics)
    put({**base, "type": "reception", "hop_limit": hop_limit,
         "hop_start": hop_start, "portnum": str(portnum)})

    if portnum == "TEXT_MESSAGE_APP":
        put({**base, "type": "message", "text": decoded.get("text", ""),
             "to": packet.get("to"), "channel": packet.get("channel", 0),
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
             "role": str(user.get("role", "")) or None})

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
        put({**base, "type": "traceroute", "to": packet.get("to"),
             "route": list(rd.get("route", [])),
             "route_back": list(rd.get("routeBack", [])),
             "snr_towards": [s / 4 for s in rd.get("snrTowards", [])],
             "snr_back": [s / 4 for s in rd.get("snrBack", [])]})

    elif portnum == "NEIGHBORINFO_APP":
        info = decoded.get("neighborinfo", {})
        neighbors = [{"num": nb.get("nodeId"), "snr": nb.get("snr")}
                     for nb in info.get("neighbors", [])]
        if neighbors:
            put({**base, "type": "neighbors", "neighbors": neighbors})


def mt_self_report(iface) -> None:
    """Report this node's own health (firmware, battery, channel/air util,
    uptime) so the dashboard has context for what it does and doesn't hear.
    Sent at connect and every 5 minutes after."""
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
        put({"type": "nodeinfo", "num": num, "ts": int(time.time()),
             "node_id": user.get("id"), "long_name": user.get("longName"),
             "short_name": user.get("shortName"),
             "hw_model": str(user.get("hwModel", "")) or None,
             "role": str(user.get("role", "")) or None,
             "firmware": str(fw) if fw else None,
             "uptime_seconds": dev.get("uptimeSeconds")})
        if dev:
            put({"type": "telemetry", "num": num, "ts": int(time.time()),
                 "battery": dev.get("batteryLevel"), "voltage": dev.get("voltage"),
                 "ch_util": dev.get("channelUtilization"),
                 "air_util": dev.get("airUtilTx")})
    except Exception as exc:
        dbg(f"self-report failed: {exc}")


def mt_seed_nodedb(interface) -> None:
    """Send the radio's node database once at startup for instant map coverage."""
    count = 0
    for node in (interface.nodes or {}).values():
        num = node.get("num")
        if num is None:
            continue
        ts = node.get("lastHeard") or int(time.time())
        user = node.get("user", {})
        put({"type": "nodeinfo", "num": num, "ts": ts,
             "node_id": user.get("id"), "long_name": user.get("longName"),
             "short_name": user.get("shortName"),
             "hw_model": str(user.get("hwModel", "")) or None,
             "role": str(user.get("role", "")) or None,
             "snr": node.get("snr")})
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
        count += 1
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


def run_meshtastic() -> None:
    global self_num
    from pubsub import pub
    pub.subscribe(mt_on_receive, "meshtastic.receive")

    while True:
        try:
            iface = mt_connect(cfg.connection)
            info = iface.getMyNodeInfo() or {}
            self_num = info.get("num")
            log(f"[ok] meshtastic connected, this node: {self_num}")
            mt_seed_nodedb(iface)
            mt_self_report(iface)          # report our own health right away
            last_self = time.time()
            while True:
                if time.time() - last_self > 300:   # refresh listener health every 5 min
                    mt_self_report(iface)
                    last_self = time.time()
                time.sleep(30)             # pubsub callbacks do the packet work
        except KeyboardInterrupt:
            return
        except Exception as exc:
            print(f"[warn] connection lost ({exc}), reconnecting in 15 s", file=sys.stderr)
            time.sleep(15)


# ====================================================================
# MeshCore (official `meshcore` python package, asyncio events)
# ====================================================================

# MeshCore adv types -> role names (matches the PotatoMesh contract)
MC_ROLES = {1: "COMPANION", 2: "REPEATER", 3: "ROOM_SERVER", 4: "SENSOR"}


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
    """PotatoMesh v1 message fingerprint: sha256, first 7 bytes, masked to 53 bits.

    Using the same scheme means a message heard by both a potato ingestor and
    this one dedupes to a single row after migration.
    """
    raw = f"v1:{sender_identity}:{sender_ts}:{discriminator}:{text}"
    digest = hashlib.sha256(raw.encode()).digest()
    return int.from_bytes(digest[:7], "big") & ((1 << 53) - 1)


def run_meshcore() -> None:
    import asyncio
    asyncio.run(mc_main())


async def mc_main() -> None:
    global self_num
    from meshcore import MeshCore, EventType

    kind, target = parse_connection(cfg.connection, default_tcp_port=5000)
    contacts_by_name: dict[str, dict] = {}
    contacts_by_prefix: dict[str, dict] = {}

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
            if real_position(info.get("adv_lat"), info.get("adv_lon")):
                put({"type": "position", "num": self_num, "ts": int(time.time()),
                     "lat": info["adv_lat"], "lon": info["adv_lon"]})

            # contact roster -> full node records (name, role, position)
            try:
                mc.auto_update_contacts = True
            except Exception:
                pass
            res = await mc.commands.get_contacts()
            contacts = getattr(res, "payload", {}) or {}
            for key, c in contacts.items():
                if isinstance(c, dict):
                    upsert_contact(str(key), c)
            log(f"[ok] seeded {len(contacts)} meshcore contacts")

            def on_advert(event):
                p = event.payload
                key = p.get("public_key") if isinstance(p, dict) else p
                num = mc_num(key)
                if num is None:
                    return
                ts = int(time.time())
                # name-optional "node was heard" upsert + reception record
                if str(key)[:12].lower() not in contacts_by_prefix:
                    put({"type": "nodeinfo", "num": num, "ts": ts,
                         "node_id": f"!{str(key)[:8].lower()}"})
                put({"type": "reception", "num": num, "ts": ts,
                     "portnum": "ADVERTISEMENT"})

            def on_channel_msg(event):
                p = event.payload or {}
                text = p.get("text") or ""
                if not text:
                    return
                num, ident, clean = sender_from_channel_text(text)
                chan = p.get("channel_idx", 0)
                sender_ts = int(p.get("timestamp") or time.time())
                put({"type": "message", "num": num, "ts": int(time.time()),
                     "msg_id": mc_msg_id(ident, sender_ts, f"c{chan}", text),
                     "channel": str(chan), "text": clean,
                     "snr": p.get("snr"), "rssi": p.get("rssi")})
                put({"type": "reception", "num": num, "ts": int(time.time()),
                     "snr": p.get("snr"), "rssi": p.get("rssi"),
                     "portnum": "CHANNEL_MSG"})

            def on_contact_msg(event):
                p = event.payload or {}
                text = p.get("text") or ""
                prefix = str(p.get("pubkey_prefix") or "")
                num = mc_num(prefix)
                if not text or num is None:
                    return
                sender_ts = int(p.get("timestamp") or time.time())
                put({"type": "message", "num": num, "ts": int(time.time()),
                     "msg_id": mc_msg_id(prefix, sender_ts, "dm", text),
                     "channel": "dm", "to": self_num, "text": text,
                     "snr": p.get("snr"), "rssi": p.get("rssi")})

            mc.subscribe(EventType.ADVERTISEMENT, on_advert)
            mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)
            mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)

            # periodic self battery -> telemetry
            while True:
                try:
                    res = await mc.commands.get_bat()
                    level = (getattr(res, "payload", {}) or {}).get("level")
                    if level is not None and self_num is not None:
                        put({"type": "telemetry", "num": self_num,
                             "ts": int(time.time()), "battery": level})
                except Exception as exc:
                    dbg(f"battery poll failed: {exc}")
                await asyncio.sleep(600)

        except KeyboardInterrupt:
            return
        except Exception as exc:
            print(f"[warn] meshcore connection lost ({exc}), reconnecting in 15 s",
                  file=sys.stderr)
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
    # potato-mesh configs often hand the radio over as a URL like
    # http://host:port. Strip the scheme (and any trailing /path) so we're left
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
    # potato uses PROTOCOL; older builds of this script used MESH_PROTOCOL.
    # Accept either so an existing potato config keeps working.
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

    # potato allows a bare host in INSTANCE_DOMAIN and adds the scheme itself,
    # so accept "map.visaliamesh.com" as well as a full URL.
    if not re.match(r"^https?://", cfg.server):
        cfg.server = "https://" + cfg.server

    # INGESTOR_NODE_ID (potato) overrides the host node number, for cases where
    # the radio can't report its own. Auto-detect takes over once connected.
    global self_num
    node_id_env = os.environ.get("INGESTOR_NODE_ID", "").strip()
    if node_id_env:
        try:
            self_num = (int(node_id_env[1:], 16) if node_id_env.startswith("!")
                        else int(node_id_env, 0)) & 0xFFFFFFFF
        except ValueError:
            log(f"[warn] INGESTOR_NODE_ID={node_id_env!r} is not a valid node id, ignoring")

    threading.Thread(target=flush_loop, daemon=True).start()
    log(f"[ok] Visalia Mesh ingestor v{__version__}: protocol={cfg.protocol}"
        f" connection={cfg.connection or 'auto serial'} server={cfg.server}")
    if cfg.protocol == "meshcore":
        run_meshcore()
    else:
        run_meshtastic()


if __name__ == "__main__":
    main()
