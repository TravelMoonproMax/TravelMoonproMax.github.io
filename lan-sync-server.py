#!/usr/bin/env python3
"""拾阶 · 局域网同步服务（零依赖）

在局域网中的一台电脑上运行：
    python lan-sync-server.py

然后在拾阶网页的「同步 -> 局域网」中填写：
    服务地址：http://<这台电脑的局域网 IP>:8787
    同步口令：任意一组设备共用的口令

数据默认保存在脚本旁的 lan-sync-data.json。
"""

from __future__ import annotations

import argparse
import json
import socket
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

MAX_BODY = 5 * 1024 * 1024
LOCK = threading.Lock()


def local_ip_addresses() -> list[str]:
    addresses: set[str] = set()
    try:
        hostname = socket.gethostname()
        for item in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = item[4][0]
            if not ip.startswith("127."):
                addresses.add(ip)
    except OSError:
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            addresses.add(sock.getsockname()[0])
    except OSError:
        pass
    return sorted(addresses)


class SyncStore:
    def __init__(self, path: Path):
        self.path = path
        self.rooms: dict[str, dict] = {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self.rooms = {str(key): value for key, value in raw.items() if isinstance(value, dict)}
        except (OSError, ValueError) as exc:
            print(f"[warn] 无法读取数据文件 {self.path}: {exc}")

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.rooms, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        temporary.replace(self.path)

    def get(self, room: str) -> dict | None:
        with LOCK:
            value = self.rooms.get(room)
            return value

    def put(self, room: str, payload: dict) -> None:
        with LOCK:
            self.rooms[room] = payload
            self.save()

    def count(self) -> int:
        with LOCK:
            return len(self.rooms)


class SyncHandler(BaseHTTPRequestHandler):
    server_version = "ShijieLanSync/1.0"
    protocol_version = "HTTP/1.1"
    store: SyncStore

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {self.client_address[0]} {fmt % args}")

    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Cache-Control", "no-store")

    def _send_json(self, status: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self._send_json(HTTPStatus.OK, {
                "ok": True,
                "service": "拾阶局域网同步",
                "rooms": self.store.count(),
                "time": int(time.time() * 1000),
            })
            return
        if parsed.path == "/api/sync":
            room = self._read_room(parsed)
            if not room:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "missing_room"})
                return
            payload = self.store.get(room)
            if payload is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "no_data"})
                return
            self._send_json(HTTPStatus.OK, payload)
            return
        if parsed.path == "/":
            self._send_json(HTTPStatus.OK, {
                "ok": True,
                "message": "拾阶局域网同步服务正在运行",
                "health": "/api/health",
                "sync": "/api/sync?room=<同步口令>",
            })
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        self._handle_write()

    def do_PUT(self) -> None:  # noqa: N802
        self._handle_write()

    def _handle_write(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/sync":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        room = self._read_room(parsed)
        if not room:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "missing_room"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "empty_body"})
            return
        if length > MAX_BODY:
            self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "too_large"})
            return
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
            return
        if not isinstance(payload, dict) or not isinstance(payload.get("goals"), list):
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_payload"})
            return
        self.store.put(room, payload)
        self._send_json(HTTPStatus.OK, {
            "ok": True,
            "room": room,
            "updatedAt": payload.get("updatedAt"),
            "savedAt": int(time.time() * 1000),
        })

    @staticmethod
    def _read_room(parsed) -> str:
        values = parse_qs(parsed.query).get("room", [])
        room = values[0].strip() if values else ""
        if len(room) < 1 or len(room) > 128:
            return ""
        return room


def main() -> None:
    parser = argparse.ArgumentParser(description="拾阶局域网同步服务")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址，默认 0.0.0.0")
    parser.add_argument("--port", type=int, default=8787, help="监听端口，默认 8787")
    parser.add_argument(
        "--data",
        default=str(Path(__file__).resolve().parent / "lan-sync-data.json"),
        help="数据文件路径",
    )
    args = parser.parse_args()

    store = SyncStore(Path(args.data))
    SyncHandler.store = store
    server = ThreadingHTTPServer((args.host, args.port), SyncHandler)

    print("拾阶 · 局域网同步服务已启动")
    print(f"监听地址: http://{args.host}:{args.port}")
    for ip in local_ip_addresses():
        print(f"局域网地址: http://{ip}:{args.port}")
    print(f"数据文件: {Path(args.data).resolve()}")
    print("按 Ctrl+C 停止服务。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭…")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
