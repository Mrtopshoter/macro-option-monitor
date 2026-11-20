#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美股期权微观结构与流动性踩踏监控看板 - 本地实时数据服务 (server.py)
启动方式：
  python3 server.py [--port 8765]
功能：
  1. 静态托管 index.html 与 data/latest_market_data.json
  2. 提供 POST /api/refresh 接口：前端点击「同步最新实盘数据」时，实时并发拉取 Cboe / Yahoo 最新数据，
     自动更新 data/latest_market_data.json 与 index.html 内嵌快照并返回最新 JSON。
"""

from __future__ import annotations

import argparse
import http.server
import json
import socketserver
import traceback
from pathlib import Path

import update_data

BASE_DIR = Path(__file__).resolve().parent


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        super().end_headers()

    def do_GET(self):
        if self.path.startswith("/api/latest"):
            if update_data.JSON_OUT.exists():
                payload = update_data.JSON_OUT.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
        return super().do_GET()

    def do_POST(self):
        if self.path.startswith("/api/refresh"):
            try:
                update_data.DATA_DIR.mkdir(parents=True, exist_ok=True)
                dataset = update_data.build_dataset()
                raw_json = json.dumps(dataset, ensure_ascii=False, indent=2)
                update_data.JSON_OUT.write_text(raw_json, encoding="utf-8")
                update_data.inject_into_html(dataset)
                body = json.dumps({"ok": True, "data": dataset}, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                traceback.print_exc()
                err = json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(err)))
                self.end_headers()
                self.wfile.write(err)
            return
        self.send_error(404, "Not Found")


class ReusableTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


def main() -> None:
    parser = argparse.ArgumentParser(description="启动美股期权微观结构监控看板本地服务")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址 (默认 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="监听端口 (默认 8765)")
    args = parser.parse_args()

    with ReusableTCPServer((args.host, args.port), DashboardHandler) as httpd:
        print(f"美股期权微观结构监控看板已启动: http://{args.host}:{args.port}")
        print("在浏览器点击右上角「同步最新实盘数据」即可实时拉取 Cboe / Yahoo 最新数据。")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n服务已停止。")


if __name__ == "__main__":
    main()
