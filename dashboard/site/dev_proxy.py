#!/usr/bin/env python3
"""Local dev server that proxies dashboard_data.json to avoid CORS."""

import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

DEFAULT_REMOTE_DATA_URL = "https://d25spyc5nz22ju.cloudfront.net/dashboard_data.json"


def load_env(path: str) -> dict:
    env: dict[str, str] = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def load_env_chain(paths: list[str]) -> dict:
    merged: dict[str, str] = {}
    for path in paths:
        merged.update(load_env(path))
    return merged


class DevProxyHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/dashboard_data.json"):
            self._proxy_dashboard_data()
            return
        super().do_GET()

    def _proxy_dashboard_data(self):
        remote_data_url = self.server.remote_data_url
        try:
            req = Request(remote_data_url, headers={"User-Agent": "dashboard-dev-proxy"})
            with urlopen(req, timeout=10) as resp:
                data = resp.read()
                content_type = resp.headers.get("Content-Type", "application/json")
        except Exception as exc:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"Proxy error: {exc}".encode("utf-8"))
            return

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    env = load_env_chain(
        [
            os.path.join(repo_root, ".env"),
            os.path.join(script_dir, ".env"),
        ]
    )
    remote_data_url = os.environ.get("DASHBOARD_DATA_URL") or env.get(
        "DASHBOARD_DATA_URL", DEFAULT_REMOTE_DATA_URL
    )
    server = ThreadingHTTPServer(("0.0.0.0", 8000), DevProxyHandler)
    server.remote_data_url = remote_data_url
    print("Serving dashboard with proxy on http://localhost:8000")
    print(f"Proxying /dashboard_data.json -> {remote_data_url}")
    server.serve_forever()


if __name__ == "__main__":
    main()
