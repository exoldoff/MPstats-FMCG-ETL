from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DIST_INDEX = ROOT_DIR / "web" / "dist" / "index.html"
HOST = os.environ.get("MPSTATS_APP_HOST", "127.0.0.1")
PORT_START = int(os.environ.get("MPSTATS_APP_PORT", "8000"))
PORT_END = int(os.environ.get("MPSTATS_APP_PORT_END", "8010"))


def log(message: str) -> None:
    print(f"[MPStats] {message}", flush=True)


def fail(message: str, code: int = 1) -> None:
    print(f"[MPStats] {message}", file=sys.stderr, flush=True)
    try:
        input("\nPress Enter to close this window...")
    except EOFError:
        pass
    raise SystemExit(code)


def port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((HOST, port))
        except OSError:
            return False
    return True


def pick_port() -> int:
    for port in range(PORT_START, PORT_END + 1):
        if port_is_free(port):
            return port
    fail(f"No free port found in range {PORT_START}-{PORT_END}.")
    raise AssertionError("unreachable")


def health_is_ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{url}/api/health", timeout=0.5) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def main() -> int:
    if not DIST_INDEX.exists():
        fail("Missing web/dist/index.html. Build web/dist before using the Windows launcher.")

    port = pick_port()
    url = f"http://{HOST}:{port}"
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "mpstats_app.main:app",
        "--host",
        HOST,
        "--port",
        str(port),
    ]

    log(f"Starting backend at {url}")
    process = subprocess.Popen(command, cwd=ROOT_DIR)
    try:
        for _ in range(40):
            if process.poll() is not None:
                fail("Backend exited before it became ready.")
            if health_is_ready(url):
                break
            time.sleep(0.25)
        else:
            fail("Backend did not respond to /api/health.")

        if os.environ.get("MPSTATS_APP_NO_BROWSER") != "1":
            log("Opening browser...")
            webbrowser.open(url)

        print()
        print(f"MPStats Local App is running: {url}")
        print("Close this window or press Ctrl+C to stop the backend.")
        print()
        return process.wait()
    except KeyboardInterrupt:
        print()
        log("Stopping backend...")
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
