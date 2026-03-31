"""
Petey Desktop — launches the web server and opens a browser.

Sets FIREBASE_AUTH_DISABLED=1 so no auth is needed locally.
"""
import os
import sys
import socket
import threading
import webbrowser
import time

# Force local mode
os.environ["FIREBASE_AUTH_DISABLED"] = "1"
os.environ["MAX_PAGES"] = "0"

# When frozen by PyInstaller, resources are in _MEIPASS
if getattr(sys, "frozen", False):
    BASE = sys._MEIPASS
    os.environ["PETEY_WEB_BASE"] = BASE
    # Add bundled binaries (tesseract, gs) to PATH
    bin_dir = os.path.join(BASE, "bin")
    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    # Point tesseract at bundled traineddata
    os.environ["TESSDATA_PREFIX"] = os.path.join(BASE, "share", "tessdata")
    # Point ghostscript at bundled resources
    gs_share = os.path.join(BASE, "share", "ghostscript")
    if os.path.isdir(gs_share):
        os.environ["GS_LIB"] = gs_share
    # Ensure bundled dylibs are found (macOS)
    lib_dir = os.path.join(BASE, "lib")
    existing = os.environ.get("DYLD_LIBRARY_PATH", "")
    os.environ["DYLD_LIBRARY_PATH"] = (
        lib_dir + os.pathsep + existing if existing else lib_dir
    )
else:
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def open_browser(port):
    """Wait for server to be ready, then open browser."""
    url = f"http://127.0.0.1:{port}"
    for _ in range(50):  # up to 5 seconds
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            webbrowser.open(url)
            return
        except OSError:
            time.sleep(0.1)
    # Open anyway after timeout
    webbrowser.open(url)


def main():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stderr),
            logging.FileHandler(os.path.join(
                os.path.expanduser("~"), ".petey-desktop.log"
            )),
        ],
    )
    log = logging.getLogger("petey-desktop")

    port = find_free_port()
    log.info(f"Starting Petey on http://127.0.0.1:{port}")

    # Open browser in background thread
    t = threading.Thread(target=open_browser, args=(port,), daemon=True)
    t.start()

    try:
        from server.app import app
        import uvicorn
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=port,
            log_level="info",
        )
    except Exception:
        log.exception("Failed to start server")
        raise


if __name__ == "__main__":
    # When frozen, redirect stdout/stderr to a log file so errors are visible
    if getattr(sys, "frozen", False):
        log_path = os.path.join(
            os.path.expanduser("~"), ".petey-desktop.log",
        )
        log_file = open(log_path, "w")
        sys.stdout = log_file
        sys.stderr = log_file
    main()
