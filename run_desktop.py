import threading
import webview
import time
import sys
import os

# Set execution environment for PyInstaller
if getattr(sys, 'frozen', False):
    os.chdir(sys._MEIPASS)

from app import app
from src.db import init_db

def run_server():
    """Start the Flask internal server."""
    # disabling reloader so it doesn't spawn multiple instances inside the executable
    app.run(port=5000, debug=False, use_reloader=False)

if __name__ == '__main__':
    # Ensure database is initialized
    init_db()
    
    # Start the server thread
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait 1 second for the server to start
    time.sleep(1)
    
    # Open the PyWebView native window
    # This blocks until the window is closed
    webview.create_window('Rechnungsverwaltung', 'http://127.0.0.1:5000/', width=1400, height=900)
    webview.start()
    
    # The daemon thread will automatically be killed when script exits.
