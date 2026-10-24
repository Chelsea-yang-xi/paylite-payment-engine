
import sys
import time
from pathlib import Path

# Allow importing main.py from the project root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import main


def run_worker():
    print("[WORKER] Starting Paylite worker")

    while True:
        try:
            processed = main.process_next_pending()

            if not processed:
                time.sleep(1)

        except Exception as error:
            print(f"[WORKER] Error: {error}")
            time.sleep(2)


if __name__ == "__main__":
    run_worker()
