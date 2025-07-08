import time


def run():
    print("Running...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interupted by the user.")
