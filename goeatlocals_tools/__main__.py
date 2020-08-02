import time


SPIN_WAIT_DELAY = 1


def spin_wait():
    while True:
        time.sleep(SPIN_WAIT_DELAY)


if __name__ == '__main__':
    spin_wait()
