import time
def countdown(n):
    while n > 0:
        print('T-minus', n)
        n -= 1
        time.sleep(1)

from threading import Thread
t = Thread(target=countdown, args=(5,), daemon=True)
t.start()

while True:
    print("running")
    if t.is_alive():
        print("Still alive")
    else:
        print("Dead")
        break
    time.sleep(1)

class CountDownTask:
    def __init__(self):
        self._running = True

    def terminate(self):
        self._running = False

    def run(self, n):
        while self._running and n > 0:
            print('T-minus', n)
            n -= 1
            time.sleep(1)

c = CountDownTask()
t = Thread(target=c.run, args=(5,))
t.start()
# c.terminate()
t.join()
if t.is_alive():
    print("Still alive")
else:
    print("Dead")