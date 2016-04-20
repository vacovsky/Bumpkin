import CONFIG
from rmq_negotiator import RMQNegotiator
import demoqueue_worker
import threading
import time

def listen_and_exec(message_queue="DemoQueue"):
    listen = RMQNegotiator(message_queue=message_queue)
    threads = [threading.Thread(target=listen.begin_listening) for _ in range(4)]
    for thread in threads:
        time.sleep(.25)
        thread.start()

    for thread in threads:
        thread.join()

    
if __name__ == '__main__':
    listen_and_exec()

