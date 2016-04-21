from rmq_negotiator import RMQNegotiator
import threading
import time

def listen_and_exec(message_queue="BabyNamesPrecache"):
    listen = RMQNegotiator(message_queue=message_queue)
    threads = [threading.Thread(target=listen.begin_listening) for _ in range(8)]
    for thread in threads:
        thread.start()
        time.sleep(.25)

    for thread in threads:
        thread.join()

    
if __name__ == '__main__':
    listen_and_exec()

