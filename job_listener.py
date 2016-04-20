import CONFIG
from rmq_negotiator import RMQNegotiator
import worker
import threading

def listen_and_exec(message_queue=CONFIG.MESSAGE_QUEUE):
    listen = RMQNegotiator(message_queue="DemoQueue")
    
    threads = [threading.Thread(target=listen.begin_listening) for _ in range(4)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    
if __name__ == '__main__':
    listen_and_exec()

