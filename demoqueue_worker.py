from Redis import Redis
import time
from datetime import datetime
from random import randrange, randint
import json


RUNNING = "RUNNING:DEMO"
COMPLETE = "COMPLETE:DEMO"
FAILED = "FAILED:DEMO"
REQUEUE = "REQUEUE:DEMO"

'''
This class executes jobs as they appear in the queue of jobs to execute.
'''
class DemoQueueWorker:
    R = None
    total = 0
    counter = 0
    id = None
    
    def __init__(self, id=-1):
        self.id = id
        self.R = Redis().Connection

        
    def start(self):
        print('starting job id: ' + str(self.id))
        self.R.sadd(RUNNING, self.id)
        self.alive()
        return self.perform_task()
        

    def alive(self):
        self.R.setex(
            str(self.id),
            30,
            json.dumps(
                {
                    'id': self.id,
                    'count': self.counter,
                    'last_total': self.total,
                    'last_update': str(datetime.now())
                }
            ),
        )
        
    def perform_task(self):
        while self.total <= 21:
            if self.counter > 6:
                self.counter = 0
                print('Seven hits and still didn\'t bust for ID: ' + str(self.id))
                time.sleep(60)
                return False
            add_me = randrange(1, 11)
            self.counter += 1
            time.sleep(.1)
            self.total += add_me
            self.alive()
            
        self.cleanup()
        return True

            
    def cleanup(self):
        self.counter = 0
        self.R.srem(RUNNING, self.id)
        self.R.sadd(COMPLETE, self.id)
        c = self.R.get("COMPLETED:DEMO:COUNT")
        if c is not None:
            self.R.set("COMPLETED:DEMO:COUNT", int(c) + 1)
        else:
            self.R.set("COMPLETED:DEMO:COUNT", 1)
        if self.id in self.R.smembers(REQUEUE):
            self.R.srem(REQUEUE, self.id)
            self.R.set("COMPLETED:DEMO:COUNT", int(c) + 1)
        
 
if __name__ == '__main__':
    DemoWorker().start()
    
