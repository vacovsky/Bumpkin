from Redis import Redis
import time
from datetime import datetime
from random import randrange, randint
import json


RUNNING = "RUNNING"
COMPLETE = "COMPLETE"
FAILED = "FAILED"
REQUEUE = "REQUEUE"

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
        while self.total < 21:
            if self.counter > 4:
                self.counter = 0
                print('Hung Job: ' + str(self.id))
                time.sleep(60)
                return False
            add_me = randrange(0, 21)
            self.counter += 1
            time.sleep(1)
            #time.sleep(randint(1, 20))
            self.total += add_me
            self.alive()
            
        self.cleanup()
        return True

            
    def cleanup(self):
        self.counter = 0
        self.R.srem(RUNNING, self.id)
        self.R.srem(REQUEUE, self.id)
        self.R.sadd(COMPLETE, self.id)
        c = self.R.get("COMPLETED:COUNT")
        if c is not None:
            self.R.set("COMPLETED:COUNT", int(c) + 1)
        else:
            self.R.set("COMPLETED:COUNT", 1)
        if self.id in self.R.smembers(REQUEUE):
            #fc = int(self.R.Connection.get("failed_count").decode('utf8'))
            #self.R.Connection.set("failed_count", fc - 1)
            self.R.srem(REQUEUE, self.id)
            self.R.set("COMPLETED:COUNT", int(c) + 1)
            
        
 
if __name__ == '__main__':
    DemoWorker().start()
    
