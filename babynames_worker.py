from Redis import Redis
import time
from datetime import datetime
from random import randrange, randint
import json
import babynames_task


RUNNING = "RUNNING:BABYNAMESCACHE"
COMPLETE = "COMPLETE:BABYNAMESCACHE"
FAILED = "FAILED:BABYNAMESCACHE"
REQUEUE = "REQUEUE:BABYNAMESCACHE"

'''
This class executes jobs as they appear in the queue of jobs to execute.
'''


class BabyNamesWorker:
    total = 0
    counter = 0

    def __init__(self, year, gender, locale):
        self.R = Redis().Connection
        self.year = year
        self.gender = gender
        self.locale = locale
        self.json_obj = json.dumps({
            "locale": self.locale,
            "year": self.year,
            "gender": self.gender
        })

    def start(self):
        print('starting job: ' + self.json_obj)
        self.R.sadd(RUNNING, self.json_obj)
        self.alive()
        return self.perform_task()

    def alive(self):
        self.R.setex(
            str(json.dumps({
                "locale": self.locale,
                "year": self.year,
                "gender": self.gender
            })),
            30,
            json.dumps(
                {
                    'count': self.counter,
                    'last_total': self.total,
                    'last_update': str(datetime.now())
                }
            ),
        )

    def perform_task(self):
        self.alive()
        babynames_task.prepopulate_cache(self.year, self.gender, self.locale)
        self.cleanup()
        return True

    def cleanup(self):
        self.counter = 0
        message = {'flash_color': 'yellow',
                   'base_color': 'purple',
                   'interval': 0.03,
                   'count': 1
                   }
        Redis().publish("BlinkBlock", message)
        self.R.srem(RUNNING, self.json_obj)
        self.R.srem(REQUEUE, self.json_obj)
        self.R.sadd(COMPLETE, self.json_obj)
        c = self.R.get("COMPLETED:BABYNAMESCACHE:COUNT")
        if c is not None:
            self.R.set("COMPLETED:BABYNAMESCACHE:COUNT", int(c) + 1)
        else:
            self.R.set("COMPLETED:BABYNAMESCACHE:COUNT", 1)
        if self.json_obj in self.R.smembers(REQUEUE):
            #fc = int(self.R.Connection.get("failed_count").decode('utf8'))
            #self.R.Connection.set("failed_count", fc - 1)
            self.R.srem(REQUEUE, self.json_obj)


if __name__ == '__main__':
    DemoWorker().start()
