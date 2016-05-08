from Redis import Redis
import sqlite3
import CONFIG
import threading
from datetime import datetime, timedelta
import time
from rmq_negotiator import RMQNegotiator
from dateutil import parser
import json

RUNNING = "RUNNING:DEMO"
COMPLETE = "COMPLETE:DEMO"
FAILED = "FAILED:DEMO"
REQUEUE = "REQUEUE:DEMO"


class DemoHeartbeatDaemon:
    R = None
    PubSub = None
    Messenger = None

    def __init__(self):
        self.R = Redis()
        self.Messenger = RMQNegotiator(message_queue="DemoQueue")

    def runQuery(self, query=tuple, connstring=CONFIG.DB):
        db = sqlite3.connect(connstring)
        cursor = db.cursor()
        cursor.execute(query)
        response = cursor.fetchall()
        db.commit()
        db.close()
        return response

    def log_failure(self, ident, starttime=None, endtime=None):
        sqlStr = """INSERT INTO failed (ident, timestamp) values (%d, '%s');""" % (
            int(ident), str(datetime.now()))
        self.runQuery(sqlStr)
        print(sqlStr)

    '''
    rabbitmq publish requeue tasks are created here
    '''

    def requeue_failed(self):
        failures = self.R.Connection.smembers(FAILED)
        for failure in failures:
            f = failure.decode('utf8')
            print(f)
            self.R.Connection.srem(FAILED, failure.decode('utf8'))
            self.R.Connection.sadd(REQUEUE, int(failure.decode('utf8')))
            self.log_failure(f)
            self.Messenger.publish_messages([
                {
                    "ident": f
                }
            ])

            if self.R.Connection.get("FAILED:DEMO:COUNT") is None:
                self.R.Connection.set("FAILED:DEMO:COUNT", 1)
            else:
                fc = int(self.R.Connection.get(
                    "FAILED:DEMO:COUNT").decode('utf8'))
                self.R.Connection.set("FAILED:DEMO:COUNT", fc + 1)

    def hb_compare(self, last_updated):
        elapsed = datetime.now() - last_updated
        if elapsed > timedelta(seconds=30):
            return True
        else:
            return False

    def monitor(self):
        while True:
            running = self.R.Connection.smembers(RUNNING)
            print("Count of running tasks: " + str(len(running)))
            for task in running:
                if self.R.Connection.get(task) is None:
                    self.R.Connection.sadd(FAILED, task)
                    self.R.Connection.srem(RUNNING, task)

            self.requeue_failed()
            time.sleep(5)

    def rerun_failures(self):
        self.R.subscribe(REQUEUE)


if __name__ == '__main__':
    monitor = DemoHeartbeatDaemon().monitor()
