from flask import Flask, render_template, request, jsonify
import setproctitle
import datetime
from rmq_negotiator import RMQNegotiator
import time
from Redis import Redis
from daemon import DemoHeartbeatDaemon
import queue_demoqueue_jobs

__author__ = 'joe vacovsky jr'
setproctitle.setproctitle("jobsinfopage")
app = Flask(__name__)


@app.route('/', methods=["GET"])
def main():
    return render_template("index.html")


@app.route('/flushall', methods=["GET"])
def flushall():
    result = {"success": True}
    r = Redis().Connection.flushall()
    return jsonify(result)


@app.route('/queue', methods=["GET"])
def queue():
    result = {"success": True}
    queue_demoqueue_jobs.queue_jobs()
    return jsonify(result)


@app.route('/data', methods=["GET"])
def data():
    r = Redis()
    results = {}
    
    results["requeued"] = len(r.Connection.smembers('REQUEUE'))
    results["running"] = len(r.Connection.smembers('RUNNING'))

    last15_q = "SELECT Count(id) FROM failed WHERE timestamp > '%s'" % str(datetime.datetime.now() - datetime.timedelta(minutes=15))
    results["last_15m_fails"] = DemoHeartbeatDaemon().runQuery(last15_q)[0][0]
    
    try:
        results["failed_count"] = int(r.Connection.get("failed_count").decode('utf8'))
    except:
        results["failed_count"] = 0
    try:
        results["completed"] = int(r.Connection.get("COMPLETED:COUNT").decode('utf8'))
    except:
        results["completed"] = 0
        
    results["queue_count"] = int(RMQNegotiator(message_queue="DemoQueue").queue_count())
    print(results)
    return jsonify(results)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5002, threaded=True)
