from flask import Flask, render_template, request, jsonify
import setproctitle
import datetime
from rmq_negotiator import RMQNegotiator
import time
from Redis import Redis
from daemon import DemoHeartbeatDaemon
import queue_demoqueue_jobs
import queue_babynames_jobs
from kpi_roller import JobsRuntimeKPIRoller

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
    queue = request.args.get('queue') or "DemoQueue"
    result = {}
    if queue == "DemoQueue":
        if int(RMQNegotiator(message_queue=queue).queue_count()) < 19000:
            queue_demoqueue_jobs.queue_jobs()
            result = {"success": True}    
        else:
            result = {
                "success": False,
                "message": "Sorry - we can't handle any more jobs right now.  Try when the queue is below 19000."
            }

    if queue == "BabyNamesPrecache":
        if int(RMQNegotiator(message_queue=queue).queue_count()) < 1:
            queue_babynames_jobs.queue_jobs()
            result = {"success": True}
        else:
            result = {
                "success": False,
                "message": "Sorry - we can't handle any more jobs right now.  Try when the queue is empty."
            }
    return jsonify(result)



@app.route('/kpis', methods=["GET"])
def kpis():
    bucket = request.args.get('bucket') or "DEMO"
    j = JobsRuntimeKPIRoller(bucket)
    j.export_runtime_kpi()
    return jsonify(j.results())
    
    

@app.route('/data', methods=["GET"])
def data():
    r = Redis()
    results = {}
    queue = request.args.get('queue') or "DemoQueue"
    
    redis_bucket = ""
    if queue == "DemoQueue":
        redis_bucket = "DEMO"
    if queue == "BabyNamesPrecache":
        redis_bucket = "BABYNAMESCACHE"

    rmq = RMQNegotiator(message_queue=queue)
    results["requeued"] = len(r.Connection.smembers('REQUEUE:' + redis_bucket))
    results["running"] = len(r.Connection.smembers('RUNNING:' + redis_bucket))

    last15_q = "SELECT Count(id) FROM failed WHERE timestamp > '%s'" % str(datetime.datetime.now() - datetime.timedelta(minutes=15))
    results["last_15m_fails"] = DemoHeartbeatDaemon().runQuery(last15_q)[0][0]
    
    try:
        results["failed_count"] = int(r.Connection.get("FAILED:" + redis_bucket + ":COUNT").decode('utf8'))
    except:
        results["failed_count"] = 0
    try:
        results["completed"] = int(r.Connection.get("COMPLETED:" + redis_bucket + ":COUNT").decode('utf8'))
    except:
        results["completed"] = 0
        
    results["queue_count"] = int(rmq.queue_count())

    results["queues"] = rmq.list_queues()
    return jsonify(results)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5002, threaded=True)
