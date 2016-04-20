
from rmq_negotiator import RMQNegotiator
import json
import random


def queue_jobs(count=1000):
    if count > 10000:
        count = 10000
    sender = RMQNegotiator(message_queue="DemoQueue")
    messages = []
    #pcstart = datetime.now()
    while count > 0:
        jobinfo = {
            "ident": random.randint(1, 99999999),
        }
        messages.append(jobinfo)
        count -= 1
        
    print("publishing ", len(messages), " jobs")
    sender.publish_messages(messages)
    print(len(messages), " jobs published")
        
if __name__ == '__main__':
    queue_jobs()
