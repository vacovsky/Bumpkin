
import CONFIG
from datetime import datetime
import rmq_negotiator
import json
import threading


def populate_redis_with_bn_totals(gender=None):
    if gender is None:
        genders = ['M', 'F']
    elif gender == 'M':
        genders = ['M']
    else:
        genders = ['F']

    locales = ['US','AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
               'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI',
               'MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND',
               'OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA',
               'WA','WV','WI','WY']

    print('Beginning pre-cache process - this dramatically improves search times for users when searching by region.')
    sender = rmq_negotiator.RMQNegotiator(message_queue="BabyNamesPrecache")
    messages = []
    for g in genders:
        for l in locales:
            for y in range(1879, 2015):
                jobinfo = {
                    "gender": g,
                    "locale": l,
                    "year": y
                }
                messages.append(jobinfo)
    print("publishing", len(messages), "jobs")
    sender.publish_messages(messages)
    print(len(messages), "jobs published")

if __name__ == '__main__':
    gender = 'MF'
    
    for g in gender:
        t = threading.Thread(target=populate_redis_with_bn_totals, args=(g))
        t.start()
