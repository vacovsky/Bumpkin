import redis
import time
import CONFIG


class Redis:

    Connection = None
    PubSub = None

    def __init__(self):
        self.create_connection()
        self.create_pubsub()

    def create_pubsub(self):
        self.PubSub = self.Connection.pubsub()

    def create_connection(self, connection=CONFIG.REDDIS_CONN):
        self.Connection = redis.StrictRedis(
            host=connection,
            port=6379,
            db=0,
            password=None)

    def subscribe(self, channel):
        self.PubSub.subscribe(channel)

    def publish(self, channel, message):
        self.Connection.publish(channel, message)

    def get_message(self):
        self.PubSub.get_message()

    def get_or_set(self, key, obj=None):
        v = self.Connection.get(key)
        if v is not None:
            return v.decode('utf8')
        else:
            if obj is not None:
                self.Connection.set(key, obj)
                return

if __name__ == '__main__':
    r = Redis()
    r.subscribe('test')
    for message in r.PubSub.listen():
        print(message)


'''
for num in range(0, 100):
    r.sadd(RUNNING, num)

for x in range(20, 25):
    r.sadd(FAILED, x)

# sets commands that matter for my needs
# sinter, sadd, srem, sdiff



print(r.sinter(RUNNING, FAILED))
print(r.sdiff(RUNNING, FAILED))
'''
