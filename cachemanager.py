import redis
import CONFIG

REDIS = redis.StrictRedis(host=CONFIG.REDDIS_CONN)


def get_or_set_obj(key, obj=None):
    v = REDIS.get(key)
    if v is not None:
        return v.decode('utf8')
    else:
        if obj is not None:
            REDIS.set(key, obj)
            return


get_or_set_obj("testkey", "testval")
if (get_or_set_obj("testkey") == "testval"):
    print("Redis Cache detected and accepting data.")
else:
    print("Redis Cache appears to be down.")
