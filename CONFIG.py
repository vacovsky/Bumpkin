import socket

REDDIS_CONN = "192.168.111.50"
DB = 'log.sqlite3'
RMQ_HOST = "192.168.111.50"
RMQ_PORT = 5672
RMQ_USER = 'rabbit'
RMQ_PASS = 'bunnyrabbit!!'
MESSAGE_QUEUE = 'nightlyscriptmockup'


DBPATH = '/home/joe/Projects/babynames_python/database.sqlite'

if socket.gethostname != "octo-pi":
    DBPATH = "/mnt/sd/Projects/Angular/babynames/database.sqlite"
    
