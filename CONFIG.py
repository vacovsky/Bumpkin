import socket

REDDIS_CONN = "192.168.111.50"
RMQ_HOST = "192.168.111.50"
RMQ_PORT = 5672
RMQ_USER = 'rabbit'
RMQ_PASS = 'bunnyrabbit!!'
MESSAGE_QUEUE = 'nightlyscriptmockup'


DBPATH = 'database.sqlite'
DB = '/var/www/Bumpkin/log.sqlite'

if socket.gethostname() != "octo-pi":
    DB = 'log.sqlite3'
    DBPATH = "/mnt/sd/Projects/Angular/babynames/database.sqlite"
