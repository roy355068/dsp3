from flask import Flask, request
from flask import render_template
from cassandra.cluster import Cluster
from redis import Redis, StrictRedis
from os import listdir
from os.path import isfile, isdir, join
import base64
import uuid

def initStore():
    # initialize redis with local image
    redis_client = StrictRedis(host='128.2.100.176', port = 6379)
    image = open('pittsburgh.jpg', 'rb')
    imagestr = base64.b64encode(image.read())
    redis_client.setex('pittsburgh', 10, imagestr)

    # initialize cassandra
    cluster1 = Cluster(['128.2.100.174'],port=9337)
    #cluster2 = Cluster(['128.2.100.178'],port=9337)
    #cluster2 = Cluster(['172.20.0.7'])
    db_session1 = cluster1.connect()
    #db_session2 = cluster2.connect()
    
    # db_session1.execute("DROP KEYSPACE IF EXISTS photostore")
    
    db_session1.execute(
	"""
	CREATE KEYSPACE IF NOT EXISTS photostore WITH REPLICATION = {
		'class': 'SimpleStrategy',
		'replication_factor': 1
	}
	"""
    )
    #db_session2.execute(
    #    """
    #    CREATE KEYSPACE IF NOT EXISTS photostore WITH REPLICATION = {
    #            'class': 'SimpleStrategy',
    #            'replication_factor': 1
    #    }
    #    """
    #)

    db_session1.set_keyspace('photostore')
    #db_session2.set_keyspace('photostore')
    #db_session2 = cluster2.connect('photostore')
    
    db_session1.execute(
	"""
	CREATE TABLE IF NOT EXISTS photo (
	    pid text primary key,
	    mimetype text,
	    payload text
	)
	"""
    )

    #db_session2.execute(
    #    """
    #    CREATE TABLE IF NOT EXISTS photo (
    #        pid text primary key,
    #        mimetype text,
    #        payload text
    #    )
    #    """
    #)

    image = open('losangeles.jpg', 'rb')
    imagestr = base64.b64encode(image.read())

    db_session1.execute(
        """
        INSERT INTO photo (pid, mimetype, payload)
        VALUES (%s, %s, %s)
        """,
        ('losangeles', 'image/jpeg', imagestr.decode("utf-8"))
    )
'''
    db_session2.execute(
        """
        INSERT INTO photo (pid, mimetype, payload)
        VALUES (%s, %s,  %s)
        """,
        ('losangeles', 'image/jpeg', imagestr.decode("utf-8"))
    )
'''
