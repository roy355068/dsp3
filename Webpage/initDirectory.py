# As specified in the Haystack paper, the URL looks like this:
# http://<Cache>/<Machine id>/<Logical volume, Photo>
# Since we don't need to take care of the CDN, we simply discard CDN in URL.
from flask import Flask, render_template, request, url_for, redirect
from cassandra.cluster import Cluster
from redis import Redis
import time
import requests
import random

def initDirectory():
    # The redis cache and cassandra hosts for the web server
    hostMap = { "ghc31": "128.2.100.164", # web server
                "ghc32": "128.2.100.165", # cache server
                "ghc33": "128.2.100.166", # store server
                "ghc51": "128.2.100.184", # directory cassandra and redis

                "cacheServer": "128.2.100.173",
                "cass1": "128.2.100.174",       # :9337
                "cass2": "128.2.100.175",       # :9337
                "cass3": "128.2.100.176",       # :9337
                "cass4": "128.2.100.177",       # :9337
                 }

    app = Flask(__name__)
    tempHost = hostMap["ghc51"]
    cluster = Cluster([ tempHost ], port = 9337)
    session = cluster.connect()
    # Create the directory keyspace
    # session.execute("DROP KEYSPACE IF EXISTS directory")

    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS directory WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }
        """
    )

    # Set the active keyspace to directory
    session.set_keyspace('directory')

    # Create the users table
    # This creates a users table with columns: logical volume id (int) and
    # the corresponding machine ids (set of int)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS store (
            lvid int primary key,
            mid set<varchar>
        );
        """
    )
    # Set the logical mapping to the machine (physical volumes)
    # If store a photo on a logical volume, Haystack should write to all corresponding
    # physical volumns on different machines.
    # TODO: Here the mids should changed to the production hosts of Store
    session.execute(
        """
        INSERT INTO store (lvid, mid)
        VALUES (%s, %s)
        """,
        (0, { hostMap["cass1"], hostMap["cass2"], hostMap["cass3"] })
    )

    session.execute(
        """
        INSERT INTO store (lvid, mid)
        VALUES (%s, %s)
        """,
        (1, { hostMap["cass2"], hostMap["cass3"], hostMap["cass4"] })
    )
    session.execute(
        """
        INSERT INTO store (lvid, mid)
        VALUES (%s, %s)
        """,
        (2, { hostMap["cass1"], hostMap["cass2"], hostMap["cass4"] })
    )
    session.execute(
        """
        INSERT INTO store (lvid, mid)
        VALUES (%s, %s)
        """,
        (3, { hostMap["cass1"], hostMap["cass3"], hostMap["cass4"] })
    )

    # session.execute(
    #     """
    #     INSERT INTO store (lvid, mid)
    #     VALUES (%s, %s)
    #     """,
    #     # (1, {"128.2.100.166", "128.2.100.167"})
    #     (0, {hostMap["cacheServer"]})
    # )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS photo (
            pid varchar primary key,
            mid set<varchar>,
            lvid int
        );
        """
    )

if __name__ == "__main__":
    initDirectory()
