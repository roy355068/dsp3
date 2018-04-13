from flask import Flask, Response, abort, render_template, request, url_for, redirect, send_file
from cassandra.cluster import Cluster
from redis import Redis, StrictRedis
import requests
import base64
from initStore import initStore

app = Flask(__name__)

hostMap = { # Hosts for Directory
            "dir1": "128.2.100.164",        # :9337
            "dir2": "128.2.100.165",        # :9337
            "dirRedis": "128.2.100.166",    # :6379
            # Hosts for Cache + Store
            "cacheServer": "128.2.100.173", # :8040
            "cass1": "128.2.100.174",       # :9337
            "cass2": "128.2.100.175",       # :9337
            "cass3": "128.2.100.176",       # :9337
            "cass4": "128.2.100.177",       # :9337
            "storeRedis": "128.2.100.178"   # :6379
            }

@app.route('/photo/<mid>/<lvid>/<pid>', methods = ["POST"])
def post_photo(mid, lvid, pid):

    if request.method == "POST":

        print(mid, lvid, pid)
        tempImage = request.files['photo']
        names = tempImage.filename.split('.')
        pid = names[0]
        mimetype = "image/" + names[1]
        print("The mimetype is :"+ mimetype + "\n")
        image = tempImage.stream.read()
        imagestr = base64.b64encode(image)
        # cluster = Cluster([ hostMap["cass1"], hostMap["cass2"] ], port = 9337)
        cluster = Cluster([ str(mid) ], port = 9337)
        db_session = cluster.connect('photostore')
        db_session.execute(
            """
            INSERT INTO photo (pid, mimetype, payload)
            VALUES (%s, %s, %s)
            """,
            (pid, mimetype, imagestr.decode("utf-8"))
        )

    return '', 200

@app.route('/<mid>/<lvid>/<pid>')
def get_photo(mid, lvid, pid):

    with open('tmp.jpg','wb') as image:
        # try to get from redis first
        print("This is from get_photo pid = %s" %pid)
        redis_client = StrictRedis(host= hostMap["storeRedis"], port = 6379)
        result = redis_client.get(pid)

        if not result: # cache miss
            print('cache missed for pid ' + pid)

        else:
            print("Cache hit for %s!!!" % pid)
            image.write(base64.decodebytes(result))
            # Response(image, content_type=result.headers['content-type'])
            return send_file('tmp.jpg', mimetype="image/jpeg")

        # try to get from cassandra if not found in redis
        cluster = Cluster([ hostMap["cass1"], hostMap["cass2"] ], port = 9337)
        db_session = cluster.connect('photostore')
        query = db_session.prepare("SELECT * FROM photo WHERE pid=?")
        try:
            result = db_session.execute(query, [pid])
            if not result:
                abort(404)
            else:
                image.write(base64.b64decode(result[0].payload))
        except IOError:
            log.exception("Query error")
        print("This is from Cassandra!!!\n")
        value = {'payload' : result[0].payload, 'mimetype' : result[0].mimetype}

    with open("tmp.jpg", "rb") as tempImage:
        # tempImage = open("tmp.jpg", "rb")
        #tempImage = open(result[0].pid + "."+ result[0].mimetype.split('/')[1].replace('e',''), 'rb')
        imagestr = base64.b64encode(tempImage.read())
        redis_client.setex(result[0].pid, 20, imagestr)

    return send_file('tmp.jpg', mimetype = value['mimetype'])

if __name__ == '__main__':
    initStore()
    app.run(host = "0.0.0.0", port = 8040)
