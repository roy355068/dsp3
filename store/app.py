from flask import Flask, Response, abort, render_template, request, url_for, redirect, send_file
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
from redis import Redis, StrictRedis
import time
import requests
import base64
import uuid
import io
from initStore import initStore

app = Flask(__name__)

@app.route('/photo/<mid>/<lvid>/<pid>', methods = ["POST"])
def post_photo(mid, lvid, pid):
    print("Post Photos!!!")
    cluster = Cluster(["128.2.100.174"], port = 9337)
    db_session = cluster.connect('photostore')
    
    if request.method == "POST":
        print("-------------------------")
        print(mid, lvid, pid)
        print("-------------------------")
        tempImage = request.files['photo']
        names = tempImage.filename.split('.')
        pid = names[0]
        if names[1] == 'jpg':
            mimetype = "image/jpeg"
        else:
            mimetype = "image/" + names[1]
        print("The mimetype is :"+ mimetype + "\n")
        image = tempImage.stream.read()
        imagestr = base64.b64encode(image)
        #if mid == '1':
        cluster = Cluster(["128.2.100.174"], port = 9337)
        #else:
        #cluster = Cluster([mid], port = 9337)
        db_session = cluster.connect('photostore')
        db_session.execute(
            """
            INSERT INTO photo (pid, mimetype, payload)
            VALUES (%s, %s, %s)
            """,
            (pid, mimetype, imagestr.decode("utf-8"))
        )
        #with open('tmp.jpg') as image1:
        #    image1.write(image)
        #img=mpimg.imread('tmp.jpg')
        #imgplot = plt.imshow(img)
    return '', 200

@app.route('/<mid>/<lvid>/<pid>', methods = ["GET"])
def get_photo(mid, lvid, pid):
    print("Get Photossss")
    result = None
    with open('tmp.jpg','wb+') as image:
        # try to get from redis first
        print("This is from get_photo pid = %s" %pid)
        redis_client = StrictRedis(host='128.2.100.176', port = 6379)
        result = redis_client.get(pid)
        if not result: # cache miss
            print('cache missed for pid ' + pid)
        else:
            print("Cache hit for %s!!!" % pid)
            image.write(base64.decodebytes(result))
            # Response(image, content_type=result.headers['content-type'])
    if result:
        return send_file('tmp.jpg', mimetype="image/jpeg")

    with open('tmp.jpg','wb+') as image:    
    # try to get from cassandra if not found in redis
        #if mid == '1':
        cluster = Cluster(["128.2.100.174"], port = 9337)
        #else:
        #cluster = Cluster([mid], port = 9337)
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
    tempImage = open("tmp.jpg", "rb")    
    #tempImage = open(result[0].pid + "."+ result[0].mimetype.split('/')[1].replace('e',''), 'rb')
    imagestr = base64.b64encode(tempImage.read())
    redis_client.setex(result[0].pid, 20, imagestr)
    tempImage.close()    
    return send_file('tmp.jpg', mimetype=value['mimetype'])

if __name__ == '__main__':
    initStore()
    app.run(host='0.0.0.0', port = 8040)
