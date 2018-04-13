from flask import Flask, render_template, request, url_for, redirect, abort
from cassandra.cluster import Cluster
from redis import Redis
import requests
import random
from initDirectory import initDirectory

""" Temporary host mapping
40     : Cache + Store server 8040
41, 42 : Store Cass 9337
43     : Redis      6379
"""
initDirectory()
hostMap = {
            "dir1": "128.2.100.164", # web server
            "dir2": "128.2.100.165", # cache server
            "dirRedis": "128.2.100.166", # store server

            "cacheServer": "128.2.100.173",
            "cass1": "128.2.100.174",
            "cass2": "128.2.100.175",
            "ghc51": "128.2.100.184",
            "localhost": "127.0.0.1"
            }
app = Flask(__name__)
# app.config['UPLOAD_FOLDER'] = 'uploads/'
cassDirectory = [ hostMap["dir1"], hostMap["dir2"] ]
redisHost = hostMap["dirRedis"]
cluster = Cluster(cassDirectory, port = 9337)
dbSession = cluster.connect('directory')
redisClient = Redis(host = tempHost, port = 6379)

@app.route('/')
def get_all_photos():

    query = "SELECT * FROM photo"
    rows = dbSession.execute(query)
    photo_paths = []

    for row in rows:
        # Get all images back from the backend server
        data = [row.mid, str(row.lvid), row.pid]
        url = "http://" + hostMap["cacheServer"] + ":8040" + "/" + "/".join(data)
        photo_paths.append(url)

    return render_template('index.html', photo_paths= photo_paths)

@app.route('/upload/', methods=['GET'])
def upload():
    return render_template('upload.html')

@app.route('/photo/', methods=['POST'])
def post_photo():

    file = request.files["photo"]
    pid = str(file.filename.split(".")[0])
    rows = dbSession.execute('SELECT lvid, mid FROM store')
    # random choose a logical volume
    local_rows = [row for row in rows]
    row = random.choice(local_rows)
    lvid = row.lvid
    # random choose a store machine of selected logical volume
    # TODO: Should write to every machine mapped to the same logical volume
    mid = random.choice(row.mid)

    # insert photo into Directory
    # When test in local, the mid is localHost nad lvid is 0
    dbSession.execute(
        "INSERT INTO photo (pid, mid, lvid) VALUES (%s, %s, %s)", (pid, mid, lvid))

    # upload to store machine
    sendFile = { "photo": (file.filename, file.stream, file.mimetype) }
    data = ["photo", mid, str(lvid), pid]
    url = "http://" + hostMap["cacheServer"] + ":8040" + "/" + "/".join(data)

    rsp = requests.post(url, files=sendFile)
    return redirect(url_for('get_photo', pid=pid))

@app.route('/photo/<pid>')
def get_photo(pid):
    url = redisClient.get(pid) # try to get from redis first
    # if cache missed
    if not url:
        print('\ncache missed for pid ' + pid)
        prepare = dbSession.prepare("SELECT * FROM photo WHERE pid=?")
        row = None

        try:
            row = dbSession.execute(prepare, [pid])[0]
        except:
            print("\nexecute errrrrrr")

        if not row:
            print('\nnot found in directory')
            abort(404)

        data = [row.mid, str(row.lvid), row.pid]
        url = "http://" + hostMap["cacheServer"] + ":8040" + "/" + "/".join(data)
        redisClient.setex(pid, url, 120)
    else:
        url = url.decode("utf-8")
        print('\ncache hit for pid %s' % pid)

    return render_template('photo.html', photo_path=url)

if __name__ == "__main__":
    app.run(host = "0.0.0.0")
