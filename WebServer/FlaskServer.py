from flask import Flask
from cassandra.cluster import Cluster
cluster = Cluster(['54.183.192.90','54.183.167.48'])
session = cluster.connect('fantasy')

app = Flask(__name__)

@app.route("/")
@app.route("/index")
def hello():
	return "Hello Silvia!"

@app.route("/api/<player>/")
def countPlayer(player):
	cluster = Cluster(['54.183.192.90','54.183.167.48'])
	session = cluster.connect('fantasy')
	rows = session.execute("SELECT * FROM fantasy.players WHERE name = '" + player + "'")
	#strng = rows[0].count + " fantasy teams have choosen " + player
	return str(rows)


if __name__ == "__main__":
	app.run(host='0.0.0.0')