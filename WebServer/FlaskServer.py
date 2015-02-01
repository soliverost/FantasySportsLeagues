import json
from flask import Flask
from flask import render_template

from cassandra.cluster import Cluster
cluster = Cluster(['54.67.124.220','54.67.122.147','54.153.20.171'])
session = cluster.connect('fantasyfootball')

app = Flask(__name__)

def query_db():
    """Queries the database and returns a list of dictionaries."""
    rows = session.execute("SELECT * FROM fantasyfootball.topusers")
    #for user_row in rows:
    #	print user_row.userid, user_row.points
    rows = sorted(rows, key=lambda user_row: user_row.points, reverse=True)
    return rows

@app.route("/")

@app.route('/index')
def index():
	user = {'nickname': 'Sili'}
	return render_template('index.html', title='Home', user=user)

@app.route("/topusers")
def topusers():
	return render_template('index.html',messages=query_db())
	
@app.route("/topusers/json")
def topusersJson():
	topUsersVar = json.dumps(query_db());
	return topUsersVar


#def countPlayer(player):
#	rows = session.execute("SELECT * FROM fantasy.players WHERE name = '" + player + "'")
	#strng = rows[0].count + " fantasy teams have choosen " + player
#	return str(rows)


if __name__ == "__main__":
	app.run(host='0.0.0.0', debug=True)


