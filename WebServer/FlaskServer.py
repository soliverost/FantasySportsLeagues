import json
from flask import Flask
from flask import render_template

from cassandra.cluster import Cluster
cluster = Cluster(['54.67.124.220','54.67.122.147','54.153.20.171'])
session = cluster.connect('fantasyfootball')

app = Flask(__name__)

def query_db():
    rows = session.execute("SELECT * FROM fantasyfootball.topusers")
    rows = sorted(rows, key=lambda user_row: user_row.points, reverse=True)
    return rows

def query_roster(userId):
    rows = session.execute("SELECT position,playername FROM fantasyfootball.userroster WHERE userid = " + userId)
    returnArr = []
    for rosterRow in rows:
    	temp = {}
    	temp["position"] = rosterRow[0]
    	temp["playername"] = rosterRow[1]
        temp["points"] = query_player(rosterRow[1].strip())
    	returnArr.append(temp)
    return returnArr

def query_player(playerName):
    rows = session.execute("SELECT points FROM fantasyfootball.playerpoints WHERE playername = \'" + playerName +"\'")
    returnArr = []
    for pointRow in rows:
        returnArr.append(pointRow[0])
    return returnArr


@app.route("/management.html")
def management():
	return render_template('management.html',title="Customer Analysis")

@app.route('/index')
def index():
	user = {'nickname': 'Sili'}
	return render_template('index.html', title='Home', user=user)

@app.route("/topusers")
def topusers():
	return render_template('management.html',messages=query_db())
	
@app.route("/topusers/json")
def topusersJson():
	topUsersVar = json.dumps(query_db());
	return topUsersVar

@app.route("/data/user/roster/<userId>/")
def userRoster(userId):
	userRosterVar = json.dumps(query_roster(userId))
	return userRosterVar

@app.route("/data/player/points/<playername>")
def playerPoints(playername):
    playerPointsVar = json.dumps(query_player(playername))
    return playerPointsVar


if __name__ == "__main__":
	app.run(host='0.0.0.0', debug=True)


