# League Generator

This script generates the users teams and leagues.
Each user gets a roster of 8 players: 
1. 1 Quarterback (QB)
2. 2 Running Backs (RB)
3. 2 Wide Receivers (WR)
4. 1 Tight End (TE)
5. 1 Kicker (K)
6. 1 Defense Team (D)

Each user is entered into a league of 10 people.
Each league cannot have the same players in it.

Each player is selected from a the lists included in the folder PlayersLists.
I had to originally reverse last name and first name and I have included the bash command to do so.

The number of leagues can be set by changing:
NUM_LEAGUES = 500000

## Reverse Names
The bash command to reverse the names is included in ReverseNames file.

## How to run
To run this program, run the following command from the terminal window:
'''
ruby LeagueGenerator.rb > fileName.csv
'''

