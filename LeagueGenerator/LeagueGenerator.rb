## This script generates a Fantasy FB Team Roster
## 11 Total. (1 QB, 2 RB, 2 WR, 1 TE, 1 K, 1 D)

NUM_LEAGUES = 500000

# Open up the files
QB_File = File.open("PlayersLists/QB_new.txt")
RB_File = File.open("PlayersLists/RB_new.txt")
WR_File = File.open("PlayersLists/WR_new.txt")
TE_File = File.open("PlayersLists/TE_new.txt")
K_File = File.open("PlayersLists/K_new.txt")
D_File = File.open("PlayersLists/D.txt")

# Start with empty arrays, these will be the truth arrays
QB_Array=[] 
RB_Array=[]
WR_Array=[]
TE_Array=[]
K_Array=[]
D_Array=[]

# Read all of the lines into their respective arrays
QB_File.each {|line|
	QB_Array.push line.chomp
}

RB_File.each {|line|
	RB_Array.push line.chomp
}

WR_File.each {|line|
	WR_Array.push line.chomp
}

TE_File.each {|line|
	TE_Array.push line.chomp
}

K_File.each {|line|
	K_Array.push line.chomp
}

D_File.each {|line|
	D_Array.push line.chomp
}

# Close the files
QB_File.close
RB_File.close
WR_File.close
TE_File.close
K_File.close
D_File.close

# Keep track of the user id (will be incremental)
userId = 1
# Intial date and time of league creation
t = Time.new(2014, 8, 2, 10, 0, 0)

for num_league in 1..NUM_LEAGUES

	## Programming for a League Starts Here:
	for i in 0..10

		if(i==0)
			# Copy the positions arrays
			qbSel = QB_Array.dup
			rbSel = RB_Array.dup
			wrSel = WR_Array.dup
			teSel = TE_Array.dup
			kSel = K_Array.dup
			dSel = D_Array.dup
		
		else
			# For any given user choose the players
			# Select random pick, remove from available list
			identifiers = "#{num_league}, #{userId}, "

			randomPick = rand(qbSel.length)
			user_QB = qbSel[randomPick]
			print identifiers + "#{user_QB}, QB, #{t}\n"
			qbSel.delete qbSel[randomPick]

			2.times do
				randomPick = rand(rbSel.length)
				user_RB = rbSel[randomPick]
				print identifiers + "#{user_RB}, RB, #{t}\n"
				rbSel.delete rbSel[randomPick]
			end

			2.times do
				randomPick = rand(wrSel.length)
				user_WR = wrSel[randomPick]
				print identifiers + "#{user_WR}, WR, #{t}\n"
				wrSel.delete wrSel[randomPick]
			end

			randomPick = rand(teSel.length)
			user_TE = teSel[randomPick]
			print identifiers + "#{user_TE}, TE, #{t}\n"
			teSel.delete teSel[randomPick]

			## Narrowing down the team
			#randomPick = rand(kSel.length)
			#user_K = kSel[randomPick]
			#print "#{user_K},"
			#kSel.delete kSel[randomPick]

			#randomPick = rand(dSel.length)
			#user_D = dSel[randomPick]
			#print "#{user_D}\n"
			#dSel.delete dSel[randomPick] 

			userId += 1
		end

	end

end






