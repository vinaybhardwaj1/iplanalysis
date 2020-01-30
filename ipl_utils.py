#Adding columns 'wickets_fallen' ,'score','last_wicket_ball','striker_runs','non_striker_runs','bowler_match_runs' and 'bowler_match_wickets'. This block #requires a lot of time as it is iterating over the past data for each match so we will store this data and re-use the new dataset after first run. ====need to #find better way
def createBallsExtended(df_balls) :
	df_balls.sort_values(by=['match_id','inning','over','ball'])

	df_balls['wickets_fallen'] = 0
	wick = 0

	df_balls['score'] = 0
	sc = 0

	df_balls['last_wicket_ball'] = 0.0
	lb = 0.0

	df_balls['striker_runs'] = 0
	df_balls['non_striker_runs'] = 0
	bruns = 0
	nsruns = 0

	df_balls['bowler_match_runs'] = 0
	bowler_match_runs = 0
	k = 0

	df_balls['bowler_match_wickets'] = 0
	bowler_match_wicks = 0
	k1 = 0

	for i,row in df_balls.iterrows():
		if i > 0:
			match = row['match_id']
			inning = row['inning']
			last_index = i - 1
			if (df_balls['match_id'][last_index] != match) or (df_balls['inning'][last_index] != inning):
				
				#for wickets_fallen and last_wicket_ball
				if row['player_dismissed'] != 'NA':
					wick = 1
					lb = 1.1
				else:
					wick = 0
					lb = 0.0
				
				df_balls['wickets_fallen'][i] = wick
				df_balls['last_wicket_ball'][i] = lb
				
				#for score    
				sc = 0
				df_balls['score'][i] = sc
				
				#for batsman_runs
				if row['batsman_runs'] > 0:
					bruns += row['batsman_runs']
				df_balls['striker_runs'][i] = bruns
				df_balls['non_striker_runs'][i] = nsruns
				
				#for bowler_match_runs
				bowler_match_runs = row['bowler_runs_conceded']
				df_balls['bowler_match_runs'][i] = bowler_match_runs
				k = i
				
				bowler_match_wicks = 0
				if row['player_dismissed'] != 'NA':
					bowler_match_wicks = 1                      
				df_balls['bowler_match_wickets'][i] = bowler_match_wicks
				k1 = i
				
				continue
			
			#for wickets_fallen
			if (row['player_dismissed'] != 'NA'):
				wick += 1
				lb = row['over'] + (0.1)*(row['ball'])    
			
			df_balls['wickets_fallen'][i] = wick
			df_balls['last_wicket_ball'][i] = lb
			
			#for score
			sc += row['total_runs']
			df_balls['score'][i] = sc
			
			#for striker_runs and non_striker_runs
			last_ball_bat = df_balls['batsman'][last_index]
			last_ball_ns = df_balls['non_striker'][last_index]
			new_ball_bat = row['batsman']
			new_ball_ns = row['non_striker']
			
			if (last_ball_bat == new_ball_ns) & (last_ball_ns == new_ball_bat):
				temp = bruns
				bruns = nsruns
				nsruns = temp
			elif (last_ball_bat != new_ball_ns) & (last_ball_ns == new_ball_bat):
				bruns = nsruns
				nsruns = 0
			elif (last_ball_bat == new_ball_ns) & (last_ball_ns != new_ball_bat):
				nsruns = bruns
				bruns = 0
			elif (last_ball_bat == new_ball_bat) & (last_ball_ns != new_ball_ns):
				nsruns = 0
			elif (last_ball_bat != new_ball_bat) & (last_ball_ns == new_ball_ns):
				bruns = 0
			
			bruns += row['batsman_runs']
			df_balls['striker_runs'][i] = bruns
			df_balls['non_striker_runs'][i] = nsruns
			
			
			#for bowler_match_runs
			checker = df_balls.iloc[k:i+1,:].groupby('bowler',as_index=False)['bowler_runs_conceded'].sum()
			bowler_match_runs = checker[checker['bowler'] == row['bowler']]
			df_balls['bowler_match_runs'][i] = bowler_match_runs['bowler_runs_conceded'].values[0]
			
			
			#for bowler_match_wickets
			player_wickets_checker = df_balls.iloc[k1:i+1,:][~df_balls['dismissal_kind'].isin(
								  ['NA','run out', 'retired hurt','obstructing the field'])
							 ].groupby('bowler',as_index=False)['player_dismissed'].count()
			bowler_match_wicks = player_wickets_checker[player_wickets_checker['bowler'] == row['bowler']]
			if bowler_match_wicks['player_dismissed'].values:
				df_balls['bowler_match_wickets'][i] = bowler_match_wicks['player_dismissed'].values[0]
			else:
				df_balls['bowler_match_wickets'][i] = 0
			
			if i%10000 == 0:
				print(i,' Done')
			
		else:
			if row['player_dismissed'] != 'NA':
				wick = 1
				lb = 1.1
				bruns = 0
			
			df_balls['last_wicket_ball'][i] = lb
			df_balls['wickets_fallen'][i] = wick
			
			#for score
			sc += row['total_runs']
			df_balls['score'][i] = sc
			
			#for batsman_runs
			if row['batsman_runs'] > 0:
				bruns += row['batsman_runs']
			df_balls['striker_runs'][i] = bruns
			df_balls['non_striker_runs'][i] = nsruns
			
			#for bowler_match_runs
			df_balls['bowler_match_runs'][i] = bowler_match_runs
			
			#for bowler_match_wickets
			df_balls['bowler_match_wickets'][i] = bowler_match_wicks

	df_balls.head()
	return df_balls

# Adding column 'score' need to find better way	
def createScoreColumn(df_balls):
	df_balls['score'] = 0
	sc = 0
	for i,row in df_balls.iterrows():
		if i > 0:
			match = row['match_id']
			inning = row['inning']
			last_index = i - 1
			if (df_balls['match_id'][last_index] != match) or (df_balls['inning'][last_index] != inning):
				sc = 0
				df_balls['score'][i] = sc
			else:
				sc += row['total_runs']
				df_balls['score'][i] = sc
		else:
			sc += row['total_runs']
			df_balls['score'][i] = sc
	df_balls.head()
	return df_balls

# Adding column 'last_wicket_ball' need to find better way
def createLastWicketBall(df_balls) :
	df_balls['last_wicket_ball'] = 0.0
	lb = 0.0
	for i,row in df_balls.iterrows():
		if i > 0:
			match = row['match_id']
			inning = row['inning']
			last_index = i - 1
			if (df_balls['match_id'][last_index] != match) or (df_balls['inning'][last_index] != inning):
				if row['player_dismissed'] != 'NA':
					lb = 1.1
					df_balls['last_wicket_ball'][i] = lb
				else:
					lb = 0.0
					df_balls['last_wicket_ball'][i] = lb
				continue
			
			if (row['player_dismissed'] != 'NA'):
				lb = row['over'] + (0.1)*(row['ball'])
				df_balls['last_wicket_ball'][i] = lb
			else:
				df_balls['last_wicket_ball'][i] = lb
		else:
			if row['player_dismissed'] != 'NA':
				lb = 1.1
				df_balls['last_wicket_ball'][i] = lb
	df_balls.head()
	return df_balls
	
# Adding columns 'striker_runs' and 'non_striker_runs'
def createS_NS_Runs(df_balls):
	df_balls['striker_runs'] = 0
	df_balls['non_striker_runs'] = 0
	bruns = 0
	nsruns = 0
	for i,row in df_balls.iterrows():
		if i > 0:
			match = row['match_id']
			inning = row['inning']
			last_index = i - 1
			if (df_balls['match_id'][last_index] != match) or (df_balls['inning'][last_index] != inning):
				if row['batsman_runs'] > 0:
					bruns += row['batsman_runs']
				df_balls['striker_runs'][i] = bruns
				df_balls['non_striker_runs'][i] = nsruns
				continue
			
			last_ball_bat = df_balls['batsman'][last_index]
			last_ball_ns = df_balls['non_striker'][last_index]
			new_ball_bat = row['batsman']
			new_ball_ns = row['non_striker']
			
			
			
			if (last_ball_bat == new_ball_ns) & (last_ball_ns == new_ball_bat):
				temp = bruns
				bruns = nsruns
				nsruns = temp
			elif (last_ball_bat != new_ball_ns) & (last_ball_ns == new_ball_bat):
				bruns = nsruns
				nsruns = 0
			elif (last_ball_bat == new_ball_ns) & (last_ball_ns != new_ball_bat):
				nsruns = bruns
				bruns = 0
			elif (last_ball_bat == new_ball_bat) & (last_ball_ns != new_ball_ns):
				nsruns = 0
			elif (last_ball_bat != new_ball_bat) & (last_ball_ns == new_ball_ns):
				bruns = 0
			
			bruns += row['batsman_runs']
			df_balls['striker_runs'][i] = bruns
			df_balls['non_striker_runs'][i] = nsruns
			
		else:
			if row['player_dismissed'] != 'NA':
				bruns = 0
			if row['batsman_runs'] > 0:
				bruns += row['batsman_runs']
			df_balls['striker_runs'][i] = bruns
			df_balls['non_striker_runs'][i] = nsruns
			
	df_balls.head()
	return df_balls
	
# Adding columns 'bowler_match_runs' and 'bowler_match_wickets'
def createBowlerColumns(df_balls):
	df_balls['bowler_match_runs'] = 0
	bowler_match_runs = 0
	k = 0
	for i,row in df_balls.iterrows():
		if i > 0:
			match = row['match_id']
			inning = row['inning']
			last_index = i - 1
			if (df_balls['match_id'][last_index] != match) or (df_balls['inning'][last_index] != inning):
				bowler_match_runs = row['bowler_runs_conceded']
				df_balls['bowler_match_runs'][i] = bowler_match_runs
				k = i
				continue
			
			checker = df_balls.iloc[k:i+1,:].groupby('bowler',as_index=False)['bowler_runs_conceded'].sum()
			bowler_match_runs = checker[checker['bowler'] == row['bowler']]
			df_balls['bowler_match_runs'][i] = bowler_match_runs['bowler_runs_conceded'].values[0]
			
			if i%10000 == 0:
				print(i,' Done')
		else:
			df_balls['bowler_match_runs'][i] = bowler_match_runs
	
	df_balls['bowler_match_wickets'] = 0
	bowler_match_wicks = 0
	k = 0
	for i,row in df_balls.iterrows():
		if i > 0:
			match = row['match_id']
			inning = row['inning']
			last_index = i - 1
			if (df_balls['match_id'][last_index] != match) or (df_balls['inning'][last_index] != inning):
				bowler_match_wicks = 0
				if row['player_dismissed'] != 'NA':
					bowler_match_wicks = 1                      
				df_balls['bowler_match_wickets'][i] = bowler_match_wicks
				k = i
				continue
			
			player_wickets_checker = df_balls.iloc[k:i+1,:][~df_balls['dismissal_kind'].isin(
								  ['NA','run out', 'retired hurt','obstructing the field'])
							 ].groupby('bowler',as_index=False)['player_dismissed'].count()
			bowler_match_wicks = player_wickets_checker[player_wickets_checker['bowler'] == row['bowler']]
			if bowler_match_wicks['player_dismissed'].values:
				df_balls['bowler_match_wickets'][i] = bowler_match_wicks['player_dismissed'].values[0]
			else:
				df_balls['bowler_match_wickets'][i] = 0
			
			if i%10000 == 0:
				print(i,' Done')
		else:
			df_balls['bowler_match_wickets'][i] = bowler_match_wicks

	return df_balls

	
