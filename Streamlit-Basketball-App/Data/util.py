# Retry Wrapper 
def retry(func, retries=3):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(30)
                attempts += 1

    return retry_wrapper

# Get Single Game aggregation columns
def getSingleGameMetrics(gameID,homeTeamID,awayTeamID,awayTeamNickname,seasonYear,gameDate):

    @retry
    def getGameStats(teamID,gameID,seasonYear):
        gameStats = cumestatsteam.CumeStatsTeam(game_ids='00'+str(gameID),league_id ="00",
                                               season=seasonYear,season_type_all_star="Regular Season",
                                               team_id = teamID).get_normalized_json()

        #print(gameStats)
        gameStats = pd.DataFrame(json.loads(gameStats)['TotalTeamStats'])

        return gameStats

    data = getGameStats(homeTeamID,gameID,seasonYear)
    #print(data)
    data.at[1,'NICKNAME'] = awayTeamNickname
    data.at[1,'TEAM_ID'] = awayTeamID
    data.at[1,'OFFENSIVE_EFFICIENCY'] = (data.at[1,'FG'] + data.at[1,'AST'])/(data.at[1,'FGA'] - data.at[1,'OFF_REB'] + data.at[1,'AST'] + data.at[1,'TOTAL_TURNOVERS'])
    data.at[1,'SCORING_MARGIN'] = data.at[1,'PTS'] - data.at[0,'PTS']

    data.at[0,'OFFENSIVE_EFFICIENCY'] = (data.at[0,'FG'] + data.at[0,'AST'])/(data.at[0,'FGA'] - data.at[0,'OFF_REB'] + data.at[0,'AST'] + data.at[0,'TOTAL_TURNOVERS'])
    data.at[0,'SCORING_MARGIN'] = data.at[0,'PTS'] - data.at[1,'PTS']

    data['SEASON'] = seasonYear
    data['GAME_DATE'] = gameDate
    data['GAME_ID'] = gameID

    return data
