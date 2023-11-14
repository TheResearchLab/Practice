import airplane


@airplane.task(
    slug="call_nba_api_test",
    name="call_nba_api_test",
)
def call_nba_api_test():
    from nba_api.stats.endpoints import cumestatsteam
    import pandas as pd
    import json
    
    data = cumestatsteam.CumeStatsTeam(game_ids='00'+str(22001066),league_id ="00",
                                               season='2020-21',season_type_all_star="Regular Season",
                                               team_id = '1610612737').get_normalized_json()

    # Sort the data in ascending order by name.
    data = pd.DataFrame(json.loads(data)['TotalTeamStats'])
    print(len(data))

    return data
