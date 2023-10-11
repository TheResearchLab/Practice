from util_funcs import mycursor


mycursor.execute("drop view vw_last_game")
mycursor.execute("""CREATE VIEW vw_last_game as 
            select game_id
                  ,game_date
                  ,city
                  ,nickname
                  ,team_id

            from
                (select sgs.*
                       ,row_number() over (partition by team_id order by game_date desc) as rn
                from stg_game_stats sgs) max_dates
            where max_dates.rn = 1""") 


