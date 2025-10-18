from table_scrapper import get_table
info = [('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_standard_9', 'arsenal_player_stats_standard'), ('https://fbref.com/en/comps/9/Premier-League-Stats', 
"results2025-202691_overall", "premier_league_stats" ), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','matchlogs_for', 'arsenal_match_logs_schedule'), 
('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_keeper_9', 'arsenal_goalkeeping'), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats',
'stats_keeper_adv_9', 'arsenal_adv_goalkeeping'), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_shooting_9', 'arsenal_shooting_stats'),  
('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_passing_9', 'arsenal_passing_stats'), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_passing_types_9', 'arsenal_passing_types_stats'), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_gca_9', 'arsenal_goal_creation_stats'),
('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_gca_9', 'arsenal_goal_creation_stats'),  ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_defense_9', 'arsenal_player_defensive_actions_stats'), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_possession_9', 'arsenal_player_possession_stats'), ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_playing_time_9', 'arsenal_playing_time_stats')]
page_url , element_id, table_name = ('https://fbref.com/en/squads/18bb7c10/Arsenal-Stats','stats_misc_9', 'arsenal_miscellaneous_stats')
# Load the page
page = get_table.get_html_page(page_url)
html = get_table.get_table_by_id(element_id)
df = get_table.html_table_to_df(html,table_name)

print(df.head(100))


