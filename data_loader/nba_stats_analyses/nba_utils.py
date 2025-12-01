# nba_utils.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text

class NBAAnalyzer:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)
        self.game_basic = None
        self.game_advanced = None
        self.load_data()
    
    def load_data(self):
        """Load data from database"""
        self.game_basic = pd.read_sql("SELECT * FROM nba_game_basic", self.engine)
        self.game_advanced = pd.read_sql("SELECT * FROM nba_game_advanced", self.engine)
        print("Data loaded successfully!")
    
    def get_player_comparison(self, players, stat='pts'):
        """Compare multiple players on a specific stat"""
        comparison_data = []
        
        for player in players:
            player_data = self.game_basic[
                (self.game_basic['player'] == player) & 
                (self.game_basic['period'] == 'game')
            ]
            
            if len(player_data) > 0:
                stats = player_data[stat].describe()
                stats['player'] = player
                stats['games'] = len(player_data)
                comparison_data.append(stats)
        
        return pd.DataFrame(comparison_data).set_index('player')
    
    def plot_player_comparison(self, players, stats=['pts', 'trb', 'ast']):
        """Plot comparison of multiple players across multiple stats"""
        fig, axes = plt.subplots(1, len(stats), figsize=(15, 5))
        
        for i, stat in enumerate(stats):
            stat_data = []
            for player in players:
                player_games = self.game_basic[
                    (self.game_basic['player'] == player) & 
                    (self.game_basic['period'] == 'game')
                ]
                if len(player_games) > 0:
                    stat_data.append(player_games[stat].mean())
                else:
                    stat_data.append(0)
            
            axes[i].bar(players, stat_data)
            axes[i].set_title(f'Average {stat.upper()}')
            axes[i].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.show()
    
    def get_team_offense_defense(self):
        """Get team offensive and defensive ratings"""
        team_ratings = self.game_advanced.groupby('team').agg({
            'off_rtg': 'mean',
            'def_rtg': 'mean'
        }).round(1)
        
        team_ratings['net_rtg'] = team_ratings['off_rtg'] - team_ratings['def_rtg']
        return team_ratings.sort_values('net_rtg', ascending=False)
    
    def plot_team_ratings(self):
        """Plot team offensive vs defensive ratings"""
        team_ratings = self.get_team_offense_defense()
        
        plt.figure(figsize=(12, 8))
        plt.scatter(team_ratings['off_rtg'], team_ratings['def_rtg'], s=100, alpha=0.7)
        
        # Add team labels
        for team in team_ratings.index:
            plt.annotate(team, 
                        (team_ratings.loc[team, 'off_rtg'], 
                         team_ratings.loc[team, 'def_rtg']),
                        xytext=(5, 5), textcoords='offset points')
        
        plt.axhline(y=team_ratings['def_rtg'].mean(), color='red', linestyle='--', alpha=0.7)
        plt.axvline(x=team_ratings['off_rtg'].mean(), color='red', linestyle='--', alpha=0.7)
        plt.xlabel('Offensive Rating')
        plt.ylabel('Defensive Rating')
        plt.title('Team Offensive vs Defensive Ratings')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

# Example usage:
# analyzer = NBAAnalyzer(DATABASE_URL)
# analyzer.plot_player_comparison(['Stephen Curry', 'Jimmy Butler', 'Jonathan Kuminga'])