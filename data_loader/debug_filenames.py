# debug_filenames.py
from nba_data_loader import NBADataLoader

def test_filename_parsing():
    """Test the filename parser with various patterns"""
    loader = NBADataLoader()
    
    test_filenames = [
        "202511100MIA_box-CLE-ot1-basic_2025-11-28.csv",  # Overtime
        "202511200MIL_box-MIL-game-basic_2025-11-28.csv",  # Same team
        "202511170TOR_box-CHO-game-basic_2025-11-28.csv",  # CHO instead of CHA
        "202510210LAL_box-GSW-h1-basic_2025-11-28.csv",    # Standard
        "202511080LAC_box-PHO-q4-basic_2025-11-28.csv",    # Standard
    ]
    
    print("Testing filename parsing...")
    for filename in test_filenames:
        result = loader.parse_filename(filename)
        if result:
            print(f"✅ {filename}")
            print(f"   Date: {result['game_date']}")
            print(f"   Home: {result['home_team']}, Away: {result['away_team']}")
            print(f"   Period: {result['period']}, Type: {result['stat_type']}")
        else:
            print(f"❌ {filename} - Failed to parse")
        
        print()

if __name__ == "__main__":
    test_filename_parsing()