# nba_data_loader.py (Complete Fixed Version)
import pandas as pd
import os
import glob
from datetime import datetime
import re
from sqlalchemy.orm import Session
from nba_database import SessionLocal, NBAGameBasic, NBAGameAdvanced

class NBADataLoader:
    def __init__(self, data_path="./nba_data"):
        self.data_path = data_path
        self.session = SessionLocal()
    
    def parse_filename(self, filename):
        """Extract metadata from filename - fixed version"""
        # Remove the .csv extension first
        base_name = filename.replace('.csv', '')
        
        # Split by underscores to get the main parts
        parts = base_name.split('_')
        
        if len(parts) < 3:
            return None
        
        # First part: YYYYMMDDTEAM (e.g., 202511200MEM)
        game_info = parts[0]
        
        # Extract date (first 8 characters) and team (rest)
        if len(game_info) < 11:
            return None
            
        game_date_str = game_info[:8]
        away_team = game_info[8:] 
        
        # Remove the leading '0' from team codes if present
        if away_team.startswith('0'):
            away_team = away_team[1:]
        
        # Second part: box-HOMETEAM-period-stattype (e.g., box-SAC-game-advanced)
        box_info = parts[1]
        box_parts = box_info.split('-')
        
        if len(box_parts) < 4:
            return None
            
        # Extract home team, period, and stat type
        home_team = box_parts[1]
        period = box_parts[2]
        stat_type = box_parts[3]
        
        # Third part: scrape date (e.g., 2025-11-28)
        scrape_date_str = parts[2]
        
        try:
            game_date = datetime.strptime(game_date_str, '%Y%m%d').date()
            scrape_date = datetime.strptime(scrape_date_str, '%Y-%m-%d').date()
            
            return {
                'game_date': game_date,
                'home_team': home_team,
                'away_team': away_team,
                'period': period,
                'stat_type': stat_type,
                'scrape_date': scrape_date,
                'filename': filename
            }
            
        except ValueError:
            return None

    def clean_dataframe(self, df, stat_type):
        """Clean the multi-level header dataframe"""
        try:
            # For multi-level headers, use the second row as column names
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten the multi-level columns
                new_columns = []
                for col in df.columns:
                    if 'Unnamed' in str(col[0]):
                        new_columns.append(col[1])
                    else:
                        new_columns.append(f"{col[0]}_{col[1]}")
                df.columns = new_columns
            
            # Clean column names
            df.columns = [col.strip() for col in df.columns]
            
            # Rename the first column to 'player' if it exists
            if len(df.columns) > 0:
                first_col = df.columns[0]
                if first_col in ['Starters', 'Reserves'] or 'Unnamed' in first_col:
                    df = df.rename(columns={first_col: 'player'})
            
            return df
            
        except Exception as e:
            print(f"Error cleaning dataframe: {e}")
            return df

    def safe_convert(self, value, convert_type=float, default=None):
        """Safely convert values"""
        if pd.isna(value) or value == '' or value is None:
            return default
        
        if isinstance(value, str) and 'Did Not Play' in value:
            return default
            
        # Handle empty percentage cells
        if isinstance(value, str) and value == ',':
            return default
            
        try:
            if convert_type == int:
                # Handle float strings for int conversion
                if isinstance(value, str) and '.' in value:
                    return int(float(value))
                return int(value)
            elif convert_type == float:
                return float(value)
            else:
                return convert_type(value)
        except (ValueError, TypeError):
            return default

    def safe_convert_mp(self, mp_value):
        """Safely convert minutes played field - handle 'Did Not Play' and time formats"""
        if pd.isna(mp_value) or mp_value == '' or mp_value is None:
            return None
        
        if isinstance(mp_value, str) and 'Did Not Play' in mp_value:
            return 'DNP'
        
        # Handle time format (e.g., "8:50")
        if isinstance(mp_value, str) and ':' in mp_value:
            try:
                minutes, seconds = mp_value.split(':')
                return f"{int(minutes):02d}:{int(seconds):02d}"
            except (ValueError, IndexError):
                return mp_value[:10]  # Truncate to 10 characters if invalid format
        
        # Truncate any other string to 10 characters
        if isinstance(mp_value, str):
            return mp_value[:10]
        
        return str(mp_value)[:10]

    def map_column_names(self, columns, stat_type):
        """Map CSV column names to database column names"""
        column_mapping = {}
        
        if stat_type == 'basic':
            mapping = {
                'Basic Box Score Stats_MP': 'mp',
                'Basic Box Score Stats_FG': 'fg',
                'Basic Box Score Stats_FGA': 'fga', 
                'Basic Box Score Stats_FG%': 'fg_pct',
                'Basic Box Score Stats_3P': 'fg3',
                'Basic Box Score Stats_3PA': 'fg3a',
                'Basic Box Score Stats_3P%': 'fg3_pct',
                'Basic Box Score Stats_FT': 'ft',
                'Basic Box Score Stats_FTA': 'fta',
                'Basic Box Score Stats_FT%': 'ft_pct',
                'Basic Box Score Stats_ORB': 'orb',
                'Basic Box Score Stats_DRB': 'drb',
                'Basic Box Score Stats_TRB': 'trb',
                'Basic Box Score Stats_AST': 'ast',
                'Basic Box Score Stats_STL': 'stl',
                'Basic Box Score Stats_BLK': 'blk',
                'Basic Box Score Stats_TOV': 'tov',
                'Basic Box Score Stats_PF': 'pf',
                'Basic Box Score Stats_PTS': 'pts',
                'Basic Box Score Stats_GmSc': 'gm_sc',
                'Basic Box Score Stats_+/-': 'plus_minus'
            }
        else:  # advanced
            mapping = {
                'Advanced Box Score Stats_MP': 'mp',
                'Advanced Box Score Stats_TS%': 'ts_pct',
                'Advanced Box Score Stats_eFG%': 'efg_pct',
                'Advanced Box Score Stats_3PAr': 'fg3a_per_fga_pct',
                'Advanced Box Score Stats_FTr': 'fta_per_fga_pct',
                'Advanced Box Score Stats_ORB%': 'orb_pct',
                'Advanced Box Score Stats_DRB%': 'drb_pct',
                'Advanced Box Score Stats_TRB%': 'trb_pct',
                'Advanced Box Score Stats_AST%': 'ast_pct',
                'Advanced Box Score Stats_STL%': 'stl_pct',
                'Advanced Box Score Stats_BLK%': 'blk_pct',
                'Advanced Box Score Stats_TOV%': 'tov_pct',
                'Advanced Box Score Stats_USG%': 'usg_pct',
                'Advanced Box Score Stats_ORtg': 'off_rtg',
                'Advanced Box Score Stats_DRtg': 'def_rtg',
                'Advanced Box Score Stats_BPM': 'bpm'
            }
        
        # Create mapping for columns that exist in the dataframe
        for csv_col, db_col in mapping.items():
            if csv_col in columns:
                column_mapping[csv_col] = db_col
                
        return column_mapping

    def process_basic_data(self, file_path, file_info):
        """Process basic box score data and insert into database"""
        try:
            # Read the file
            df = pd.read_csv(file_path, header=[0, 1])
            df = self.clean_dataframe(df, 'basic')
            
            # Map column names
            column_mapping = self.map_column_names(df.columns, 'basic')
            df = df.rename(columns=column_mapping)
            
            # Determine which team this data belongs to
            team_code = file_info['away_team']
            opponent_code = file_info['home_team']
            
            is_starter_section = True
            records_processed = 0
            
            for _, row in df.iterrows():
                player_name = row.get('player', '')
                
                # Skip empty rows and section headers
                if (pd.isna(player_name) or player_name == '' or 
                    player_name in ['Team Totals', 'Starters', 'Reserves']):
                    if player_name == 'Reserves':
                        is_starter_section = False
                    continue
                
                # Skip "Did Not Play" players
                if 'Did Not Play' in str(player_name):
                    continue
                
                # Skip empty player rows
                if not player_name or player_name.strip() == '':
                    continue
                
                # Determine home/away
                home_away = 'AWAY' if team_code != file_info['home_team'] else 'HOME'
                
                # Safely handle minutes played field
                mp_value = row.get('mp', '')
                safe_mp = self.safe_convert_mp(mp_value)
                
                # Create basic stats record
                basic_data = NBAGameBasic(
                    player=player_name.strip(),
                    pos=row.get('pos', ''),
                    mp=safe_mp,  # Use the safely converted MP value
                    fg=self.safe_convert(row.get('fg'), int, 0),
                    fga=self.safe_convert(row.get('fga'), int, 0),
                    fg_pct=self.safe_convert(row.get('fg_pct'), float, 0.0),
                    fg3=self.safe_convert(row.get('fg3'), int, 0),
                    fg3a=self.safe_convert(row.get('fg3a'), int, 0),
                    fg3_pct=self.safe_convert(row.get('fg3_pct'), float, 0.0),
                    ft=self.safe_convert(row.get('ft'), int, 0),
                    fta=self.safe_convert(row.get('fta'), int, 0),
                    ft_pct=self.safe_convert(row.get('ft_pct'), float, 0.0),
                    orb=self.safe_convert(row.get('orb'), int, 0),
                    drb=self.safe_convert(row.get('drb'), int, 0),
                    trb=self.safe_convert(row.get('trb'), int, 0),
                    ast=self.safe_convert(row.get('ast'), int, 0),
                    stl=self.safe_convert(row.get('stl'), int, 0),
                    blk=self.safe_convert(row.get('blk'), int, 0),
                    tov=self.safe_convert(row.get('tov'), int, 0),
                    pf=self.safe_convert(row.get('pf'), int, 0),
                    pts=self.safe_convert(row.get('pts'), int, 0),
                    gm_sc=self.safe_convert(row.get('gm_sc'), float, 0.0),
                    plus_minus=self.safe_convert(row.get('plus_minus'), int, 0),
                    
                    # Metadata
                    game_date=file_info['game_date'],
                    team=team_code,
                    opponent=opponent_code,
                    home_away=home_away,
                    period=file_info['period'],
                    is_starter=is_starter_section,
                    source_file=file_info['filename']
                )
                
                self.session.add(basic_data)
                records_processed += 1
            
            self.session.commit()
            return records_processed
            
        except Exception as e:
            self.session.rollback()
            print(f"  ❌ Error processing basic data: {e}")
            return 0

    def process_advanced_data(self, file_path, file_info):
        """Process advanced box score data and insert into database"""
        try:
            # Read the file
            df = pd.read_csv(file_path, header=[0, 1])
            df = self.clean_dataframe(df, 'advanced')
            
            # Map column names
            column_mapping = self.map_column_names(df.columns, 'advanced')
            df = df.rename(columns=column_mapping)
            
            # Determine which team this data belongs to
            team_code = file_info['away_team']
            opponent_code = file_info['home_team']
            
            is_starter_section = True
            records_processed = 0
            
            for _, row in df.iterrows():
                player_name = row.get('player', '')
                
                # Skip empty rows and section headers
                if (pd.isna(player_name) or player_name == '' or 
                    player_name in ['Team Totals', 'Starters', 'Reserves']):
                    if player_name == 'Reserves':
                        is_starter_section = False
                    continue
                
                # Skip "Did Not Play" players
                if 'Did Not Play' in str(player_name):
                    continue
                
                # Skip empty player rows
                if not player_name or player_name.strip() == '':
                    continue
                
                # Determine home/away
                home_away = 'AWAY' if team_code != file_info['home_team'] else 'HOME'
                
                # Safely handle minutes played field
                mp_value = row.get('mp', '')
                safe_mp = self.safe_convert_mp(mp_value)
                
                # Create advanced stats record
                advanced_data = NBAGameAdvanced(
                    player=player_name.strip(),
                    pos=row.get('pos', ''),
                    mp=safe_mp,  # Use the safely converted MP value
                    ts_pct=self.safe_convert(row.get('ts_pct'), float, 0.0),
                    efg_pct=self.safe_convert(row.get('efg_pct'), float, 0.0),
                    fg3a_per_fga_pct=self.safe_convert(row.get('fg3a_per_fga_pct'), float, 0.0),
                    fta_per_fga_pct=self.safe_convert(row.get('fta_per_fga_pct'), float, 0.0),
                    orb_pct=self.safe_convert(row.get('orb_pct'), float, 0.0),
                    drb_pct=self.safe_convert(row.get('drb_pct'), float, 0.0),
                    trb_pct=self.safe_convert(row.get('trb_pct'), float, 0.0),
                    ast_pct=self.safe_convert(row.get('ast_pct'), float, 0.0),
                    stl_pct=self.safe_convert(row.get('stl_pct'), float, 0.0),
                    blk_pct=self.safe_convert(row.get('blk_pct'), float, 0.0),
                    tov_pct=self.safe_convert(row.get('tov_pct'), float, 0.0),
                    usg_pct=self.safe_convert(row.get('usg_pct'), float, 0.0),
                    off_rtg=self.safe_convert(row.get('off_rtg'), float, 0.0),
                    def_rtg=self.safe_convert(row.get('def_rtg'), float, 0.0),
                    bpm=self.safe_convert(row.get('bpm'), float, 0.0),
                    
                    # Metadata
                    game_date=file_info['game_date'],
                    team=team_code,
                    opponent=opponent_code,
                    home_away=home_away,
                    is_starter=is_starter_section,
                    source_file=file_info['filename']
                )
                
                self.session.add(advanced_data)
                records_processed += 1
            
            self.session.commit()
            return records_processed
            
        except Exception as e:
            self.session.rollback()
            print(f"  ❌ Error processing advanced data: {e}")
            return 0

    def load_all_data(self):
        """Load all CSV files into the database"""
        csv_files = glob.glob(os.path.join(self.data_path, "*.csv"))
        print(f"Found {len(csv_files)} CSV files to process...")
        
        successful = 0
        failed = 0
        total_records = 0
        
        for i, file_path in enumerate(csv_files):
            filename = os.path.basename(file_path)
            file_info = self.parse_filename(filename)
            
            if not file_info:
                print(f"✗ Could not parse filename: {filename}")
                failed += 1
                continue
            
            if i % 100 == 0:  # Progress update every 100 files
                print(f"Processed {i}/{len(csv_files)} files...")
            
            try:
                if file_info['stat_type'] == 'basic':
                    records = self.process_basic_data(file_path, file_info)
                    if records > 0:
                        successful += 1
                        total_records += records
                        print(f"✓ Loaded {file_info['period']} basic: {filename} ({records} records)")
                    else:
                        failed += 1
                elif file_info['stat_type'] == 'advanced':
                    records = self.process_advanced_data(file_path, file_info)
                    if records > 0:
                        successful += 1
                        total_records += records
                        print(f"✓ Loaded advanced: {filename} ({records} records)")
                    else:
                        failed += 1
                else:
                    print(f"⚠ Unknown stat type: {file_info['stat_type']} in {filename}")
                    failed += 1
                    
            except Exception as e:
                print(f"✗ Processing failed for {filename}: {e}")
                failed += 1
        
        print(f"\nData loading completed!")
        print(f"✅ Successful files: {successful}")
        print(f"❌ Failed files: {failed}")
        print(f"📊 Total records loaded: {total_records}")
        print(f"🎯 Success rate: {successful/(successful+failed)*100:.1f}%" if (successful+failed) > 0 else "N/A")
    
    def close(self):
        """Close database session"""
        self.session.close()