from . import models
from pybaseball import (
    batting_stats,
    pitching_stats,
    playerid_lookup,
    statcast_pitcher,
    statcast_batter,
    fielding_stats
)
from pybaseball.datahelpers import postprocessing
from pybaseball.datasources.fangraphs import fg_batting_data, fg_pitching_data
from sqlalchemy.orm import Session
from datetime import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_collection.log'
)

class MLBDataCollector:
    def __init__(self, db: Session):
        self.db = db
        self.stats_collected = {
            'batting': 0,
            'pitching': 0,
            'fielding': 0
        }

    def fetch_batting_fangraphs(self, start_year: int, end_year: int):
        """Fetch all available batting metrics from Fangraphs without any thresholds"""
        try:
            print(f"Fetching batting stats for {start_year}...")
            
            # Remove qualification threshold completely
            stats_df = batting_stats(start_year, end_year, qual=0)  # No minimum PA requirement
            if stats_df is None:
                raise Exception("Failed to fetch base batting stats")
            
            try:
                # Standard Stats
                standard = fg_batting_data(start_year, end_year,
                    stat_columns='c,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20',
                    qual=0)  # No minimum PA
                if standard is not None:
                    stats_df = pd.merge(stats_df, standard,
                        on=['Name', 'Season'], how='outer')  # Changed to outer join
                time.sleep(1)
                
                # Advanced Stats
                advanced = fg_batting_data(start_year, end_year, 
                    stat_columns='c,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60',
                    qual=0)  # No minimum PA
                if advanced is not None:
                    stats_df = pd.merge(stats_df, advanced, 
                        on=['Name', 'Season'], how='outer')
                
                return stats_df
                
            except Exception as fetch_error:
                logging.warning(f"Error fetching additional stats for {start_year}: {str(fetch_error)}")
                return stats_df
                
        except Exception as e:
            logging.error(f"Error fetching batting data for {start_year}: {str(e)}")
            return None

    def fetch_pitching_fangraphs(self, start_year: int, end_year: int):
        """Fetch all available pitching metrics without any thresholds"""
        try:
            print("Fetching comprehensive pitching stats...")
            
            # Remove IP qualification
            stats_df = pitching_stats(start_year, end_year, qual=0)  # No minimum IP
            if stats_df is None:
                raise Exception("Failed to fetch base pitching stats")
            
            try:
                # Standard Stats
                standard = fg_pitching_data(start_year, end_year,
                    stat_columns='c,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25',
                    qual=0)  # No minimum IP
                if standard is not None:
                    stats_df = pd.merge(stats_df, standard,
                        on=['Name', 'Season'], how='outer')
                
                # Advanced Stats
                advanced = fg_pitching_data(start_year, end_year,
                    stat_columns='c,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65',
                    qual=0)
                if advanced is not None:
                    stats_df = pd.merge(stats_df, advanced,
                        on=['Name', 'Season'], how='outer')
                
                # Add all other stat categories with qual=0
                # ... (rest of the stat categories)
                
                return stats_df
                
            except Exception as fetch_error:
                logging.warning(f"Error fetching additional stats: {str(fetch_error)}")
                return stats_df
                
        except Exception as e:
            logging.error(f"Error fetching pitching data: {str(e)}")
            return None

    def fetch_fielding_data(self, start_year: int, end_year: int):
        """Fetch all available fielding metrics"""
        try:
            fielding_df = fielding_stats(start_year, end_year)
            if fielding_df is not None:
                if 'Season' not in fielding_df.columns and 'year' in fielding_df.columns:
                    fielding_df = fielding_df.rename(columns={'year': 'Season'})
                if 'Name' not in fielding_df.columns and 'player_name' in fielding_df.columns:
                    fielding_df = fielding_df.rename(columns={'player_name': 'Name'})
            return fielding_df
        except Exception as e:
            logging.error(f"Error fetching fielding data: {str(e)}")
            return None

    def _get_or_create_player(self, stats_row):
        """Helper method to get or create a player record"""
        player = self.db.query(models.Player).filter(
            models.Player.name == stats_row['Name']
        ).first()
        
        if not player:
            try:
                name_parts = stats_row['Name'].split()
                player_lookup = playerid_lookup(
                    name_parts[-1],  # Last name
                    name_parts[0]    # First name
                )
                birth_date = player_lookup.iloc[0]['birth_date'] if not player_lookup.empty else None
            except:
                birth_date = None

            player = models.Player(
                name=stats_row['Name'],
                team=stats_row.get('Team', 'Unknown'),
                position=stats_row.get('Pos', 'Unknown'),
                birth_date=birth_date
            )
            self.db.add(player)
            self.db.commit()
            self.db.refresh(player)
        
        return player

    def create_batting_record(self, player_id: int, row: pd.Series) -> models.BattingStats:
        """Create a BattingStats record from a row of data"""
        return models.BattingStats(
            player_id=player_id,
            year=int(row.get('Season', 0)),  # Year must have a value
            
            # Standard Stats
            games=int(row['G']) if 'G' in row and pd.notna(row['G']) else None,
            pa=int(row['PA']) if 'PA' in row and pd.notna(row['PA']) else None,
            ab=int(row['AB']) if 'AB' in row and pd.notna(row['AB']) else None,
            runs=int(row['R']) if 'R' in row and pd.notna(row['R']) else None,
            hits=int(row['H']) if 'H' in row and pd.notna(row['H']) else None,
            doubles=int(row['2B']) if '2B' in row and pd.notna(row['2B']) else None,
            triples=int(row['3B']) if '3B' in row and pd.notna(row['3B']) else None,
            hr=int(row['HR']) if 'HR' in row and pd.notna(row['HR']) else None,
            rbi=int(row['RBI']) if 'RBI' in row and pd.notna(row['RBI']) else None,
            sb=int(row['SB']) if 'SB' in row and pd.notna(row['SB']) else None,
            cs=int(row['CS']) if 'CS' in row and pd.notna(row['CS']) else None,
            bb=int(row['BB']) if 'BB' in row and pd.notna(row['BB']) else None,
            ibb=int(row['IBB']) if 'IBB' in row and pd.notna(row['IBB']) else None,
            so=int(row['SO']) if 'SO' in row and pd.notna(row['SO']) else None,
            hbp=int(row['HBP']) if 'HBP' in row and pd.notna(row['HBP']) else None,
            sf=int(row['SF']) if 'SF' in row and pd.notna(row['SF']) else None,
            sh=int(row['SH']) if 'SH' in row and pd.notna(row['SH']) else None,
            gdp=int(row['GDP']) if 'GDP' in row and pd.notna(row['GDP']) else None,
            
            # Rate Stats
            avg=float(row['AVG']) if 'AVG' in row and pd.notna(row['AVG']) else None,
            obp=float(row['OBP']) if 'OBP' in row and pd.notna(row['OBP']) else None,
            slg=float(row['SLG']) if 'SLG' in row and pd.notna(row['SLG']) else None,
            ops=float(row['OPS']) if 'OPS' in row and pd.notna(row['OPS']) else None,
            iso=float(row['ISO']) if 'ISO' in row and pd.notna(row['ISO']) else None,
            babip=float(row['BABIP']) if 'BABIP' in row and pd.notna(row['BABIP']) else None,
            
            # Advanced Stats
            woba=float(row['wOBA']) if 'wOBA' in row and pd.notna(row['wOBA']) else None,
            wrc_plus=float(row['wRC+']) if 'wRC+' in row and pd.notna(row['wRC+']) else None,
            war=float(row['WAR']) if 'WAR' in row and pd.notna(row['WAR']) else None,
            
            # Plate Discipline
            o_swing_pct=float(row.get('O-Swing%', 0)),
            z_swing_pct=float(row.get('Z-Swing%', 0)),
            swing_pct=float(row.get('Swing%', 0)),
            o_contact_pct=float(row.get('O-Contact%', 0)),
            z_contact_pct=float(row.get('Z-Contact%', 0)),
            contact_pct=float(row.get('Contact%', 0)),
            zone_pct=float(row.get('Zone%', 0)),
            f_strike_pct=float(row.get('F-Strike%', 0)),
            swstr_pct=float(row.get('SwStr%', 0)),
            cstr_pct=float(row.get('CStr%', 0)),
            csw_pct=float(row.get('CSW%', 0)),
            
            # Batted Ball
            gb_pct=float(row.get('GB%', 0)),
            fb_pct=float(row.get('FB%', 0)),
            ld_pct=float(row.get('LD%', 0)),
            iffb_pct=float(row.get('IFFB%', 0)),
            hr_fb=float(row.get('HR/FB', 0)),
            pull_pct=float(row.get('Pull%', 0)),
            cent_pct=float(row.get('Cent%', 0)),
            oppo_pct=float(row.get('Oppo%', 0)),
            soft_pct=float(row.get('Soft%', 0)),
            med_pct=float(row.get('Med%', 0)),
            hard_pct=float(row.get('Hard%', 0)),
            
            # Value Stats
            batting_runs=float(row.get('Batting', 0)),
            baserunning_runs=float(row.get('BsR', 0)),
            fielding_runs=float(row.get('Fielding', 0)),
            positional=float(row.get('Positional', 0)),
            offense=float(row.get('Off', 0)),
            defense=float(row.get('Def', 0)),
            league=float(row.get('League', 0)),
            replacement=float(row.get('Replacement', 0)),
            rar=float(row.get('RAR', 0)),
            dollars=float(row.get('Dollars', 0)),
            
            # Win Probability
            wpa=float(row.get('WPA', 0)),
            neg_wpa=float(row.get('-WPA', 0)),
            pos_wpa=float(row.get('+WPA', 0)),
            re24=float(row.get('RE24', 0)),
            rew=float(row.get('REW', 0)),
            pli=float(row.get('pLI', 0)),
            phli=float(row.get('phLI', 0)),
            clutch=float(row.get('Clutch', 0)),
            
            # Pitch Values
            wfb=float(row.get('wFB', 0)),
            wsl=float(row.get('wSL', 0)),
            wct=float(row.get('wCT', 0)),
            wcb=float(row.get('wCB', 0)),
            wch=float(row.get('wCH', 0)),
        )

    def create_pitching_record(self, player_id: int, row: pd.Series) -> models.PitchingStats:
        """Create a PitchingStats record from a row of data"""
        return models.PitchingStats(
            player_id=player_id,
            year=int(row.get('Season', 0)),
            
            # Traditional Stats
            games=int(row['G']) if 'G' in row and pd.notna(row['G']) else None,
            games_started=int(row['GS']) if 'GS' in row and pd.notna(row['GS']) else None,
            wins=int(row['W']) if 'W' in row and pd.notna(row['W']) else None,
            losses=int(row['L']) if 'L' in row and pd.notna(row['L']) else None,
            saves=int(row['SV']) if 'SV' in row and pd.notna(row['SV']) else None,
            holds=int(row['HLD']) if 'HLD' in row and pd.notna(row['HLD']) else None,
            innings=float(row['IP']) if 'IP' in row and pd.notna(row['IP']) else None,
            hits_allowed=int(row['H']) if 'H' in row and pd.notna(row['H']) else None,
            runs=int(row['R']) if 'R' in row and pd.notna(row['R']) else None,
            earned_runs=int(row['ER']) if 'ER' in row and pd.notna(row['ER']) else None,
            hr_allowed=int(row['HR']) if 'HR' in row and pd.notna(row['HR']) else None,
            bb=int(row['BB']) if 'BB' in row and pd.notna(row['BB']) else None,
            so=int(row['SO']) if 'SO' in row and pd.notna(row['SO']) else None,
            
            # Rate Stats
            era=float(row['ERA']) if 'ERA' in row and pd.notna(row['ERA']) else None,
            whip=float(row['WHIP']) if 'WHIP' in row and pd.notna(row['WHIP']) else None,
            k_9=float(row['K/9']) if 'K/9' in row and pd.notna(row['K/9']) else None,
            bb_9=float(row['BB/9']) if 'BB/9' in row and pd.notna(row['BB/9']) else None,
            hr_9=float(row['HR/9']) if 'HR/9' in row and pd.notna(row['HR/9']) else None,
            k_bb=float(row.get('K/BB', 0)),
            
            # Advanced Stats
            fip=float(row['FIP']) if 'FIP' in row and pd.notna(row['FIP']) else None,
            xfip=float(row['xFIP']) if 'xFIP' in row and pd.notna(row['xFIP']) else None,
            siera=float(row.get('SIERA', 0)),
            war=float(row['WAR']) if 'WAR' in row and pd.notna(row['WAR']) else None,
            babip=float(row.get('BABIP', 0)),
            lob_pct=float(row.get('LOB%', 0)),
            k_pct=float(row.get('K%', 0)),
            bb_pct=float(row.get('BB%', 0)),
            hr_fb=float(row.get('HR/FB', 0)),
            gb_pct=float(row.get('GB%', 0)),
            fb_pct=float(row.get('FB%', 0)),
            ld_pct=float(row.get('LD%', 0)),
            
            # Win Probability
            wpa=float(row.get('WPA', 0)),
            neg_wpa=float(row.get('-WPA', 0)),
            pos_wpa=float(row.get('+WPA', 0)),
            re24=float(row.get('RE24', 0)),
            rew=float(row.get('REW', 0)),
            pli=float(row.get('pLI', 0)),
            inli=float(row.get('inLI', 0)),
            clutch=float(row.get('Clutch', 0)),
            
            # Pitch Type Stats
            fa_pct=float(row.get('FA%', 0)),
            fc_pct=float(row.get('FC%', 0)),
            fs_pct=float(row.get('FS%', 0)),
            si_pct=float(row.get('SI%', 0)),
            sl_pct=float(row.get('SL%', 0)),
            cu_pct=float(row.get('CU%', 0)),
            kc_pct=float(row.get('KC%', 0)),
            ch_pct=float(row.get('CH%', 0)),
            
            # Pitch Values
            wfb=float(row.get('wFB', 0)),
            wsl=float(row.get('wSL', 0)),
            wct=float(row.get('wCT', 0)),
            wcb=float(row.get('wCB', 0)),
            wch=float(row.get('wCH', 0)),
        )

    def test_run(self):
        """Full historical data collection"""
        try:
            print("Starting full historical data collection...")
            
            # Start with a single recent year as a test
            test_year = 2024
            print(f"\nTesting with year {test_year}...")
            
            try:
                # Fetch and store batting stats
                print(f"Fetching batting stats for {test_year}...")
                batting_df = self.fetch_batting_fangraphs(test_year, test_year)
                if batting_df is not None:
                    print(f"Found {len(batting_df)} batting records")
                    self._store_stats_batch(batting_df, 'batting')
                    print(f"Successfully stored batting stats for {test_year}")
                
                time.sleep(2)  # Delay between stat types
                
                # Fetch and store pitching stats
                print(f"Fetching pitching stats for {test_year}...")
                pitching_df = self.fetch_pitching_fangraphs(test_year, test_year)
                if pitching_df is not None:
                    print(f"Found {len(pitching_df)} pitching records")
                    self._store_stats_batch(pitching_df, 'pitching')
                    print(f"Successfully stored pitching stats for {test_year}")
                
                # Print test year summary
                print(f"\nTest Year {test_year} Summary:")
                test_stats = {
                    'batting': len(batting_df) if batting_df is not None else 0,
                    'pitching': len(pitching_df) if pitching_df is not None else 0
                }
                for stat_type, count in test_stats.items():
                    print(f"{stat_type.capitalize()} records: {count}")
                
                # If test year was successful, ask to continue
                print("\nTest year completed successfully.")
                print("Check the numbers above to confirm they look correct.")
                print("Would you like to proceed with full historical collection? (y/n)")
                
                return True, "Test run completed successfully"
                
            except Exception as e:
                self.db.rollback()
                error_msg = f"Error during test run: {str(e)}"
                logging.error(error_msg)
                return False, error_msg
            
        except Exception as e:
            self.db.rollback()
            error_msg = f"Error during test run: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    def _store_stats_batch(self, df: pd.DataFrame, stat_type: str):
        """Helper method to store stats in batches"""
        if df is None or df.empty:
            return
        
        total_rows = len(df)
        print(f"\nProcessing {total_rows} {stat_type} records...")
        
        # Process in smaller batches
        batch_size = 50  # Reduced batch size
        processed = 0
        
        with tqdm(total=total_rows, desc=f"Storing {stat_type} data") as pbar:
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                try:
                    batch_records = []
                    for _, row in batch_df.iterrows():
                        try:
                            # Get or create player
                            player = self._get_or_create_player(row)
                            
                            # Create stat record
                            if stat_type == 'batting':
                                stat_record = self.create_batting_record(player.id, row)
                            elif stat_type == 'pitching':
                                stat_record = self.create_pitching_record(player.id, row)
                            elif stat_type == 'fielding':
                                continue  # Skip fielding for now
                            
                            batch_records.append(stat_record)
                            processed += 1
                            pbar.update(1)  # Update progress for each record
                            
                        except Exception as e:
                            logging.error(f"Error processing {stat_type} record: {str(e)}")
                            continue
                    
                    # Bulk add records for the batch
                    if batch_records:
                        self.db.bulk_save_objects(batch_records)
                        self.stats_collected[stat_type] += len(batch_records)
                        
                        # Commit after each batch
                        try:
                            self.db.commit()
                            logging.info(f"Committed batch {start_idx}-{end_idx} ({len(batch_records)} records)")
                        except Exception as e:
                            self.db.rollback()
                            logging.error(f"Error committing {stat_type} batch: {str(e)}")
                        
                except Exception as batch_error:
                    logging.error(f"Error processing batch {start_idx}-{end_idx}: {str(batch_error)}")
                    self.db.rollback()
                    continue
                
                time.sleep(0.1)  # Small delay between batches
        
        # Final progress check
        if processed != total_rows:
            logging.warning(f"Processed {processed}/{total_rows} {stat_type} records")

    def collect_historical_data(self, start_year=1876, end_year=2024, chunk_size=5):
        """Collect all MLB stats from start_year to end_year"""
        try:
            print(f"\nStarting historical collection from {start_year} to {end_year}")
            current_year = end_year
            
            while current_year >= start_year:
                chunk_end = current_year
                chunk_start = max(start_year, current_year - chunk_size + 1)
                
                print(f"\nCollecting years {chunk_start}-{chunk_end}...")
                
                # Collect batting stats
                print(f"Fetching batting stats...")
                batting_df = self.fetch_batting_fangraphs(chunk_start, chunk_end)
                if batting_df is not None:
                    print(f"Found {len(batting_df)} batting records")
                    self._store_stats_batch(batting_df, 'batting')
                    print(f"Successfully stored batting stats")
                
                time.sleep(2)  # Delay between stat types
                
                # Collect pitching stats
                print(f"Fetching pitching stats...")
                pitching_df = self.fetch_pitching_fangraphs(chunk_start, chunk_end)
                if pitching_df is not None:
                    print(f"Found {len(pitching_df)} pitching records")
                    self._store_stats_batch(pitching_df, 'pitching')
                    print(f"Successfully stored pitching stats")
                
                # Update progress
                print(f"\nCompleted {chunk_start}-{chunk_end}")
                print(f"Total batting records: {self.stats_collected['batting']}")
                print(f"Total pitching records: {self.stats_collected['pitching']}")
                
                current_year -= chunk_size
                time.sleep(5)  # Delay between chunks to avoid rate limiting
                
            return True, "Historical collection completed successfully"
            
        except Exception as e:
            error_msg = f"Error during historical collection: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    def analyze_data_completeness(self, start_year=1876, end_year=2024, era_size=20):
        """
        Analyze data completeness across different baseball eras
        """
        try:
            print(f"\nAnalyzing data completeness from {start_year} to {end_year}...")
            
            # Query data in eras
            current_year = end_year
            while current_year >= start_year:
                era_end = current_year
                era_start = max(start_year, current_year - era_size + 1)
                
                print(f"\n{'='*20} {era_start}-{era_end} {'='*20}")
                
                # Batting Stats Analysis
                batting_query = f"""
                    WITH stats AS (
                        SELECT 
                            year,
                            COUNT(*) as total_players,
                            COUNT(CASE WHEN pa IS NOT NULL THEN 1 END) as has_pa,
                            COUNT(CASE WHEN war IS NOT NULL THEN 1 END) as has_war,
                            COUNT(CASE WHEN woba IS NOT NULL THEN 1 END) as has_woba,
                            COUNT(CASE WHEN wrc_plus IS NOT NULL THEN 1 END) as has_wrc,
                            COUNT(CASE WHEN barrel_pct IS NOT NULL THEN 1 END) as has_barrel,
                            COUNT(CASE WHEN hard_pct IS NOT NULL THEN 1 END) as has_hard_hit,
                            AVG(CASE WHEN pa IS NOT NULL THEN pa END) as avg_pa
                        FROM batting_stats
                        WHERE year BETWEEN {era_start} AND {era_end}
                        GROUP BY year
                        ORDER BY year DESC
                    )
                    SELECT 
                        MIN(year) as start_year,
                        MAX(year) as end_year,
                        ROUND(AVG(total_players)) as avg_players_per_year,
                        ROUND(AVG(has_pa)::numeric / AVG(total_players) * 100, 1) as pct_with_pa,
                        ROUND(AVG(has_war)::numeric / AVG(total_players) * 100, 1) as pct_with_war,
                        ROUND(AVG(has_woba)::numeric / AVG(total_players) * 100, 1) as pct_with_woba,
                        ROUND(AVG(has_wrc)::numeric / AVG(total_players) * 100, 1) as pct_with_wrc,
                        ROUND(AVG(has_barrel)::numeric / AVG(total_players) * 100, 1) as pct_with_barrel,
                        ROUND(AVG(has_hard_hit)::numeric / AVG(total_players) * 100, 1) as pct_with_hard_hit,
                        ROUND(AVG(avg_pa)) as avg_pa_per_player
                    FROM stats;
                """
                
                batting_result = pd.read_sql(batting_query, self.db.bind)
                print("\nBATTING STATS COMPLETENESS:")
                print(batting_result.to_string(index=False))
                
                # Pitching Stats Analysis
                pitching_query = f"""
                    WITH stats AS (
                        SELECT 
                            year,
                            COUNT(*) as total_pitchers,
                            COUNT(CASE WHEN innings IS NOT NULL THEN 1 END) as has_ip,
                            COUNT(CASE WHEN war IS NOT NULL THEN 1 END) as has_war,
                            COUNT(CASE WHEN fip IS NOT NULL THEN 1 END) as has_fip,
                            COUNT(CASE WHEN xfip IS NOT NULL THEN 1 END) as has_xfip,
                            COUNT(CASE WHEN fa_pct IS NOT NULL THEN 1 END) as has_pitch_mix,
                            COUNT(CASE WHEN wfb IS NOT NULL THEN 1 END) as has_pitch_values,
                            AVG(CASE WHEN innings IS NOT NULL THEN innings END) as avg_ip
                        FROM pitching_stats
                        WHERE year BETWEEN {era_start} AND {era_end}
                        GROUP BY year
                        ORDER BY year DESC
                    )
                    SELECT 
                        MIN(year) as start_year,
                        MAX(year) as end_year,
                        ROUND(AVG(total_pitchers)) as avg_pitchers_per_year,
                        ROUND(AVG(has_ip)::numeric / AVG(total_pitchers) * 100, 1) as pct_with_ip,
                        ROUND(AVG(has_war)::numeric / AVG(total_pitchers) * 100, 1) as pct_with_war,
                        ROUND(AVG(has_fip)::numeric / AVG(total_pitchers) * 100, 1) as pct_with_fip,
                        ROUND(AVG(has_xfip)::numeric / AVG(total_pitchers) * 100, 1) as pct_with_xfip,
                        ROUND(AVG(has_pitch_mix)::numeric / AVG(total_pitchers) * 100, 1) as pct_with_pitch_mix,
                        ROUND(AVG(has_pitch_values)::numeric / AVG(total_pitchers) * 100, 1) as pct_with_pitch_values,
                        ROUND(AVG(avg_ip), 1) as avg_ip_per_pitcher
                    FROM stats;
                """
                
                pitching_result = pd.read_sql(pitching_query, self.db.bind)
                print("\nPITCHING STATS COMPLETENESS:")
                print(pitching_result.to_string(index=False))
                
                current_year -= era_size
                
            return True, "Data completeness analysis completed"
            
        except Exception as e:
            error_msg = f"Error analyzing data completeness: {str(e)}"
            logging.error(error_msg)
            return False, error_msg