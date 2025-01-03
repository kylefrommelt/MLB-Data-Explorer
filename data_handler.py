from sqlalchemy import create_engine, text
import pandas as pd
import logging

class MLBDataHandler:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)
        
    def get_hitter_list(self):
        """Get list of all hitters"""
        query = """
            SELECT DISTINCT 
                player_id
            FROM batting_stats
            ORDER BY player_id
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting hitter list: {str(e)}")
            return pd.DataFrame()
    
    def get_pitcher_list(self):
        """Get list of all pitchers"""
        query = """
            SELECT DISTINCT 
                player_id
            FROM pitching_stats
            ORDER BY player_id
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting pitcher list: {str(e)}")
            return pd.DataFrame()
    
    def get_batting_stats_range(self, player_ids, start_year, end_year):
        """Get batting statistics for selected players within year range"""
        player_ids_str = ','.join(str(id) for id in player_ids)
        query = f"""
            SELECT 
                player_id,
                year,
                CAST(games AS INTEGER) as games,
                CAST(pa AS INTEGER) as pa,
                CAST(ab AS INTEGER) as ab,
                CAST(runs AS INTEGER) as runs,
                CAST(hits AS INTEGER) as hits,
                CAST(doubles AS INTEGER) as doubles,
                CAST(triples AS INTEGER) as triples,
                CAST(hr AS INTEGER) as hr,
                CAST(rbi AS INTEGER) as rbi,
                CAST(sb AS INTEGER) as sb,
                CAST(cs AS INTEGER) as cs,
                CAST(bb AS INTEGER) as bb,
                CAST(ibb AS INTEGER) as ibb,
                CAST(so AS INTEGER) as so,
                CAST(hbp AS INTEGER) as hbp,
                CAST(sf AS INTEGER) as sf,
                CAST(sh AS INTEGER) as sh,
                CAST(gdp AS INTEGER) as gdp,
                CAST(avg AS FLOAT) as avg,
                CAST(obp AS FLOAT) as obp,
                CAST(slg AS FLOAT) as slg,
                CAST(ops AS FLOAT) as ops,
                CAST(iso AS FLOAT) as iso,
                CAST(babip AS FLOAT) as babip,
                CAST(woba AS FLOAT) as woba,
                CAST(wrc_plus AS FLOAT) as wrc_plus,
                CAST(war AS FLOAT) as war,
                CAST(o_swing_pct AS FLOAT) as o_swing_pct,
                CAST(z_swing_pct AS FLOAT) as z_swing_pct,
                CAST(swing_pct AS FLOAT) as swing_pct,
                CAST(o_contact_pct AS FLOAT) as o_contact_pct,
                CAST(z_contact_pct AS FLOAT) as z_contact_pct,
                CAST(contact_pct AS FLOAT) as contact_pct,
                CAST(zone_pct AS FLOAT) as zone_pct,
                CAST(f_strike_pct AS FLOAT) as f_strike_pct,
                CAST(swstr_pct AS FLOAT) as swstr_pct,
                CAST(cstr_pct AS FLOAT) as cstr_pct,
                CAST(csw_pct AS FLOAT) as csw_pct,
                CAST(gb_pct AS FLOAT) as gb_pct,
                CAST(fb_pct AS FLOAT) as fb_pct,
                CAST(ld_pct AS FLOAT) as ld_pct,
                CAST(iffb_pct AS FLOAT) as iffb_pct,
                CAST(hr_fb AS FLOAT) as hr_fb,
                CAST(pull_pct AS FLOAT) as pull_pct,
                CAST(cent_pct AS FLOAT) as cent_pct,
                CAST(oppo_pct AS FLOAT) as oppo_pct,
                CAST(soft_pct AS FLOAT) as soft_pct,
                CAST(med_pct AS FLOAT) as med_pct,
                CAST(hard_pct AS FLOAT) as hard_pct,
                CAST(batting_runs AS FLOAT) as batting_runs,
                CAST(baserunning_runs AS FLOAT) as baserunning_runs,
                CAST(fielding_runs AS FLOAT) as fielding_runs,
                CAST(positional AS FLOAT) as positional,
                CAST(offense AS FLOAT) as offense,
                CAST(defense AS FLOAT) as defense,
                CAST(replacement AS FLOAT) as replacement,
                CAST(rar AS FLOAT) as rar,
                CAST(dollars AS FLOAT) as dollars,
                CAST(wpa AS FLOAT) as wpa,
                CAST(neg_wpa AS FLOAT) as neg_wpa,
                CAST(pos_wpa AS FLOAT) as pos_wpa,
                CAST(re24 AS FLOAT) as re24,
                CAST(rew AS FLOAT) as rew,
                CAST(pli AS FLOAT) as pli,
                CAST(phli AS FLOAT) as phli,
                CAST(clutch AS FLOAT) as clutch,
                CAST(wfb AS FLOAT) as wfb,
                CAST(wsl AS FLOAT) as wsl,
                CAST(wct AS FLOAT) as wct,
                CAST(wcb AS FLOAT) as wcb,
                CAST(wch AS FLOAT) as wch,
                CAST(exit_velocity AS FLOAT) as exit_velocity,
                CAST(launch_angle AS FLOAT) as launch_angle,
                CAST(barrel_pct AS FLOAT) as barrel_pct,
                CAST(hard_hit_pct AS FLOAT) as hard_hit_pct
            FROM batting_stats
            WHERE player_id IN ({player_ids_str})
            AND year BETWEEN {start_year} AND {end_year}
            AND pa > 0
            AND games > 0
            ORDER BY year
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting batting stats: {str(e)}")
            return pd.DataFrame()
            
    def get_table_columns(self, table_name):
        """Utility function to check available columns"""
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting columns: {str(e)}")
            return pd.DataFrame()
    
    def get_hitter_list_with_names(self):
        """Get list of all hitters with names and years played"""
        query = """
            SELECT DISTINCT 
                b.player_id,
                p.name,
                MIN(b.year) || '-' || MAX(b.year) as years
            FROM batting_stats b
            JOIN players p ON b.player_id = p.id  -- Changed to join on players.id
            WHERE b.pa >= 50  -- Only include players with meaningful PAs
            GROUP BY b.player_id, p.name
            ORDER BY p.name
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting hitter list: {str(e)}")
            print(f"SQL Error: {str(e)}")  # Added for debugging
            return pd.DataFrame()
    
    def get_player_names(self, player_ids):
        """Get player names for given IDs"""
        player_ids_str = ','.join(f"'{id}'" for id in player_ids)
        query = f"""
            SELECT id, name
            FROM players
            WHERE id IN ({player_ids_str})
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting player names: {str(e)}")
            return pd.DataFrame()
    
    def get_pitcher_list_with_names(self):
        """Get list of all pitchers with names and years played"""
        query = """
            SELECT DISTINCT 
                p.player_id,
                pl.name,
                MIN(p.year) || '-' || MAX(p.year) as years
            FROM pitching_stats p
            JOIN players pl ON p.player_id = pl.id
            WHERE p.innings >= 10  -- Only include pitchers with meaningful innings
            GROUP BY p.player_id, pl.name
            ORDER BY pl.name
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting pitcher list: {str(e)}")
            return pd.DataFrame()
    
    def get_pitching_stats_range(self, player_ids, start_year, end_year):
        """Get pitching statistics for selected players within year range"""
        player_ids_str = ','.join(str(id) for id in player_ids)
        query = f"""
            SELECT 
                player_id,
                year,
                innings,
                games,
                games_started,
                wins,
                losses,
                saves,
                holds,
                hits_allowed,
                runs,
                earned_runs,
                hr_allowed,
                bb,
                ibb,
                so,
                hbp,
                wp,
                bk,
                era,
                whip,
                k_9,
                bb_9,
                hr_9,
                k_bb,
                k_pct,
                bb_pct,
                fip,
                xfip,
                siera,
                war,
                babip,
                lob_pct,
                gb_pct,
                fb_pct,
                ld_pct,
                hr_fb,
                hard_hit_pct,
                barrel_pct,
                avg_velocity,
                max_velocity,
                spin_rate,
                chase_rate,
                whiff_pct,
                csw_rate,
                fa_pct,
                fc_pct,
                fs_pct,
                si_pct,
                sl_pct,
                cu_pct,
                ch_pct,
                kc_pct,
                wpa,
                neg_wpa,
                pos_wpa,
                re24,
                rew,
                pli,
                inli,
                clutch,
                wfb,
                wsl,
                wct,
                wcb,
                wch
            FROM pitching_stats
            WHERE player_id IN ({player_ids_str})
            AND year BETWEEN {start_year} AND {end_year}
            AND innings >= 1  -- Filter out rows with no innings pitched
            ORDER BY year
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting pitching stats: {str(e)}")
            return pd.DataFrame()
