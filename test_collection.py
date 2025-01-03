from app.database import SessionLocal
from app.data_collector import MLBDataCollector
import logging
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_test_collection():
    db = SessionLocal()
    try:
        collector = MLBDataCollector(db)
        logger.info("Starting comprehensive MLB data collection...")
        
        # Test with 2024 first
        test_year = 2024
        logger.info(f"Testing collection for {test_year}...")
        
        # Collect batting stats
        batting_df = collector.fetch_batting_fangraphs(test_year, test_year)
        if batting_df is not None:
            logger.info(f"Found {len(batting_df)} batting records")
            logger.info(f"PA Range: {batting_df['PA'].min()}-{batting_df['PA'].max()}")
            logger.info(f"Position breakdown:")
            logger.info(batting_df['Pos'].value_counts())
            
            # Log missing data percentages
            missing_stats = batting_df.isna().sum()
            if len(missing_stats[missing_stats > 0]) > 0:
                logger.info("Missing batting stats:")
                for stat, count in missing_stats[missing_stats > 0].items():
                    pct = (count / len(batting_df)) * 100
                    logger.info(f"  {stat}: {count} records ({pct:.1f}%)")
            
            # Store batting stats
            collector._store_stats_batch(batting_df, 'batting')
            logger.info("Successfully stored batting stats")
        
        # Collect pitching stats
        pitching_df = collector.fetch_pitching_fangraphs(test_year, test_year)
        if pitching_df is not None:
            logger.info(f"Found {len(pitching_df)} pitching records")
            logger.info(f"IP Range: {pitching_df['IP'].min()}-{pitching_df['IP'].max()}")
            
            # Log missing data percentages
            missing_stats = pitching_df.isna().sum()
            if len(missing_stats[missing_stats > 0]) > 0:
                logger.info("Missing pitching stats:")
                for stat, count in missing_stats[missing_stats > 0].items():
                    pct = (count / len(pitching_df)) * 100
                    logger.info(f"  {stat}: {count} records ({pct:.1f}%)")
            
            # Store pitching stats
            collector._store_stats_batch(pitching_df, 'pitching')
            logger.info("Successfully stored pitching stats")
            
        if batting_df is not None and pitching_df is not None:
            logger.info("Test collection successful!")
            logger.info(f"Stored {collector.stats_collected['batting']} batting records")
            logger.info(f"Stored {collector.stats_collected['pitching']} pitching records")
            logger.info("Ready to proceed with full historical collection")
            return True
        else:
            logger.error("Test collection failed")
            return False
            
    except Exception as e:
        logger.error(f"Error during test collection: {e}")
        return False
    finally:
        db.close()

def run_historical_collection():
    db = SessionLocal()
    try:
        collector = MLBDataCollector(db)
        
        # First test with 2024
        logger.info("Testing with current year (2024) first...")
        success = run_test_collection()
        
        if success:
            print("\nProceed with historical collection? (y/n)")
            response = input().lower()
            
            if response == 'y':
                # Historical collection in chunks
                success, msg = collector.collect_historical_data(
                    start_year=1876,  # First MLB season
                    end_year=2023,    # Up to last year
                    chunk_size=5      # 5 years at a time
                )
                
                if success:
                    logger.info("Historical collection completed successfully!")
                    logger.info(f"Total batting records: {collector.stats_collected['batting']}")
                    logger.info(f"Total pitching records: {collector.stats_collected['pitching']}")
                    
                    # Run completeness analysis
                    print("\nAnalyzing data completeness across eras...")
                    collector.analyze_data_completeness(
                        start_year=1876,
                        end_year=2024,
                        era_size=20
                    )
                else:
                    logger.error(f"Historical collection failed: {msg}")
            else:
                logger.info("Historical collection cancelled by user")
                
    except Exception as e:
        logger.error(f"Error during collection: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    run_historical_collection()
