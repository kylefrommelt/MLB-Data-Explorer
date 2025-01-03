from shiny import App, render, ui
from app.data_handler import MLBDataHandler
from app.viz_handler import MLBVizHandler
import os

# Test database connection and basic functionality
def test_app():
    print("Starting test run...")
    
    # Initialize handlers with your database URL
    try:
        db_handler = MLBDataHandler('postgresql://postgres:Clecavs2@localhost:5432/mlb_stats')
        viz_handler = MLBVizHandler(db_handler)
        print("✓ Successfully initialized handlers")
        
        # Check table structure
        print("\nChecking batting_stats columns:")
        batting_cols = db_handler.get_table_columns('batting_stats')
        print(batting_cols)
        
        print("\nChecking pitching_stats columns:")
        pitching_cols = db_handler.get_table_columns('pitching_stats')
        print(pitching_cols)
        
        # Test getting player lists
        hitters = db_handler.get_hitter_list()
        pitchers = db_handler.get_pitcher_list()
        print(f"\n✓ Found {len(hitters)} hitters and {len(pitchers)} pitchers")
        
        # Test getting stats for a sample player
        if len(hitters) > 0:
            sample_hitter = hitters.iloc[0]['player_id']
            batting_stats = db_handler.get_batting_stats_range(
                player_ids=[sample_hitter],
                start_year=2020,
                end_year=2024
            )
            print(f"✓ Successfully retrieved batting stats for sample player")
            print(f"  Sample data shape: {batting_stats.shape}")
            print("\nSample data:")
            print(batting_stats.head())
        
            # Test visualization with the correct column names
            test_plot = viz_handler.create_custom_plot(
                data=batting_stats,
                x_stat='year',
                y_stat='war',
                plot_type='line',
                options=None  # Removed trend option for initial test
            )
            print("\n✓ Successfully created test visualization")
        
        print("\nAll tests passed! You can now run the full app.")
        print("\nTo run the app, use:")
        print("shiny run app/app.py")
        
    except Exception as e:
        print(f"Error during testing: {str(e)}")
        import traceback
        print("\nFull error traceback:")
        print(traceback.format_exc())

if __name__ == "__main__":
    test_app() 