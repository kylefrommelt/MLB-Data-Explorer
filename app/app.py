from shiny import App, ui, render
from .data_handler import MLBDataHandler
from .viz_handler import MLBVizHandler
import plotly.express as px
from plotly.io import to_html

# Initialize handlers with your actual connection string
db_handler = MLBDataHandler('postgresql://postgres:Clecavs2@localhost:5432/mlb_stats')
viz_handler = MLBVizHandler(db_handler)

# Available batting statistics grouped by category
BATTING_STATS = {
    'Traditional': {
        'ab': 'At Bats',
        'games': 'Games',
        'pa': 'Plate Appearances',
        'runs': 'Runs',
        'hits': 'Hits',
        'doubles': 'Doubles',
        'triples': 'Triples',
        'hr': 'Home Runs',
        'rbi': 'RBI',
        'sb': 'Stolen Bases',
        'cs': 'Caught Stealing',
        'bb': 'Walks',
        'ibb': 'Intentional Walks',
        'so': 'Strikeouts',
        'hbp': 'Hit By Pitch',
        'sf': 'Sacrifice Flies',
        'sh': 'Sacrifice Hits',
        'gdp': 'Ground Into DP'
    },
    'Rate Stats': {
        'avg': 'Batting Average',
        'obp': 'On-Base %',
        'slg': 'Slugging %',
        'ops': 'OPS',
        'iso': 'Isolated Power',
        'babip': 'BABIP',
        'woba': 'wOBA',
        'wrc_plus': 'wRC+',
        'war': 'WAR'
    },
    'Plate Discipline': {
        'o_swing_pct': 'Chase %',
        'z_swing_pct': 'Zone Swing %',
        'swing_pct': 'Swing %',
        'o_contact_pct': 'Outside Contact %',
        'z_contact_pct': 'Zone Contact %',
        'contact_pct': 'Contact %',
        'zone_pct': 'Zone %',
        'f_strike_pct': 'First Strike %',
        'swstr_pct': 'Swinging Strike %',
        'cstr_pct': 'Called Strike %',
        'csw_pct': 'Called + Swinging Strike %'
    },
    'Batted Ball': {
        'gb_pct': 'Ground Ball %',
        'fb_pct': 'Fly Ball %',
        'ld_pct': 'Line Drive %',
        'iffb_pct': 'Infield Fly Ball %',
        'hr_fb': 'HR/FB Ratio',
        'pull_pct': 'Pull %',
        'cent_pct': 'Center %',
        'oppo_pct': 'Opposite Field %',
        'soft_pct': 'Soft Contact %',
        'med_pct': 'Medium Contact %',
        'hard_pct': 'Hard Contact %',
        'exit_velocity': 'Exit Velocity',
        'launch_angle': 'Launch Angle',
        'barrel_pct': 'Barrel %',
        'hard_hit_pct': 'Hard Hit %'
    },
    'Value Metrics': {
        'batting_runs': 'Batting Runs',
        'baserunning_runs': 'Baserunning Runs',
        'fielding_runs': 'Fielding Runs',
        'positional': 'Positional Adjustment',
        'offense': 'Offensive Runs',
        'defense': 'Defensive Runs',
        'replacement': 'Replacement Runs',
        'rar': 'Runs Above Replacement',
        'dollars': 'Value in $',
        'clutch': 'Clutch Score'
    },
    'Win Probability': {
        'wpa': 'Win Probability Added',
        'neg_wpa': 'Negative WPA',
        'pos_wpa': 'Positive WPA',
        're24': 'Run Expectancy 24',
        'rew': 'Run Expectancy Wins',
        'pli': 'Leverage Index',
        'phli': 'PH Leverage Index'
    },
    'Pitch Values': {
        'wfb': 'Fastball Runs',
        'wsl': 'Slider Runs',
        'wct': 'Cutter Runs',
        'wcb': 'Curveball Runs',
        'wch': 'Changeup Runs'
    },
    'Expected Stats': {
        'xba': 'Expected BA',
        'xslg': 'Expected SLG',
        'xwoba': 'Expected wOBA'
    }
}

# Available pitching statistics grouped by category
PITCHING_STATS = {
    'Traditional': {
        'innings': 'Innings Pitched',
        'games': 'Games',
        'games_started': 'Games Started',
        'wins': 'Wins',
        'losses': 'Losses',
        'saves': 'Saves',
        'holds': 'Holds',
        'hits_allowed': 'Hits Allowed',
        'runs': 'Runs',
        'earned_runs': 'Earned Runs',
        'hr_allowed': 'Home Runs',
        'bb': 'Walks',
        'ibb': 'Intentional Walks',
        'so': 'Strikeouts',
        'hbp': 'Hit By Pitch',
        'wp': 'Wild Pitches',
        'bk': 'Balks'
    },
    'Rate Stats': {
        'era': 'ERA',
        'whip': 'WHIP',
        'k_9': 'K/9',
        'bb_9': 'BB/9',
        'hr_9': 'HR/9',
        'k_bb': 'K/BB',
        'k_pct': 'K%',
        'bb_pct': 'BB%',
        'fip': 'FIP',
        'xfip': 'xFIP',
        'siera': 'SIERA',
        'war': 'WAR'
    },
    'Batted Ball': {
        'babip': 'BABIP',
        'lob_pct': 'LOB%',
        'gb_pct': 'Ground Ball%',
        'fb_pct': 'Fly Ball%',
        'ld_pct': 'Line Drive%',
        'hr_fb': 'HR/FB%',
        'hard_hit_pct': 'Hard Hit%',
        'barrel_pct': 'Barrel%'
    },
    'Pitch Info': {
        'avg_velocity': 'Average Velocity',
        'max_velocity': 'Max Velocity',
        'spin_rate': 'Spin Rate',
        'chase_rate': 'Chase Rate',
        'whiff_pct': 'Whiff%',
        'csw_rate': 'Called + Swinging Strike%'
    },
    'Pitch Mix': {
        'fa_pct': 'Fastball%',
        'fc_pct': 'Cutter%',
        'fs_pct': 'Splitter%',
        'si_pct': 'Sinker%',
        'sl_pct': 'Slider%',
        'cu_pct': 'Curveball%',
        'ch_pct': 'Changeup%',
        'kc_pct': 'Knuckle Curve%'
    },
    'Win Probability': {
        'wpa': 'Win Probability Added',
        'neg_wpa': 'Negative WPA',
        'pos_wpa': 'Positive WPA',
        're24': 'Run Expectancy 24',
        'rew': 'Run Expectancy Wins',
        'pli': 'Leverage Index',
        'inli': 'Initial Leverage Index',
        'clutch': 'Clutch Score'
    },
    'Pitch Values': {
        'wfb': 'Fastball Runs',
        'wsl': 'Slider Runs',
        'wct': 'Cutter Runs',
        'wcb': 'Curveball Runs',
        'wch': 'Changeup Runs'
    }
}

app_ui = ui.page_navbar(
    ui.nav_panel("Batting Stats",
        ui.layout_sidebar(
            ui.sidebar(
                ui.h4("Batting Visualization Builder"),
                ui.input_selectize(
                    "hitters",
                    "Select Hitters (max 5)",
                    choices=[],
                    multiple=True,
                    options={
                        "maxItems": 5,
                        "placeholder": "Type to search players...",
                        "openOnFocus": True,
                        "hideSelected": True,
                        "searchField": ["label"],
                        "sortField": "label",
                        "create": False,
                        "persist": False
                    }
                ),
                ui.input_select(
                    "batting_plot_type",
                    "Plot Type",
                    {
                        "line": "Line",
                        "scatter": "Scatter",
                        "bar": "Bar",
                        "box": "Box"
                    }
                ),
                ui.input_select(
                    "batting_x",
                    "X-Axis Stat",
                    choices=BATTING_STATS
                ),
                ui.input_select(
                    "batting_y",
                    "Y-Axis Stat",
                    choices=BATTING_STATS
                ),
                ui.input_slider(
                    "batting_years",
                    "Year Range",
                    min=1900,
                    max=2024,
                    value=[2015, 2024],
                    step=1,
                    sep="",
                    drag_range=True,
                    ticks=True
                )
            ),
            ui.output_ui("batting_content")
        )
    ),
    ui.nav_panel("Pitching Stats",
        ui.layout_sidebar(
            ui.sidebar(
                ui.h4("Pitching Visualization Builder"),
                ui.input_selectize(
                    "pitchers",
                    "Select Pitchers (max 5)",
                    choices=[],
                    multiple=True,
                    options={
                        "maxItems": 5,
                        "placeholder": "Type to search players...",
                        "openOnFocus": True,
                        "hideSelected": True,
                        "searchField": ["label"],
                        "sortField": "label",
                        "create": False,
                        "persist": False
                    }
                ),
                ui.input_select(
                    "pitching_plot_type",
                    "Plot Type",
                    {
                        "line": "Line",
                        "scatter": "Scatter",
                        "bar": "Bar",
                        "box": "Box"
                    }
                ),
                ui.input_select(
                    "pitching_x",
                    "X-Axis Stat",
                    choices=PITCHING_STATS
                ),
                ui.input_select(
                    "pitching_y",
                    "Y-Axis Stat",
                    choices=PITCHING_STATS
                ),
                ui.input_slider(
                    "pitching_years",
                    "Year Range",
                    min=1900,
                    max=2024,
                    value=[2015, 2024],
                    step=1,
                    sep="",
                    drag_range=True,
                    ticks=True
                )
            ),
            ui.output_ui("pitching_content")
        )
    ),
    title="MLB Stats Explorer"
)

def server(input, output, session):
    # Initialize player lists
    @output
    @render.ui
    def main_content():
        return ui.div(
            ui.output_ui("batting_plot"),
            ui.output_table("batting_stats")
        )
    
    # Initialize the player list when the app starts
    players = db_handler.get_hitter_list_with_names()
    players = players.sort_values('name')
    
    # Update the selectize input with player choices
    ui.update_selectize(
        "hitters",
        choices={
            str(row['player_id']): f"{row['name']} ({row['years']})"
            for _, row in players.iterrows()
        },
        selected=None
    )
    
    @output
    @render.ui
    def batting_plot():
        if not (input.hitters() and input.batting_x() and input.batting_y()):
            return None
            
        data = db_handler.get_batting_stats_range(
            player_ids=input.hitters(),
            start_year=input.batting_years()[0],
            end_year=input.batting_years()[1]
        )
        
        fig = viz_handler.create_custom_plot(
            data=data,
            x_stat=input.batting_x(),
            y_stat=input.batting_y(),
            plot_type=input.batting_plot_type(),
            options=input.batting_options()
        )
        
        # Convert Plotly figure to HTML
        return ui.HTML(to_html(fig, full_html=False))
    
    @output
    @render.ui
    def pitching_content():
        return ui.div(
            ui.output_ui("pitching_plot"),
            ui.output_table("pitching_stats")
        )
    
    # Initialize pitcher list when the app starts
    pitchers = db_handler.get_pitcher_list_with_names()
    pitchers = pitchers.sort_values('name')
    
    ui.update_selectize(
        "pitchers",
        choices={
            str(row['player_id']): f"{row['name']} ({row['years']})"
            for _, row in pitchers.iterrows()
        },
        selected=None
    )
    
    @output
    @render.ui
    def pitching_plot():
        if not (input.pitchers() and input.pitching_x() and input.pitching_y()):
            return None
            
        data = db_handler.get_pitching_stats_range(
            player_ids=input.pitchers(),
            start_year=input.pitching_years()[0],
            end_year=input.pitching_years()[1]
        )
        
        fig = viz_handler.create_custom_plot(
            data=data,
            x_stat=input.pitching_x(),
            y_stat=input.pitching_y(),
            plot_type=input.pitching_plot_type(),
            options=input.pitching_options(),
            is_pitching=True
        )
        
        # Convert Plotly figure to HTML
        return ui.HTML(to_html(fig, full_html=False))

app = App(app_ui, server)
