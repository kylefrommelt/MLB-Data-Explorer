import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

class MLBVizHandler:
    def __init__(self, data_handler):
        self.data = data_handler
        # Define batting rate stats
        self.batting_rate_stats = {
            'avg', 'obp', 'slg', 'ops', 'iso', 'babip', 'woba', 'wrc_plus',
            'o_swing_pct', 'z_swing_pct', 'swing_pct', 'o_contact_pct',
            'z_contact_pct', 'contact_pct', 'zone_pct', 'f_strike_pct',
            'swstr_pct', 'cstr_pct', 'csw_pct', 'gb_pct', 'fb_pct',
            'ld_pct', 'iffb_pct', 'hr_fb', 'pull_pct', 'cent_pct',
            'oppo_pct', 'soft_pct', 'med_pct', 'hard_pct', 'barrel_pct',
            'hard_hit_pct'
        }
        # Define one comprehensive set of pitching rate stats
        self.pitching_rate_stats = {
            'era', 'whip', 'k_9', 'bb_9', 'hr_9', 'k_bb', 'k_pct', 'bb_pct',
            'fip', 'xfip', 'siera', 'babip', 'lob_pct', 'gb_pct', 'fb_pct',
            'ld_pct', 'hr_fb', 'hard_hit_pct', 'barrel_pct', 'whiff_pct',
            'chase_rate', 'csw_rate', 'fa_pct', 'fc_pct', 'fs_pct', 'si_pct',
            'sl_pct', 'cu_pct', 'ch_pct', 'kc_pct'
        }
        
    def create_custom_plot(self, data, x_stat, y_stat, plot_type, options=None, is_pitching=False):
        """Create a custom visualization based on user selections"""
        if options is None:
            options = []
            
        # Check if selected stats exist in the data
        if x_stat not in data.columns or y_stat not in data.columns:
            missing_stats = []
            if x_stat not in data.columns:
                missing_stats.append(x_stat)
            if y_stat not in data.columns:
                missing_stats.append(y_stat)
            
            # Create error message figure
            fig = go.Figure()
            fig.add_annotation(
                text=f"Statistics currently unavailable: {', '.join(missing_stats)}",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=16, color="red"),
                align="center"
            )
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                xaxis=dict(showgrid=False, showticklabels=False),
                yaxis=dict(showgrid=False, showticklabels=False)
            )
            return fig
            
        # Use the class-level rate stats instead of redefining
        rate_stats = self.pitching_rate_stats if is_pitching else self.batting_rate_stats
        
        # Get player names and merge
        player_names = self.data.get_player_names(data['player_id'].unique())
        data = data.merge(player_names, left_on='player_id', right_on='id')
        data = data.sort_values(['name', 'year'])
        
        # Convert stats to numeric if they aren't already
        data[x_stat] = pd.to_numeric(data[x_stat], errors='coerce')
        data[y_stat] = pd.to_numeric(data[y_stat], errors='coerce')
        
        # Define a custom color palette
        custom_colors = [
            '#2E86AB',  # Blue
            '#A23B72',  # Purple
            '#F18F01',  # Orange
            '#C73E1D',  # Red
            '#3B7A57',  # Green
        ]
        
        if plot_type == "line":
            # Calculate cumulative stats only for non-rate statistics
            if x_stat not in rate_stats:
                data[f'{x_stat}_plot'] = data.groupby('name')[x_stat].cumsum()
                x_label = f'Cumulative {x_stat.replace("_", " ").title()}'
            else:
                data[f'{x_stat}_plot'] = data[x_stat]
                x_label = x_stat.replace("_", " ").title()
            
            if y_stat not in rate_stats:
                data[f'{y_stat}_plot'] = data.groupby('name')[y_stat].cumsum()
                y_label = f'Cumulative {y_stat.replace("_", " ").title()}'
            else:
                data[f'{y_stat}_plot'] = data[y_stat]
                y_label = y_stat.replace("_", " ").title()
            
            fig = px.scatter(
                data,
                x=f'{x_stat}_plot',
                y=f'{y_stat}_plot',
                color='name',
                text='year',
                title=f"{y_label} vs {x_label}",
                labels={
                    f'{x_stat}_plot': x_label,
                    f'{y_stat}_plot': y_label,
                    'name': 'Player'
                },
                color_discrete_sequence=custom_colors
            )
            
            # Add connecting lines with matching colors
            for idx, player in enumerate(data['name'].unique()):
                player_data = data[data['name'] == player].sort_values('year')
                fig.add_trace(
                    go.Scatter(
                        x=player_data[f'{x_stat}_plot'],
                        y=player_data[f'{y_stat}_plot'],
                        mode='lines',
                        name=player,
                        showlegend=False,
                        line=dict(color=custom_colors[idx % len(custom_colors)])
                    )
                )
            
            # Update hover template
            fig.update_traces(
                hovertemplate="<br>".join([
                    "Year: %{text}",
                    f"{x_label}: %{{x:,}}",
                    f"{y_label}: %{{y:,}}",
                    "<extra></extra>"
                ])
            )
        
        elif plot_type == "scatter":
            fig = px.scatter(
                data,
                x=x_stat,
                y=y_stat,
                color='name',
                hover_data=['year', 'name'],
                title=f"{y_stat.replace('_', ' ').title()} vs {x_stat.replace('_', ' ').title()}"
            )
            
        elif plot_type == "bar":
            fig = px.bar(
                data,
                x='name',
                y=y_stat,
                color='name',
                title=f"{y_stat.replace('_', ' ').title()} by Player"
            )
        
        elif plot_type == "box":
            fig = px.box(
                data,
                x='name',
                y=y_stat,
                title=f"{y_stat.replace('_', ' ').title()} Distribution by Player"
            )
        
        # Update layout for better appearance
        fig.update_layout(
            hovermode='closest',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(255, 255, 255, 0.8)'
            ),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(
                family="Arial, sans-serif",
                size=12
            ),
            margin=dict(l=80, r=30, t=50, b=80)
        )
        
        # Add grid lines
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='LightGray'
        )
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='LightGray'
        )
        
        return fig 