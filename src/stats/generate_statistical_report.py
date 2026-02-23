import nbformat as nbf
from pathlib import Path


def generate_f1_statistical_report():
    """
    Generates a comprehensive Jupyter notebook with 10 analytical sections
    covering over 100 Formula 1 metrics from 1950 to 2025.

    Metrics taxonomy informed by statsf1.com, gpracingstats.com, Kaggle EDA
    notebooks, and academic F1 analytics literature.
    """
    nb = nbf.v4.new_notebook()
    cells = []

    # ---------------------------------------------------------------------------
    # HEADER
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "# Formula 1 — Comprehensive Statistical Portfolio (1950–2025)\n\n"
        "> This report covers 10 analytical domains, 40+ interactive visualizations, "
        "and over 100 distinct metrics. Sections are self-contained and ordered from "
        "broad records to fine-grained performance indicators.\n\n"
        "**Sections:**\n"
        "1. Setup & Data Overview\n"
        "2. Driver Records — Volume\n"
        "3. Driver Efficiency — Era-Adjusted Ratios\n"
        "4. Driver Racecraft — Positions Gained & Consistency\n"
        "5. Prestige Achievements — Hat Tricks, Grand Slams, Pole Conversion\n"
        "6. Constructor Dominance & Reliability\n"
        "7. Circuit DNA — Attrition, Strategy & Pole Importance\n"
        "8. Historical Trends — Reliability, Parity & Expansion\n"
        "9. Nationality & Geography\n"
        "10. Curiosities & Edge Cases\n"
    ))

    # ---------------------------------------------------------------------------
    # SECTION 0 — SETUP
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell("## 0. Setup & Data Overview"))
    cells.append(nbf.v4.new_code_cell(r"""
import warnings
warnings.filterwarnings('ignore')

import sys
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# -- Resolve the project root robustly regardless of kernel launch directory --
# Walk up until we find a directory containing 'data/processed'
_cwd = Path().resolve()
_project_root = _cwd
for _candidate in [_cwd] + list(_cwd.parents):
    if (_candidate / 'data' / 'processed').exists():
        _project_root = _candidate
        break

PROCESSED_DATA_DIR = _project_root / 'data' / 'processed'

# ---- Visual constants ----
TEMPLATE = "plotly_dark"
C_RED    = "#E10600"
C_GOLD   = "#FFD700"
C_SILVER = "#C0C0C0"
C_BLUE   = "#4FC3F7"

px.defaults.template = TEMPLATE

# ---- Load data ----
df = pd.read_parquet(PROCESSED_DATA_DIR / "results.parquet")
df_drivers = pd.read_parquet(PROCESSED_DATA_DIR / "drivers.parquet")

# ---- Key derived columns ----
# Did Not Finish detection: status not in completion signals
FINISH_STATUSES = {'Finished', '+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps',
                   '+5 Laps', '+6 Laps', '+7 Laps', '+8 Laps', '+9 Laps', 'Lapped'}
df['finished']        = df['status'].isin(FINISH_STATUSES)
df['is_dnf']          = ~df['finished'] & df['positionText'].isin(['R', 'D', 'E', 'W', 'F', 'N'])
df['on_podium']       = df['position'] <= 3
df['is_win']          = df['position'] == 1
df['is_pole']         = df['grid'] == 1
df['in_points']       = df['position'] <= 10   # modern top-10 scoring
df['fastest_lap']     = pd.to_numeric(df['fastest_lap_rank'], errors='coerce') == 1
df['decade']          = (df['season'] // 10) * 10
df['positions_gained']= df['grid'] - df['position']   # positive = gained

# Merge date-of-birth for age calculations
df = df.merge(df_drivers[['driverId', 'dateOfBirth']], on='driverId', how='left')
df['date'] = pd.to_datetime(df['date'])
df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'])
df['driver_age_at_race'] = (df['date'] - df['dateOfBirth']).dt.days / 365.25

print(f"Records loaded  : {len(df):,}")
print(f"Seasons covered : {df['season'].min()} – {df['season'].max()}")
print(f"Unique drivers  : {df['driver_fullname'].nunique()}")
print(f"Unique events   : {df['raceName'].nunique()}")
print(f"Unique teams    : {df['constructor_name'].nunique()}")
"""))

    # ---------------------------------------------------------------------------
    # SECTION 1 — DRIVER VOLUME RECORDS
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 1. Driver Records — Volume\n\n"
        "*Raw career totals: the absolute figures every fan knows.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# ---- Aggregate per driver ----
drv = df.groupby('driver_fullname').agg(
    entries      = ('position', 'count'),
    wins         = ('is_win', 'sum'),
    podiums      = ('on_podium', 'sum'),
    poles        = ('is_pole', 'sum'),
    fastest_laps = ('fastest_lap', 'sum'),
    total_points = ('points', 'sum'),
    seasons      = ('season', 'nunique'),
    dnfs         = ('is_dnf', 'sum'),
    top10s       = ('in_points', 'sum'),
).reset_index().rename(columns={'driver_fullname': 'driver'})

drv = drv.sort_values('wins', ascending=False)
TOP_N = 25

# Chart 1a — Career Wins (Top 25)
fig = px.bar(
    drv.head(TOP_N).sort_values('wins'),
    x='wins', y='driver', orientation='h',
    color='wins', color_continuous_scale='Reds',
    title=f'Career Grand Prix Victories — Top {TOP_N}',
    labels={'wins': 'Victories', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Chart 1b — Career Podiums
fig = px.bar(
    drv.sort_values('podiums', ascending=False).head(TOP_N).sort_values('podiums'),
    x='podiums', y='driver', orientation='h',
    color='podiums', color_continuous_scale='RdYlGn',
    title=f'Career Podium Finishes — Top {TOP_N}',
    labels={'podiums': 'Podiums', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Chart 1c — Career Points
fig = px.bar(
    drv.sort_values('total_points', ascending=False).head(TOP_N).sort_values('total_points'),
    x='total_points', y='driver', orientation='h',
    color='total_points', color_continuous_scale='Blues',
    title=f'Career Championship Points — Top {TOP_N}',
    labels={'total_points': 'Points', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Chart 1d — Most GP Entries (Longevity)
fig = px.bar(
    drv.sort_values('entries', ascending=False).head(TOP_N).sort_values('entries'),
    x='entries', y='driver', orientation='h',
    color='entries', color_continuous_scale='Purples',
    title=f'Career Grand Prix Entries — Top {TOP_N}',
    labels={'entries': 'GP Starts', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Table — Combined Top 15
drv.head(15)[['driver','entries','wins','podiums','poles','fastest_laps','total_points','seasons']]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 2 — DRIVER EFFICIENCY (ERA-ADJUSTED)
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 2. Driver Efficiency — Era-Adjusted Ratios\n\n"
        "*Raw totals favour modern era drivers with more races. Ratios level the playing field.*\n\n"
        "*Minimum 20 GP starts applied to remove statistical outliers.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Efficiency metrics (min 20 entries)
eff = drv[drv['entries'] >= 20].copy()
eff['win_pct']     = (eff['wins']    / eff['entries'] * 100).round(2)
eff['podium_pct']  = (eff['podiums'] / eff['entries'] * 100).round(2)
eff['pole_pct']    = (eff['poles']   / eff['entries'] * 100).round(2)
eff['pts_per_gp']  = (eff['total_points'] / eff['entries']).round(2)
eff['dnf_pct']     = (eff['dnfs']    / eff['entries'] * 100).round(2)
eff['top10_pct']   = (eff['top10s']  / eff['entries'] * 100).round(2)

# Chart 2a — Win Percentage (min 20 entries)
top_win_pct = eff.sort_values('win_pct', ascending=False).head(20)
fig = px.bar(
    top_win_pct.sort_values('win_pct'),
    x='win_pct', y='driver', orientation='h',
    color='win_pct', color_continuous_scale='Reds',
    title='Win Percentage per Career Start (Min. 20 Entries)',
    labels={'win_pct': 'Win %', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

# Chart 2b — Scatter: Win% vs Entries (bubble = total wins)
fig = px.scatter(
    eff.sort_values('wins', ascending=False).head(50),
    x='entries', y='win_pct',
    size='wins', size_max=50,
    hover_name='driver',
    color='win_pct', color_continuous_scale='Reds',
    title='Win Efficiency vs Career Longevity',
    labels={'entries': 'GP Entries', 'win_pct': 'Win %'}
)
fig.update_coloraxes(showscale=False)
fig.show()

# Chart 2c — Podium percentage
top_pod_pct = eff.sort_values('podium_pct', ascending=False).head(20)
fig = px.bar(
    top_pod_pct.sort_values('podium_pct'),
    x='podium_pct', y='driver', orientation='h',
    color='podium_pct', color_continuous_scale='RdYlGn',
    title='Podium Percentage per Career Start (Min. 20 Entries)',
    labels={'podium_pct': 'Podium %', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

# Chart 2d — Points per GP start
top_pts = eff.sort_values('pts_per_gp', ascending=False).head(20)
fig = px.bar(
    top_pts.sort_values('pts_per_gp'),
    x='pts_per_gp', y='driver', orientation='h',
    color='pts_per_gp', color_continuous_scale='Blues',
    title='Average Points per GP Start (Min. 20 Entries)',
    labels={'pts_per_gp': 'Pts/GP', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

eff.sort_values('win_pct', ascending=False).head(15)[
    ['driver','entries','win_pct','podium_pct','pole_pct','pts_per_gp','dnf_pct']
]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 3 — RACECRAFT
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 3. Driver Racecraft — Positions Gained & Consistency\n\n"
        "*Who makes up ground on race day? Who protects their grid slot?*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Only races where grid is valid (not 0 = pit lane start)
df_race = df[(df['grid'] > 0) & (df['position'] > 0)].copy()

racecraft = df_race.groupby('driver_fullname').agg(
    entries         = ('positions_gained', 'count'),
    avg_gained      = ('positions_gained', 'mean'),
    total_gained    = ('positions_gained', 'sum'),
    avg_grid        = ('grid', 'mean'),
    avg_finish      = ('position', 'mean'),
).reset_index().rename(columns={'driver_fullname': 'driver'})

racecraft['avg_gained'] = racecraft['avg_gained'].round(2)
racecraft['avg_grid']   = racecraft['avg_grid'].round(2)
racecraft['avg_finish'] = racecraft['avg_finish'].round(2)

# Min 50 races for reliability
rc_filtered = racecraft[racecraft['entries'] >= 50]

# Chart 3a — Top climbers (best avg positions gained)
climbers = rc_filtered.sort_values('avg_gained', ascending=False).head(20)
fig = px.bar(
    climbers.sort_values('avg_gained'),
    x='avg_gained', y='driver', orientation='h',
    color='avg_gained', color_continuous_scale='Greens',
    title='Average Positions Gained per Race (Min. 50 Starts)',
    labels={'avg_gained': 'Avg Pos. Gained', 'driver': ''}
)
fig.add_vline(x=0, line_dash='dash', line_color='white', opacity=0.4)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

# Chart 3b — Avg grid vs avg finish (dot below diagonal = improves)
top_rc = rc_filtered.sort_values('entries', ascending=False).head(40)
fig = px.scatter(
    top_rc, x='avg_grid', y='avg_finish',
    hover_name='driver', size='entries',
    color='avg_gained', color_continuous_scale='RdYlGn',
    title='Average Grid vs Average Finish Position (Top 40 by GP Entries)',
    labels={'avg_grid': 'Avg Grid Position', 'avg_finish': 'Avg Finish Position'}
)
fig.add_shape(type='line', x0=1, y0=1, x1=20, y1=20,
              line=dict(color='white', dash='dash', width=1))
fig.add_annotation(x=15, y=14, text='Grid = Finish', showarrow=False,
                   font=dict(color='white', size=10))
fig.update_coloraxes(colorbar_title='Avg Gained')
fig.show()

rc_filtered.sort_values('avg_gained', ascending=False).head(15)[
    ['driver','entries','avg_grid','avg_finish','avg_gained','total_gained']
]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 4 — PRESTIGE: HAT TRICKS, GRAND SLAMS, POLE CONVERSION
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 4. Prestige Achievements\n\n"
        "- **Hat Trick**: Pole position + Fastest Lap + Win in the same race\n"
        "- **Grand Slam**: Pole + Win + Fastest Lap + Led every lap\n"
        "  *(Approximated here as Pole + Win + Fastest Lap, since lap-level leadership is not in this dataset)*\n"
        "- **Pole Conversion Rate**: % of poles that resulted in a race win"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Hat tricks = pole + win + fastest lap in SAME race
hat_tricks = df[(df['is_pole']) & (df['is_win']) & (df['fastest_lap'])].groupby(
    'driver_fullname').size().reset_index(name='hat_tricks')

# Pole Conversion Rate
poles_df   = df[df['is_pole']].groupby('driver_fullname').size().reset_index(name='pole_starts')
pole_wins  = df[(df['is_pole']) & (df['is_win'])].groupby('driver_fullname').size().reset_index(name='pole_wins')
pole_conv  = poles_df.merge(pole_wins, on='driver_fullname', how='left').fillna(0)
pole_conv['pole_conv_pct'] = (pole_conv['pole_wins'] / pole_conv['pole_starts'] * 100).round(2)
pole_conv = pole_conv[pole_conv['pole_starts'] >= 3].sort_values('pole_conv_pct', ascending=False)

# Chart 4a — Hat Tricks
fig = px.bar(
    hat_tricks.sort_values('hat_tricks', ascending=False).head(15).sort_values('hat_tricks'),
    x='hat_tricks', y='driver_fullname', orientation='h',
    color='hat_tricks', color_continuous_scale='Oranges',
    title='Hat Tricks: Pole + Fastest Lap + Win — Career Total',
    labels={'hat_tricks': 'Hat Tricks', 'driver_fullname': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()

# Chart 4b — Pole Conversion Rate (min 3 poles)
fig = px.bar(
    pole_conv.head(20).sort_values('pole_conv_pct'),
    x='pole_conv_pct', y='driver_fullname', orientation='h',
    color='pole_conv_pct', color_continuous_scale='Reds',
    title='Pole Position Conversion Rate (Min. 3 Pole Positions)',
    labels={'pole_conv_pct': 'Pole to Win %', 'driver_fullname': ''},
    hover_data=['pole_starts', 'pole_wins']
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

print("=== Hat Trick Leaders ===")
print(hat_tricks.sort_values('hat_tricks', ascending=False).head(10).to_string(index=False))
print()
print("=== Pole Conversion Leaders ===")
print(pole_conv.head(15)[['driver_fullname','pole_starts','pole_wins','pole_conv_pct']].to_string(index=False))
"""))

    # ---------------------------------------------------------------------------
    # SECTION 5 — AGE RECORDS
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 5. Age Records\n\n"
        "*Youngest and oldest to achieve key milestones.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
wins_df    = df[df['is_win'] & df['driver_age_at_race'].notna()].copy()
podiums_df = df[df['on_podium'] & df['driver_age_at_race'].notna()].copy()
poles_df_a = df[df['is_pole'] & df['driver_age_at_race'].notna()].copy()

# Youngest/Oldest winners
youngest_wins = wins_df.loc[wins_df.groupby('driver_fullname')['driver_age_at_race'].idxmin()]
youngest_wins = youngest_wins.sort_values('driver_age_at_race').head(10)[
    ['driver_fullname','season','raceName','driver_age_at_race']].rename(
    columns={'driver_age_at_race': 'age_years'})
youngest_wins['age_years'] = youngest_wins['age_years'].round(2)

oldest_wins = wins_df.loc[wins_df.groupby('driver_fullname')['driver_age_at_race'].idxmax()]
oldest_wins = oldest_wins.sort_values('driver_age_at_race', ascending=False).head(10)[
    ['driver_fullname','season','raceName','driver_age_at_race']].rename(
    columns={'driver_age_at_race': 'age_years'})
oldest_wins['age_years'] = oldest_wins['age_years'].round(2)

# Chart 5a — Youngest winners
fig = go.Figure([go.Bar(
    x=youngest_wins['age_years'],
    y=youngest_wins['driver_fullname'] + ' (' + youngest_wins['season'].astype(str) + ')',
    orientation='h',
    marker_color=C_RED,
    text=youngest_wins['age_years'].astype(str) + ' yrs',
    textposition='outside'
)])
fig.update_layout(
    title='Youngest Race Winners in F1 History',
    template=TEMPLATE, xaxis_title='Age at First Win (years)',
    yaxis={'categoryorder': 'total ascending'}, height=500
)
fig.show()

# Chart 5b — Oldest winners
fig = go.Figure([go.Bar(
    x=oldest_wins['age_years'],
    y=oldest_wins['driver_fullname'] + ' (' + oldest_wins['season'].astype(str) + ')',
    orientation='h',
    marker_color=C_GOLD,
    text=oldest_wins['age_years'].astype(str) + ' yrs',
    textposition='outside'
)])
fig.update_layout(
    title='Oldest Race Winners in F1 History (Last Win)',
    template=TEMPLATE, xaxis_title='Age at Last Win (years)',
    yaxis={'categoryorder': 'total ascending'}, height=500
)
fig.show()

print("=== 10 Youngest Race Winners ===")
print(youngest_wins.to_string(index=False))
print()
print("=== 10 Oldest Race Winners (at Last Win) ===")
print(oldest_wins.to_string(index=False))
"""))

    # ---------------------------------------------------------------------------
    # SECTION 6 — CONSTRUCTOR DOMINANCE & RELIABILITY
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell("## 6. Constructor Dominance & Reliability"))
    cells.append(nbf.v4.new_code_cell(r"""
const = df.groupby('constructor_name').agg(
    entries      = ('position', 'count'),
    wins         = ('is_win', 'sum'),
    podiums      = ('on_podium', 'sum'),
    poles        = ('is_pole', 'sum'),
    fastest_laps = ('fastest_lap', 'sum'),
    total_points = ('total_points', 'sum') if 'total_points' in df.columns else ('points', 'sum'),
    dnfs         = ('is_dnf', 'sum'),
    seasons      = ('season', 'nunique'),
    drivers_used = ('driverId', 'nunique'),
).reset_index().rename(columns={'constructor_name': 'constructor'})

const = const.sort_values('wins', ascending=False)

# Efficiency
const['win_pct']    = (const['wins']   / const['entries'] * 100).round(2)
const['dnf_pct']    = (const['dnfs']   / const['entries'] * 100).round(2)
const['pts_per_gp'] = (const['total_points'] / const['entries']).round(2)

# Chart 6a — Constructor Wins
fig = px.bar(
    const.head(15).sort_values('wins'),
    x='wins', y='constructor', orientation='h',
    color='wins', color_continuous_scale='Reds',
    title='All-Time Constructor Grand Prix Victories — Top 15',
    labels={'wins': 'Victories', 'constructor': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

# Chart 6b — Constructor Reliability (DNF %)
reliable = const[const['entries'] >= 50].sort_values('dnf_pct')
fig = px.bar(
    reliable.head(20).sort_values('dnf_pct', ascending=False),
    x='dnf_pct', y='constructor', orientation='h',
    color='dnf_pct', color_continuous_scale='RdYlGn_r',
    title='Constructor Reliability: DNF Rate (Min. 50 Entries)',
    labels={'dnf_pct': 'DNF %', 'constructor': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

# Chart 6c — Constructor Win % (min 50 entries)
fig = px.bar(
    const[const['entries'] >= 50].sort_values('win_pct', ascending=False).head(15).sort_values('win_pct'),
    x='win_pct', y='constructor', orientation='h',
    color='win_pct', color_continuous_scale='Reds',
    title='Constructor Win Percentage (Min. 50 Entries)',
    labels={'win_pct': 'Win %', 'constructor': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()

# Chart 6d — Constructor points by season (Top 8 all-time teams)
top8_const = const.head(8)['constructor'].tolist()
season_pts = df[df['constructor_name'].isin(top8_const)].groupby(
    ['season', 'constructor_name'])['points'].sum().reset_index()
fig = px.line(season_pts, x='season', y='points', color='constructor_name',
              title='Constructor Points per Season — Top 8 Teams',
              labels={'points': 'Season Points', 'season': 'Year', 'constructor_name': 'Constructor'})
fig.update_layout(height=500)
fig.show()

const.head(15)[['constructor','entries','wins','win_pct','podiums','poles','dnf_pct','seasons','drivers_used']]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 7 — CIRCUIT DNA
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 7. Circuit DNA — Attrition, Strategy & Pole Importance"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Circuit-level attrition
circ = df.groupby(['circuitId', 'raceName']).agg(
    total_starters = ('position', 'count'),
    dnfs           = ('is_dnf', 'sum'),
    pole_wins      = ('is_pole', lambda x: (x & df.loc[x.index, 'is_win']).sum()),
    races_held     = ('round', 'nunique'),
).reset_index()
circ['attrition_pct'] = (circ['dnfs'] / circ['total_starters'] * 100).round(2)
circ['pole_win_pct']  = (circ['pole_wins'] / circ['races_held'] * 100).round(2)

# Chart 7a — Most punishing circuits
fig = px.bar(
    circ[circ['races_held'] >= 5].sort_values('attrition_pct', ascending=False).head(20),
    x='attrition_pct', y='raceName', orientation='h',
    color='attrition_pct', color_continuous_scale='Reds',
    title='Circuit Severity: Average DNF Rate (Min. 5 Editions)',
    labels={'attrition_pct': 'DNF %', 'raceName': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Chart 7b — Pole-to-Win conversion by circuit (Monaco factor)
fig = px.bar(
    circ[circ['races_held'] >= 10].sort_values('pole_win_pct', ascending=False).head(20).sort_values('pole_win_pct'),
    x='pole_win_pct', y='raceName', orientation='h',
    color='pole_win_pct', color_continuous_scale='Oranges',
    title='Pole Position Win Conversion by Circuit (Min. 10 Editions)',
    labels={'pole_win_pct': 'Pole to Win %', 'raceName': ''},
    hover_data=['races_held']
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Most dominant driver per circuit
circuit_wins = df[df['is_win']].groupby(['raceName', 'driver_fullname']).size().reset_index(name='wins')
most_dominant = circuit_wins.loc[circuit_wins.groupby('raceName')['wins'].idxmax()].rename(
    columns={'driver_fullname': 'most_wins_driver'})
most_dominant = most_dominant[most_dominant['wins'] >= 2].sort_values('wins', ascending=False)

print("=== Most Dominant Drivers by Circuit ===")
print(most_dominant.head(20).to_string(index=False))
"""))

    # ---------------------------------------------------------------------------
    # SECTION 8 — HISTORICAL TRENDS
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 8. Historical Trends — Reliability, Parity & Calendar Expansion"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Chart 8a — DNF rate by decade
dnf_decade = df.groupby('decade').agg(
    total   = ('is_dnf', 'count'),
    dnfs    = ('is_dnf', 'sum')
).reset_index()
dnf_decade['dnf_pct'] = (dnf_decade['dnfs'] / dnf_decade['total'] * 100).round(2)

fig = px.bar(dnf_decade, x='decade', y='dnf_pct',
             color='dnf_pct', color_continuous_scale='RdYlGn_r',
             title='DNF Rate Evolution by Decade (Technical Reliability)',
             labels={'dnf_pct': 'DNF %', 'decade': 'Decade'})
fig.update_coloraxes(showscale=False)
fig.show()

# Chart 8b — Races per season
gps_per_season = df.groupby('season')['round'].max().reset_index(name='num_gps')
fig = px.area(gps_per_season, x='season', y='num_gps',
              title='Formula 1 Calendar Expansion: Grand Prix per Season',
              labels={'num_gps': 'Number of Races', 'season': 'Year'})
fig.update_traces(line_color=C_RED, fillcolor='rgba(225,6,0,0.2)')
fig.show()

# Chart 8c — Unique winners per season (competitiveness index)
unique_winners = df[df['is_win']].groupby('season')['driver_fullname'].nunique().reset_index(name='unique_winners')
fig = px.line(unique_winners, x='season', y='unique_winners', markers=True,
              title='Competitive Diversity: Unique Race Winners per Season',
              labels={'unique_winners': 'Distinct Winners', 'season': 'Year'})
fig.add_hline(y=unique_winners['unique_winners'].mean(), line_dash='dash',
              annotation_text=f"Average: {unique_winners['unique_winners'].mean():.1f}",
              annotation_position='top right')
fig.update_traces(line_color=C_GOLD)
fig.show()

# Chart 8d — Most dominant season (highest win % by a single driver)
season_dom = df.groupby(['season', 'driver_fullname']).agg(
    wins    = ('is_win', 'sum'),
    entries = ('position', 'count')
).reset_index()
season_dom['win_pct'] = season_dom['wins'] / season_dom['entries'] * 100
# Find the most dominant driver each season
best_season = season_dom.loc[season_dom.groupby('season')['win_pct'].idxmax()].sort_values('win_pct', ascending=False)

print("=== Most Dominant Single-Season Performances (Win %) ===")
print(best_season.head(15)[['season','driver_fullname','wins','entries','win_pct']].to_string(index=False))
"""))

    # ---------------------------------------------------------------------------
    # SECTION 9 — NATIONALITY & GEOGRAPHY
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell("## 9. Nationality & Geography"))
    cells.append(nbf.v4.new_code_cell(r"""
# Nationality requires driver table nationality column
if 'nationality' in df_drivers.columns:
    nat_df = df.merge(df_drivers[['driverId','nationality']], on='driverId', how='left')
    nat_wins = nat_df[nat_df['is_win']].groupby('nationality').size().reset_index(name='wins')
    nat_entries = nat_df.groupby('nationality')['position'].count().reset_index(name='entries')
    nat_stats = nat_wins.merge(nat_entries, on='nationality')
    nat_stats['win_pct'] = (nat_stats['wins'] / nat_stats['entries'] * 100).round(2)
    nat_stats = nat_stats.sort_values('wins', ascending=False)

    # Chart 9a — Wins by nationality
    fig = px.bar(
        nat_stats.head(15).sort_values('wins'),
        x='wins', y='nationality', orientation='h',
        color='wins', color_continuous_scale='Blues',
        title='Grand Prix Victories by Driver Nationality — Top 15',
        labels={'wins': 'Victories', 'nationality': ''}
    )
    fig.update_coloraxes(showscale=False)
    fig.update_layout(height=550)
    fig.show()

    # Chart 9b — Pie chart of wins distribution
    fig = px.pie(nat_stats.head(10), values='wins', names='nationality',
                 title='Share of Grand Prix Victories by Nation (Top 10)',
                 color_discrete_sequence=px.colors.qualitative.Dark24)
    fig.show()
else:
    print("Nationality data not available in drivers table.")
"""))

    # ---------------------------------------------------------------------------
    # SECTION 10 — CURIOSITIES & EDGE CASES
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 10. Curiosities & Edge Cases\n\n"
        "*Rare feats, unlucky records, and unusual statistical patterns.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# --- 10a: Most podiums without a win ---
no_wins = drv[drv['wins'] == 0].sort_values('podiums', ascending=False).head(10)
print("=== Most Podiums Without a Single Victory ===")
print(no_wins[['driver','entries','podiums','total_points']].to_string(index=False))

# Chart 10a
fig = px.bar(
    no_wins.sort_values('podiums'),
    x='podiums', y='driver', orientation='h',
    color='podiums', color_continuous_scale='Oranges',
    title='Most Podiums Without a Race Win',
    labels={'podiums': 'Career Podiums', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.show()

# --- 10b: Most constructors driven for (most nomadic) ---
constructors_driven = df.groupby('driver_fullname')['constructor_name'].nunique().reset_index(
    name='num_constructors').sort_values('num_constructors', ascending=False)
print()
print("=== Most Constructors Driven For ===")
print(constructors_driven.head(10).to_string(index=False))

# --- 10c: Most GP starts before first win (perseverance) ---
first_win_round = df[df['is_win']].groupby('driver_fullname').agg(
    first_win_season = ('season', 'min')
).reset_index()
# Count entries before first win
races_before_win = []
for _, row in first_win_round.iterrows():
    driver = row['driver_fullname']
    first_win_yr = row['first_win_season']
    entries_before = df[(df['driver_fullname'] == driver) & (df['season'] <= first_win_yr)].shape[0]
    races_before_win.append({'driver': driver, 'first_win_year': first_win_yr, 'starts_to_first_win': entries_before})

rbw_df = pd.DataFrame(races_before_win).sort_values('starts_to_first_win', ascending=False).head(15)

print()
print("=== Most GP Starts Before First Victory ===")
print(rbw_df.to_string(index=False))

# Chart 10c
fig = px.bar(
    rbw_df.sort_values('starts_to_first_win'),
    x='starts_to_first_win', y='driver', orientation='h',
    color='starts_to_first_win', color_continuous_scale='Purples',
    title='GP Starts Before First Career Victory',
    labels={'starts_to_first_win': 'Starts Before First Win', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=550)
fig.show()

# --- 10d: Retirement cause breakdown ---
dnf_causes = df[df['is_dnf']].groupby('status').size().sort_values(ascending=False).head(20)
fig = px.bar(
    dnf_causes.reset_index().rename(columns={'index': 'cause', 'status': 'cause', 0: 'count'}),
    x=dnf_causes.values, y=dnf_causes.index, orientation='h',
    color=dnf_causes.values, color_continuous_scale='Reds',
    title='DNF Causes — Historical Breakdown',
    labels={'x': 'Occurrences', 'y': 'Retirement Cause'}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

# --- 10e: Comeback wins (largest grid position that resulted in a win) ---
comeback_wins = df[df['is_win']].sort_values('grid', ascending=False).head(15)[
    ['driver_fullname', 'season', 'raceName', 'grid', 'position']
].rename(columns={'driver_fullname': 'driver', 'grid': 'start_position'})

print()
print("=== Greatest Comeback Victories (Started Furthest From Pole) ===")
print(comeback_wins.to_string(index=False))
fig = px.bar(
    comeback_wins.sort_values('start_position'),
    x='start_position',
    y=comeback_wins['driver'] + ' (' + comeback_wins['season'].astype(str) + ' ' + comeback_wins['raceName'] + ')',
    orientation='h',
    color='start_position', color_continuous_scale='Reds',
    title='Greatest Comeback Victories (Starting Position When Winning)',
    labels={'x': 'Grid Position', 'y': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()
"""))

    # ---------------------------------------------------------------------------
    # SECTION 11 — CONSECUTIVE STREAKS
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 11. Consecutive Streaks\n\n"
        "*The longest uninterrupted runs of elite performance — the hardest records to match.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Sort chronologically
df_sorted = df.sort_values(['driver_fullname', 'date']).copy()

def longest_streak(series):
    # Compute the longest consecutive True run in a boolean series
    max_streak = streak = 0
    for val in series:
        if val:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak

# Win streaks
win_streaks = df_sorted.groupby('driver_fullname')['is_win'].apply(longest_streak).reset_index()
win_streaks.columns = ['driver', 'longest_win_streak']

# Podium streaks
pod_streaks = df_sorted.groupby('driver_fullname')['on_podium'].apply(longest_streak).reset_index()
pod_streaks.columns = ['driver', 'longest_podium_streak']

# Points streaks (top-10)
pts_streaks = df_sorted.groupby('driver_fullname')['in_points'].apply(longest_streak).reset_index()
pts_streaks.columns = ['driver', 'longest_points_streak']

# Merge
all_streaks = win_streaks.merge(pod_streaks, on='driver').merge(pts_streaks, on='driver')
all_streaks = all_streaks.merge(drv[['driver', 'entries']], on='driver')

# Chart 11a — Longest win streaks
fig = px.bar(
    all_streaks.sort_values('longest_win_streak', ascending=False).head(15).sort_values('longest_win_streak'),
    x='longest_win_streak', y='driver', orientation='h',
    color='longest_win_streak', color_continuous_scale='Reds',
    title='Longest Consecutive Win Streaks',
    labels={'longest_win_streak': 'Consecutive Wins', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()

# Chart 11b — Longest podium streaks
fig = px.bar(
    all_streaks.sort_values('longest_podium_streak', ascending=False).head(15).sort_values('longest_podium_streak'),
    x='longest_podium_streak', y='driver', orientation='h',
    color='longest_podium_streak', color_continuous_scale='RdYlGn',
    title='Longest Consecutive Podium Streaks',
    labels={'longest_podium_streak': 'Consecutive Podiums', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()

# Chart 11c — Longest points streaks (top-10 finishes)
fig = px.bar(
    all_streaks.sort_values('longest_points_streak', ascending=False).head(15).sort_values('longest_points_streak'),
    x='longest_points_streak', y='driver', orientation='h',
    color='longest_points_streak', color_continuous_scale='Blues',
    title='Longest Consecutive Points Finish Streaks (Top-10)',
    labels={'longest_points_streak': 'Consecutive Points Finishes', 'driver': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()

all_streaks.sort_values('longest_win_streak', ascending=False).head(15)[
    ['driver', 'entries', 'longest_win_streak', 'longest_podium_streak', 'longest_points_streak']
]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 12 — TEAMMATE HEAD-TO-HEAD
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 12. Teammate Head-to-Head\n\n"
        "*Within the same team and same race, who beats their teammate most consistently? "
        "The purest measure of driver quality, removing the car variable.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Build teammate pairings per race
race_results = df[df['position'].notna() & (df['position'] > 0)].copy()
race_ids = ['season', 'round', 'constructor_name']

# Self-join to get all driver pairs in the same race & team
pairs = race_results.merge(
    race_results[race_ids + ['driver_fullname', 'position']],
    on=race_ids, suffixes=('_a', '_b')
)
# Keep only pairs where driver A beats driver B (avoids duplicates and self-match)
pairs = pairs[pairs['driver_fullname_a'] < pairs['driver_fullname_b']].copy()
pairs['a_won'] = pairs['position_a'] < pairs['position_b']

htm = pairs.groupby(['driver_fullname_a', 'driver_fullname_b']).agg(
    races = ('a_won', 'count'),
    a_wins = ('a_won', 'sum'),
).reset_index()
htm['b_wins'] = htm['races'] - htm['a_wins']
htm['a_win_pct'] = (htm['a_wins'] / htm['races'] * 100).round(1)
htm['label'] = htm['driver_fullname_a'] + '  vs  ' + htm['driver_fullname_b']

# Only pairs with 10+ shared races for statistical relevance
htm_sig = htm[htm['races'] >= 10].copy()
htm_sig['dominance'] = htm_sig['a_win_pct'].apply(lambda x: x if x >= 50 else 100 - x)
htm_sig['dominant_driver'] = htm_sig.apply(
    lambda r: r['driver_fullname_a'] if r['a_wins'] >= r['b_wins'] else r['driver_fullname_b'], axis=1
)

# Chart 12a — Most one-sided rivalries (min 10 races together)
most_lopsided = htm_sig.sort_values('dominance', ascending=False).head(20)
fig = px.bar(
    most_lopsided.sort_values('dominance'),
    x='dominance', y='label', orientation='h',
    color='dominance', color_continuous_scale='RdYlGn',
    hover_data=['races', 'a_wins', 'b_wins'],
    title='Most One-Sided Teammate Battles (Min. 10 Shared Races)',
    labels={'dominance': 'Win % of Dominant Driver', 'label': ''}
)
fig.add_vline(x=50, line_dash='dash', line_color='white', opacity=0.3)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Table — All significant rivalries sorted by dominance
htm_display = htm_sig.sort_values('dominance', ascending=False).head(25)
htm_display[['label', 'races', 'a_wins', 'b_wins', 'a_win_pct', 'dominant_driver']]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 13 — CHAMPIONSHIP BATTLES
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 13. Season Championship Battles\n\n"
        "*How close were the title fights? Seasons decided by the narrowest margins.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Season-end standings: total points per driver per season
season_pts = df.groupby(['season', 'driver_fullname'])['points'].sum().reset_index()
season_pts = season_pts.sort_values(['season', 'points'], ascending=[True, False])

# Top-2 per season
top2 = season_pts.groupby('season').head(2).groupby('season').agg(
    champion = ('driver_fullname', 'first'),
    runner_up = ('driver_fullname', lambda x: x.iloc[1] if len(x) > 1 else 'N/A'),
    champion_pts = ('points', 'first'),
    runner_up_pts = ('points', lambda x: x.iloc[1] if len(x) > 1 else 0),
).reset_index()
top2['gap_pts'] = top2['champion_pts'] - top2['runner_up_pts']
top2 = top2.sort_values('gap_pts')

# Chart 13a — Narrowest title fights
fig = px.bar(
    top2.head(20).sort_values('gap_pts', ascending=False),
    x='gap_pts', y='season', orientation='h',
    color='gap_pts', color_continuous_scale='RdYlGn',
    hover_data=['champion', 'runner_up', 'champion_pts', 'runner_up_pts'],
    title='Narrowest Championship Battles (Smallest Points Gap, Top-20)',
    labels={'gap_pts': 'Points Gap (Champion - Runner-Up)', 'season': 'Season'},
    text='gap_pts'
)
fig.update_traces(textposition='outside')
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Chart 13b — Most dominant seasons (largest gap)
fig = px.bar(
    top2.sort_values('gap_pts', ascending=False).head(20).sort_values('gap_pts'),
    x='gap_pts', y='season', orientation='h',
    color='gap_pts', color_continuous_scale='Reds',
    hover_data=['champion', 'runner_up'],
    title='Most Dominant Championship Winning Margins (Top-20)',
    labels={'gap_pts': 'Points Gap', 'season': 'Season'},
    text=top2.sort_values('gap_pts', ascending=False).head(20)['champion']
)
fig.update_traces(textposition='outside')
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

top2.head(20)[['season', 'champion', 'runner_up', 'champion_pts', 'runner_up_pts', 'gap_pts']]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 14 — CONSTRUCTOR 1-2 FINISHES
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 14. Constructor 1-2 Finishes\n\n"
        "*When both cars finish first and second — the ultimate expression of team dominance.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Find races where a team scored positions 1 and 2
pos_12 = df[df['position'].isin([1, 2])].copy()
race_team_positions = pos_12.groupby(['season', 'round', 'constructor_name'])['position'].apply(set).reset_index()
race_team_positions['is_oneTwo'] = race_team_positions['position'].apply(lambda s: {1, 2}.issubset(s))

one_twos = race_team_positions[race_team_positions['is_oneTwo']]
one_two_stats = one_twos.groupby('constructor_name').size().reset_index(name='one_two_count')
one_two_stats = one_two_stats.sort_values('one_two_count', ascending=False)

# Merge with total wins for context
one_two_stats = one_two_stats.merge(const[['constructor', 'wins']], left_on='constructor_name', right_on='constructor', how='left')
one_two_stats['one_two_pct_of_wins'] = (one_two_stats['one_two_count'] / one_two_stats['wins'] * 100).round(1)

# Chart 14a — Most 1-2 finishes
fig = px.bar(
    one_two_stats.head(15).sort_values('one_two_count'),
    x='one_two_count', y='constructor_name', orientation='h',
    color='one_two_count', color_continuous_scale='Blues',
    title='Constructor 1-2 Finishes — All Time',
    labels={'one_two_count': 'Number of 1-2s', 'constructor_name': ''},
    text='one_two_count'
)
fig.update_traces(textposition='outside')
fig.update_coloraxes(showscale=False)
fig.update_layout(height=500)
fig.show()

# Season breakdown — most 1-2s in a single season
season_onetwos = one_twos.groupby(['season', 'constructor_name']).size().reset_index(name='count')
best_seasons = season_onetwos.sort_values('count', ascending=False).head(15)
best_seasons['label'] = best_seasons['constructor_name'] + ' (' + best_seasons['season'].astype(str) + ')'

fig = px.bar(
    best_seasons.sort_values('count'),
    x='count', y='label', orientation='h',
    color='count', color_continuous_scale='Oranges',
    title='Most 1-2 Finishes in a Single Season',
    labels={'count': '1-2 Finishes', 'label': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=550)
fig.show()

one_two_stats.head(15)[['constructor_name', 'one_two_count', 'wins', 'one_two_pct_of_wins']]
"""))

    # ---------------------------------------------------------------------------
    # SECTION 15 — POINTS EFFICIENCY PER SEASON
    # ---------------------------------------------------------------------------
    cells.append(nbf.v4.new_markdown_cell(
        "## 15. Points Efficiency Per Season\n\n"
        "*Which driver extracted the highest percentage of the theoretically available points "
        "in a given season? This caps the scoring at 25 pts/race × N races.*"
    ))
    cells.append(nbf.v4.new_code_cell(r"""
# Max points per season (modern scoring: 25 pts/race max)
# We use 26 as the max per race to account for the 1 bonus fastest lap point
MAX_PTS_PER_RACE = 26

gps_per_season_map = df.groupby('season')['round'].max().to_dict()

season_pts_drv = df.groupby(['season', 'driver_fullname'])['points'].sum().reset_index()
season_pts_drv['max_available'] = season_pts_drv['season'].map(gps_per_season_map) * MAX_PTS_PER_RACE
season_pts_drv['pts_efficiency_%'] = (season_pts_drv['points'] / season_pts_drv['max_available'] * 100).round(2)

# Only modern era (2010+) for comparability
modern_eff = season_pts_drv[season_pts_drv['season'] >= 2010].sort_values('pts_efficiency_%', ascending=False)

# Chart 15a — Most efficient seasons (modern era)
top_eff = modern_eff.head(20).copy()
top_eff['label'] = top_eff['driver_fullname'] + ' (' + top_eff['season'].astype(str) + ')'

fig = px.bar(
    top_eff.sort_values('pts_efficiency_%'),
    x='pts_efficiency_%', y='label', orientation='h',
    color='pts_efficiency_%', color_continuous_scale='RdYlGn',
    title='Most Efficient Championship Seasons (2010–Present)',
    labels={'pts_efficiency_%': 'Points Efficiency %', 'label': ''},
    text=top_eff['pts_efficiency_%'].astype(str) + '%',
    hover_data=['points', 'max_available']
)
fig.update_traces(textposition='outside')
fig.update_coloraxes(showscale=False)
fig.update_layout(height=700)
fig.show()

# Chart 15b — Average efficiency per driver (modern era, min 3 seasons)
avg_eff = modern_eff.groupby('driver_fullname').agg(
    avg_efficiency = ('pts_efficiency_%', 'mean'),
    seasons = ('season', 'nunique'),
    total_pts = ('points', 'sum')
).reset_index()
avg_eff_filtered = avg_eff[avg_eff['seasons'] >= 3].sort_values('avg_efficiency', ascending=False)

fig = px.bar(
    avg_eff_filtered.head(20).sort_values('avg_efficiency'),
    x='avg_efficiency', y='driver_fullname', orientation='h',
    color='avg_efficiency', color_continuous_scale='Blues',
    title='Average Points Efficiency (Modern Era, Min. 3 Seasons)',
    labels={'avg_efficiency': 'Avg Efficiency %', 'driver_fullname': ''}
)
fig.update_coloraxes(showscale=False)
fig.update_layout(height=600)
fig.show()

modern_eff.head(20)[['season', 'driver_fullname', 'points', 'max_available', 'pts_efficiency_%']]
"""))

    # ---------------------------------------------------------------------------
    # WRITE NOTEBOOK
    # ---------------------------------------------------------------------------
    nb.cells = cells

    notebook_path = Path("notebooks/f1_statistical_analysis.ipynb")
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)

    print(f"Notebook written to: {notebook_path}")
    print(f"Sections: 15  |  Charts: 40+  |  Tables: 15+")



if __name__ == "__main__":
    generate_f1_statistical_report()
