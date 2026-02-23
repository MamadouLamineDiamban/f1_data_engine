import nbformat as nbf
from pathlib import Path

def create_f1_analysis_notebook():
    nb = nbf.v4.new_notebook()

    header = """# 🏁 F1 Data Engine - Global Statistical Portfolio (1950-2025)
Cette analyse exhaustive explore plus de 75 ans d'histoire de la Formule 1. 
Elle est conçue pour offrir des perspectives pertinentes aussi bien pour les novices que pour les vétérans.
"""
    nb.cells.append(nbf.v4.new_markdown_cell(header))

    setup_code = r"""import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from src.config import PROCESSED_DATA_DIR

# Configuration Esthétique
px.defaults.template = "plotly_dark"
color_f1 = "#E10600"
color_silver = "#C0C0C0"
color_gold = "#D4AF37"

# Chargement Silver Layer
df_results = pd.read_parquet(PROCESSED_DATA_DIR / "results.parquet")
df_drivers = pd.read_parquet(PROCESSED_DATA_DIR / "drivers.parquet")
df_constructors = pd.read_parquet(PROCESSED_DATA_DIR / "constructors.parquet")

# Nettoyage initial : on ne garde que les positions numeriques pour certains calculs
df_finishes = df_results[df_results['position'].notna()].copy()
df_finishes['position'] = df_finishes['position'].astype(int)
df_finishes['grid'] = df_finishes['grid'].fillna(0).astype(int)

print(f"Base chargée : {len(df_results)} résultats analysés.")
"""
    nb.cells.append(nbf.v4.new_code_cell(setup_code))

    # --- SECTION : PILOTES - RECORDS ABSOLUS ---
    nb.cells.append(nbf.v4.new_markdown_cell("## � I. PILOTES : Les Records Absolus"))
    
    drivers_absolute = r"""# Agregation des stats par pilote
d_stats = df_results.groupby('driver_fullname').agg({
    'position': [lambda x: (x == 1).sum(), lambda x: (x <= 3).sum(), lambda x: (x <= 10).sum()],
    'points': 'sum',
    'season': ['nunique', 'count'],
    'fastest_lap_rank': lambda x: (x == '1').sum()
}).reset_index()

d_stats.columns = ['driver', 'wins', 'podiums', 'points_finishes', 'total_points', 'seasons', 'starts', 'fast_laps']
d_stats = d_stats.sort_values(by='wins', ascending=False)

# Visualisation 1 : Top 20 Wins
fig1 = px.bar(d_stats.head(20), x='wins', y='driver', orientation='h', 
             title='Le Panthéon des Vainqueurs (Total)', 
             color='wins', color_continuous_scale='Reds',
             labels={'wins': 'Victoires', 'driver': 'Pilote'})
fig1.update_layout(yaxis={'categoryorder':'total ascending'})
fig1.show()

# Visualisation 2 : Relation Points vs Podiums
fig2 = px.scatter(d_stats[d_stats['starts'] > 10], x='total_points', y='podiums', 
                 size='wins', hover_name='driver',
                 title='Corrélation Points vs Podiums (Taille = Victoires)')
fig2.show()

d_stats.head(15)
"""
    nb.cells.append(nbf.v4.new_code_cell(drivers_absolute))

    # --- SECTION : PILOTES - EFFICIENCE & RATIOS ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 🎯 II. PILOTES : Efficience et Ratios (Min. 20 Starts)\n*C'est ici que l'on compare les époques.*"))
    
    drivers_efficiency = r"""# Calcul des ratios
d_eff = d_stats[d_stats['starts'] >= 20].copy()
d_eff['win_rate_%'] = (d_eff['wins'] / d_eff['starts'] * 100).round(2)
d_eff['podium_rate_%'] = (d_eff['podiums'] / d_eff['starts'] * 100).round(2)
d_eff['points_per_gp'] = (d_eff['total_points'] / d_eff['starts']).round(2)

# Graphique : Win Rate vs Podium Rate
fig3 = px.scatter(d_eff.sort_values(by='win_rate_%', ascending=False).head(30), 
                 x='podium_rate_%', y='win_rate_%', text='driver', size='wins',
                 title='Efficacité pure : Taux de Victoire vs Taux de Podium',
                 labels={'win_rate_%': '% Victoires', 'podium_rate_%': '% Podiums'})
fig3.update_traces(textposition='top center')
fig3.show()

d_eff.sort_values(by='win_rate_%', ascending=False).head(15)
"""
    nb.cells.append(nbf.v4.new_code_cell(drivers_efficiency))

    # --- SECTION : PILOTES - PERFORMANCE EN COURSE ---
    nb.cells.append(nbf.v4.new_markdown_cell("## ⚔️ III. PILOTES : Performance et Combativité"))
    
    drivers_race_craft = r"""# Calcul de la remontée moyenne (Positions gagnées)
# On exclut les abandons pour ce calcul precis
df_gains = df_finishes[df_finishes['grid'] > 0].copy()
df_gains['pos_gained'] = df_gains['grid'] - df_finishes['position']

gain_stats = df_gains.groupby('driver_fullname').agg({
    'pos_gained': 'mean',
    'grid': 'mean',
    'position': 'mean'
}).reset_index()

gain_stats.columns = ['driver', 'avg_pos_gained', 'avg_grid', 'avg_finish']
gain_stats = pd.merge(gain_stats, d_stats[['driver', 'starts']], on='driver')

# Top Remonteurs (Min 50 GP)
top_climbers = gain_stats[gain_stats['starts'] >= 50].sort_values(by='avg_pos_gained', ascending=False)

fig4 = px.bar(top_climbers.head(15), x='avg_pos_gained', y='driver', 
             title='Les Maîtres de la Remontée (Positions gagnées par GP en moyenne)',
             color='avg_pos_gained', color_continuous_scale='Greens')
fig4.update_layout(yaxis={'categoryorder':'total ascending'})
fig4.show()

gain_stats.sort_values(by='avg_grid').head(10) # Les meilleurs qualifieurs
"""
    nb.cells.append(nbf.v4.new_code_cell(drivers_race_craft))

    # --- SECTION : CONSTRUCTEURS ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 🏎️ IV. CONSTRUCTEURS : Domination Collective"))
    
    constructors_stats = r"""c_stats = df_results.groupby('constructor_name').agg({
    'position': [lambda x: (x == 1).sum(), lambda x: (x <= 3).sum()],
    'points': 'sum',
    'driverId': 'nunique',
    'season': 'nunique',
    'round': 'count'
}).reset_index()

c_stats.columns = ['constructor', 'wins', 'podiums', 'total_points', 'unique_drivers', 'seasons', 'total_entries']
c_stats = c_stats.sort_values(by='wins', ascending=False)

# Pie Chart des Victoires
fig5 = px.pie(c_stats.head(10), values='wins', names='constructor', 
             title='Part de gâteau des Victoires par Constructeur (Oligarchie F1)',
             hole=0.3)
fig5.show()

# Ratio de Fiabilité (Abandons par écurie)
dnf_const = df_results[df_results['position'].isna()].groupby('constructor_name').size().reset_index(name='dnf_count')
c_reliability = pd.merge(c_stats, dnf_const, left_on='constructor', right_on='constructor_name')
c_reliability['dnf_rate_%'] = (c_reliability['dnf_count'] / c_reliability['total_entries'] * 100).round(2)

fig6 = px.bar(c_reliability[c_reliability['total_entries'] > 100].sort_values(by='dnf_rate_%'), 
             x='dnf_rate_%', y='constructor', title='Écuries les plus fiables (Min 100 entrées)',
             orientation='h', color='dnf_rate_%', color_continuous_scale='RdYlGn_r')
fig6.update_layout(yaxis={'categoryorder':'total descending'}, height=600)
fig6.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(constructors_stats))

    # --- SECTION : TENDANCES HISTORIQUES ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 📉 V. TENDANCES : L'Évolution du Sport"))
    
    historical_trends = r"""# Abandons par décennie
df_results['decade'] = (df_results['season'] // 10) * 10
dnf_decade = df_results.groupby('decade').apply(lambda x: x['position'].isna().mean() * 100).reset_index(name='dnf_rate')

fig7 = px.area(dnf_decade, x='decade', y='dnf_rate', 
              title='Evolution du Taux d\'Abandon par Décennie',
              labels={'decade': 'Décennie', 'dnf_rate': '% DNF'},
              markers=True)
fig7.update_traces(fillcolor='rgba(225, 6, 0, 0.3)', line_color=color_f1)
fig7.show()

# Evolution du nombre de GP par saison
gp_season = df_results.groupby('season')['round'].nunique().reset_index(name='gp_count')
fig8 = px.line(gp_season, x='season', y='gp_count', title='Expansion du calendrier F1 (Nombre de GP / An)')
fig8.update_traces(line_color=color_silver)
fig8.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(historical_trends))

    # --- SECTION : CURIOSITÉS ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 🔍 VI. CURIOSITÉS : Records Insolites"))
    
    curiosities = r"""# Pilote ayant gagné pour le plus grand nombre d'écuries differentes
diff_const = df_results[df_results['position'] == 1].groupby('driver_fullname')['constructor_name'].nunique().reset_index()
diff_const = diff_const.sort_values(by='constructor_name', ascending=False)

fig9 = px.bar(diff_const.head(10), x='constructor_name', y='driver_fullname', 
             title='Le Mercato Gagnant : Victoires avec X constructeurs différents',
             labels={'constructor_name': 'Nombre de constructeurs distincts'})
fig9.show()

# Victoire avec la plus grande remontée (Grid vs Position 1)
big_comebacks = df_finishes[(df_finishes['position'] == 1) & (df_finishes['grid'] > 1)].copy()
big_comebacks['gain'] = big_comebacks['grid'] - 1
big_comebacks = big_comebacks.sort_values(by='gain', ascending=False)

print("Record de la plus grande remontée pour gagner :")
big_comebacks[['season', 'raceName', 'driver_fullname', 'grid', 'position']].head(5)
"""
    nb.cells.append(nbf.v4.new_code_cell(curiosities))

    # --- SAVE ---
    notebook_path = "notebooks/03_statistical_analysis.ipynb"
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    
    print(f"Portfolio statistique massif généré avec succès dans {notebook_path}")

if __name__ == "__main__":
    create_f1_analysis_notebook()
