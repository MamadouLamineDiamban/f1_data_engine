import nbformat as nbf
from pathlib import Path

def create_f1_analysis_notebook():
    nb = nbf.v4.new_notebook()

    # --- CELL 1: SETUP ---
    header = """# 🏁 F1 Data Engine - Analyse Statistique Gold
Ce notebook présente une analyse approfondie de l'histoire de la Formule 1 (1950-2025). 
L'objectif est de transformer nos données "Silver" en insights visuels premium.
"""
    nb.cells.append(nbf.v4.new_markdown_cell(header))

    setup_code = """import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from src.config import PROCESSED_DATA_DIR

# Configuration du theme Plotly pour un look "Premium F1"
px.defaults.template = "plotly_dark"
px.defaults.color_continuous_scale = px.colors.sequential.Reds
f1_red = "#E10600"

# Chargement des donnees unifiees
df_results = pd.read_parquet(PROCESSED_DATA_DIR / "results.parquet")
df_drivers = pd.read_parquet(PROCESSED_DATA_DIR / "drivers.parquet")
df_constructors = pd.read_parquet(PROCESSED_DATA_DIR / "constructors.parquet")

print(f"Base de donnees chargee : {len(df_results)} lignes de resultats.")
"""
    nb.cells.append(nbf.v4.new_code_cell(setup_code))

    # --- CELL 2: DRIVER STATS (TOTALS) ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 🏆 1. Le Panthéon des Pilotes (Hall of Fame)"))
    
    driver_totals = r"""# Calcul des victoires par pilote
wins = df_results[df_results['position'] == 1].groupby('driver_fullname').size().reset_index(name='Wins')
wins = wins.sort_values(by='Wins', ascending=False)

# Creation du graphique
fig_wins = px.bar(wins.head(20), x='Wins', y='driver_fullname', 
                 title="Top 20 des plus grands vainqueurs de l'histoire",
                 orientation='h', color='Wins',
                 color_continuous_scale='Reds',
                 labels={'driver_fullname': 'Pilote', 'Wins': 'Nombre de Victoires'})
fig_wins.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
fig_wins.show()

# Affichage du tableau
wins.head(10)
"""
    nb.cells.append(nbf.v4.new_code_cell(driver_totals))

    # --- CELL 3: PODIUMS ---
    podiums_code = r"""# Calcul des podiums (Positions 1, 2, 3)
podiums = df_results[df_results['position'] <= 3].groupby('driver_fullname').size().reset_index(name='Podiums')
podiums = podiums.sort_values(by='Podiums', ascending=False)

fig_podiums = px.bar(podiums.head(20), x='Podiums', y='driver_fullname', 
                    title="Top 20 des rois du podium",
                    orientation='h', color='Podiums',
                    color_continuous_scale='Tealgrn')
fig_podiums.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
fig_podiums.show()

podiums.head(10)
"""
    nb.cells.append(nbf.v4.new_code_cell(podiums_code))

    # --- CELL 4: EFFICIENCY RATE ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 🎯 2. L'Indice d'Efficacité (Win Rate %)\n*Qui est le plus grand, proportionnellement à ses départs ? (Min 50 GP)*"))
    
    efficiency_code = r"""# On compte le nombre de departs par pilote
gp_starts = df_results.groupby('driver_fullname').size().reset_index(name='Starts')

# Jointure avec les victoires
efficiency = pd.merge(wins, gp_starts, on='driver_fullname')
efficiency['Win_Rate_%'] = (efficiency['Wins'] / efficiency['Starts'] * 100).round(2)

# Filtre sur les pilotes ayant une carriere significative (min 50 GP)
top_efficiency = efficiency[efficiency['Starts'] >= 50].sort_values(by='Win_Rate_%', ascending=False)

fig_eff = px.scatter(top_efficiency.head(20), x='Starts', y='Win_Rate_%',
                    size='Wins', color='Win_Rate_%',
                    hover_name='driver_fullname', title="Efficacité : % de Victoires par GP disputé",
                    labels={'Starts': 'Nombre de GP disputés', 'Win_Rate_%': '% de Victoires'},
                    text='driver_fullname')
fig_eff.update_traces(textposition='top center')
fig_eff.show()

top_efficiency.head(10)
"""
    nb.cells.append(nbf.v4.new_code_cell(efficiency_code))

    # --- CELL 5: CONSTRUCTORS ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 🏎️ 3. Domination des Constructeurs"))
    
    constructor_code = r"""const_wins = df_results[df_results['position'] == 1].groupby('constructor_name').size().reset_index(name='Wins')
const_wins = const_wins.sort_values(by='Wins', ascending=False).head(15)

fig_const = px.pie(const_wins, values='Wins', names='constructor_name', 
                  title='Répartition des Victoires par Constructeur (Top 15)',
                  hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
fig_const.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(constructor_code))

    # --- CELL 6: RELIABILITY EVOLUTION ---
    nb.cells.append(nbf.v4.new_markdown_cell("## 📉 4. L'Évolution de la Fiabilité (DNF %)\n*Comment la Formule 1 est devenue une science exacte.*"))
    
    reliability_code = r"""# On définit un DNF comme toute position non numérique (R, D, N, etc.) ou statut specifique
# On va simplifier : tout ce qui n'est pas classé numeriquement dans la colonne 'position'
df_results['is_dnf'] = df_results['position'].isna()

dnf_trend = df_results.groupby('season')['is_dnf'].mean().reset_index()
dnf_trend['is_dnf'] *= 100 # Passage en pourcentage

fig_dnf = px.line(dnf_trend, x='season', y='is_dnf', 
                 title="Evolution du Taux d'Abandon (DNF %) par Saison",
                 labels={'season': 'Saison', 'is_dnf': '% d\'Abandons'},
                 line_shape='spline')
fig_dnf.update_traces(line_color=f1_red, line_width=3)
fig_dnf.add_hline(y=dnf_trend['is_dnf'].mean(), line_dash="dash", annotation_text="Moyenne Historique")
fig_dnf.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(reliability_code))

    # --- CELL 7: LONGEVITY ---
    nb.cells.append(nbf.v4.new_markdown_cell("## ⏳ 5. Les Iron Men de la F1\n*Les pilotes ayant disputé le plus de Grands Prix.*"))
    
    longevity_code = r"""longevity = gp_starts.sort_values(by='Starts', ascending=False).head(20)

fig_long = px.bar(longevity, x='Starts', y='driver_fullname', 
                 title='Longévité : Record de départs en Grand Prix',
                 orientation='h', color='Starts',
                 color_continuous_scale='Viridis')
fig_long.update_layout(yaxis={'categoryorder':'total ascending'})
fig_long.show()
"""
    nb.cells.append(nbf.v4.new_code_cell(longevity_code))

    # --- SAVE ---
    notebook_path = "notebooks/03_statistical_analysis.ipynb"
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    
    print(f"Notebook généré avec succès dans {notebook_path}")

if __name__ == "__main__":
    create_f1_analysis_notebook()
