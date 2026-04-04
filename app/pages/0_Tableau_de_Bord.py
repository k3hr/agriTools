"""
agriTools — Tableau de bord datalake
Page d'accueil qui affiche l'état de santé de toutes les sources.
"""
import streamlit as st
from datetime import date
from app.components.data import datalake_status

st.set_page_config(
    page_title="Tableau de bord",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Tableau de bord datalake")
st.caption("État de fraîcheur et couverture des données sources")

st.markdown("---")

# Charger l'état du datalake
status = datalake_status()

# Affichage en trois colonnes (une par source)
col1, col2, col3 = st.columns(3)

sources_config = {
    "meteo": {
        "col": col1,
        "icon": "📡",
        "title": "Météo Open-Meteo",
        "thresholds": {"fresh": 3, "warning": 7}
    },
    "prix": {
        "col": col2,
        "icon": "💶",
        "title": "Prix RNM (MIN)",
        "thresholds": {"fresh": 10, "warning": 20}
    },
    "rpg": {
        "col": col3,
        "icon": "🗺️",
        "title": "RPG (Parcelles)",
        "thresholds": {"fresh": 365, "warning": 500}
    }
}

for source_key, config in sources_config.items():
    data = status[source_key]
    col = config["col"]
    
    with col:
        # Carte conteneur avec status
        with st.container(border=True):
            st.subheader(f"{config['icon']} {config['title']}")
            
            # Statut principal
            st.markdown(f"### {data['status']}")
            
            # Chiffres clés
            col_a, col_b = st.columns(2)
            with col_a:
                last_update_str = str(data['last_update']) if data['last_update'] else "—"
                delta_str = f"{data['days_old']} j.ago" if data['days_old'] is not None else "N/A"
                st.text(f"📅 Dernière MAJ: {last_update_str}")
                st.text(f"⏱️ Âge: {delta_str}")
            with col_b:
                st.metric(
                    "Données",
                    f"{data['row_count']:,}",
                    "lignes"
                )
            
            # Alerte si nécessaire
            if data['alert']:
                st.warning(f"⚠️ {data['alert']}")

# Section détails enrichie
st.markdown("---")
st.header("📋 Détails par source")

# Onglets pour chaque source
tabs = st.tabs(["📡 Météo", "💶 Prix", "🗺️ RPG"])

with tabs[0]:  # Météo
    meteo_data = status["meteo"]
    col1, col2 = st.columns([1, 1])
    with col1:
        st.write("**Période couverte**")
        st.write(f"De {meteo_data['last_update']} (dernier jour avail.)")
    with col2:
        st.write("**Fréquence de mise à jour**")
        st.write("Quotidienne (6h du matin)")
    
    st.info(
        "**Open-Meteo** : données météo historiques (2021–2026) + prévisions 7j. "
        "Refresh automatique tous les jours avec recouvrement pour consolider les derniers jours."
    )

with tabs[1]:  # Prix
    prix_data = status["prix"]
    col1, col2 = st.columns([1, 1])
    with col1:
        st.write("**Années disponibles**")
        st.write("2024, 2025, 2026")
    with col2:
        st.write("**Fréquence de mise à jour**")
        st.write("Hebdomadaire (vendredi 7h du matin)")
    
    st.info(
        "**RNM (FranceAgriMer)** : cotations hebdomadaires de marchés de gros (MIN) français. "
        "Filtrage : marchés Bretagne + Rhône-Alpes, stade GROS + Expédition."
    )

with tabs[2]:  # RPG
    rpg_data = status["rpg"]
    col1, col2 = st.columns([1, 1])
    with col1:
        st.write("**Zones couvertes**")
        st.write("67 081 parcelles, Rayon 25 km du siège")
    with col2:
        st.write("**Fréquence de mise à jour**")
        st.write("Annuelle (IGN RPG 2023)")
    
    st.info(
        "**RPG (Registre Parcellaire Graphique)** : polygones de parcelles agricoles avec cultures "
        "déclarées. Données publiques IGN, usage open data reconnu."
    )

# Section commandes de maintenance
st.markdown("---")
st.header("🔧 Commandes de maintenance")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Refresh manuel")
    st.code("""
# Météo Open-Meteo
uv run python -m ingestion.meteo.open_meteo

# Prix RNM
uv run python -m ingestion.prix.rnm

# Les données sont actualisées dans datalake/processed/
    """, language="bash")

with col2:
    st.subheader("Vérification des données")
    st.code("""
# Résumé météo
uv run python -c "
import duckdb as dc
print(dc.sql('SELECT COUNT(*) FROM read_parquet(
    \"datalake/processed/meteo/*.parquet\"
)'))
"
    """, language="bash")

st.markdown("---")
st.caption("💡 Les tâches planifiées Windows se chargent automatiquement chaque jour/semaine.")
