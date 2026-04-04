"""
agriTools — Application Streamlit
Point d'entrée : streamlit run app/main.py
"""
import streamlit as st

st.set_page_config(
    page_title="agriTools",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded",
)

from app.components.data import meteo_date_range

st.title("🌱 agriTools")
st.caption("Suite d'outils pour le pilotage d'une ferme maraîchère")

st.markdown("---")

col1, col2, col3 = st.columns(3)

try:
    d_min, d_max = meteo_date_range()
    with col1:
        st.metric("📡 Météo", f"{d_min.year} → {d_max.year}", f"{(d_max - d_min).days} jours")
except Exception:
    with col1:
        st.metric("📡 Météo", "—", "non disponible")

with col2:
    st.metric("💶 Prix MIN", "2024 → 2026", "RNM / FranceAgriMer")

with col3:
    st.metric("🗺️ RPG", "2023", "67 081 parcelles")

st.markdown("---")
st.info("👈 Naviguez via le menu latéral.")
