"""
Dashboard météo — agriTools
Température, précipitations, ETP sur fenêtre glissante configurable.
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import streamlit as st

from app.components.data import load_meteo, meteo_date_range

# ---------------------------------------------------------------------------
# Config page
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Météo — agriTools", page_icon="🌤️", layout="wide")
st.title("🌤️ Météo locale")

# ---------------------------------------------------------------------------
# Sidebar — sélecteur de période
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Période")

    d_min, d_max = meteo_date_range()

    periode = st.radio(
        "Fenêtre",
        ["30 jours", "90 jours", "1 an", "5 ans", "Personnalisée"],
        index=2,
    )

    if periode == "30 jours":
        start = d_max - timedelta(days=30)
        end = d_max
    elif periode == "90 jours":
        start = d_max - timedelta(days=90)
        end = d_max
    elif periode == "1 an":
        start = d_max - timedelta(days=365)
        end = d_max
    elif periode == "5 ans":
        start = d_min
        end = d_max
    else:
        start = st.date_input("Début", value=d_max - timedelta(days=365), min_value=d_min, max_value=d_max)
        end = st.date_input("Fin", value=d_max, min_value=d_min, max_value=d_max)
        if start >= end:
            st.error("La date de début doit être antérieure à la date de fin.")
            st.stop()

    st.caption(f"Données disponibles : {d_min} → {d_max}")

    st.header("Affichage")
    show_wind = st.checkbox("Vent", value=False)
    show_radiation = st.checkbox("Rayonnement solaire", value=False)
    rolling_days = st.slider("Moyenne mobile (jours)", min_value=1, max_value=30, value=7)

# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------
with st.spinner("Chargement..."):
    df = load_meteo(start=start, end=end)

if df.is_empty():
    st.warning("Aucune donnée météo sur cette période.")
    st.stop()

n_jours = len(df)
n_manquants = df["t_max"].null_count()

# ---------------------------------------------------------------------------
# Métriques clés
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Période", f"{n_jours} jours")
with col2:
    t_moy = df["t_moy"].mean()
    st.metric("T° moyenne", f"{t_moy:.1f} °C")
with col3:
    t_max_abs = df["t_max"].max()
    st.metric("T° max absolue", f"{t_max_abs:.1f} °C")
with col4:
    pluie_tot = df["pluie_mm"].sum()
    st.metric("Précipitations", f"{pluie_tot:.0f} mm")
with col5:
    etp_tot = df["etp_mm"].sum()
    st.metric("ETP cumulée", f"{etp_tot:.0f} mm")

st.markdown("---")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def with_rolling(df: pl.DataFrame, col: str, n: int) -> pl.DataFrame:
    """Ajoute une colonne de moyenne mobile."""
    alias = f"{col}_roll{n}"
    return df.with_columns(
        pl.col(col).rolling_mean(window_size=n).alias(alias)
    )


# ---------------------------------------------------------------------------
# Températures
# ---------------------------------------------------------------------------
st.subheader("Températures (°C)")

df_temp = df.select(["date", "t_max", "t_min", "t_moy"])
if rolling_days > 1:
    df_temp = with_rolling(df_temp, "t_moy", rolling_days)
    df_temp = with_rolling(df_temp, "t_max", rolling_days)
    df_temp = with_rolling(df_temp, "t_min", rolling_days)
    chart_cols = [f"t_max_roll{rolling_days}", f"t_moy_roll{rolling_days}", f"t_min_roll{rolling_days}"]
    labels = {"value": "°C", "variable": ""}
else:
    chart_cols = ["t_max", "t_moy", "t_min"]

st.line_chart(
    df_temp.select(["date"] + chart_cols).to_pandas().set_index("date"),
    color=["#e74c3c", "#f39c12", "#3498db"],
)

# Jours de gel
gel = df.filter(pl.col("t_min") < 0)
n_gel = len(gel)
if n_gel:
    st.caption(f"❄️ {n_gel} jour(s) de gel (T° min < 0°C) sur la période")

st.markdown("---")

# ---------------------------------------------------------------------------
# Précipitations
# ---------------------------------------------------------------------------
st.subheader("Précipitations (mm)")

df_pluie = df.select(["date", "pluie_mm"]).to_pandas().set_index("date")
st.bar_chart(df_pluie, color="#2980b9")

# Cumul mensuel
st.caption("Cumul mensuel")
df_mensuel = (
    df.with_columns(pl.col("date").dt.truncate("1mo").alias("mois"))
    .group_by("mois")
    .agg(pl.col("pluie_mm").sum().round(1).alias("pluie_mm"))
    .sort("mois")
    .with_columns(pl.col("mois").cast(pl.Utf8))
)
st.bar_chart(df_mensuel.to_pandas().set_index("mois"), color="#2980b9")

st.markdown("---")

# ---------------------------------------------------------------------------
# ETP — évapotranspiration de référence
# ---------------------------------------------------------------------------
st.subheader("ETP FAO-56 (mm) — Évapotranspiration de référence")

df_etp = df.select(["date", "etp_mm"])
if rolling_days > 1:
    df_etp = with_rolling(df_etp, "etp_mm", rolling_days)
    etp_col = f"etp_mm_roll{rolling_days}"
else:
    etp_col = "etp_mm"

st.line_chart(
    df_etp.select(["date", etp_col]).to_pandas().set_index("date"),
    color=["#27ae60"],
)

# Bilan hydrique simplifié pluie - ETP
st.caption("Bilan hydrique (Pluie − ETP, mm)")
df_bilan = df.with_columns(
    (pl.col("pluie_mm") - pl.col("etp_mm")).alias("bilan_mm")
).select(["date", "bilan_mm"])
st.bar_chart(df_bilan.to_pandas().set_index("date"), color="#8e44ad")

st.markdown("---")

# ---------------------------------------------------------------------------
# Vent (optionnel)
# ---------------------------------------------------------------------------
if show_wind:
    st.subheader("Vent maximal (km/h)")
    df_vent = df.select(["date", "vent_kmh"]).to_pandas().set_index("date")
    st.line_chart(df_vent, color=["#95a5a6"])
    st.markdown("---")

# ---------------------------------------------------------------------------
# Rayonnement (optionnel)
# ---------------------------------------------------------------------------
if show_radiation:
    st.subheader("Rayonnement solaire (MJ/m²)")
    df_ray = df.select(["date", "rayonnement_mj"]).to_pandas().set_index("date")
    st.line_chart(df_ray, color=["#f1c40f"])
    st.markdown("---")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
if n_manquants:
    st.caption(f"⚠️ {n_manquants} valeurs manquantes dans la période (lag API Open-Meteo).")
st.caption(f"Source : Open-Meteo archive | Coordonnées ferme : chargées depuis config.local.toml")
