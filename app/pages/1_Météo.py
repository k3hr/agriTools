"""
Dashboard météo — agriTools
Température, précipitations, ETP sur fenêtre glissante configurable.
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import streamlit as st

from app.components.data import (
    load_meteo,
    load_meteo_france,
    meteo_date_range,
    meteo_france_date_range,
)

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
# Footer Open-Meteo
# ---------------------------------------------------------------------------
if n_manquants:
    st.caption(f"⚠️ {n_manquants} valeurs manquantes dans la période (lag API Open-Meteo).")
st.caption("Source : Open-Meteo archive | Coordonnées ferme : chargées depuis config.local.toml")

# ===========================================================================
# Section Météo-France
# ===========================================================================
st.markdown("---")
st.header("🇫🇷 Données Météo-France (DPObs)")

mf_range = meteo_france_date_range()

if mf_range is None:
    st.info(
        "Aucune donnée Météo-France disponible. "
        "Lancer l'ingestion :\n\n"
        "```\npython -m ingestion.meteo.meteo_france\n```\n\n"
        "Une clé API est nécessaire : [portail-api.meteofrance.fr](https://portail-api.meteofrance.fr)"
    )
    st.stop()

mf_start, mf_end = mf_range

# Intersection entre la période sélectionnée et la disponibilité MF
mf_query_start = max(start, mf_start)
mf_query_end = min(end, mf_end)

if mf_query_start > mf_query_end:
    st.warning(
        f"Aucune donnée Météo-France sur la période sélectionnée. "
        f"Disponible du {mf_start} au {mf_end}."
    )
    st.stop()

st.caption(f"Données disponibles : {mf_start} → {mf_end}")

with st.spinner("Chargement Météo-France..."):
    df_mf = load_meteo_france(start=mf_query_start, end=mf_query_end)

if df_mf.is_empty():
    st.warning("Aucune donnée Météo-France sur cette période.")
    st.stop()

n_jours_mf = len(df_mf)
n_manquants_mf = df_mf["t_max"].null_count()

# ── Métriques MF ──────────────────────────────────────────────────────────
mf_cols = st.columns(5)
with mf_cols[0]:
    st.metric("Période", f"{n_jours_mf} jours")
with mf_cols[1]:
    st.metric("T° moyenne", f"{df_mf['t_moy'].mean():.1f} °C")
with mf_cols[2]:
    st.metric("T° max absolue", f"{df_mf['t_max'].max():.1f} °C")
with mf_cols[3]:
    st.metric("Précipitations", f"{df_mf['pluie_mm'].sum():.0f} mm")
with mf_cols[4]:
    etp_mf = df_mf["etp_mm"].drop_nulls()
    if len(etp_mf):
        st.metric("ETP cumulée", f"{etp_mf.sum():.0f} mm")
    else:
        st.metric("ETP cumulée", "—")

st.markdown("---")

# ── Températures MF ───────────────────────────────────────────────────────
st.subheader("Températures (°C)")

df_mf_temp = df_mf.select(["date", "t_max", "t_min", "t_moy"])
if rolling_days > 1:
    df_mf_temp = with_rolling(df_mf_temp, "t_moy", rolling_days)
    df_mf_temp = with_rolling(df_mf_temp, "t_max", rolling_days)
    df_mf_temp = with_rolling(df_mf_temp, "t_min", rolling_days)
    mf_temp_cols = [f"t_max_roll{rolling_days}", f"t_moy_roll{rolling_days}", f"t_min_roll{rolling_days}"]
else:
    mf_temp_cols = ["t_max", "t_moy", "t_min"]

st.line_chart(
    df_mf_temp.select(["date"] + mf_temp_cols),
    x="date",
    color=["#e74c3c", "#f39c12", "#3498db"],
)

gel_mf = df_mf.filter(pl.col("t_min") < 0)
if len(gel_mf):
    st.caption(f"❄️ {len(gel_mf)} jour(s) de gel (T° min < 0°C) sur la période")

st.markdown("---")

# ── Précipitations MF ─────────────────────────────────────────────────────
st.subheader("Précipitations (mm)")

st.bar_chart(df_mf.select(["date", "pluie_mm"]), x="date", y="pluie_mm", color="#2980b9")

st.caption("Cumul mensuel")
df_mf_mensuel = (
    df_mf.with_columns(pl.col("date").dt.truncate("1mo").alias("mois"))
    .group_by("mois")
    .agg(pl.col("pluie_mm").sum().round(1).alias("pluie_mm"))
    .sort("mois")
    .with_columns(pl.col("mois").cast(pl.Utf8))
)
st.bar_chart(df_mf_mensuel, x="mois", y="pluie_mm", color="#2980b9")

st.markdown("---")

# ── ETP MF ────────────────────────────────────────────────────────────────
if df_mf["etp_mm"].drop_nulls().len() > 0:
    st.subheader("ETP (mm)")

    df_mf_etp = df_mf.select(["date", "etp_mm"])
    if rolling_days > 1:
        df_mf_etp = with_rolling(df_mf_etp, "etp_mm", rolling_days)
        mf_etp_col = f"etp_mm_roll{rolling_days}"
    else:
        mf_etp_col = "etp_mm"

    st.line_chart(df_mf_etp.select(["date", mf_etp_col]), x="date", color=["#27ae60"])

    df_mf_bilan = df_mf.with_columns(
        (pl.col("pluie_mm") - pl.col("etp_mm")).alias("bilan_mm")
    ).select(["date", "bilan_mm"])
    st.caption("Bilan hydrique (Pluie − ETP, mm)")
    st.bar_chart(df_mf_bilan, x="date", y="bilan_mm", color="#8e44ad")

    st.markdown("---")

# ── Ensoleillement (colonne spécifique MF) ────────────────────────────────
if df_mf["ensoleillement_min"].drop_nulls().len() > 0:
    st.subheader("Ensoleillement (min/jour)")
    df_mf_sol = df_mf.select(["date", "ensoleillement_min"])
    if rolling_days > 1:
        df_mf_sol = with_rolling(df_mf_sol, "ensoleillement_min", rolling_days)
        sol_col = f"ensoleillement_min_roll{rolling_days}"
    else:
        sol_col = "ensoleillement_min"
    st.line_chart(df_mf_sol.select(["date", sol_col]), x="date", color=["#f1c40f"])
    st.markdown("---")

# ── Vent MF ───────────────────────────────────────────────────────────────
if show_wind:
    st.subheader("Vent maximal (km/h)")
    st.line_chart(df_mf.select(["date", "vent_kmh"]), x="date", color=["#95a5a6"])
    st.markdown("---")

# ── Comparaison Open-Meteo vs Météo-France ────────────────────────────────
df_compare = (
    df.select(["date", "t_moy", "pluie_mm"])
    .join(
        df_mf.select(["date", "t_moy", "pluie_mm"]).rename(
            {"t_moy": "t_moy_mf", "pluie_mm": "pluie_mm_mf"}
        ),
        on="date",
        how="inner",
    )
)

if not df_compare.is_empty():
    st.subheader("Comparaison Open-Meteo vs Météo-France")

    st.caption("Température moyenne (°C)")
    st.line_chart(
        df_compare.select(["date", "t_moy", "t_moy_mf"]).rename(
            {"t_moy": "Open-Meteo", "t_moy_mf": "Météo-France"}
        ),
        x="date",
        color=["#3498db", "#e67e22"],
    )

    st.caption("Précipitations (mm)")
    st.bar_chart(
        df_compare.select(["date", "pluie_mm", "pluie_mm_mf"]).rename(
            {"pluie_mm": "Open-Meteo", "pluie_mm_mf": "Météo-France"}
        ),
        x="date",
    )

    st.markdown("---")

# ── Footer MF ─────────────────────────────────────────────────────────────
if n_manquants_mf:
    st.caption(f"⚠️ {n_manquants_mf} valeurs manquantes dans la période (lag DPObs).")
st.caption(f"Source : Météo-France DPObs | Période : {mf_query_start} → {mf_query_end}")
