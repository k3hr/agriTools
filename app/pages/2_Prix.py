"""
Dashboard prix MIN — agriTools
Cotations RNM (expédition départ bassin) : évolution temporelle,
comparaison multi-produits, tableau des dernières cotations.
"""
from __future__ import annotations

import polars as pl
import streamlit as st

from app.components.data import load_prix

# ---------------------------------------------------------------------------
# Config page
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Prix MIN — agriTools", page_icon="💶", layout="wide")
st.title("💶 Prix marchés — RNM")
st.caption("Source : FranceAgriMer / RNM — Cotations expédition départ bassin (2024–2026)")

# ---------------------------------------------------------------------------
# Chargement
# ---------------------------------------------------------------------------
with st.spinner("Chargement des cotations..."):
    df_all = load_prix()

familles = sorted(df_all["famille"].unique().to_list())
marches = sorted(df_all["marche"].unique().to_list())

# ---------------------------------------------------------------------------
# Sidebar — filtres
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filtres")

    famille_sel = st.selectbox("Famille", familles, index=familles.index("TOMATE") if "TOMATE" in familles else 0)

    produits_famille = sorted(
        df_all.filter(pl.col("famille") == famille_sel)["produit"].unique().to_list()
    )
    produits_sel = st.multiselect(
        "Produit(s)",
        produits_famille,
        default=produits_famille[:3] if len(produits_famille) >= 3 else produits_famille,
        help="Sélectionnez jusqu'à 5 produits pour la comparaison",
    )

    marches_sel = st.multiselect("Marché(s)", marches, default=marches)

    st.header("Affichage")
    agg = st.radio("Agrégation temporelle", ["Hebdomadaire", "Mensuelle"], index=0)
    show_table = st.checkbox("Afficher le tableau des cotations", value=True)

# ---------------------------------------------------------------------------
# Filtrage
# ---------------------------------------------------------------------------
if not produits_sel:
    st.info("Sélectionnez au moins un produit dans la sidebar.")
    st.stop()

if not marches_sel:
    st.info("Sélectionnez au moins un marché.")
    st.stop()

df = df_all.filter(
    pl.col("produit").is_in(produits_sel) & pl.col("marche").is_in(marches_sel)
)

if df.is_empty():
    st.warning("Aucune cotation pour cette sélection.")
    st.stop()

# ---------------------------------------------------------------------------
# Métriques
# ---------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Cotations", f"{len(df):,}")
with col2:
    d_min = df["date_cot"].min()
    d_max = df["date_cot"].max()
    st.metric("Période", f"{d_min} → {d_max}")
with col3:
    p_moy = df["prix_moyen"].mean()
    st.metric("Prix moyen global", f"{p_moy:.2f} €/unité")
with col4:
    # Variation entre première et dernière semaine disponible
    df_sorted = df.sort("date_cot")
    p_debut = df_sorted.head(max(1, len(df_sorted) // 10))["prix_moyen"].mean()
    p_fin = df_sorted.tail(max(1, len(df_sorted) // 10))["prix_moyen"].mean()
    delta_pct = ((p_fin - p_debut) / p_debut * 100) if p_debut else 0
    st.metric("Tendance (début → fin)", f"{p_fin:.2f} €", f"{delta_pct:+.1f}%")

st.markdown("---")

# ---------------------------------------------------------------------------
# Évolution prix — un graphique par produit sélectionné
# ---------------------------------------------------------------------------
st.subheader("Évolution du prix moyen (€/unité)")

# Agrégation temporelle
if agg == "Hebdomadaire":
    trunc = "1w"
    fmt = "%Y-S%V"
else:
    trunc = "1mo"
    fmt = "%Y-%m"

# Pivot : une colonne par produit — labels courts pour lisibilité
def short_label(p: str, max_len: int = 40) -> str:
    return p if len(p) <= max_len else p[:max_len] + "…"

label_map = {p: short_label(p) for p in produits_sel}

dfs: list[pl.DataFrame] = []
for produit in produits_sel:
    lbl = label_map[produit]
    sub = (
        df.filter(pl.col("produit") == produit)
        .with_columns(pl.col("date_cot").dt.truncate(trunc).alias("periode"))
        .group_by("periode")
        .agg(pl.col("prix_moyen").mean().round(3).alias(lbl))
        .sort("periode")
    )
    dfs.append(sub)

# Jointure sur la période
from functools import reduce
df_pivot = reduce(lambda a, b: a.join(b, on="periode", how="full", coalesce=True), dfs).sort("periode")

st.line_chart(
    df_pivot.to_pandas().set_index("periode"),
    use_container_width=True,
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Distribution des prix (boîte à moustaches simulée avec métriques)
# ---------------------------------------------------------------------------
st.subheader("Distribution des prix par produit")

stats_rows = []
for produit in produits_sel:
    sub = df.filter(pl.col("produit") == produit)["prix_moyen"]
    stats_rows.append({
        "Produit": short_label(produit, 50),
        "N cotations": len(sub),
        "Min (€)": round(sub.min(), 3),
        "P25 (€)": round(sub.quantile(0.25), 3),
        "Médiane (€)": round(sub.median(), 3),
        "Moyenne (€)": round(sub.mean(), 3),
        "P75 (€)": round(sub.quantile(0.75), 3),
        "Max (€)": round(sub.max(), 3),
        "Unité": df.filter(pl.col("produit") == produit)["unite"][0],
    })

st.dataframe(
    pl.DataFrame(stats_rows).to_pandas(),
    use_container_width=True,
    hide_index=True,
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Saisonnalité — prix moyen par mois calendaire
# ---------------------------------------------------------------------------
st.subheader("Saisonnalité — prix moyen par mois calendaire")

df_saison = (
    df.filter(pl.col("produit").is_in(produits_sel))
    .with_columns(pl.col("date_cot").dt.month().alias("mois"))
    .group_by(["mois", "produit"])
    .agg(pl.col("prix_moyen").mean().round(3).alias("prix_moy"))
    .sort(["produit", "mois"])
)

MOIS_LABELS = {1:"Jan",2:"Fév",3:"Mar",4:"Avr",5:"Mai",6:"Jun",
               7:"Jul",8:"Aoû",9:"Sep",10:"Oct",11:"Nov",12:"Déc"}
df_saison = df_saison.with_columns(
    pl.col("mois").map_elements(lambda m: MOIS_LABELS.get(m, str(m)), return_dtype=pl.Utf8).alias("mois_label")
)

# Pivot mois × produit
saison_rows = []
for produit in produits_sel:
    row = {"Produit": short_label(produit, 50)}
    sub = df_saison.filter(pl.col("produit") == produit)
    for _, mois_lbl in sorted(MOIS_LABELS.items()):
        match = sub.filter(pl.col("mois_label") == mois_lbl)
        row[mois_lbl] = round(match["prix_moy"][0], 3) if len(match) else None
    saison_rows.append(row)

st.dataframe(
    pl.DataFrame(saison_rows).to_pandas(),
    use_container_width=True,
    hide_index=True,
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Tableau des dernières cotations
# ---------------------------------------------------------------------------
if show_table:
    st.subheader("Dernières cotations")

    df_table = (
        df.sort("date_cot", descending=True)
        .select(["date_cot", "produit", "marche", "prix_moyen", "unite"])
        .with_columns(
            pl.col("produit").map_elements(lambda p: short_label(p, 55), return_dtype=pl.Utf8),
            pl.col("marche").str.slice(0, 35),
            pl.col("prix_moyen").round(3),
        )
        .rename({
            "date_cot": "Date",
            "produit": "Produit",
            "marche": "Marché",
            "prix_moyen": "Prix moy. (€)",
            "unite": "Unité",
        })
        .head(200)
    )

    st.dataframe(df_table.to_pandas(), use_container_width=True, hide_index=True)
