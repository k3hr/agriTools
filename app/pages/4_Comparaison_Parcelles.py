"""
Page de comparaison multi-parcelles enregistrées.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from app.components.comparaison import (
    build_chart_data,
    build_parcelle_options,
    build_summary_df,
    select_parcelles,
)
from app.components.parcelle import load_parcelles
from implantation.scoring.engine import ScoringEngine

st.set_page_config(
    page_title="Comparaison parcelles — agriTools",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Comparaison multi-parcelles")

st.markdown(
    """
    Cette page permet de comparer plusieurs parcelles enregistrées dans le datalake personnel.
    Sélectionnez jusqu'à 4 parcelles pour afficher leurs scores, axes et critères côte à côte.
    """
)

parcelles = load_parcelles()
if not parcelles:
    st.info(
        "Aucune parcelle enregistrée. Allez sur la page Parcelle pour créer et enrichir des fiches puis revenez ici."
    )
    st.stop()

options = build_parcelle_options(parcelles)
selected = st.multiselect(
    "Sélectionnez jusqu'à 4 parcelles à comparer",
    options,
    default=options[:2],
    max_selections=4,
)

if not selected:
    st.warning("Sélectionnez au moins une parcelle pour comparer.")
    st.stop()

selected_parcelles = select_parcelles(parcelles, selected)
engine = ScoringEngine()
scores = engine.score_multiple(selected_parcelles, sort_by_score=True)

st.markdown("### Résumé des parcelles sélectionnées")
summary_df = build_summary_df(scores)
st.dataframe(summary_df.sort_values("Score global", ascending=False))

st.markdown("### Visualisation des axes de scoring")
chart_data = build_chart_data(scores)
st.bar_chart(chart_data)

st.markdown("### Détails par parcelle")
for score in scores:
    with st.expander(f"{score.parcelle_nom} — Score global {score.global_score}/100", expanded=False):
        details = {
            "Score économique": score.score_economique_logistique.score,
            "Score eau": score.score_eau_irrigation.score,
            "Score topographie": score.score_topographie_exposition.score,
        }
        st.table(pd.DataFrame.from_dict(details, orient="index", columns=["Valeur"]))

        criterion_rows = []
        for criterion, crit_score in score.score_economique_logistique.criteria.items():
            criterion_rows.append({"Axe": "Économique", "Critère": criterion.replace("_", " ").title(), "Score": crit_score})
        for criterion, crit_score in score.score_eau_irrigation.criteria.items():
            criterion_rows.append({"Axe": "Eau", "Critère": criterion.replace("_", " ").title(), "Score": crit_score})
        for criterion, crit_score in score.score_topographie_exposition.criteria.items():
            criterion_rows.append({"Axe": "Topographie", "Critère": criterion.replace("_", " ").title(), "Score": crit_score})

        st.dataframe(pd.DataFrame(criterion_rows))
        st.markdown("---")
