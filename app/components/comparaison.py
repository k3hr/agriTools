"""
Helpers for multi-parcel comparison views.
"""
from __future__ import annotations

from typing import Any

import polars as pl


def build_parcelle_options(parcelles: list[Any]) -> list[str]:
    """Build selectbox labels for saved parcels."""
    return [f"{p.id} — {p.nom}" for p in parcelles]


def select_parcelles(parcelles: list[Any], selected_labels: list[str]) -> list[Any]:
    """Return selected parcels in the order of the source list."""
    options = build_parcelle_options(parcelles)
    return [p for p, label in zip(parcelles, options) if label in selected_labels]


def get_selection_message(selected_labels: list[str]) -> str | None:
    """Return the UI message for the current selection state."""
    if not selected_labels:
        return "Sélectionnez au moins une parcelle pour comparer."
    return None


def get_selection_info_message(selected_labels: list[str]) -> str | None:
    """Return an informational message for non-blocking selection states."""
    if len(selected_labels) == 1:
        return "Une seule parcelle sélectionnée : affichage comparatif en mode solo."
    return None


def build_summary_df(scores: list[Any]) -> pl.DataFrame:
    """Build the main side-by-side comparison table."""
    return pl.DataFrame(
        [
            {
                "Parcelle": score.parcelle_nom,
                "ID": score.parcelle_id,
                "Score global": score.global_score,
                "Éco (35%)": score.score_economique_logistique.score,
                "Eau (35%)": score.score_eau_irrigation.score,
                "Topo (30%)": score.score_topographie_exposition.score,
            }
            for score in scores
        ]
    )


def build_chart_data(scores: list[Any]) -> pl.DataFrame:
    """Build the axis chart dataset from parcelle scores (wide format, Axe column as x-axis)."""
    return pl.DataFrame(
        [
            {
                "Axe": axe,
                **{score.parcelle_nom: getattr(score, attr).score for score in scores},
            }
            for axe, attr in [
                ("Économique", "score_economique_logistique"),
                ("Eau", "score_eau_irrigation"),
                ("Topographie", "score_topographie_exposition"),
            ]
        ]
    )
