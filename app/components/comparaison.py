"""
Helpers for multi-parcel comparison views.
"""
from __future__ import annotations

from typing import Any

import pandas as pd


def build_parcelle_options(parcelles: list[Any]) -> list[str]:
    """Build selectbox labels for saved parcels."""
    return [f"{p.id} — {p.nom}" for p in parcelles]


def select_parcelles(parcelles: list[Any], selected_labels: list[str]) -> list[Any]:
    """Return selected parcels in the order of the source list."""
    options = build_parcelle_options(parcelles)
    return [p for p, label in zip(parcelles, options) if label in selected_labels]


def build_summary_df(scores: list[Any]) -> pd.DataFrame:
    """Build the main side-by-side comparison table."""
    summary_rows = []
    for score in scores:
        summary_rows.append(
            {
                "Parcelle": score.parcelle_nom,
                "ID": score.parcelle_id,
                "Score global": score.global_score,
                "Éco (35%)": score.score_economique_logistique.score,
                "Eau (35%)": score.score_eau_irrigation.score,
                "Topo (30%)": score.score_topographie_exposition.score,
            }
        )
    return pd.DataFrame(summary_rows).set_index("Parcelle")


def build_chart_data(scores: list[Any]) -> pd.DataFrame:
    """Build the axis chart dataset from parcelle scores."""
    return pd.DataFrame(
        {
            score.parcelle_nom: {
                "Économique": score.score_economique_logistique.score,
                "Eau": score.score_eau_irrigation.score,
                "Topographie": score.score_topographie_exposition.score,
            }
            for score in scores
        }
    )

