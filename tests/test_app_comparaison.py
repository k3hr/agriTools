from app.components.comparaison import (
    build_chart_data,
    build_parcelle_options,
    build_summary_df,
    select_parcelles,
)
from implantation.models.parcelle import Parcelle
from implantation.scoring.engine import AxisScore, ParcelleScore


def _make_parcelle(parcelle_id: str, nom: str) -> Parcelle:
    return Parcelle(
        id=parcelle_id,
        nom=nom,
        surface_ha=2.0,
        commune="Sablé-sur-Sarthe",
        departement="72",
        coords_centroid=(47.85, -0.34),
    )


def _make_score(
    parcelle_id: str,
    nom: str,
    global_score: int,
    eco: int,
    eau: int,
    topo: int,
) -> ParcelleScore:
    return ParcelleScore(
        parcelle_id=parcelle_id,
        parcelle_nom=nom,
        global_score=global_score,
        score_economique_logistique=AxisScore(
            name="Économique & Logistique",
            score=eco,
            weight=0.35,
            criteria={"prix_achat": eco},
        ),
        score_eau_irrigation=AxisScore(
            name="Eau & Irrigation",
            score=eau,
            weight=0.35,
            criteria={"acces_eau": eau},
        ),
        score_topographie_exposition=AxisScore(
            name="Topographie & Exposition",
            score=topo,
            weight=0.30,
            criteria={"pente": topo},
        ),
    )


def test_selecting_two_parcelles_builds_coherent_side_by_side_table():
    parcelles = [
        _make_parcelle("p1", "Parcelle Nord"),
        _make_parcelle("p2", "Parcelle Sud"),
        _make_parcelle("p3", "Parcelle Est"),
    ]
    selected = select_parcelles(
        parcelles,
        selected_labels=build_parcelle_options(parcelles)[:2],
    )
    scores = [
        _make_score("p1", "Parcelle Nord", 71, 68, 75, 70),
        _make_score("p2", "Parcelle Sud", 84, 88, 80, 83),
    ]

    summary_df = build_summary_df(scores)
    chart_df = build_chart_data(scores)

    assert [p.id for p in selected] == ["p1", "p2"]
    assert list(summary_df.index) == ["Parcelle Nord", "Parcelle Sud"]
    assert list(summary_df.columns) == ["ID", "Score global", "Éco (35%)", "Eau (35%)", "Topo (30%)"]
    assert summary_df.loc["Parcelle Nord", "ID"] == "p1"
    assert summary_df.loc["Parcelle Nord", "Score global"] == 71
    assert summary_df.loc["Parcelle Sud", "Éco (35%)"] == 88
    assert summary_df.loc["Parcelle Sud", "Eau (35%)"] == 80
    assert summary_df.loc["Parcelle Sud", "Topo (30%)"] == 83

    assert list(chart_df.index) == ["Économique", "Eau", "Topographie"]
    assert list(chart_df.columns) == ["Parcelle Nord", "Parcelle Sud"]
    assert chart_df.loc["Économique", "Parcelle Nord"] == 68
    assert chart_df.loc["Eau", "Parcelle Sud"] == 80
    assert chart_df.loc["Topographie", "Parcelle Sud"] == 83
