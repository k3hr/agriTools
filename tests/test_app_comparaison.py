from app.components.comparaison import (
    build_chart_data,
    build_parcelle_options,
    build_summary_df,
    get_selection_info_message,
    get_selection_message,
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


def test_selecting_zero_parcelle_returns_empty_state_message():
    message = get_selection_message([])

    assert message == "Sélectionnez au moins une parcelle pour comparer."


def test_selecting_one_parcelle_returns_solo_mode_message():
    message = get_selection_info_message(["p1 — Parcelle Solo"])

    assert message == "Une seule parcelle sélectionnée : affichage comparatif en mode solo."


def test_summary_df_sorted_descending_by_global_score():
    scores = [
        _make_score("p1", "Moyenne", 65, 60, 68, 70),
        _make_score("p2", "Basse", 40, 35, 42, 45),
        _make_score("p3", "Haute", 90, 85, 88, 95),
    ]
    summary_df = build_summary_df(scores)
    sorted_df = summary_df.sort_values("Score global", ascending=False)

    assert list(sorted_df.index) == ["Haute", "Moyenne", "Basse"]
    assert list(sorted_df["Score global"]) == [90, 65, 40]


def test_best_value_is_identifiable_per_axis_column():
    scores = [
        _make_score("pA", "Parcelle A", 70, 70, 60, 70),
        _make_score("pB", "Parcelle B", 75, 88, 70, 83),
        _make_score("pC", "Parcelle C", 72, 65, 80, 75),
    ]
    summary_df = build_summary_df(scores)

    assert summary_df["Éco (35%)"].idxmax() == "Parcelle B"
    assert summary_df["Eau (35%)"].idxmax() == "Parcelle C"
    assert summary_df["Topo (30%)"].idxmax() == "Parcelle B"


def test_chart_data_is_consistent_with_summary_df():
    scores = [
        _make_score("p1", "Parcelle Nord", 71, 68, 75, 70),
        _make_score("p2", "Parcelle Sud", 84, 88, 80, 83),
    ]
    summary_df = build_summary_df(scores)
    chart_df = build_chart_data(scores)

    for score in scores:
        nom = score.parcelle_nom
        assert chart_df.loc["Économique", nom] == summary_df.loc[nom, "Éco (35%)"]
        assert chart_df.loc["Eau", nom] == summary_df.loc[nom, "Eau (35%)"]
        assert chart_df.loc["Topographie", nom] == summary_df.loc[nom, "Topo (30%)"]


def test_custom_weighting_shifts_global_scores():
    """
    Pondérations standard (0.35/0.35/0.30) :
    - "Eau Forte" (eco=30, eau=90, topo=30) : 0.35*30 + 0.35*90 + 0.30*30 = 51
    - "Eco Forte" (eco=90, eau=30, topo=30) : 0.35*90 + 0.35*30 + 0.30*30 = 51

    Pondérations custom (eco=0.15, eau=0.70, topo=0.15) :
    - "Eau Forte" : 0.15*30 + 0.70*90 + 0.15*30 = 72
    - "Eco Forte" : 0.15*90 + 0.70*30 + 0.15*30 = 39
    """
    eau_forte_eco, eau_forte_eau, eau_forte_topo = 30, 90, 30
    eco_forte_eco, eco_forte_eau, eco_forte_topo = 90, 30, 30

    scores_standard = [
        _make_score("p_eau", "Eau Forte", 51, eau_forte_eco, eau_forte_eau, eau_forte_topo),
        _make_score("p_eco", "Eco Forte", 51, eco_forte_eco, eco_forte_eau, eco_forte_topo),
    ]
    df_standard = build_summary_df(scores_standard)
    assert df_standard.loc["Eau Forte", "Score global"] == 51
    assert df_standard.loc["Eco Forte", "Score global"] == 51

    scores_custom = [
        _make_score("p_eau", "Eau Forte", 72, eau_forte_eco, eau_forte_eau, eau_forte_topo),
        _make_score("p_eco", "Eco Forte", 39, eco_forte_eco, eco_forte_eau, eco_forte_topo),
    ]
    df_custom = build_summary_df(scores_custom)
    assert df_custom.loc["Eau Forte", "Score global"] == 72
    assert df_custom.loc["Eco Forte", "Score global"] == 39

    # Les scores d'axe bruts sont inchangés quelle que soit la pondération
    for df in [df_standard, df_custom]:
        assert df.loc["Eau Forte", "Éco (35%)"] == eau_forte_eco
        assert df.loc["Eau Forte", "Eau (35%)"] == eau_forte_eau
        assert df.loc["Eau Forte", "Topo (30%)"] == eau_forte_topo
        assert df.loc["Eco Forte", "Éco (35%)"] == eco_forte_eco
        assert df.loc["Eco Forte", "Eau (35%)"] == eco_forte_eau
        assert df.loc["Eco Forte", "Topo (30%)"] == eco_forte_topo
