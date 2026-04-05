"""
Tests de flux pour app/pages/4_Comparaison_Parcelles.py.

Utilise streamlit.testing.v1.AppTest pour simuler l'exécution complète de la page
et vérifier son comportement selon les données disponibles dans le datalake.

load_parcelles() est mocké pour isoler les tests de l'état du datalake local.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from implantation.models.parcelle import Parcelle

PAGE_PATH = str(
    Path(__file__).parent.parent / "app" / "pages" / "4_Comparaison_Parcelles.py"
)
# Point de patch : l'attribut du module source, pas la référence locale de la page
LOAD_PARCELLES = "app.components.parcelle.load_parcelles"


def _make_parcelle(parcelle_id: str, nom: str, surface_ha: float = 2.0) -> Parcelle:
    return Parcelle(
        id=parcelle_id,
        nom=nom,
        surface_ha=surface_ha,
        commune="Sablé-sur-Sarthe",
        departement="72",
        coords_centroid=(47.85, -0.34),
    )


def _run_page(parcelles: list[Parcelle]) -> AppTest:
    """Lance la page en mockant load_parcelles. Retourne l'AppTest après exécution."""
    with patch(LOAD_PARCELLES, return_value=parcelles):
        at = AppTest.from_file(PAGE_PATH)
        at.run()
    return at


# ── Branche "datalake vide" ───────────────────────────────────────────────────


def test_empty_datalake_shows_info_message_and_stops():
    """
    Avec 0 parcelles en datalake, la page doit :
    - afficher un message st.info contenant "Aucune parcelle"
    - appeler st.stop() → le multiselect n'est jamais rendu
    - ne pas lever d'exception
    """
    at = _run_page([])

    assert not at.exception
    assert len(at.info) >= 1
    assert any("Aucune parcelle" in msg.value for msg in at.info)
    # st.stop() est appelé avant le multiselect → pas de widget de sélection
    assert len(at.multiselect) == 0


# ── Branche "sélection valide — 2 parcelles" ─────────────────────────────────


def test_two_parcelles_renders_without_exception():
    """Deux parcelles valides → la page s'exécute entièrement sans exception."""
    parcelles = [
        _make_parcelle("p1", "Parcelle Nord"),
        _make_parcelle("p2", "Parcelle Sud"),
    ]
    at = _run_page(parcelles)

    assert not at.exception


def test_two_parcelles_shows_multiselect_with_all_options():
    """
    Le multiselect expose une option par parcelle chargée,
    au format "{id} — {nom}".
    """
    parcelles = [
        _make_parcelle("p1", "Parcelle Nord"),
        _make_parcelle("p2", "Parcelle Sud"),
    ]
    at = _run_page(parcelles)

    assert not at.exception
    assert len(at.multiselect) == 1
    opts = at.multiselect[0].options
    assert len(opts) == 2
    assert any("p1" in opt and "Parcelle Nord" in opt for opt in opts)
    assert any("p2" in opt and "Parcelle Sud" in opt for opt in opts)


def test_two_parcelles_renders_summary_dataframe():
    """
    Avec 2 parcelles sélectionnées, au moins un st.dataframe est affiché
    (tableau de résumé + tableaux de détail).
    """
    parcelles = [
        _make_parcelle("p1", "Parcelle Nord"),
        _make_parcelle("p2", "Parcelle Sud"),
    ]
    at = _run_page(parcelles)

    assert not at.exception
    assert len(at.dataframe) >= 1


def test_two_parcelles_default_selection_is_first_two():
    """
    Avec 3 parcelles disponibles, le multiselect sélectionne les 2 premières
    par défaut (options[:2]).
    """
    parcelles = [
        _make_parcelle("p1", "Parcelle A"),
        _make_parcelle("p2", "Parcelle B"),
        _make_parcelle("p3", "Parcelle C"),
    ]
    at = _run_page(parcelles)

    assert not at.exception
    ms = at.multiselect[0]
    assert len(ms.options) == 3
    # Par défaut : 2 premières sélectionnées
    assert len(ms.value) == 2
    assert any("p1" in v for v in ms.value)
    assert any("p2" in v for v in ms.value)
    assert not any("p3" in v for v in ms.value)


# ── Branche "sélection solo — 1 parcelle" ────────────────────────────────────


def test_one_parcelle_triggers_solo_info_message():
    """
    Avec une seule parcelle disponible, la page doit afficher le message
    mode solo via st.info (get_selection_info_message) sans crasher.
    """
    parcelles = [_make_parcelle("p1", "Parcelle Solo")]
    at = _run_page(parcelles)

    assert not at.exception
    assert any("solo" in msg.value.lower() for msg in at.info)


def test_one_parcelle_renders_without_exception():
    """Une seule parcelle → la page ne crash pas et affiche un résultat."""
    parcelles = [_make_parcelle("p1", "Parcelle Solo")]
    at = _run_page(parcelles)

    assert not at.exception
    assert len(at.dataframe) >= 1


# ── Cohérence scoring ─────────────────────────────────────────────────────────


def test_page_scores_are_between_0_and_100():
    """
    Les scores affichés dans les expanders doivent être dans l'intervalle [0, 100].
    Vérifie la cohérence du moteur de scoring via le rendu de la page.
    """
    parcelles = [
        _make_parcelle("p1", "Parcelle Test", surface_ha=3.5),
        _make_parcelle("p2", "Parcelle Ref", surface_ha=1.2),
    ]
    at = _run_page(parcelles)

    assert not at.exception
    # Les métriques affichées via st.metric ou st.table doivent être dans [0, 100]
    # On vérifie via les dataframes de détail (criterion rows)
    for df_element in at.dataframe:
        df = df_element.value
        if "Score" in df.columns:
            scores = df["Score"].dropna()
            assert (scores >= 0).all(), f"Score négatif détecté : {scores.min()}"
            assert (scores <= 100).all(), f"Score > 100 détecté : {scores.max()}"
