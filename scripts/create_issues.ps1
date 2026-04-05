# =============================================================================
# create_issues.ps1 — Création des issues GitHub Jour 1 (nettoyage)
#
# Prérequis : GitHub CLI installé et authentifié (gh auth login)
# Usage     : .\scripts\create_issues.ps1
# =============================================================================

$repo = "k3hr/agriTools"

$issues = @(
    @{
        title  = "fix: retirer les artéfacts TEMP de config.toml"
        body   = @"
## Problème

La section ``[prix]`` de ``config.toml`` contient des entrées et commentaires de debug qui n'auraient pas dû être committés.

**Lignes concernées :**
``````toml
# TEMP: Added actual market from ZIP data for testing
marches = ["NANTES SAINT LUCE", "RUNGIS", "Bassin Auvergne Rhône-Alpes : kiwi, châtaigne-marron, noix"]

# TEMP: Added actual stage from ZIP data for testing
stades = ["GROS", "Expédition"]
``````

## Correction attendue

``````toml
# Marchés MIN à conserver — MIN Nantes (référence locale) + Rungis (référence nationale)
marches = ["NANTES SAINT LUCE", "RUNGIS"]

# Stade de commercialisation retenu
stades = ["GROS", "Expédition"]
``````

## Fichiers impactés
- ``config.toml``
"@
        labels = @("bug", "cleanup")
    },
    @{
        title  = "chore: déplacer tmp_inspect_rnm.py et test_scoring_integration.py hors de la racine"
        body   = @"
## Problème

Deux fichiers temporaires/de debug sont présents à la racine du dépôt, ce qui nuit à la lisibilité :

- ``tmp_inspect_rnm.py`` — script d'inspection ad hoc, devrait être dans ``scripts/`` ou supprimé
- ``test_scoring_integration.py`` — test d'intégration hors du dossier ``tests/``, non détecté par pytest

## Correction attendue

- ``tmp_inspect_rnm.py`` → supprimer (usage ponctuel terminé) ou déplacer dans ``scripts/diag_rnm.py``
- ``test_scoring_integration.py`` → déplacer dans ``tests/test_scoring_integration.py`` et vérifier qu'il passe avec ``pytest``

## Fichiers impactés
- ``tmp_inspect_rnm.py`` (racine)
- ``test_scoring_integration.py`` (racine)
"@
        labels = @("cleanup")
    },
    @{
        title  = "fix: corriger test_parse_csv_accepts_comma_decimals (DVF)"
        body   = @"
## Problème

Le test ``tests/test_dvf.py::TestParseCSV::test_parse_csv_accepts_comma_decimals`` échoue depuis plusieurs sessions.

Il vérifie que ``parse_csv()`` accepte les montants avec virgule décimale (``"125,000"`` → ``125000.0``), cas fréquent dans les exports DVF FR.

## Comportement actuel

``````
FAILED tests/test_dvf.py::TestParseCSV::test_parse_csv_accepts_comma_decimals
``````

## Correction attendue

- Identifier pourquoi la conversion ``"125,000"`` → float échoue dans ``ingestion/prix/dvf.py``
- Corriger la logique de parsing (probablement un ``str.replace(",", ".")`` manquant ou mal placé)
- Vérifier que tous les autres tests DVF restent verts

## Fichiers impactés
- ``ingestion/prix/dvf.py``
- ``tests/test_dvf.py``
"@
        labels = @("bug", "tests")
    },
    @{
        title  = "docs: mettre à jour le compte de tests dans ROADMAP.md"
        body   = @"
## Problème

La section **Archives — Phase 3** du ``ROADMAP.md`` et la section **Métriques de succès** indiquent **127 tests**, mais la suite en exécute aujourd'hui **139+** (ajout des tests enrichissement, persistance parcelle, etc.).

## Correction attendue

- Mettre à jour le chiffre dans ``ROADMAP.md`` (section Métriques + section Archives Phase 2/3)
- Vérifier le chiffre exact avec ``pytest --collect-only -q | tail -1``

## Fichiers impactés
- ``ROADMAP.md``
"@
        labels = @("documentation")
    },
    @{
        title  = "docs: corriger la numérotation dupliquée des sections dans ROADMAP.md"
        body   = @"
## Problème

Le fichier ``ROADMAP.md`` contient deux sections numérotées ``## 5.`` :

- ``## 5. Phase 4 — Consolidation & profondeur (backlog)``
- ``## 5. Phase 5 — Usage réel & industrialisation (planifié)``

La seconde devrait être ``## 6.``, et les sections suivantes renumérotées en conséquence.

## Correction attendue

``````markdown
## 5. Phase 4 — Consolidation & profondeur (backlog)
## 6. Phase 5 — Usage réel & industrialisation (planifié)
## 7. Métriques de succès        ← actuellement ## 7, vérifier
## 8. Archives — Phases 0–3      ← actuellement ## 8, vérifier
``````

## Fichiers impactés
- ``ROADMAP.md``
"@
        labels = @("documentation")
    },
    @{
        title  = "docs: mettre à jour implantation/README.md — Phase 3 est terminée"
        body   = @"
## Problème

Le fichier ``implantation/README.md`` contient encore des références à la Phase 3 comme si elle était à venir ou en cours :

- Section **Prochaines étapes → UI Streamlit (Phase 3 - Suite)** liste des pages "à construire" qui sont en réalité déjà livrées (``3_Parcelle.py``, ``4_Comparaison_Parcelles.py``, PDF report)
- Les sections enrichissement listées comme "à faire" correspondent à ``implantation/enrichment/service.py`` déjà implémenté

## Correction attendue

- Mettre à jour la section **Prochaines étapes** pour refléter l'état réel (Phase 3 terminée)
- Documenter ``ParcelleEnricher`` et son usage
- Mentionner ``4_Comparaison_Parcelles.py`` et le rapport PDF comme livrés
- Pointer vers Phase 4 pour les enrichissements pédologiques et l'intégration UI de l'enrichissement automatique

## Fichiers impactés
- ``implantation/README.md``
"@
        labels = @("documentation")
    }
)

Write-Host "Création de $($issues.Count) issues sur $repo..." -ForegroundColor Cyan
Write-Host ""

foreach ($issue in $issues) {
    Write-Host "  → $($issue.title)" -ForegroundColor Yellow

    $labelsArg = ($issue.labels | ForEach-Object { "--label `"$_`"" }) -join " "

    $result = gh issue create `
        --repo $repo `
        --title $issue.title `
        --body $issue.body `
        --label ($issue.labels -join ",") 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-Host "    ✓ $result" -ForegroundColor Green
    } else {
        Write-Host "    ✗ Erreur : $result" -ForegroundColor Red
    }

    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "Terminé. Voir https://github.com/$repo/issues" -ForegroundColor Cyan
