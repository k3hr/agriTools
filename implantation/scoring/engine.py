"""
Moteur de scoring pour l'évaluation des parcelles candidates.

Le scoring fonctionne sur 3 axes pondérables par l'utilisateur :
1. Économique & logistique
2. Eau & irrigation  
3. Topographie & exposition

Chaque axe est une agrégation pondérée de critères individuels (0–100).
Le score global est une moyenne pondérée des 3 axes.
"""
from dataclasses import dataclass, field
from typing import Optional

from implantation.models.parcelle import Parcelle
from implantation.scoring.criteria import ScoringCriteria


@dataclass
class ScoringWeights:
    """
    Poids de chaque axe (somme = 100 ou 1.0).
    Permet à l'utilisateur de définir ses priorités.
    """
    economique_logistique: float = field(default=0.35)
    eau_irrigation: float = field(default=0.35)
    topographie_exposition: float = field(default=0.30)
    
    def __post_init__(self):
        """Valide que les poids somment à 1.0."""
        total = (
            self.economique_logistique +
            self.eau_irrigation +
            self.topographie_exposition
        )
        if not (0.99 <= total <= 1.01):
            raise ValueError(f"Les poids doivent sommer à 1.0, got {total:.2f}")
    
    @property
    def as_pct(self) -> dict[str, int]:
        """Retourne les poids en pourcentages."""
        return {
            "economique_logistique": int(self.economique_logistique * 100),
            "eau_irrigation": int(self.eau_irrigation * 100),
            "topographie_exposition": int(self.topographie_exposition * 100),
        }


@dataclass
class AxisScore:
    """Score d'un axe avec détails des critères contributifs."""
    name: str
    score: int
    weight: float
    criteria: dict[str, int]  # nom_critère → score (0–100)
    
    def weighted_contribution(self) -> float:
        """Contribution pondérée au score global."""
        return self.score * self.weight


@dataclass
class ParcelleScore:
    """Score complet d'une parcelle."""
    parcelle_id: str
    parcelle_nom: str
    global_score: int
    
    score_economique_logistique: AxisScore
    score_eau_irrigation: AxisScore
    score_topographie_exposition: AxisScore
    
    def summary(self) -> str:
        """Résumé formaté du scoring."""
        lines = [
            f"Parcelle: {self.parcelle_nom}",
            f"Score global: {self.global_score}/100",
            f"",
            f"  Economique & logistique: {self.score_economique_logistique.score}/100 "
            f"(poids {int(self.score_economique_logistique.weight*100)}%)",
            f"  Eau & irrigation:        {self.score_eau_irrigation.score}/100 "
            f"(poids {int(self.score_eau_irrigation.weight*100)}%)",
            f"  Topographie & exposition: {self.score_topographie_exposition.score}/100 "
            f"(poids {int(self.score_topographie_exposition.weight*100)}%)",
        ]
        return "\n".join(lines)


class ScoringEngine:
    """Moteur de scoring pour l'évaluation des parcelles."""
    
    def __init__(self, weights: Optional[ScoringWeights] = None):
        """
        Initialise le moteur.
        
        Args:
            weights: Poids des 3 axes (défaut: équilibré 0.35/0.35/0.30)
        """
        self.weights = weights or ScoringWeights()
    
    def score_economique_logistique(self, parcelle: Parcelle) -> AxisScore:
        """Score de l'axe économique & logistique."""
        criteria = ScoringCriteria()
        
        # Critères individuels
        prix_score = criteria.prix_achat_reasonableness(
            parcelle.prix_achat,
            parcelle.prix_comparable_eur_ha
        )
        marche_score = criteria.distance_marche(parcelle.distance_marche_km)
        acces_score = criteria.acces_vehicule(parcelle.acces_vehicule)
        
        # Scores dans le dictionnaire
        criteria_scores = {
            "prix_achat": prix_score,
            "distance_marche": marche_score,
            "acces_vehicule": acces_score,
        }
        
        # Agrégation simple (moyenne pondérée, poids égaux pour démarrer)
        axis_score = int(
            (prix_score * 0.4 + marche_score * 0.35 + acces_score * 0.25)
        )
        
        return AxisScore(
            name="Économique & Logistique",
            score=axis_score,
            weight=self.weights.economique_logistique,
            criteria=criteria_scores
        )
    
    def score_eau_irrigation(self, parcelle: Parcelle) -> AxisScore:
        """Score de l'axe eau & irrigation."""
        criteria = ScoringCriteria()
        
        # Critères individuels
        acces_eau_score = criteria.acces_eau(parcelle.acces_eau)
        debit_ajustement = criteria.debit_eau(
            parcelle.debit_estime_m3h,
            parcelle.acces_eau
        )
        precip_score = criteria.precipitation_adequacy(
            parcelle.meteo_precip_annuelle_mm
        )
        
        # Appliquer ajustement
        acces_eau_score = max(0, min(100, acces_eau_score + debit_ajustement))
        
        criteria_scores = {
            "acces_eau": acces_eau_score,
            "precipitation": precip_score,
        }
        
        # Agrégation
        axis_score = int(acces_eau_score * 0.6 + precip_score * 0.4)
        
        return AxisScore(
            name="Eau & Irrigation",
            score=axis_score,
            weight=self.weights.eau_irrigation,
            criteria=criteria_scores
        )
    
    def score_topographie_exposition(self, parcelle: Parcelle) -> AxisScore:
        """Score de l'axe topographie & exposition."""
        criteria = ScoringCriteria()
        
        # Critères individuels
        pente_score = criteria.pente_adequacy(parcelle.pente_pct)
        exposition_score = criteria.exposition_adequacy(parcelle.exposition)
        gel_score = criteria.risque_gel(
            parcelle.risque_gel_tardif,
            parcelle.meteo_jours_gel
        )
        altitude_score = criteria.altitude_adequacy(parcelle.altitude_m)
        
        criteria_scores = {
            "pente": pente_score,
            "exposition": exposition_score,
            "risque_gel": gel_score,
            "altitude": altitude_score,
        }
        
        # Agrégation pondérée
        axis_score = int(
            pente_score * 0.3 +
            exposition_score * 0.25 +
            gel_score * 0.25 +
            altitude_score * 0.2
        )
        
        return AxisScore(
            name="Topographie & Exposition",
            score=axis_score,
            weight=self.weights.topographie_exposition,
            criteria=criteria_scores
        )
    
    def score_parcelle(self, parcelle: Parcelle) -> ParcelleScore:
        """
        Score complet d'une parcelle sur les 3 axes.
        
        Retourne un ParcelleScore avec scores détaillés et global.
        """
        # Calculer les 3 axes
        score_eco = self.score_economique_logistique(parcelle)
        score_eau = self.score_eau_irrigation(parcelle)
        score_topo = self.score_topographie_exposition(parcelle)
        
        # Score global pondéré
        global_score = int(
            score_eco.weighted_contribution() +
            score_eau.weighted_contribution() +
            score_topo.weighted_contribution()
        )
        
        return ParcelleScore(
            parcelle_id=parcelle.id,
            parcelle_nom=parcelle.nom,
            global_score=global_score,
            score_economique_logistique=score_eco,
            score_eau_irrigation=score_eau,
            score_topographie_exposition=score_topo,
        )
    
    def score_multiple(
        self,
        parcelles: list[Parcelle],
        sort_by_score: bool = True
    ) -> list[ParcelleScore]:
        """
        Score un lot de parcelles.
        
        Args:
            parcelles: Liste des parcelles à évaluer
            sort_by_score: Tri par score décroissant
        
        Returns:
            Liste des ParcelleScore, optionnellement triée
        """
        scores = [self.score_parcelle(p) for p in parcelles]
        
        if sort_by_score:
            scores.sort(key=lambda s: s.global_score, reverse=True)
        
        return scores
