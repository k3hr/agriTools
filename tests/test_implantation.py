"""Test suites for the implantation module."""
import pytest
from datetime import datetime

from implantation.models.parcelle import Parcelle
from implantation.scoring.engine import ScoringEngine, ScoringWeights
from implantation.scoring.criteria import ScoringCriteria


class TestParcelle:
    """Tests for the Parcelle model."""
    
    def test_parcelle_creation_minimal(self):
        """Constructor with only required fields."""
        p = Parcelle(
            id="test_001",
            nom="Test Parcelle",
            surface_ha=2.0,
            commune="Sablé",
            departement="72",
            coords_centroid=(47.85, -0.34)
        )
        assert p.id == "test_001"
        assert p.statut == "prospect"
        assert p.notes == ""
    
    def test_parcelle_creation_full(self):
        """Constructor with all fields."""
        p = Parcelle(
            id="test_002",
            nom="Prairie Complète",
            surface_ha=5.0,
            commune="Sablé",
            departement="72",
            coords_centroid=(47.85, -0.34),
            prix_achat=10000.0,
            prix_location_annuel=250.0,
            acces_eau="forage",
            debit_estime_m3h=2.5,
            pente_pct=2.0,
            exposition="S",
            altitude_m=50,
            distance_marche_km=12.0,
            acces_vehicule="facile",
            notes="Good access",
            statut="visite"
        )
        assert p.surface_ha == 5.0
        assert p.prix_achat == 10000.0
        assert p.statut == "visite"
    
    def test_parcelle_validation_surface(self):
        """Surface must be positive."""
        with pytest.raises(ValueError):
            Parcelle(
                id="test_003",
                nom="Invalid",
                surface_ha=-1.0,
                commune="Sablé",
                departement="72",
                coords_centroid=(47.85, -0.34)
            )
    
    def test_parcelle_validation_pente(self):
        """Pente must be 0–100%."""
        with pytest.raises(ValueError):
            Parcelle(
                id="test_004",
                nom="Invalid",
                surface_ha=1.0,
                commune="Sablé",
                departement="72",
                coords_centroid=(47.85, -0.34),
                pente_pct=150.0  # Invalid
            )


class TestScoringCriteria:
    """Tests for individual scoring criteria."""
    
    def test_prix_achat_excellent(self):
        """Price well below comparable = excellent."""
        score = ScoringCriteria.prix_achat_reasonableness(9000, 10000)
        assert score >= 80
    
    def test_prix_achat_good(self):
        """Price equal to comparable = good."""
        score = ScoringCriteria.prix_achat_reasonableness(10000, 10000)
        assert 60 <= score < 80
    
    def test_prix_achat_overpriced(self):
        """Price above comparable = bad."""
        score = ScoringCriteria.prix_achat_reasonableness(13000, 10000)
        assert score < 40
    
    def test_prix_achat_missing_data(self):
        """Missing data = neutral."""
        score = ScoringCriteria.prix_achat_reasonableness(None, 10000)
        assert score == 50
    
    def test_distance_marche_close(self):
        """Close to market = excellent."""
        score = ScoringCriteria.distance_marche(3.0)
        assert score >= 90
    
    def test_distance_marche_far(self):
        """Far from market = bad."""
        score = ScoringCriteria.distance_marche(40.0)
        assert score < 40
    
    def test_acces_eau_reseau(self):
        """Network access = best."""
        score = ScoringCriteria.acces_eau("reseau")
        assert score >= 90
    
    def test_acces_eau_none(self):
        """No water access = worst."""
        score = ScoringCriteria.acces_eau("aucun")
        assert score <= 10
    
    def test_pente_adequacy_flat(self):
        """Flat land = excellent."""
        score = ScoringCriteria.pente_adequacy(0.5)
        assert score >= 90
    
    def test_pente_adequacy_steep(self):
        """Steep = bad."""
        score = ScoringCriteria.pente_adequacy(30.0)
        assert score < 20
    
    def test_exposition_south(self):
        """South exposure = best."""
        score = ScoringCriteria.exposition_adequacy("S")
        assert score >= 80
    
    def test_exposition_north(self):
        """North exposure = poor."""
        score = ScoringCriteria.exposition_adequacy("N")
        assert score <= 50


class TestScoringWeights:
    """Tests for ScoringWeights."""
    
    def test_weights_valid_sum(self):
        """Weights must sum to 1.0."""
        w = ScoringWeights(0.4, 0.4, 0.2)
        assert w.economique_logistique == 0.4
    
    def test_weights_invalid_sum(self):
        """Invalid weights raise ValueError."""
        with pytest.raises(ValueError):
            ScoringWeights(0.5, 0.3, 0.1)  # Sums to 0.9
    
    def test_weights_as_pct(self):
        """Convert to percentages."""
        w = ScoringWeights(0.35, 0.35, 0.30)
        pct = w.as_pct
        assert pct["economique_logistique"] == 35
        assert pct["eau_irrigation"] == 35
        assert pct["topographie_exposition"] == 30


class TestScoringEngine:
    """Tests for the ScoringEngine."""
    
    @pytest.fixture
    def engine(self):
        """Create a scoring engine with default weights."""
        return ScoringEngine()
    
    @pytest.fixture
    def parcelle_ideal(self):
        """Create an ideal parcelle for testing."""
        return Parcelle(
            id="ideal_01",
            nom="Ideal Parcel",
            surface_ha=2.0,
            commune="Sablé",
            departement="72",
            coords_centroid=(47.85, -0.34),
            prix_achat=8000.0,
            prix_location_annuel=250.0,
            prix_comparable_eur_ha=10000.0,
            acces_eau="reseau",
            debit_estime_m3h=2.5,
            distance_cours_eau_m=100.0,
            pente_pct=1.0,
            exposition="S",
            altitude_m=50,
            risque_gel_tardif=False,
            distance_marche_km=5.0,
            acces_vehicule="facile",
            meteo_precip_annuelle_mm=450,
            meteo_jours_gel=3,
        )
    
    def test_score_economique_logistique(self, engine, parcelle_ideal):
        """Test economic axis scoring."""
        score = engine.score_economique_logistique(parcelle_ideal)
        assert score.name == "Économique & Logistique"
        assert 0 <= score.score <= 100
        assert len(score.criteria) > 0
    
    def test_score_eau_irrigation(self, engine, parcelle_ideal):
        """Test water axis scoring."""
        score = engine.score_eau_irrigation(parcelle_ideal)
        assert score.name == "Eau & Irrigation"
        assert 0 <= score.score <= 100
    
    def test_score_topographie_exposition(self, engine, parcelle_ideal):
        """Test topography axis scoring."""
        score = engine.score_topographie_exposition(parcelle_ideal)
        assert score.name == "Topographie & Exposition"
        assert 0 <= score.score <= 100
    
    def test_score_parcelle_complete(self, engine, parcelle_ideal):
        """Test complete parcelle scoring."""
        parcel_score = engine.score_parcelle(parcelle_ideal)
        
        assert parcel_score.parcelle_id == "ideal_01"
        assert 0 <= parcel_score.global_score <= 100
        assert 0 <= parcel_score.score_economique_logistique.score <= 100
        assert 0 <= parcel_score.score_eau_irrigation.score <= 100
        assert 0 <= parcel_score.score_topographie_exposition.score <= 100
    
    def test_score_multiple(self, engine):
        """Test scoring multiple parcelles."""
        parcelles = [
            Parcelle(
                id=f"test_{i}",
                nom=f"Parcel {i}",
                surface_ha=2.0,
                commune="Sablé",
                departement="72",
                coords_centroid=(47.85 + i*0.01, -0.34),
                prix_achat=10000 + i*1000,
                distance_marche_km=10 + i*2,
                acces_vehicule="facile" if i % 2 == 0 else "limite"
            )
            for i in range(3)
        ]
        
        scores = engine.score_multiple(parcelles, sort_by_score=True)
        assert len(scores) == 3
        assert scores[0].global_score >= scores[1].global_score >= scores[2].global_score
    
    def test_scoring_summary(self, engine, parcelle_ideal):
        """Test scoring summary output."""
        parcel_score = engine.score_parcelle(parcelle_ideal)
        summary = parcel_score.summary()
        
        assert "Ideal Parcel" in summary
        assert "Score global" in summary
        assert "Économique & logistique" in summary
        assert "Eau & irrigation" in summary
        assert "Topographie & exposition" in summary


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
