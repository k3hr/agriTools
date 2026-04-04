"""
Modèle de données pour une parcelle candidate.

Une Parcelle représente une parcelle agricole que le maraîcher envisage d'acquérir
ou de louer. Elle regroupe des informations géographiques, économiques, hydrologiques
et topographiques pour l'évaluer via le moteur de scoring.
"""
from pydantic import BaseModel, Field
from typing import Literal, Optional
from datetime import datetime


class Parcelle(BaseModel):
    """
    Modèle Pydantic pour une parcelle candidate.
    
    Tous les champs sont optionnels au départ, certains sont enrichis automatiquement
    depuis le datalake (météo, prix comparables, etc.).
    """
    
    # --- Identité & localisation ---
    id: str = Field(..., description="Identifiant unique (ex: numéro cadastral)")
    nom: str = Field(..., description="Nom ou libellé de la parcelle")
    surface_ha: float = Field(..., ge=0.1, description="Surface en hectares")
    commune: str = Field(..., description="Commune (ex: Sablé-sur-Sarthe)")
    departement: str = Field(..., description="Département (ex: 72)")
    coords_centroid: tuple[float, float] = Field(
        ...,
        description="Latitude, Longitude (WGS84, ex: (47.85, -0.33))"
    )
    
    # --- Économique ---
    prix_achat: Optional[float] = Field(
        None,
        ge=0,
        description="Prix d'achat proposé (€). Peut être comparé aux prix DVF médians."
    )
    prix_location_annuel: Optional[float] = Field(
        None,
        ge=0,
        description="Prix de location annuel (€/ha). Permet évaluation du ROI."
    )
    
    # --- Eau & irrigation ---
    acces_eau: Literal["forage", "riviere", "reseau", "aucun", "inconnu"] = Field(
        "inconnu",
        description="Type d'accès à l'eau"
    )
    debit_estime_m3h: Optional[float] = Field(
        None,
        ge=0,
        description="Débit estimé en m³/h (pour forage ou rivière)"
    )
    distance_cours_eau_m: Optional[float] = Field(
        None,
        ge=0,
        description="Distance au cours d'eau le plus proche (m)"
    )
    
    # --- Topographie & exposition ---
    pente_pct: Optional[float] = Field(
        None,
        ge=0,
        le=100,
        description="Pente moyenne en %"
    )
    exposition: Literal["N", "NE", "E", "SE", "S", "SO", "O", "NO", "plat"] = Field(
        "plat",
        description="Exposition nord/sud (cardinal ou plat)"
    )
    altitude_m: Optional[float] = Field(
        None,
        ge=-100,
        le=3000,
        description="Altitude en mètres"
    )
    risque_gel_tardif: Optional[bool] = Field(
        None,
        description="Exposition connue aux gels tardifs (mai-juin) ?"
    )
    
    # --- Logistique ---
    distance_marche_km: Optional[float] = Field(
        None,
        ge=0,
        description="Distance au marché de référence (km, ex: Nantes Saint-Lucé)"
    )
    distance_agglo_km: Optional[float] = Field(
        None,
        ge=0,
        description="Distance à l'agglomération la plus proche (km)"
    )
    acces_vehicule: Literal["facile", "limite", "difficile"] = Field(
        "facile",
        description="Accès routier vehicle"
    )
    
    # --- Métadonnées ---
    notes: str = Field(
        "",
        description="Notes libres sur la parcelle (visite impressions, etc.)"
    )
    statut: Literal["prospect", "visite", "evalue", "archive"] = Field(
        "prospect",
        description="État d'avancement de l'évaluation"
    )
    date_creation: datetime = Field(
        default_factory=datetime.utcnow,
        description="Date de création de la fiche"
    )
    date_modification: datetime = Field(
        default_factory=datetime.utcnow,
        description="Date dernière modification"
    )
    
    # --- Enrichissements depuis le datalake (calculés) ---
    meteo_precip_annuelle_mm: Optional[float] = Field(
        None,
        description="Précipitation moyenne annuelle (mm), extrait depuis Open-Meteo"
    )
    meteo_jours_gel: Optional[int] = Field(
        None,
        description="Nombre moyen de jours de gel (jour min < 0°C sur 30 ans)"
    )
    meteo_etp_annuelle_mm: Optional[float] = Field(
        None,
        description="ETP annuelle (mm), depuis Open-Meteo"
    )
    prix_comparable_eur_ha: Optional[float] = Field(
        None,
        description="Prix comparable médian du secteur (€/ha), depuis DVF"
    )
    forages_brgm_count: Optional[int] = Field(
        None,
        description="Nombre de forages BRGM dans rayon 5 km"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "72181_2024_0123",
                "nom": "Prairie Sablé Ouest",
                "surface_ha": 2.5,
                "commune": "Sablé-sur-Sarthe",
                "departement": "72",
                "coords_centroid": (47.8474, -0.9416),
                "prix_achat": 15000,
                "prix_location_annuel": 300,
                "acces_eau": "forage",
                "debit_estime_m3h": 2.5,
                "pente_pct": 3.0,
                "exposition": "S",
                "altitude_m": 45,
                "distance_marche_km": 12.5,
                "acces_vehicule": "facile",
                "statut": "visite",
                "notes": "Accès facile, forage fonctionnel",
            }
        }
