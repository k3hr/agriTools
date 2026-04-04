"""
Critères individuels de scoring pour le moteur d'évaluation parcellaire.

Chaque critère est une fonction qui prend un champ de la Parcelle et retourne
un score 0–100. Les scores sont indépendants, pondérés à l'agrégation.
"""
from typing import Optional


class ScoringCriteria:
    """Catalogue de critères de scoring avec logique d'évaluation."""
    
    # ==========================================================================
    # AXE ÉCONOMIQUE & LOGISTIQUE
    # ==========================================================================
    
    @staticmethod
    def prix_achat_reasonableness(
        prix_demande_eur_ha: Optional[float],
        prix_comparable_eur_ha: Optional[float]
    ) -> int:
        """
        Score d'opportunité du prix d'achat.
        
        Comparaison prix_demande vs prix_comparable (DVF).
        - Si prix_demande ≤ prix_comparable * 0.9 : excellent (90–100)
        - Si prix_demande ≤ prix_comparable * 1.0 : bon (70–90)
        - Si prix_demande ≤ prix_comparable * 1.2 : acceptable (40–70)
        - Si prix_demande > prix_comparable * 1.2 : mauvais (0–40)
        - Si données manquantes : 50 (neutre)
        """
        if prix_demande_eur_ha is None or prix_comparable_eur_ha is None:
            return 50
        
        ratio = prix_demande_eur_ha / prix_comparable_eur_ha
        
        if ratio <= 0.9:
            return int(90 + (0.9 - ratio) / 0.1 * 10)  # 90–100
        elif ratio <= 1.0:
            return int(70 + (1.0 - ratio) / 0.1 * 20)  # 70–90
        elif ratio <= 1.2:
            return int(40 + (1.2 - ratio) / 0.2 * 30)  # 40–70
        else:
            return max(0, int(40 - (ratio - 1.2) * 10))  # 0–40
    
    @staticmethod
    def distance_marche(distance_km: Optional[float]) -> int:
        """
        Score d'accessibilité au marché.
        
        - 0–5 km : excellent (90–100)
        - 5–15 km : bon (70–90)
        - 15–30 km : acceptable (40–70)
        - 30+ km : mauvais (0–40)
        - Données manquantes : 50
        """
        if distance_km is None:
            return 50
        
        if distance_km <= 5:
            return int(90 + min(10, (5 - distance_km) / 5 * 10))  # 90–100
        elif distance_km <= 15:
            return int(70 + (15 - distance_km) / 10 * 20)  # 70–90
        elif distance_km <= 30:
            return int(40 + (30 - distance_km) / 15 * 30)  # 40–70
        else:
            return max(0, int(40 - (distance_km - 30) / 20 * 40))  # 0–40
    
    @staticmethod
    def acces_vehicule(acces: str) -> int:
        """
        Score d'accès routier.
        
        - facile : 90
        - limite : 60
        - difficile : 20
        """
        mapping = {"facile": 90, "limite": 60, "difficile": 20}
        return mapping.get(acces, 50)
    
    # ==========================================================================
    # AXE EAU & IRRIGATION
    # ==========================================================================
    
    @staticmethod
    def acces_eau(acces_eau: str) -> int:
        """
        Score qualité d'accès à l'eau.
        
        - reseau : excellent (95)
        - forage + débit Ok : bon (80)
        - riviere : bon (75)
        - forage + débit faible : acceptable (40)
        - aucun : très mauvais (5)
        - inconnu : neutre (30)
        """
        mapping = {
            "reseau": 95,
            "riviere": 75,
            "forage": 80,  # Sera nuancé par debit_estime
            "aucun": 5,
            "inconnu": 30
        }
        return mapping.get(acces_eau, 30)
    
    @staticmethod
    def debit_eau(debit_m3h: Optional[float], acces_eau: str) -> int:
        """
        Score complément du débit estimé.
        
        Affine le score d'accès eau si forage/rivière.
        - Forage < 1 m³/h : pénalité forte (−30)
        - Forage 1–2 m³/h : pénalité légère (−10)
        - Forage > 2 m³/h : pas de pénalité
        - Rivière : données non pertinentes ici
        """
        if debit_m3h is None or acces_eau not in ("forage", "riviere"):
            return 0  # Pas d'ajustement
        
        if acces_eau == "riviere":
            return 0  # On suppose accès ok
        
        # Forage
        if debit_m3h < 1:
            return -30
        elif debit_m3h < 2:
            return -10
        else:
            return 0
    
    @staticmethod
    def precipitation_adequacy(precip_mm: Optional[float]) -> int:
        """
        Score adéquation pluviométrie annuelle.
        
        Légumes à tige, feuille = 300–600 mm (optimal 400–500).
        Fruits = besoin plus élevé.
        
        - 300–700 mm : bon (70–90)
        - 200–300 mm ou 700–900 mm : acceptable (50–70)
        - < 200 mm ou > 900 mm : mauvais (20–50)
        """
        if precip_mm is None:
            return 50
        
        if 300 <= precip_mm <= 700:
            # Optimal proche de 500
            if 400 <= precip_mm <= 600:
                return int(85 + min(5, abs(500 - precip_mm) / 100))  # 85–90
            else:
                return int(70 + (min(abs(300 - precip_mm), abs(700 - precip_mm)) / 150))  # 70–85
        elif 200 <= precip_mm < 300:
            return int(50 + (precip_mm - 200) / 100 * 20)  # 50–70
        elif 700 < precip_mm <= 900:
            return int(50 + (900 - precip_mm) / 200 * 20)  # 50–70
        else:
            return max(20, int(min(50, precip_mm / 10)))  # 20–50
    
    # ==========================================================================
    # AXE TOPOGRAPHIE & EXPOSITION
    # ==========================================================================
    
    @staticmethod
    def pente_adequacy(pente_pct: Optional[float]) -> int:
        """
        Score adéquation pente.
        
        Maraîchage optimal : 0–5 %. Pente > 15 % devient problématique.
        
        - 0–3 % : excellent (90–100)
        - 3–8 % : bon (70–90)
        - 8–15 % : acceptable (40–70)
        - 15–25 % : mauvais (15–40)
        - > 25 % : très mauvais (0–15)
        """
        if pente_pct is None:
            return 50
        
        if pente_pct <= 3:
            return int(90 + min(10, (3 - pente_pct) / 3 * 10))  # 90–100
        elif pente_pct <= 8:
            return int(70 + (8 - pente_pct) / 5 * 20)  # 70–90
        elif pente_pct <= 15:
            return int(40 + (15 - pente_pct) / 7 * 30)  # 40–70
        elif pente_pct <= 25:
            return int(15 + (25 - pente_pct) / 10 * 25)  # 15–40
        else:
            return max(0, int(15 - (pente_pct - 25) / 10 * 15))  # 0–15
    
    @staticmethod
    def exposition_adequacy(exposition: str) -> int:
        """
        Score orientation soleil.
        
        Maraîchage bénéficie du soleil : S & SE & SO > N & NO & NE.
        
        - S, SE, SO : excellent (85–95)
        - E, O : bon (70–80)
        - NE, NO : acceptable (50–60)
        - N : mauvais (30–40)
        - plat : neutre (65)
        """
        mapping = {
            "S": 90,
            "SE": 85,
            "SO": 85,
            "E": 75,
            "O": 75,
            "NE": 55,
            "NO": 55,
            "N": 35,
            "plat": 65
        }
        return mapping.get(exposition, 50)
    
    @staticmethod
    def risque_gel(risque_gel: Optional[bool], jours_gel: Optional[int]) -> int:
        """
        Score risque gel tardif (mai–juin).
        
        Gels tardifs = risque majeur pour semis mai (tomate, courges, etc.).
        
        - Pas de risque déclaré : 80
        - Risque déclaré : pénalité −30 à −50
        - Nombre de jours gel connu :
          - < 5 j : bon (70–80)
          - 5–10 j : acceptable (50–70)
          - > 10 j : mauvais (20–50)
        """
        if risque_gel is None and jours_gel is None:
            return 70  # Neutre, on assume pas de grosse exposition
        
        if jours_gel is not None:
            if jours_gel < 5:
                return int(75 + min(5, (5 - jours_gel) / 5 * 5))  # 75–80
            elif jours_gel <= 10:
                return int(50 + (10 - jours_gel) / 5 * 20)  # 50–70
            else:
                return max(20, int(50 - (jours_gel - 10) / 10 * 30))  # 20–50
        
        # Juste le flag booléen
        if risque_gel:
            return 40  # Mauvais, risque déclaré
        else:
            return 80  # Bon
    
    @staticmethod
    def altitude_adequacy(altitude_m: Optional[float]) -> int:
        """
        Score altitude.
        
        Maraîchage optimal : 0–200 m. Au-delà = saison raccourcie.
        
        - 0–150 m : excellent (85–95)
        - 150–250 m : bon (70–85)
        - 250–400 m : acceptable (50–70)
        - 400–600 m : mauvais (20–50)
        - > 600 m : très mauvais (0–20)
        """
        if altitude_m is None:
            return 65  # Neutre
        
        if altitude_m <= 150:
            return int(85 + min(10, (150 - altitude_m) / 100 * 10))  # 85–95
        elif altitude_m <= 250:
            return int(70 + (250 - altitude_m) / 100 * 15)  # 70–85
        elif altitude_m <= 400:
            return int(50 + (400 - altitude_m) / 150 * 20)  # 50–70
        elif altitude_m <= 600:
            return int(20 + (600 - altitude_m) / 200 * 30)  # 20–50
        else:
            return max(0, int(20 - (altitude_m - 600) / 400 * 20))  # 0–20
