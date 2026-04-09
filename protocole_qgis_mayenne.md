# Protocole QGIS — Analyse concurrentielle territoriale Mayenne
## Projet maraîchage / pépinière / myciculture — Sud Mayenne

---

## PARTIE 1 — Carte de chaleur concurrentielle (logique "trou géographique")

### Objectif
Identifier les zones du rayon 30 min autour de Laval qui sont sous-couvertes
en producteurs bio en vente directe, pondérées par la densité de population.

---

### Étape 0 — Setup QGIS

```
QGIS version recommandée : 3.34 LTS ou supérieure
Plugins à installer (Gestionnaire d'extensions) :
  - ORS Tools          → isochrones (OpenRouteService)
  - QuickMapServices   → fonds de carte OSM/IGN
  - MMQGIS             → géocodage CSV par adresse
  - DataPlotly         → visualisation optionnelle
CRS de travail : EPSG:2154 (RGF93 / Lambert-93) — obligatoire pour les calculs de distance en France
```

---

### Étape 1 — Collecte des données sources

#### 1a. Producteurs bio en vente directe (concurrents)

**Source principale : CIVAM Bio 53**
```
URL : https://www.civambio53.fr/
Action : carte interactive → filtrer "Légumes" + "Vente directe"
         Pas d'export direct disponible → scraping manuel ou copie
Format cible : CSV avec colonnes [nom, adresse, commune, cp, type_produit, circuit_vente]
```

**Sources complémentaires à croiser :**
```
https://www.acheteralasource.com/producteurs-en-france/legumes/departement/53/
https://www.jours-de-marche.fr/producteur-local/53-mayenne/
https://www.reseau-amap.org/amap-53.htm        → AMAP avec producteur légumes associé
https://www.avenir-bio.fr/amap,mayenne,53.html → idem
Pages Jaunes : "maraîchers bio" 53
```

**Script Python pour consolider les CSV :**
```python
import pandas as pd
import glob

# Consolider tous les CSV sources dans un dossier /data/sources/
dfs = []
for f in glob.glob("data/sources/*.csv"):
    df = pd.read_csv(f)
    df["source"] = f
    dfs.append(df)

producteurs = pd.concat(dfs, ignore_index=True)

# Normaliser les colonnes minimales attendues
producteurs = producteurs.rename(columns={
    # adapter selon les colonnes réelles de chaque source
    "Nom": "nom",
    "Adresse": "adresse",
    "Commune": "commune",
    "Code Postal": "cp",
})

# Dédupliquer sur (nom, commune)
producteurs = producteurs.drop_duplicates(subset=["nom", "commune"])

# Exporter
producteurs[["nom", "adresse", "commune", "cp", "type_produit"]].to_csv(
    "data/producteurs_53_consolide.csv", index=False
)
print(f"{len(producteurs)} producteurs uniques")
```

#### 1b. Données population INSEE (carroyage 200m)

```
URL : https://www.insee.fr/fr/statistiques/6215140
Fichier : Filosofi2019_carreaux_200m_gpkg.zip  (~1.5 Go France entière)
Couche à charger dans QGIS : carreaux_200m
Champ utile : Ind (nombre d'individus par carreau)

Alternative légère si le fichier complet est trop lourd :
  → Télécharger uniquement la dalle couvrant la Mayenne via
    https://geoservices.ign.fr/filosofi  (découpage par département)
```

#### 1c. Communes Mayenne (contours)

```
Source : IGN Admin Express
URL : https://geoservices.ign.fr/adminexpress
Fichier : ADMIN-EXPRESS-COG_3-2__SHP_LAMB93_FXX_2024-02-22.7z
Couche : COMMUNE
Filtre : INSEE_DEP = '53'
```

#### 1d. Réseau routier (pour isochrones)

```
Pas besoin de télécharger — ORS Tools utilise l'API OpenRouteService
qui s'appuie sur OpenStreetMap en ligne.
Créer un compte gratuit sur https://openrouteservice.org/
→ API key gratuite : 2 000 requêtes/jour (largement suffisant)
```

---

### Étape 2 — Géocodage des producteurs

#### Option A : MMQGIS (dans QGIS)
```
Menu : MMQGIS → Geocode → Geocode CSV with Web Service
Service : Nominatim (OpenStreetMap, gratuit, pas de clé requise)
Champ adresse : concaténer "adresse + commune + cp + France"
Output : couche vectorielle points "producteurs_geocodes"

⚠️  Nominatim limite à ~1 req/seconde.
    Pour 150 producteurs : ~3 minutes. Acceptable.
```

#### Option B : Python (batch plus fiable)
```python
import pandas as pd
import requests
import time

df = pd.read_csv("data/producteurs_53_consolide.csv")

def geocode_nominatim(row):
    query = f"{row['adresse']}, {row['commune']}, {row['cp']}, France"
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 1}
    headers = {"User-Agent": "analyse-marche-maraichage-53/1.0"}
    r = requests.get(url, params=params, headers=headers)
    time.sleep(1.1)  # respect rate limit Nominatim
    if r.ok and r.json():
        hit = r.json()[0]
        return float(hit["lat"]), float(hit["lon"])
    return None, None

df[["lat", "lon"]] = df.apply(geocode_nominatim, axis=1, result_type="expand")
df.dropna(subset=["lat", "lon"]).to_csv("data/producteurs_geocodes.csv", index=False)

# Convertir en GeoJSON pour QGIS
import geopandas as gpd
from shapely.geometry import Point

gdf = gpd.GeoDataFrame(
    df.dropna(subset=["lat", "lon"]),
    geometry=gpd.points_from_xy(df["lon"], df["lat"]),
    crs="EPSG:4326"
).to_crs("EPSG:2154")

gdf.to_file("data/producteurs_geocodes.gpkg", driver="GPKG")
print(f"Exporté : {len(gdf)} producteurs géocodés")
```

---

### Étape 3 — Isochrones autour de chaque producteur

#### Dans QGIS avec ORS Tools
```
Menu : ORS Tools → Isochrones → Isochrones from Layer
  Input layer    : producteurs_geocodes
  Profile        : driving-car
  Dimension      : time
  Ranges         : 15,30          (minutes)
  API Key        : [ta clé ORS]

→ Résultat : polygones isochrones 15 min et 30 min autour de chaque producteur
→ Nommer la couche : "isochrones_producteurs"
```

**Astuce pour économiser les requêtes API :**
```
Si tu as 150 producteurs × 2 plages = 300 requêtes.
Bien en dessous du quota gratuit ORS (2000/jour).
Mais si plusieurs producteurs sont dans la même commune :
regrouper en un seul point représentatif avant les requêtes.
```

---

### Étape 4 — Zone d'étude (isochrone 30 min depuis Laval)

```
Menu : ORS Tools → Isochrones → Isochrones from Point
  Point : centre de Laval (lon=-0.7726, lat=48.0742)
  Profile : driving-car
  Range : 30 (minutes)
→ Nommer : "zone_etude_30min_laval"
```

---

### Étape 5 — Calcul de la densité de couverture

#### 5a. Union des isochrones producteurs
```
Menu : Vecteur → Outils de géotraitement → Fusionner
  Input : isochrones_producteurs (uniquement les 15 min)
→ Résultat : "zone_couverte_15min" (union de tous les bassins à 15 min)
```

#### 5b. Zone non couverte dans le rayon d'étude
```
Menu : Vecteur → Outils de géotraitement → Différence
  Input layer    : zone_etude_30min_laval
  Overlay layer  : zone_couverte_15min
→ Résultat : "zone_non_couverte"  ← ce sont tes "trous"
```

#### 5c. Pondération par population
```
Menu : Vecteur → Outils d'analyse → Somme des longueurs de lignes
  (utiliser plutôt "Statistiques zonales" pour rasters)

En réalité : joindre les carreaux INSEE 200m à la zone non couverte :
  Menu : Vecteur → Outils de géotraitement → Intersection
    Input  : carreaux_200m (filtré sur l'emprise Mayenne)
    Overlay : zone_non_couverte
  → Résultat : carreaux avec leur population dans les zones non couvertes

  Puis : Calculatrice de champ → nouveau champ "pop_non_couverte" = Ind
  Résumé par zone : somme de pop_non_couverte par secteur
```

---

### Étape 6 — Carte de chaleur (heatmap)

#### Option A : Heatmap sur les producteurs existants (densité offre)
```
Couche producteurs_geocodes → Propriétés → Symbologie
  Type de rendu : Carte de chaleur (Heatmap)
  Rayon        : 15 000 m  (15 km)
  Pondération  : aucune (ou par nb_produits si disponible)
  Palette      : Vert (forte densité) → Blanc → Rouge (faible densité)
  → Plus c'est rouge = moins il y a de producteurs = opportunité
```

#### Option B : Raster densité population non couverte (plus précis)
```python
# Générer un raster de population dans les zones non couvertes
# depuis les carreaux INSEE intersectés

import geopandas as gpd
import numpy as np
from rasterio.transform import from_bounds
import rasterio

zones_non_couvertes = gpd.read_file("data/zone_non_couverte.gpkg")
carreaux = gpd.read_file("data/carreaux_200m_53.gpkg")

# Intersection
pop_trous = gpd.overlay(carreaux, zones_non_couvertes, how="intersection")
pop_trous["pop"] = pop_trous["Ind"]

# Exporter pour visualisation QGIS
pop_trous[["geometry", "pop"]].to_file("data/population_trous.gpkg", driver="GPKG")
```

---

### Étape 7 — Mise en page finale

```
Couches à superposer (ordre d'affichage, du bas vers le haut) :
  1. Fond IGN ou OSM (via QuickMapServices)
  2. Communes 53 (contour fin gris)
  3. Carreaux population (transparence 60%, dégradé blanc→orange)
  4. zone_etude_30min_laval (contour bleu épais, fond transparent)
  5. isochrones_producteurs 30min (remplissage vert très transparent)
  6. isochrones_producteurs 15min (remplissage vert semi-transparent)
  7. zone_non_couverte (remplissage rouge transparent = "trous")
  8. producteurs_geocodes (points noirs avec étiquette nom)
  9. Laval + villes principales (étoile ou carré)

Mise en page (Layout) :
  - Titre : "Couverture maraîchage bio vente directe — Mayenne 30 min Laval"
  - Légende : couches 4,5,6,7,8
  - Échelle : 1:200 000
  - Nord
  - Source : CIVAM Bio 53, INSEE Filosofi, OpenStreetMap, ORS Tools
```

---

## PARTIE 2 — Analyse "produit absent" (logique de niche)

### Objectif
Identifier quels produits sont absents ou sous-représentés dans le maillage
existant, et dans quelles zones la demande potentielle est la plus forte
pour ces produits.

---

### Logique générale

La carte concurrentielle (Partie 1) raisonne en **offre géographique**.
L'analyse produit raisonne en **offre × demande par type de produit**.

Formule : `Score_opportunité(produit, zone) = Demande_estimée - Offre_existante`

---

### Étape 1 — Matrice produits × présence en 53

#### 1a. Recenser les produits par producteur

Reprendre le CSV producteurs consolidé et ajouter une colonne `type_produit`
(multi-valeur si nécessaire). Catégories à distinguer :

```
légumes_generaux | légumes_rares | plants_maraîchers | fruitiers_anciens |
petits_fruits | aromatiques_medicinales | champignons | conserves_lacto |
conserves_sucrées | miel_apiculture | spiruline | oeufs | viande |
produits_laitiers | pain_farine | jus_cidre
```

**Script de comptage par catégorie :**
```python
import pandas as pd

df = pd.read_csv("data/producteurs_53_consolide.csv")

# Exploser les types produits (si plusieurs par ligne)
df_exploded = df.assign(
    type_produit=df["type_produit"].str.split("|")
).explode("type_produit")

df_exploded["type_produit"] = df_exploded["type_produit"].str.strip()

# Comptage
comptage = (
    df_exploded
    .groupby("type_produit")
    .agg(
        nb_producteurs=("nom", "nunique"),
        zones=("commune", lambda x: ", ".join(x.unique()[:5]))
    )
    .sort_values("nb_producteurs")
    .reset_index()
)

print(comptage.to_string())
comptage.to_csv("data/matrice_produits_53.csv", index=False)
```

**Résultat attendu (estimé d'après les données collectées) :**

| Produit | Nb producteurs estimés en 53 | Statut |
|---|---|---|
| légumes_generaux | 20–30 | Présent |
| miel_apiculture | 15–20 | Présent |
| oeufs | 15–20 | Présent |
| pain_farine | 8–12 | Présent |
| jus_cidre | 8–12 | Présent |
| aromatiques_medicinales | 3–5 | Rare |
| conserves_sucrées | 3–5 | Rare |
| plants_maraîchers | 2–4 | Rare |
| fruitiers_anciens | 1–2 | Quasi-absent |
| champignons | 0–1 | **Absent** |
| conserves_lacto | 0–1 | **Absent** |
| spiruline | 0 | **Absent** |
| petits_fruits | 1–3 | Rare |

---

### Étape 2 — Estimation de la demande par produit et par zone

#### 2a. Proxies de demande disponibles en open data

```
Restaurants gastronomiques / bistrots locaux
  Source : Sirene (INSEE) — NAF 5610A (restaurants)
  URL    : https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises/
  Filtre : dep=53, naf=5610A, etatadministratifetablissement=A (actif)
  Intérêt : clients potentiels pour champignons, herbes, conserves B2B

Épiceries bio (magasins spécialisés)
  Source : OpenStreetMap (Overpass API)
  Requête : shop=organic dans bbox Mayenne
  Intérêt : points de vente potentiels pour pépinière, conserves, spiruline

Marchés de plein air
  Source : data.gouv.fr dataset "Marchés" ou OpenStreetMap
  amenity=marketplace dans bbox Mayenne
  Intérêt : débouchés vente directe ponctuels

CSP+ / revenus médians par commune (proxy demande premium)
  Source : INSEE Filosofi revenus 2020
  URL    : https://www.insee.fr/fr/statistiques/6036907
  Champ  : MED20 (revenu médian par UC)
  Intérêt : pondérer les zones selon la solvabilité de la clientèle cible

Population 25–55 ans (cœur de cible circuits courts)
  Source : INSEE RP 2020 par commune
  URL    : https://www.insee.fr/fr/statistiques/6543200
  Champ  : P20_POP2554
```

#### 2b. Script de collecte Sirene (restaurants actifs 53)
```python
import requests
import pandas as pd

# API Sirene publique (sans authentification)
url = "https://api.insee.fr/entreprises/sirene/V3.11/siret"
# ⚠️ nécessite un token INSEE gratuit : https://api.insee.fr/catalogue/
# Alternative sans token : télécharger le fichier Sirene complet

# Version fichier local (plus fiable) :
# Télécharger StockEtablissement_utf8.zip sur data.gouv.fr
# puis filtrer localement :

sirene = pd.read_csv(
    "data/StockEtablissement_utf8.csv",
    usecols=["siret", "denominationUsuelleEtablissement",
             "codePostalEtablissement", "libelleCommuneEtablissement",
             "activitePrincipaleEtablissement", "etatAdministratifEtablissement"],
    dtype=str
)

restaurants_53 = sirene[
    (sirene["codePostalEtablissement"].str.startswith("53")) &
    (sirene["activitePrincipaleEtablissement"].isin(["56.10A", "56.10B"])) &
    (sirene["etatAdministratifEtablissement"] == "A")
].copy()

restaurants_53.to_csv("data/restaurants_actifs_53.csv", index=False)
print(f"{len(restaurants_53)} restaurants actifs en Mayenne")
```

#### 2c. Collecte OSM (épiceries bio + marchés)
```python
import requests
import geopandas as gpd
from shapely.geometry import Point

# Overpass API — épiceries bio en Mayenne
overpass_url = "https://overpass-api.de/api/interpreter"

query_bio = """
[out:json][timeout:30];
area["ISO3166-2"="FR-53"]->.searchArea;
(
  node["shop"="organic"](area.searchArea);
  way["shop"="organic"](area.searchArea);
  node["shop"="health_food"](area.searchArea);
);
out center;
"""

query_marches = """
[out:json][timeout:30];
area["ISO3166-2"="FR-53"]->.searchArea;
(
  node["amenity"="marketplace"](area.searchArea);
  way["amenity"="marketplace"](area.searchArea);
);
out center;
"""

def fetch_osm(query):
    r = requests.post(overpass_url, data={"data": query})
    elements = r.json()["elements"]
    rows = []
    for e in elements:
        lat = e.get("lat") or e.get("center", {}).get("lat")
        lon = e.get("lon") or e.get("center", {}).get("lon")
        rows.append({
            "nom": e.get("tags", {}).get("name", "?"),
            "lat": lat, "lon": lon,
            "tags": str(e.get("tags", {}))
        })
    return pd.DataFrame(rows)

epiceries = fetch_osm(query_bio)
marches = fetch_osm(query_marches)

epiceries.to_csv("data/epiceries_bio_53.csv", index=False)
marches.to_csv("data/marches_53.csv", index=False)
print(f"{len(epiceries)} épiceries bio, {len(marches)} marchés")
```

---

### Étape 3 — Score d'opportunité par produit × zone

```python
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point

# Charger les couches
communes = gpd.read_file("data/communes_53.gpkg").to_crs("EPSG:2154")
producteurs = gpd.read_file("data/producteurs_geocodes.gpkg")
restaurants = pd.read_csv("data/restaurants_actifs_53.csv")
epiceries = pd.read_csv("data/epiceries_bio_53.csv")

# Géocoder restaurants et épiceries si besoin (même script que Partie 1)
# [...]

# Joindre les producteurs aux communes (spatial join)
prod_communes = gpd.sjoin(
    producteurs, communes[["NOM_COM", "geometry"]],
    how="left", predicate="within"
)

# Compter les producteurs de chaque type par commune
for produit in ["champignons", "conserves_lacto", "plants_maraîchers",
                "fruitiers_anciens", "aromatiques_medicinales"]:
    col = f"nb_{produit}"
    counts = (
        prod_communes[prod_communes["type_produit"].str.contains(produit, na=False)]
        .groupby("NOM_COM")
        .size()
        .reset_index(name=col)
    )
    communes = communes.merge(counts, on="NOM_COM", how="left")
    communes[col] = communes[col].fillna(0)

# Joindre revenus médians INSEE
revenus = pd.read_csv("data/revenus_medians_53.csv")  # MED20 par commune
communes = communes.merge(revenus[["CODGEO", "MED20"]], left_on="INSEE_COM",
                          right_on="CODGEO", how="left")

# Compter les restaurants par commune (proxy demande B2B)
rest_gdf = gpd.GeoDataFrame(
    restaurants.dropna(subset=["lat", "lon"]),
    geometry=gpd.points_from_xy(restaurants["lon"], restaurants["lat"]),
    crs="EPSG:4326"
).to_crs("EPSG:2154")

rest_communes = gpd.sjoin(rest_gdf, communes[["NOM_COM", "geometry"]],
                          how="left", predicate="within")
rest_count = rest_communes.groupby("NOM_COM").size().reset_index(name="nb_restaurants")
communes = communes.merge(rest_count, on="NOM_COM", how="left")
communes["nb_restaurants"] = communes["nb_restaurants"].fillna(0)

# Score opportunité champignons (exemple)
# Formule : (nb_restaurants × 2 + revenu_médian_normalisé) - (nb_champignons × 10)
rev_max = communes["MED20"].max()
communes["score_champignons"] = (
    (communes["nb_restaurants"] * 2 +
     communes["MED20"].fillna(0) / rev_max * 5)
    - communes["nb_champignons"] * 10
).clip(lower=0)

# Normaliser entre 0 et 10
s = communes["score_champignons"]
communes["score_champignons_norm"] = (s - s.min()) / (s.max() - s.min()) * 10

communes.to_file("data/communes_scores.gpkg", driver="GPKG")
print("Scores calculés et exportés")
```

---

### Étape 4 — Visualisation dans QGIS

#### Carte choroplèthe par score produit
```
Charger : communes_scores.gpkg
Propriétés → Symbologie :
  Type      : Graduated (gradué)
  Colonne   : score_champignons_norm  (ou autre produit)
  Classes   : 5
  Palette   : Blanc → Rouge foncé  (ou YlOrRd)
  Mode      : Natural Breaks (Jenks)

Répéter pour chaque produit cible → créer un groupe de couches
avec visibilité exclusive (une seule visible à la fois)
→ utiliser le panneau "Couches" pour switcher rapidement
```

#### Superposition des deux analyses
```
Couches finales superposées :
  1. Fond OSM
  2. Communes 53 (contour)
  3. Choroplèthe score produit (une couche par produit)
  4. Zone 30 min Laval (contour bleu)
  5. Isochrones producteurs existants (semi-transparent)
  6. Points restaurants actifs (triangles jaunes)
  7. Épiceries bio (étoiles vertes)
  8. Points producteurs existants (cercles noirs)
  9. Villes principales (labels)
```

---

### Étape 5 — Export des zones cibles

```python
# Identifier les communes dans le top 20% de score ET dans le rayon 30 min Laval

zone_30min = gpd.read_file("data/zone_30min_laval.gpkg").to_crs("EPSG:2154")
communes_scores = gpd.read_file("data/communes_scores.gpkg")

# Filtrer communes dans la zone d'étude
communes_zone = gpd.overlay(communes_scores, zone_30min, how="intersection")

# Top 20% pour chaque produit
for produit in ["champignons", "conserves_lacto", "plants_maraîchers"]:
    col = f"score_{produit}_norm"
    if col in communes_zone.columns:
        seuil = communes_zone[col].quantile(0.8)
        top = communes_zone[communes_zone[col] >= seuil].sort_values(col, ascending=False)
        print(f"\n=== Top communes pour {produit} ===")
        print(top[["NOM_COM", col, "nb_restaurants", "MED20"]].head(10).to_string())
        top.to_file(f"data/top_communes_{produit}.gpkg", driver="GPKG")
```

---

## Récapitulatif des fichiers produits

```
data/
├── producteurs_53_consolide.csv      ← sources fusionnées
├── producteurs_geocodes.gpkg         ← points géocodés
├── isochrones_producteurs.gpkg       ← zones de couverture 15/30 min
├── zone_etude_30min_laval.gpkg       ← périmètre d'étude
├── zone_non_couverte.gpkg            ← "trous" géographiques
├── restaurants_actifs_53.csv         ← proxy demande B2B
├── epiceries_bio_53.csv              ← points de vente potentiels
├── marches_53.csv                    ← marchés OSM
├── communes_scores.gpkg              ← scores opportunité par produit
└── top_communes_[produit].gpkg       ← zones cibles par produit
```

---

## Sources de données — récapitulatif

| Donnée | Source | URL | Format | Gratuit |
|---|---|---|---|---|
| Producteurs bio 53 | CIVAM Bio 53 | civambio53.fr | Manuel/scraping | Oui |
| AMAP 53 | Réseau AMAP | reseau-amap.org | Manuel | Oui |
| Population carroyée | INSEE Filosofi | insee.fr | GPKG | Oui |
| Communes contours | IGN Admin Express | geoservices.ign.fr | SHP | Oui |
| Revenus médians | INSEE Filosofi | insee.fr | CSV | Oui |
| Restaurants actifs | INSEE Sirene | data.gouv.fr | CSV | Oui |
| Épiceries bio + marchés | OpenStreetMap | Overpass API | JSON | Oui |
| Isochrones routiers | OpenRouteService | openrouteservice.org | Via API | Oui (2000/j) |

**Coût total : 0€**

---

## Ordre de réalisation recommandé

```
Semaine 1 :
  □ Installer QGIS + plugins (ORS Tools, MMQGIS, QuickMapServices)
  □ Créer compte OpenRouteService (API key gratuite)
  □ Télécharger IGN Admin Express + INSEE Filosofi (population)
  □ Consolider manuellement les producteurs CIVAM + AMAP → CSV

Semaine 2 :
  □ Géocoder les producteurs (script Python ou MMQGIS)
  □ Générer les isochrones (ORS Tools)
  □ Calculer zones non couvertes (différence vectorielle QGIS)
  □ Première carte "trous géographiques"

Semaine 3 :
  □ Collecter données demande (Sirene, OSM Overpass)
  □ Télécharger revenus médians INSEE
  □ Calculer scores opportunité par produit (script Python)
  □ Cartes choropl��thes par produit

Semaine 4 :
  □ Superposer les deux analyses
  □ Identifier les 3-5 communes/zones cibles
  □ Export PDF des cartes finales
  □ Croiser avec données foncières SAFER / DVF+ (déjà maîtrisé)
```
