"""
Diagnostic — sources alternatives pour les cotations RNM.

Teste plusieurs datasets data.gouv.fr et analyse la structure des pages
rnm.franceagrimer.fr pour trouver une source de données accessible.

Usage :
    python scripts/diag_rnm_sources.py
"""
import json
import re
import sys

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# ---------------------------------------------------------------------------
# 1. Candidats data.gouv.fr
# ---------------------------------------------------------------------------
DATASET_SLUGS = [
    "cotations-du-reseau-des-nouvelles-des-marches",               # l'ancien (vide)
    "cotations-des-fruits-et-legumes-par-marche-et-par-produit-572288",
    "cotations-des-fruits-et-legumes-par-marche-et-par-produit-573051",
    "cotations-des-fruits-et-legumes-par-marche-et-par-produit",
]

def check_datagouv_dataset(slug: str) -> None:
    url = f"https://www.data.gouv.fr/api/1/datasets/{slug}/"
    print(f"\n{'─'*70}")
    print(f"Dataset : {slug}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        print(f"  Status : {r.status_code}")
        if r.status_code != 200:
            return
        data = r.json()
        resources = data.get("resources", [])
        print(f"  Titre  : {data.get('title', '?')}")
        print(f"  Ressources : {len(resources)}")
        for i, res in enumerate(resources):
            fmt   = res.get("format", "?")
            title = res.get("title", "?")
            dl    = res.get("url", "?")
            size  = res.get("filesize", "?")
            print(f"    [{i}] fmt={fmt}  size={size}  title={title!r}")
            print(f"         url={dl}")
    except Exception as e:
        print(f"  ERREUR : {e}")


# ---------------------------------------------------------------------------
# 2. Page rnm.franceagrimer.fr — analyse contenu brut MESSAGE=1779
# ---------------------------------------------------------------------------
def analyze_rnm_page(msg_id: int) -> None:
    url = f"https://rnm.franceagrimer.fr/prix?MESSAGE={msg_id}"
    print(f"\n{'─'*70}")
    print(f"Page RNM MESSAGE={msg_id}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        print(f"  Status : {r.status_code}  |  taille : {len(r.text)} chars")
        if r.status_code != 200:
            return
        content = r.text

        # Recherche de prix
        prices = re.findall(r"\b\d+[.,]\d{2}\b", content)
        print(f"  Patterns prix (xx.xx ou xx,xx) : {len(prices)}")
        if prices:
            print(f"  Exemples : {prices[:15]}")

        # Balises contenant des nombres
        num_lines = [l.strip() for l in content.splitlines()
                     if re.search(r"\d+[.,]\d", l) and 10 < len(l.strip()) < 300]
        print(f"  Lignes avec nombres : {len(num_lines)}")
        for l in num_lines[:10]:
            print(f"    {l[:120]}")

        # Blobs JSON inline
        json_matches = re.findall(r'(\{[^<>{}\n]{30,400}\})', content)
        print(f"  Blobs JSON-like : {len(json_matches)}")
        for b in json_matches[:3]:
            print(f"    {b[:200]}")

        # Recherche de liens de téléchargement
        dl_links = re.findall(r'href=["\']([^"\']*(?:csv|xls|xlsx|zip|download)[^"\']*)["\']',
                              content, re.IGNORECASE)
        print(f"  Liens téléchargement : {dl_links or 'aucun'}")

        # Attributs data-*
        data_attrs = re.findall(r'data-[\w-]+=["\'"][^"\']{1,80}["\']', content)
        print(f"  Attributs data-* : {len(data_attrs)}")
        for a in data_attrs[:5]:
            print(f"    {a}")

        # Balises <td> ou <tr> (même vides)
        tds = re.findall(r'<td[^>]*>', content)
        trs = re.findall(r'<tr[^>]*>', content)
        print(f"  Balises <td> : {len(tds)}  |  <tr> : {len(trs)}")

        # Balises <div class="..."> contenant des nombres
        div_with_num = re.findall(r'<div[^>]*class="[^"]*"[^>]*>[^<]*\d+[.,]\d[^<]*</div>',
                                  content)
        print(f"  <div> avec nombres : {len(div_with_num)}")
        for d in div_with_num[:5]:
            print(f"    {d[:120]}")

    except Exception as e:
        print(f"  ERREUR : {e}")


# ---------------------------------------------------------------------------
# 3. Page principale rnm.franceagrimer.fr/prix
# ---------------------------------------------------------------------------
def analyze_rnm_main() -> None:
    url = "https://rnm.franceagrimer.fr/prix"
    print(f"\n{'─'*70}")
    print(f"Page principale RNM : {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        print(f"  Status : {r.status_code}  |  taille : {len(r.text)} chars")
        content = r.text

        # Liens de la page principale
        links = re.findall(r'href=["\']([^"\']+)["\']', content)
        prix_links = [l for l in links if "prix" in l.lower() or "message" in l.lower()
                      or "csv" in l.lower() or "download" in l.lower()]
        print(f"  Liens relatifs à prix/message/csv : {len(prix_links)}")
        for l in prix_links[:20]:
            print(f"    {l}")

        # Formulaires
        forms = re.findall(r'<form[^>]*>.*?</form>', content, re.DOTALL | re.IGNORECASE)
        print(f"  Formulaires : {len(forms)}")
        for f in forms[:2]:
            print(f"    {f[:300]}")

        # Scripts src externes
        scripts = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', content)
        print(f"  Scripts externes : {scripts[:10]}")

    except Exception as e:
        print(f"  ERREUR : {e}")


# ---------------------------------------------------------------------------
# 4. Tester une plage de MESSAGE IDs autour de 1779
# ---------------------------------------------------------------------------
def probe_message_range(start: int = 1750, end: int = 1800, step: int = 5) -> None:
    print(f"\n{'─'*70}")
    print(f"Sonde MESSAGE IDs {start}..{end} (step={step})")
    for msg_id in range(start, end + 1, step):
        url = f"https://rnm.franceagrimer.fr/prix?MESSAGE={msg_id}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            size = len(r.text)
            prices = len(re.findall(r"\b\d+[.,]\d{2}\b", r.text)) if r.status_code == 200 else 0
            status_tag = "✓" if r.status_code == 200 else "✗"
            print(f"  {status_tag} MESSAGE={msg_id}  status={r.status_code}  "
                  f"size={size:7d}  prix_patterns={prices:4d}")
        except Exception as e:
            print(f"  ✗ MESSAGE={msg_id}  ERREUR: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print("DIAGNOSTIC — Sources cotations RNM")
    print("=" * 70)

    # data.gouv.fr
    for slug in DATASET_SLUGS:
        check_datagouv_dataset(slug)

    # Page principale
    analyze_rnm_main()

    # Page MESSAGE=1779 en détail
    analyze_rnm_page(1779)

    # Sonde plage de IDs
    probe_message_range(1760, 1790, step=2)

    print("\n" + "=" * 70)
    print("Diagnostic terminé.")
