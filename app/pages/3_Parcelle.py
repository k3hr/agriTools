"""
Formulaire de saisie de parcelle candidate pour le module implantation.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import streamlit as st
from implantation.models.parcelle import Parcelle
from implantation.reports.pdf_report import generate_pdf
from implantation.scoring.engine import ScoringEngine
from app.components.parcelle import list_parcelles, save_parcelle

st.set_page_config(page_title="Parcelle — agriTools", page_icon="🧭", layout="wide")
st.title("🧭 Parcelle candidate")

st.markdown(
    """
    ## Saisie d'une parcelle

    Ce formulaire permet de créer une fiche de parcelle candidate.
    Les données sont validées via le modèle Pydantic `Parcelle`.
    """
)

with st.form("parcelle_form"):
    st.subheader("Informations principales")
    col1, col2, col3 = st.columns(3)
    with col1:
        parcelle_id = st.text_input("Identifiant", placeholder="72181_2026_0123")
        nom = st.text_input("Nom / libellé", placeholder="Prairie Ouest")
        surface_ha = st.number_input("Surface (ha)", min_value=0.1, value=1.0, step=0.1)
        commune = st.text_input("Commune", placeholder="Sablé-sur-Sarthe")
        departement = st.text_input("Département", placeholder="72")
    with col2:
        coords_lat = st.number_input("Latitude", format="%.6f", value=47.847400)
        coords_lon = st.number_input("Longitude", format="%.6f", value=-0.941600)
        prix_achat = st.number_input("Prix d'achat (€)", min_value=0.0, value=0.0, step=100.0)
        prix_location_annuel = st.number_input("Prix location annuel (€)", min_value=0.0, value=0.0, step=50.0)
        prix_comparable = st.number_input("Prix comparable €/ha", min_value=0.0, value=0.0, step=100.0)
    with col3:
        acces_eau = st.selectbox(
            "Accès à l'eau",
            ["forage", "riviere", "reseau", "aucun", "inconnu"],
            index=4,
        )
        debit_estime_m3h = st.number_input("Débit estimé (m³/h)", min_value=0.0, value=0.0, step=0.1)
        distance_cours_eau_m = st.number_input("Distance cours d'eau (m)", min_value=0.0, value=0.0, step=10.0)
        distance_marche_km = st.number_input("Distance marché (km)", min_value=0.0, value=10.0, step=1.0)
        distance_agglo_km = st.number_input("Distance agglo (km)", min_value=0.0, value=20.0, step=1.0)

    st.markdown("---")
    st.subheader("Topographie & logistique")
    col4, col5, col6 = st.columns(3)
    with col4:
        pente_pct = st.number_input("Pente (%)", min_value=0.0, max_value=100.0, value=2.0, step=0.5)
        exposition = st.selectbox(
            "Exposition",
            ["N", "NE", "E", "SE", "S", "SO", "O", "NO", "plat"],
            index=4,
        )
    with col5:
        altitude_m = st.number_input("Altitude (m)", min_value=-100.0, max_value=2000.0, value=50.0, step=1.0)
        risque_gel_tardif = st.checkbox("Risque gel tardif")
        acces_vehicule = st.selectbox(
            "Accès véhicule",
            ["facile", "limite", "difficile"],
            index=0,
        )
    with col6:
        statut = st.selectbox(
            "Statut",
            ["prospect", "visite", "evalue", "archive"],
            index=0,
        )
        notes = st.text_area("Notes", height=150)

    submit = st.form_submit_button("Enregistrer la parcelle")

if submit:
    try:
        parcelle = Parcelle(
            id=parcelle_id.strip(),
            nom=nom.strip(),
            surface_ha=surface_ha,
            commune=commune.strip(),
            departement=departement.strip(),
            coords_centroid=(coords_lat, coords_lon),
            prix_achat=prix_achat if prix_achat > 0 else None,
            prix_location_annuel=prix_location_annuel if prix_location_annuel > 0 else None,
            prix_comparable_eur_ha=prix_comparable if prix_comparable > 0 else None,
            acces_eau=acces_eau,
            debit_estime_m3h=debit_estime_m3h if debit_estime_m3h > 0 else None,
            distance_cours_eau_m=distance_cours_eau_m if distance_cours_eau_m > 0 else None,
            distance_marche_km=distance_marche_km if distance_marche_km > 0 else None,
            distance_agglo_km=distance_agglo_km if distance_agglo_km > 0 else None,
            pente_pct=pente_pct,
            exposition=exposition,
            altitude_m=altitude_m,
            risque_gel_tardif=risque_gel_tardif,
            acces_vehicule=acces_vehicule,
            notes=notes.strip(),
            statut=statut,
        )
        saved_path = save_parcelle(parcelle)
        st.success(f"Parcelle enregistrée avec succès dans `{saved_path.name}`")
        st.json(parcelle.model_dump())

        # Calculer et afficher le scoring
        st.markdown("---")
        st.subheader("📊 Évaluation automatique")

        engine = ScoringEngine()
        score = engine.score_parcelle(parcelle)

        # Score global en évidence
        col_score, col_details = st.columns([1, 2])
        with col_score:
            st.metric(
                label="Score global",
                value=f"{score.global_score}/100",
                delta="Recommandé" if score.global_score >= 70 else "À améliorer"
            )

        with col_details:
            st.markdown(score.summary())

        # Détails par axe avec barres de progression
        st.markdown("### Détails par axe")

        axes = [
            ("Economique & Logistique", score.score_economique_logistique),
            ("Eau & Irrigation", score.score_eau_irrigation),
            ("Topographie & Exposition", score.score_topographie_exposition),
        ]

        for axe_name, axe_score in axes:
            with st.expander(f"{axe_name}: {axe_score.score}/100", expanded=True):
                st.progress(axe_score.score / 100)

                # Afficher les critères contributifs
                criteria_cols = st.columns(len(axe_score.criteria))
                for i, (crit_name, crit_score) in enumerate(axe_score.criteria.items()):
                    with criteria_cols[i]:
                        st.metric(
                            label=crit_name.replace("_", " ").title(),
                            value=f"{crit_score}/100"
                        )

        # --- Rapport PDF ---
        st.markdown("---")
        st.subheader("📄 Rapport PDF")
        if st.button("Générer le rapport d'implantation", type="primary"):
            with st.spinner("Génération du rapport PDF…"):
                try:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        pdf_path = generate_pdf(parcelle, score, output_dir=Path(tmpdir))
                        pdf_bytes = pdf_path.read_bytes()

                    date_str = __import__("datetime").date.today().strftime("%Y%m%d")
                    filename = f"rapport_{parcelle.id}_{date_str}.pdf"

                    st.download_button(
                        label="⬇ Télécharger le rapport PDF",
                        data=pdf_bytes,
                        file_name=filename,
                        mime="application/pdf",
                    )
                    st.success(f"Rapport généré : `{filename}`")
                except Exception as exc:
                    st.error(f"Erreur génération PDF : {exc}")

    except Exception as exc:
        st.error(f"Erreur de validation : {exc}")

st.markdown("---")

st.subheader("Parcelles enregistrées")
parcelles = list_parcelles()
if not parcelles:
    st.info("Aucune parcelle enregistrée pour l'instant.")
else:
    for p in parcelles[:10]:
        with st.expander(f"{p['id']} — {p['nom']}"):
            st.write(f"Fichier : `{p['path'].split('/')[-1]}`")
            st.write(f"Créé : {p['created']}")
            st.json(p['raw'])

            # Rapport PDF depuis la liste
            if st.button("📄 Générer rapport PDF", key=f"pdf_{p['id']}"):
                with st.spinner("Génération…"):
                    try:
                        parc = Parcelle.model_validate(p['raw'])
                        with tempfile.TemporaryDirectory() as tmpdir:
                            pdf_path = generate_pdf(parc, output_dir=Path(tmpdir))
                            pdf_bytes = pdf_path.read_bytes()
                        date_str = __import__("datetime").date.today().strftime("%Y%m%d")
                        st.download_button(
                            label="⬇ Télécharger",
                            data=pdf_bytes,
                            file_name=f"rapport_{p['id']}_{date_str}.pdf",
                            mime="application/pdf",
                            key=f"dl_{p['id']}",
                        )
                    except Exception as exc:
                        st.error(f"Erreur : {exc}")
