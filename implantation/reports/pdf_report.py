"""
Rapport d'implantation PDF — agriTools

Génère un rapport structuré A4 pour une parcelle candidate,
incluant ses caractéristiques et les scores d'évaluation.

Usage programmatique :
    from implantation.reports.pdf_report import generate_pdf
    path = generate_pdf(parcelle, score, output_dir=Path("datalake/reports"))

Usage CLI :
    python -m implantation.reports.pdf_report <json_parcelle_path>
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.flowables import Flowable

from implantation.models.parcelle import Parcelle
from implantation.scoring.engine import ParcelleScore, ScoringEngine

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------
VERT_FONCE   = colors.HexColor("#2D6A4F")   # header / accents
VERT_MOYEN   = colors.HexColor("#52B788")   # bonnes valeurs
VERT_CLAIR   = colors.HexColor("#D8F3DC")   # fonds tableaux
ORANGE       = colors.HexColor("#F4A261")   # valeurs moyennes
ROUGE        = colors.HexColor("#E63946")   # mauvaises valeurs
GRIS_CLAIR   = colors.HexColor("#F8F9FA")
GRIS_MOYEN   = colors.HexColor("#DEE2E6")
GRIS_TEXTE   = colors.HexColor("#495057")
BLANC        = colors.white
NOIR         = colors.black

PAGE_W, PAGE_H = A4
MARGIN_LR = 1.8 * cm
MARGIN_TB = 2.0 * cm
CONTENT_W = PAGE_W - 2 * MARGIN_LR


# ---------------------------------------------------------------------------
# Helpers couleur selon score
# ---------------------------------------------------------------------------
def _score_color(score: int) -> colors.Color:
    if score >= 70:
        return VERT_MOYEN
    if score >= 45:
        return ORANGE
    return ROUGE


def _score_label(score: int) -> str:
    if score >= 70:
        return "Favorable"
    if score >= 45:
        return "Mitigé"
    return "Défavorable"


def _fmt_opt(v, unit: str = "", none_str: str = "—") -> str:
    if v is None:
        return none_str
    if isinstance(v, float):
        return f"{v:,.1f}{(' ' + unit) if unit else ''}"
    if isinstance(v, bool):
        return "Oui" if v else "Non"
    return f"{v}{(' ' + unit) if unit else ''}"


# ---------------------------------------------------------------------------
# Flowable barre de score colorée
# ---------------------------------------------------------------------------
class ScoreBar(Flowable):
    """Barre horizontale représentant un score /100 avec couleur dynamique."""

    def __init__(self, score: int, width: float, height: float = 14):
        super().__init__()
        self.score = score
        self.width = width
        self.height = height
        self._color = _score_color(score)

    def wrap(self, *args):
        return self.width, self.height + 4

    def draw(self):
        c = self.canv
        w = self.width
        h = self.height

        # Fond gris
        c.setFillColor(GRIS_MOYEN)
        c.roundRect(0, 2, w, h, 3, fill=1, stroke=0)

        # Barre remplie
        filled_w = max(6, w * self.score / 100)
        c.setFillColor(self._color)
        c.roundRect(0, 2, filled_w, h, 3, fill=1, stroke=0)

        # Texte score
        c.setFillColor(BLANC if self.score > 30 else GRIS_TEXTE)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(6, 5, f"{self.score}/100")


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
def _build_styles():
    base = getSampleStyleSheet()

    styles = {}

    styles["titre_rapport"] = ParagraphStyle(
        "titre_rapport",
        fontName="Helvetica-Bold",
        fontSize=20,
        textColor=BLANC,
        alignment=TA_LEFT,
        spaceAfter=0,
    )
    styles["sous_titre"] = ParagraphStyle(
        "sous_titre",
        fontName="Helvetica",
        fontSize=10,
        textColor=BLANC,
        alignment=TA_LEFT,
        spaceAfter=0,
    )
    styles["section"] = ParagraphStyle(
        "section",
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=VERT_FONCE,
        spaceBefore=14,
        spaceAfter=4,
    )
    styles["normal"] = ParagraphStyle(
        "normal",
        fontName="Helvetica",
        fontSize=9,
        textColor=GRIS_TEXTE,
        spaceAfter=2,
    )
    styles["note"] = ParagraphStyle(
        "note",
        fontName="Helvetica-Oblique",
        fontSize=9,
        textColor=GRIS_TEXTE,
        spaceAfter=4,
        leftIndent=8,
    )
    styles["footer"] = ParagraphStyle(
        "footer",
        fontName="Helvetica",
        fontSize=7,
        textColor=GRIS_TEXTE,
        alignment=TA_CENTER,
    )
    styles["score_global"] = ParagraphStyle(
        "score_global",
        fontName="Helvetica-Bold",
        fontSize=36,
        textColor=VERT_FONCE,
        alignment=TA_CENTER,
        spaceAfter=0,
    )
    styles["label_score"] = ParagraphStyle(
        "label_score",
        fontName="Helvetica",
        fontSize=10,
        textColor=GRIS_TEXTE,
        alignment=TA_CENTER,
        spaceAfter=0,
    )
    return styles


# ---------------------------------------------------------------------------
# Section : en-tête coloré
# ---------------------------------------------------------------------------
class HeaderBanner(Flowable):
    """Bandeau d'en-tête vert avec titre et sous-titre."""

    def __init__(self, parcelle: Parcelle, width: float):
        super().__init__()
        self.p = parcelle
        self.width = width
        self.height = 60

    def wrap(self, *args):
        return self.width, self.height

    def draw(self):
        c = self.canv
        w, h = self.width, self.height

        # Fond vert
        c.setFillColor(VERT_FONCE)
        c.rect(0, 0, w, h, fill=1, stroke=0)

        # Titre
        c.setFillColor(BLANC)
        c.setFont("Helvetica-Bold", 18)
        c.drawString(14, h - 26, "Rapport d'implantation")

        # Sous-titre parcelle
        c.setFont("Helvetica", 11)
        c.drawString(14, h - 42, f"{self.p.nom}  —  {self.p.commune} ({self.p.departement})")

        # Date et ID en haut à droite
        c.setFont("Helvetica", 8)
        date_str = date.today().strftime("%d/%m/%Y")
        c.drawRightString(w - 14, h - 14, f"agriTools  •  {date_str}")
        c.drawRightString(w - 14, h - 26, f"ID : {self.p.id}")
        c.drawRightString(w - 14, h - 38, f"Statut : {self.p.statut.upper()}")


# ---------------------------------------------------------------------------
# Constructeurs de sections
# ---------------------------------------------------------------------------
def _section_identite(p: Parcelle, styles: dict) -> list:
    elems = []
    elems.append(Paragraph("Identité & localisation", styles["section"]))
    elems.append(HRFlowable(width=CONTENT_W, thickness=1, color=VERT_FONCE, spaceAfter=6))

    data = [
        ["Surface", _fmt_opt(p.surface_ha, "ha"),
         "Commune", p.commune],
        ["Département", p.departement,
         "Coordonnées",
         f"{p.coords_centroid[0]:.5f}, {p.coords_centroid[1]:.5f}"],
        ["Prix d'achat", _fmt_opt(p.prix_achat, "€"),
         "Prix location/an", _fmt_opt(p.prix_location_annuel, "€")],
        ["Prix comparable", _fmt_opt(p.prix_comparable_eur_ha, "€/ha"),
         "Accès véhicule", p.acces_vehicule],
    ]

    col_w = CONTENT_W / 4
    tbl = Table(data, colWidths=[col_w * 0.6, col_w * 0.9, col_w * 0.7, col_w * 0.8])
    tbl.setStyle(TableStyle([
        ("FONTNAME",     (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME",     (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME",     (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, -1), 9),
        ("TEXTCOLOR",    (0, 0), (-1, -1), GRIS_TEXTE),
        ("TEXTCOLOR",    (0, 0), (0, -1), NOIR),
        ("TEXTCOLOR",    (2, 0), (2, -1), NOIR),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [BLANC, GRIS_CLAIR]),
        ("GRID",         (0, 0), (-1, -1), 0.3, GRIS_MOYEN),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ("LEFTPADDING",  (0, 0), (-1, -1), 6),
    ]))
    elems.append(tbl)
    return elems


def _section_score_global(score: ParcelleScore, styles: dict) -> list:
    elems = []
    elems.append(Paragraph("Score d'évaluation globale", styles["section"]))
    elems.append(HRFlowable(width=CONTENT_W, thickness=1, color=VERT_FONCE, spaceAfter=8))

    sc = score.global_score
    color = _score_color(sc)
    label = _score_label(sc)

    # Score global + mention + barre — en tableau 3 colonnes
    score_para = Paragraph(
        f'<font color="{color.hexval()}" size="34"><b>{sc}</b></font>'
        f'<font color="{GRIS_TEXTE.hexval()}" size="16">/100</font>',
        ParagraphStyle("sg", alignment=TA_CENTER),
    )
    mention_para = Paragraph(
        f'<font color="{color.hexval()}" size="13"><b>{label}</b></font>',
        ParagraphStyle("sm", alignment=TA_CENTER),
    )

    axes = [
        (score.score_economique_logistique, "Économique & Logistique"),
        (score.score_eau_irrigation, "Eau & Irrigation"),
        (score.score_topographie_exposition, "Topographie & Exposition"),
    ]

    axes_rows = []
    bar_w = CONTENT_W * 0.30
    for ax, label_ax in axes:
        pct = int(ax.weight * 100)
        name_para = Paragraph(
            f'<b>{label_ax}</b><br/><font color="{GRIS_TEXTE.hexval()}" size="8">'
            f'Poids {pct}%</font>',
            ParagraphStyle("al", fontSize=9),
        )
        bar = ScoreBar(ax.score, bar_w)
        score_ax_para = Paragraph(
            f'<font color="{_score_color(ax.score).hexval()}"><b>{ax.score}/100</b></font>',
            ParagraphStyle("as", fontSize=9, alignment=TA_RIGHT),
        )
        axes_rows.append([name_para, bar, score_ax_para])

    # Tableau axes
    axes_tbl = Table(
        axes_rows,
        colWidths=[CONTENT_W * 0.38, bar_w + 10, CONTENT_W * 0.18],
    )
    axes_tbl.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",   (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 6),
        ("LEFTPADDING",  (0, 0), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [BLANC, GRIS_CLAIR]),
        ("GRID",         (0, 0), (-1, -1), 0.3, GRIS_MOYEN),
    ]))

    # Assembler score global + axes côte à côte
    score_block = Table(
        [[score_para, mention_para]],
        colWidths=[CONTENT_W * 0.18, CONTENT_W * 0.18],
    )
    score_block.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN",  (0, 0), (-1, -1), "CENTER"),
        ("BOX",    (0, 0), (-1, -1), 0.5, GRIS_MOYEN),
        ("BACKGROUND", (0, 0), (-1, -1), GRIS_CLAIR),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))

    outer = Table(
        [[score_block, axes_tbl]],
        colWidths=[CONTENT_W * 0.22, CONTENT_W * 0.78],
        hAlign="LEFT",
    )
    outer.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    elems.append(outer)
    return elems


def _section_criteres(score: ParcelleScore, styles: dict) -> list:
    elems = []
    elems.append(Paragraph("Détail des critères", styles["section"]))
    elems.append(HRFlowable(width=CONTENT_W, thickness=1, color=VERT_FONCE, spaceAfter=6))

    header = [
        Paragraph("<b>Axe</b>", ParagraphStyle("h", fontSize=9)),
        Paragraph("<b>Critère</b>", ParagraphStyle("h", fontSize=9)),
        Paragraph("<b>Score</b>", ParagraphStyle("h", fontSize=9, alignment=TA_RIGHT)),
        Paragraph("<b>Évaluation</b>", ParagraphStyle("h", fontSize=9)),
    ]
    rows = [header]

    axes = [
        (score.score_economique_logistique, "Économique & Log."),
        (score.score_eau_irrigation, "Eau & Irrigation"),
        (score.score_topographie_exposition, "Topographie & Expo."),
    ]

    for ax_score, ax_label in axes:
        first = True
        for crit_name, crit_val in ax_score.criteria.items():
            label_cell = Paragraph(ax_label, ParagraphStyle("cn", fontSize=8)) if first else Paragraph("", ParagraphStyle("cn", fontSize=8))
            crit_para = Paragraph(
                crit_name.replace("_", " ").capitalize(),
                ParagraphStyle("cp", fontSize=8),
            )
            val_para = Paragraph(
                f'<font color="{_score_color(crit_val).hexval()}"><b>{crit_val}</b></font>',
                ParagraphStyle("cv", fontSize=8, alignment=TA_RIGHT),
            )
            mention_para = Paragraph(
                f'<font color="{_score_color(crit_val).hexval()}">{_score_label(crit_val)}</font>',
                ParagraphStyle("cm", fontSize=8),
            )
            rows.append([label_cell, crit_para, val_para, mention_para])
            first = False

    col_w = CONTENT_W
    tbl = Table(rows, colWidths=[col_w * 0.28, col_w * 0.35, col_w * 0.12, col_w * 0.25])
    tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), VERT_FONCE),
        ("TEXTCOLOR",    (0, 0), (-1, 0), BLANC),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BLANC, GRIS_CLAIR]),
        ("GRID",         (0, 0), (-1, -1), 0.3, GRIS_MOYEN),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ("LEFTPADDING",  (0, 0), (-1, -1), 6),
        ("ALIGN",        (2, 0), (2, -1), "RIGHT"),
    ]))
    elems.append(tbl)
    return elems


def _section_caracteristiques(p: Parcelle, styles: dict) -> list:
    elems = []
    elems.append(Paragraph("Caractéristiques détaillées", styles["section"]))
    elems.append(HRFlowable(width=CONTENT_W, thickness=1, color=VERT_FONCE, spaceAfter=6))

    sections = [
        ("Eau & Irrigation", [
            ("Accès eau", _fmt_opt(p.acces_eau)),
            ("Débit estimé", _fmt_opt(p.debit_estime_m3h, "m³/h")),
            ("Distance cours d'eau", _fmt_opt(p.distance_cours_eau_m, "m")),
            ("Forages BRGM (5 km)", _fmt_opt(p.forages_brgm_count)),
        ]),
        ("Topographie", [
            ("Pente", _fmt_opt(p.pente_pct, "%")),
            ("Exposition", _fmt_opt(p.exposition)),
            ("Altitude", _fmt_opt(p.altitude_m, "m")),
            ("Risque gel tardif", _fmt_opt(p.risque_gel_tardif)),
        ]),
        ("Météo (données datalake)", [
            ("Précip. annuelle", _fmt_opt(p.meteo_precip_annuelle_mm, "mm")),
            ("Jours de gel/an", _fmt_opt(p.meteo_jours_gel, "j")),
            ("ETP annuelle", _fmt_opt(p.meteo_etp_annuelle_mm, "mm")),
        ]),
        ("Logistique", [
            ("Accès véhicule", _fmt_opt(p.acces_vehicule)),
            ("Distance marché", _fmt_opt(p.distance_marche_km, "km")),
            ("Distance agglomération", _fmt_opt(p.distance_agglo_km, "km")),
        ]),
    ]

    # Deux blocs côte à côte
    left_data, right_data = [], []
    for i, (sec_title, fields) in enumerate(sections):
        block = [[
            Paragraph(f"<b>{sec_title}</b>", ParagraphStyle("st", fontSize=9, textColor=VERT_FONCE)),
            "",
        ]]
        for fname, fval in fields:
            block.append([
                Paragraph(fname, ParagraphStyle("fn", fontSize=8, textColor=GRIS_TEXTE)),
                Paragraph(fval, ParagraphStyle("fv", fontSize=8)),
            ])
        if i % 2 == 0:
            left_data.extend(block)
        else:
            right_data.extend(block)

    # Padder pour égaliser les longueurs
    while len(left_data) < len(right_data):
        left_data.append(["", ""])
    while len(right_data) < len(left_data):
        right_data.append(["", ""])

    combined_rows = [[l[0], l[1], r[0], r[1]] for l, r in zip(left_data, right_data)]
    half = CONTENT_W / 2
    tbl = Table(combined_rows, colWidths=[half * 0.5, half * 0.5, half * 0.5, half * 0.5])
    tbl.setStyle(TableStyle([
        ("FONTSIZE",     (0, 0), (-1, -1), 8),
        ("FONTNAME",     (0, 0), (-1, -1), "Helvetica"),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [BLANC, GRIS_CLAIR]),
        ("GRID",         (0, 0), (-1, -1), 0.2, GRIS_MOYEN),
        ("TOPPADDING",   (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("LEFTPADDING",  (0, 0), (-1, -1), 6),
        ("SPAN",         (0, 0), (1, 0)),   # En-tête gauche sur 2 cols
        ("SPAN",         (2, 0), (3, 0)),   # En-tête droite sur 2 cols
    ]))
    elems.append(tbl)
    return elems


def _section_notes(p: Parcelle, styles: dict) -> list:
    if not p.notes.strip():
        return []
    elems = []
    elems.append(Paragraph("Notes", styles["section"]))
    elems.append(HRFlowable(width=CONTENT_W, thickness=1, color=VERT_FONCE, spaceAfter=4))
    elems.append(Paragraph(p.notes.replace("\n", "<br/>"), styles["note"]))
    return elems


# ---------------------------------------------------------------------------
# Pied de page via canvas hook
# ---------------------------------------------------------------------------
def _on_page(canvas, doc, parcelle: Parcelle):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(GRIS_TEXTE)
    footer_y = MARGIN_TB - 8 * mm
    canvas.drawString(MARGIN_LR, footer_y,
                      f"agriTools — Rapport d'implantation — {parcelle.nom} ({parcelle.id})")
    canvas.drawRightString(
        PAGE_W - MARGIN_LR, footer_y,
        f"Page {doc.page} — Généré le {date.today().strftime('%d/%m/%Y')}",
    )
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------
def generate_pdf(
    parcelle: Parcelle,
    score: Optional[ParcelleScore] = None,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Génère le rapport PDF d'implantation pour une parcelle.

    Args:
        parcelle:   Objet Parcelle (modèle Pydantic)
        score:      ParcelleScore pré-calculé. Si None, calculé automatiquement.
        output_dir: Dossier de sortie. Défaut : datalake/reports/implantation/

    Returns:
        Chemin du fichier PDF généré.
    """
    # Score
    if score is None:
        engine = ScoringEngine()
        score = engine.score_parcelle(parcelle)

    # Dossier de sortie
    if output_dir is None:
        output_dir = Path("datalake/reports/implantation")
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = date.today().strftime("%Y%m%d")
    filename = f"rapport_{parcelle.id}_{date_str}.pdf"
    pdf_path = output_dir / filename

    # Document
    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=MARGIN_LR,
        rightMargin=MARGIN_LR,
        topMargin=MARGIN_TB,
        bottomMargin=MARGIN_TB + 8 * mm,
        title=f"Rapport implantation — {parcelle.nom}",
        author="agriTools",
        subject=f"Évaluation parcelle {parcelle.id}",
    )

    styles = _build_styles()

    story = []

    # Bandeau en-tête
    story.append(HeaderBanner(parcelle, CONTENT_W))
    story.append(Spacer(1, 10))

    # Sections
    story.extend(_section_identite(parcelle, styles))
    story.append(Spacer(1, 8))
    story.extend(_section_score_global(score, styles))
    story.append(Spacer(1, 8))
    story.extend(_section_criteres(score, styles))
    story.append(Spacer(1, 8))
    story.extend(_section_caracteristiques(parcelle, styles))
    story.extend(_section_notes(parcelle, styles))

    # Build
    doc.build(
        story,
        onFirstPage=lambda c, d: _on_page(c, d, parcelle),
        onLaterPages=lambda c, d: _on_page(c, d, parcelle),
    )

    return pdf_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Génère un rapport PDF pour une parcelle (JSON → PDF)"
    )
    parser.add_argument(
        "parcelle_json",
        type=Path,
        help="Chemin vers le JSON de la parcelle (ex: datalake/raw/perso/parcelles/xxx.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datalake/reports/implantation"),
        help="Dossier de sortie (défaut: datalake/reports/implantation)",
    )
    args = parser.parse_args()

    with open(args.parcelle_json) as f:
        data = json.load(f)
    parcelle = Parcelle.model_validate(data)

    pdf_path = generate_pdf(parcelle, output_dir=args.output_dir)
    print(f"✓ Rapport généré : {pdf_path}")


if __name__ == "__main__":
    main()
