"""
Chargement de la configuration agriTools.
Supporte la surcharge via config.local.toml (gitignored).
"""
try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]
from pathlib import Path
from functools import lru_cache


def _find_root() -> Path:
    """Remonte l'arborescence jusqu'à trouver pyproject.toml."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise FileNotFoundError("Racine du projet introuvable (pyproject.toml manquant)")


@lru_cache(maxsize=1)
def load_config() -> dict:
    """
    Charge config.toml, puis fusionne config.local.toml si présent.
    config.local.toml est gitignored — y mettre les coordonnées GPS réelles.
    """
    root = _find_root()

    with open(root / "config.toml", "rb") as f:
        cfg = tomllib.load(f)

    local = root / "config.local.toml"
    if local.exists():
        with open(local, "rb") as f:
            local_cfg = tomllib.load(f)
        # Fusion superficielle par section
        for section, values in local_cfg.items():
            if section in cfg and isinstance(cfg[section], dict):
                cfg[section].update(values)
            else:
                cfg[section] = values

    # Résolution des chemins relatifs → absolus
    for key in ("raw", "processed", "catalog", "duckdb"):
        cfg["paths"][key] = str(root / cfg["paths"][key])

    return cfg


def get_root() -> Path:
    return _find_root()
