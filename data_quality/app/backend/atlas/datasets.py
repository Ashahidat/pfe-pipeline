import logging
import sys
sys.path.append("/home/ashahi/PFE/pip/data_quality/app/backend")
from .client import ATLAS_SEARCH_URL, ATLAS_ENTITY_BULK_URL, ATLAS_RELATIONSHIP_URL, atlas_get, atlas_post
from .text import extract_base_name
from .similarity import global_similarity, load_quality_results, hash_partial
import requests
import pandas as pd

logger = logging.getLogger("atlas.datasets")

# 🔹 Helpers pour récupérer tous les datasets depuis Atlas
def get_all_datasets_from_atlas():
    try:
        search_url = f"{ATLAS_SEARCH_URL}?typeName=DataSet&query=*"
        res = atlas_get(search_url)
        return res.json().get("entities", [])
    except Exception as e:
        logger.warning(f"Erreur get_all_datasets_from_atlas: {e}")
        return []

# 🔹 Helper pour récupérer le DataFrame d’un dataset parent
# Ici, tu peux remplacer par lecture locale ou session si dataset déjà en mémoire
def load_df_from_atlas(entity):
    """
    Simulé : renvoie un DataFrame Pandas.
    Adapter si tu peux accéder au CSV réel ou via Atlas.
    """
    qn = entity["attributes"]["qualifiedName"]
    try:
        # Exemple si tu as un mapping fichier <-> qualifiedName
        file_path = f"/home/ashahi/PFE/pip/data_quality/data/{qn}.csv"
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.warning(f"Impossible de charger DF parent {qn}: {e}")
        return pd.DataFrame()  # DF vide si erreur

def find_parent_dataset(current_name: str, current_df, current_hash, current_tests):
    """
    Cherche le meilleur dataset parent selon global_similarity
    """
    entities = get_all_datasets_from_atlas()
    best_parent = None
    best_score = 0.0

    for e in entities:
        if e.get("status") == "DELETED":
            continue

        parent_df = load_df_from_atlas(e)
        if parent_df.empty:
            continue

        # Résumé qualité du parent
        parent_tests = load_quality_results(f"/home/ashahi/PFE/pip/data_quality/results/{e['attributes']['qualifiedName']}_validation.json")
        parent_hash = hash_partial(parent_df)

        score = global_similarity(
            current_df, parent_df,
            current_hash, parent_hash,
            current_tests, parent_tests
        )

        if score > best_score and score > 0.8:  # seuil ajustable
            best_parent = (e["guid"], e["attributes"]["qualifiedName"])
            best_score = score

    return best_parent if best_parent else (None, None)


def create_dataset(hash_value: str, original_name: str, file_path: str, parent_qualified_name: str | None):
    """Crée un DataSet dans Atlas."""
    payload = {
        "entities": [{
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": hash_value,
                "name": original_name,
                "description": f"Dataset importé depuis {file_path}",
                "versionComment": f"Version dérivée de {parent_qualified_name}" if parent_qualified_name else "Version initiale"
            },
            "guid": "-100"
        }]
    }

    res = atlas_post(ATLAS_ENTITY_BULK_URL, payload)
    return res.json().get("guidAssignments", {}).get("-100")


def link_versioning(parent_guid: str, child_guid: str):
    """Crée relation dataset_versioning"""
    relationship_payload = {
        "typeName": "dataset_versioning",
        "end1": {"guid": child_guid, "typeName": "DataSet"},  
        "end2": {"guid": parent_guid, "typeName": "DataSet"},  
        "attributes": {"versionDate": "now()"}
    }

    try:
        res = requests.post(ATLAS_RELATIONSHIP_URL, auth=("admin", "admin"),
                            json=relationship_payload, headers={"Content-Type": "application/json"})
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logger.error(f"Erreur relation versioning: {e}")
        return None
