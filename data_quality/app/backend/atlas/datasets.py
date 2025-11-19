import logging
from .client import ATLAS_SEARCH_URL, ATLAS_ENTITY_BULK_URL, ATLAS_RELATIONSHIP_URL, atlas_get, atlas_post
from utils.text import string_similarity, extract_base_name
import requests

logger = logging.getLogger("atlas.datasets")

def find_parent_dataset(current_name: str, exclude_hash: str):
    """Cherche un dataset parent avec une similarité de nom."""
    try:
        search_url = f"{ATLAS_SEARCH_URL}?typeName=DataSet&query=*"
        res = atlas_get(search_url)
        entities = res.json().get("entities", [])

        current_base = extract_base_name(current_name)

        best = None
        best_score = 0.7

        for e in entities:
            qn = e["attributes"]["qualifiedName"]
            name = e["attributes"].get("name", "")
            guid = e["guid"]

            if qn == exclude_hash or e.get("status") == "DELETED":
                continue

            score = string_similarity(current_base, extract_base_name(name))
            if score > best_score:
                best = (guid, qn)
                best_score = score

        return best if best else (None, None)

    except Exception as e:
        logger.warning(f"Erreur recherche parent: {e}")
        return None, None


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
