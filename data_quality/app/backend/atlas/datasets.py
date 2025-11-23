import logging
import sys
import os
sys.path.append("/home/ashahi/PFE/pip/data_quality/app/backend")
from .client import ATLAS_SEARCH_URL, ATLAS_ENTITY_BULK_URL, ATLAS_RELATIONSHIP_URL, atlas_get, atlas_post
from config import spark
from .similarity import global_similarity_optimized, load_quality_results, hash_partial
import requests
from pyspark.sql.types import StructType

DATA_DIR = "/home/ashahi/PFE/datasets/new"
logger = logging.getLogger("atlas.datasets")

# -------------------------
# RÉCUPÉRATION DES DATASETS
# -------------------------
def get_all_datasets_from_atlas():
    """Récupère tous les datasets My_DataSet avec debug détaillé"""
    try:
        print("🎯 DEBUG - Récupération datasets depuis Atlas...")
        search_url = f"{ATLAS_SEARCH_URL}?typeName=My_DataSet&query=*"
        res = atlas_get(search_url)
        entities = res.json().get("entities", [])
        
        print(f"📦 DEBUG - {len(entities)} datasets trouvés dans Atlas:")
        for e in entities:
            name = e["attributes"].get("name", "Inconnu")
            guid = e["guid"]
            status = e.get("status", "ACTIVE")
            hash_value = e["attributes"].get("hashPartial", "NO_HASH")
            print(f"   - {name} -> {guid} [status: {status}, hash: {hash_value}]")
            
        return entities
    except Exception as e:
        print(f"❌ DEBUG - Erreur get_all_datasets_from_atlas: {e}")
        return []

# -------------------------
# EXTRACTION MÉTADATA PARENT
# -------------------------
def extract_parent_metadata(entity):
    """Extrait les métadonnées avec debug"""
    attributes = entity["attributes"]
    metadata = {
        "hashPartial": attributes.get("hashPartial"),
        "rowCount": attributes.get("rowCount", 0),
        "columnCount": attributes.get("columnCount", 0),
        "qualifiedName": attributes.get("qualifiedName"),
        "name": attributes.get("name")
    }
    print(f"🎯 DEBUG - Métadonnées extraites: {metadata}")
    return metadata

# -------------------------
# RECHERCHE PARENT
# -------------------------
def find_parent_dataset(current_name: str, current_df, current_hash, current_tests):
    """Recherche parent avec debug"""
    print(f"\n🎯🎯🎯 DEBUG - DÉBUT RECHERCHE PARENT POUR: {current_name}")
    print(f"🎯 DEBUG - Hash courant: {current_hash}")
    
    entities = get_all_datasets_from_atlas()
    
    if not entities:
        print("❌ DEBUG - AUCUN DATASET DANS ATLAS!")
        return None, None

    best_parent = None
    best_score = 0.0

    for idx, e in enumerate(entities):
        if e.get("status") == "DELETED":
            print(f"🎯 DEBUG - Dataset {idx} DELETED, skip")
            continue

        print(f"\n🎯 DEBUG - Examen dataset {idx}:")
        parent_metadata = extract_parent_metadata(e)
        parent_name = parent_metadata.get("name", "unknown")
        parent_hash = parent_metadata.get("hashPartial")
        
        print(f"🎯 DEBUG - Parent: {parent_name}")
        print(f"🎯 DEBUG - Hash parent: {parent_hash}")

        if not parent_hash:
            print("❌ DEBUG - Pas de hash dans Atlas, skip")
            continue

        try:
            score = global_similarity_optimized(
                current_df=current_df,
                current_hash=current_hash,
                current_stats={"rowCount": 0, "columnCount": 0},
                parent_hash=parent_hash,
                parent_stats={"rowCount": 0, "columnCount": 0},
                current_tests={},
                parent_tests={}
            )

            print(f"🎯 DEBUG - Score obtenu: {score}")

            if score > best_score:
                best_parent = (e["guid"], parent_metadata["qualifiedName"])
                best_score = score
                print(f"🏆 DEBUG - NOUVEAU MEILLEUR PARENT! Score: {score}")

        except Exception as ex:
            print(f"❌ DEBUG - Erreur calcul similarité: {ex}")
            continue

    print(f"\n🎯🎯🎯 DEBUG - FIN RECHERCHE PARENT")
    print(f"🎯 DEBUG - Meilleur parent: {best_parent}")
    print(f"🎯 DEBUG - Meilleur score: {best_score}")
    
    return best_parent if best_parent else (None, None)

# -------------------------
# CRÉATION DATASET
# -------------------------
# def create_dataset(hash_value: str, original_name: str, file_path: str, parent_qualified_name: str | None):
#     """Crée un dataset My_DataSet dans Atlas et récupère le GUID en toute sécurité"""
#     from session import session_data

#     df = session_data.get("df")
#     payload = {
#         "entities": [{
#             "typeName": "My_DataSet",
#             "attributes": {
#                 "qualifiedName": hash_value,
#                 "name": original_name,
#                 "description": f"DataSet importé depuis {file_path}",
#                 "versionComment": f"Version dérivée de {parent_qualified_name}" if parent_qualified_name else "Version initiale",
#                 "filePath": file_path,
#                 "rowCount": int(df.count()) if df is not None else 0,
#                 "columnCount": int(len(df.columns)) if df is not None else 0,
#                 "hashPartial": hash_value,
#             }
#         }]
#     }

#     print(f"🎯 DEBUG - Création dataset My_DataSet: {original_name}")
#     res = atlas_post(ATLAS_ENTITY_BULK_URL, payload)
#     res_json = res.json()

#     # Récupération sécurisée du GUID
#     guid = None
#     if "guidAssignments" in res_json and "-100" in res_json["guidAssignments"]:
#         guid = res_json["guidAssignments"]["-100"]
#     elif "entities" in res_json and len(res_json["entities"]) > 0:
#         guid = res_json["entities"][0].get("guid")

#     if not guid:
#         raise Exception("GUID non récupéré après création du dataset")

#     print(f"✅ DEBUG - Dataset créé avec GUID: {guid}")
#     return guid



# -------------------------
# VERSIONNING
# -------------------------
def link_versioning(parent_guid: str, child_guid: str):
    """Crée relation versionning pour My_DataSet"""
    relationship_payload = {
        "typeName": "dataset_versioning",
        "end1": {"guid": child_guid, "typeName": "My_DataSet"},  
        "end2": {"guid": parent_guid, "typeName": "My_DataSet"},  
        "attributes": {"versionDate": "now()"}
    }

    try:
        res = atlas_post(ATLAS_RELATIONSHIP_URL, relationship_payload)
        print(f"✅ DEBUG - Relation versionning créée: {parent_guid} -> {child_guid}")
        return res.json()
    except Exception as e:
        print(f"❌ DEBUG - Erreur relation versionning: {e}")
        return None
