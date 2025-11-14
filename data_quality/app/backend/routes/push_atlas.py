import os
import requests
from fastapi import APIRouter, HTTPException
from session import session_data
import logging
import json
import re
from difflib import SequenceMatcher

router = APIRouter()

# 🔹 Config Atlas (inchangé)
ATLAS_ENTITY_BULK_URL = "http://localhost:21000/api/atlas/v2/entity/bulk"
ATLAS_TYPEDEF_URL = "http://localhost:21000/api/atlas/v2/types/typedefs"
ATLAS_RELATIONSHIP_URL = "http://localhost:21000/api/atlas/v2/relationship"
ATLAS_SEARCH_URL = "http://localhost:21000/api/atlas/v2/search/basic"
AUTH = ("admin", "admin")
HEADERS = {"Content-Type": "application/json"}

# 🔹 Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("routes.push_atlas")

# 🔹 Typedef (inchangé)
typedefs_payload = {
    "entityDefs": [
        {
            "name": "Column",
            "superTypes": ["Asset"],
            "attributeDefs": [
                {"name": "type", "typeName": "string", "isOptional": True},
                {
                    "name": "dataset",
                    "typeName": "DataSet",
                    "isOptional": False,
                    "cardinality": "SINGLE"
                }
            ]
        }
    ],
    "relationshipDefs": [
        {
            "name": "dataset_columns",
            "typeVersion": "1.0",
            "relationshipCategory": "COMPOSITION",
            "endDef1": {
                "type": "DataSet",
                "name": "columns",
                "isContainer": True,
                "cardinality": "SET"
            },
            "endDef2": {
                "type": "Column",
                "name": "dataset",
                "isContainer": False,
                "cardinality": "SINGLE"
            }
        },
        {
            "name": "dataset_versioning",
            "typeVersion": "1.0",
            "relationshipCategory": "ASSOCIATION",
            "endDef1": {"type": "DataSet", "name": "previous", "isContainer": False, "cardinality": "SINGLE"},
            "endDef2": {"type": "DataSet", "name": "next", "isContainer": False, "cardinality": "SINGLE"}
        }
    ]
}

# 🔹 Helper POST avec logs
def post_atlas(url: str, payload: dict):
    logger.info("➡️ POST %s", url)
    try:
        res = requests.post(url, auth=AUTH, headers=HEADERS, json=payload, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logger.exception("❌ Exception réseau Atlas")
        raise
    logger.info("⬅️ Status: %s", res.status_code)
    return res

# 🔹 Similarité entre deux strings
def string_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# 🔹 Extraire nom de base pour versioning (SIMPLIFIÉ)
def extract_base_name(name: str) -> str:
    # Juste le nom sans extension
    base = os.path.splitext(os.path.basename(name))[0]
    return base.lower()

# 🔹 Rechercher parent existant (AMÉLIORÉ)
def find_parent_guid(current_name: str, exclude_hash: str) -> tuple[str | None, str | None]:
    """
    Cherche un dataset parent avec un nom similaire
    """
    try:
        # Rechercher tous les datasets
        search_url = f"{ATLAS_SEARCH_URL}?typeName=DataSet&query=*"
        res = requests.get(search_url, auth=AUTH, headers=HEADERS, timeout=10)
        res.raise_for_status()
        
        entities = res.json().get("entities", [])
        current_base = extract_base_name(current_name)
        
        best_match = None
        best_similarity = 0.7  # Seuil de similarité minimum
        
        for e in entities:
            guid = e["guid"]
            qualified_name = e["attributes"]["qualifiedName"]
            entity_name = e["attributes"].get("name", "")
            
            # Exclure le dataset actuel et les entités supprimées
            if (qualified_name == exclude_hash or 
                e.get("status", "").lower() == "deleted"):
                continue
            
            entity_base = extract_base_name(entity_name)
            
            # Calculer la similarité
            similarity = string_similarity(current_base, entity_base)
            
            logger.debug("Comparaison: %s vs %s → similarité: %.2f", 
                        current_base, entity_base, similarity)
            
            # Vérifier si c'est une meilleure correspondance
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = (guid, qualified_name, entity_name, similarity)
        
        if best_match:
            guid, qualified_name, entity_name, similarity = best_match
            logger.info("🎯 Parent trouvé: %s (similarité: %.2f%%)", entity_name, similarity * 100)
            return guid, qualified_name
        else:
            logger.info("🔍 Aucun parent trouvé pour: %s", current_name)
            return None, None
            
    except Exception as e:
        logger.warning("⚠️ Erreur recherche parent: %s", e)
        return None, None

# 🔹 Créer une relation de versioning
def create_versioning_relationship(parent_guid: str, child_guid: str):
    try:
        relationship_payload = {
            "typeName": "dataset_versioning",
            "end1": {"guid": parent_guid, "typeName": "DataSet"},
            "end2": {"guid": child_guid, "typeName": "DataSet"},
            "attributes": {"versionDate": "now()"}
        }
        
        res = requests.post(ATLAS_RELATIONSHIP_URL, auth=AUTH, headers=HEADERS, 
                           json=relationship_payload, timeout=10)
        res.raise_for_status()
        
        logger.info("✅ Relation de versioning créée: %s -> %s", parent_guid, child_guid)
        return res.json()
        
    except Exception as e:
        logger.warning("⚠️ Erreur création relation versioning: %s", e)
        
        # Fallback: mettre à jour via API d'entité
        try:
            update_payload = {
                "relationshipAttributes": {
                    "dataset_versioning": {"previous": {"guid": parent_guid}}
                }
            }
            update_url = f"http://localhost:21000/api/atlas/v2/entity/guid/{child_guid}"
            requests.put(update_url, auth=AUTH, headers=HEADERS, json=update_payload, timeout=10)
            logger.info("✅ Relation versioning créée via update fallback")
            return {"status": "created_via_fallback"}
        except Exception as fallback_error:
            logger.error("❌ Échec fallback relation versioning: %s", fallback_error)
            return None

# 🔹 Endpoint FastAPI
@router.post("/push-atlas")
def push_atlas():
    try:
        df = session_data.get("df")
        hash_value = session_data.get("hash")
        file_path = session_data.get("file_path")
        original_name = session_data.get("original_name")

        if df is None or not hash_value or not file_path:
            raise ValueError("CSV, hash ou file_path manquant dans la session")

        dataset_qualified_name = hash_value

        # 0️⃣ Créer typedefs si nécessaire
        try:
            post_atlas(ATLAS_TYPEDEF_URL, typedefs_payload)
            logger.info("✅ Typedefs créés ou déjà existants")
        except Exception as e:
            logger.warning("⚠️ Typedefs non créés: %s", e)

        # 1️⃣ Chercher dataset existant par hash
        search_ds_url = f"http://localhost:21000/api/atlas/v2/entity/uniqueAttribute/type/DataSet?attr:qualifiedName={dataset_qualified_name}"
        res_search_ds = requests.get(search_ds_url, auth=AUTH, headers=HEADERS)
        
        if res_search_ds.status_code == 200:
            ds_json = res_search_ds.json()
            dataset_guid = ds_json.get("entity", {}).get("guid")
            if dataset_guid:
                logger.info("⚠️ DataSet existant trouvé (hash identique) GUID: %s", dataset_guid)
                return {"message": "Dataset déjà présent", "dataset_guid": dataset_guid}

        # 2️⃣ Chercher parent possible basé sur le nom
        parent_guid, parent_qualified_name = find_parent_guid(original_name, hash_value)
        
        if parent_guid:
            logger.info("🔹 Parent détecté: GUID=%s, QN=%s", parent_guid, parent_qualified_name)
        else:
            logger.info("🔹 Aucun parent détecté pour: %s", original_name)

        # 3️⃣ Créer dataset
        dataset_payload = {
            "entities": [
                {
                    "typeName": "DataSet",
                    "attributes": {
                        "qualifiedName": dataset_qualified_name,
                        "name": original_name or dataset_qualified_name,
                        "description": f"Dataset importé depuis {file_path}",
                        "versionComment": f"Version dérivée de {parent_qualified_name}" if parent_guid else "Version initiale"
                    },
                    "guid": "-100"
                }
            ]
        }

        res_ds = post_atlas(ATLAS_ENTITY_BULK_URL, dataset_payload)
        ds_json = res_ds.json()
        dataset_guid = ds_json.get("guidAssignments", {}).get("-100")
        
        if not dataset_guid:
            logger.error("❌ Impossible de récupérer le GUID du dataset créé")
            raise ValueError("GUID du dataset non trouvé dans la réponse")
            
        logger.info("✅ DataSet créé GUID: %s", dataset_guid)

        # 4️⃣ Créer la relation de versioning si parent trouvé
        if parent_guid:
            relationship_result = create_versioning_relationship(parent_guid, dataset_guid)
            if relationship_result:
                logger.info("✅ Relation de versioning établie avec le parent")
            else:
                logger.warning("⚠️ Relation de versioning non créée (mais dataset créé)")

        # 5️⃣ Créer les colonnes
        column_entities = []
        column_guids = []

        for col_name, col_dtype in zip(df.columns, df.dtypes):
            col_qualified_name = f"{dataset_qualified_name}.{col_name}"
            
            column_entities.append({
                "typeName": "Column",
                "attributes": {
                    "name": col_name,
                    "qualifiedName": col_qualified_name,
                    "type": str(col_dtype),
                    "dataset": {"typeName": "DataSet", "guid": dataset_guid},
                    "description": "Colonne importée du CSV"
                },
                "guid": f"-col-{len(column_entities)}"
            })

        if column_entities:
            cols_payload = {"entities": column_entities}
            res_cols = post_atlas(ATLAS_ENTITY_BULK_URL, cols_payload)
            
            # Récupérer les GUIDs des colonnes créées
            guid_assignments = res_cols.json().get("guidAssignments", {})
            for guid_key in guid_assignments:
                if guid_key.startswith("-col-"):
                    column_guids.append(guid_assignments[guid_key])
            
            logger.info("✅ %d colonnes créées", len(column_guids))

        return {
            "message": f"DataSet et {len(column_guids)} colonnes envoyés vers Atlas",
            "dataset_guid": dataset_guid,
            "column_guids": column_guids,
            "fileName": original_name,
            "filePath": file_path,
            "parent_guid": parent_guid,
            "parent_name": parent_qualified_name if parent_guid else None
        }

    except Exception as e:
        logger.exception("❌ Erreur push_atlas: %s", e)
        raise HTTPException(status_code=500, detail=f"Erreur push_atlas: {str(e)}")