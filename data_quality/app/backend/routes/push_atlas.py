from fastapi import APIRouter, HTTPException
import sys
sys.path.append("/home/ashahi/PFE/pip/data_quality/app/backend")

from session import session_data
from atlas.client import atlas_post, ATLAS_TYPEDEF_URL, ATLAS_ENTITY_BULK_URL, ATLAS_RELATIONSHIP_URL
from atlas.typedefs import typedefs_payload
from atlas.datasets import find_parent_dataset, link_versioning
from atlas.columns import create_columns
from atlas.similarity import hash_partial, load_quality_results, global_similarity

router = APIRouter()

def safe_post_typedef(payload_section: dict):
    """Poste un typedef Atlas et ignore les erreurs 409 ou 400"""
    try:
        atlas_post(ATLAS_TYPEDEF_URL, payload_section)
    except Exception as e:
        if "409" in str(e) or "400" in str(e):
            print("⚡ Typedef déjà existant ou conflict ignoré")
        else:
            raise

def create_dataset(hash_value: str, original_name: str, file_path: str, parent_qualified_name: str | None):
    """Crée un dataset My_DataSet dans Atlas et récupère le GUID en toute sécurité"""
    from session import session_data

    df = session_data.get("df")
    payload = {
        "entities": [{
            "typeName": "My_DataSet",
            "attributes": {
                "qualifiedName": hash_value,
                "name": original_name,
                "description": f"DataSet importé depuis {file_path}",
                "versionComment": f"Version dérivée de {parent_qualified_name}" if parent_qualified_name else "Version initiale",
                "filePath": file_path,
                "rowCount": int(df.count()) if df is not None else 0,
                "columnCount": int(len(df.columns)) if df is not None else 0,
                "hashPartial": hash_value,
            }
        }]
    }

    print(f"🎯 DEBUG - Création dataset My_DataSet: {original_name}")
    res = atlas_post(ATLAS_ENTITY_BULK_URL, payload)
    res_json = res.json()

    # Récupération sécurisée du GUID
    guid = None
    if "guidAssignments" in res_json and "-100" in res_json["guidAssignments"]:
        guid = res_json["guidAssignments"]["-100"]
    elif "entities" in res_json and len(res_json["entities"]) > 0:
        guid = res_json["entities"][0].get("guid")

    if not guid:
        raise Exception("GUID non récupéré après création du dataset")

    print(f"✅ DEBUG - Dataset créé avec GUID: {guid}")
    return guid

@router.post("/push-atlas")
def push_atlas():
    try:
        # 🔹 Charger le dataset en session
        df = session_data.get("df")
        if df is None:
            raise HTTPException(status_code=400, detail="Aucun dataset en session")

        hash_value = session_data.get("hash")
        file_path = session_data.get("file_path")
        original_name = session_data.get("original_name")

        # 🔹 Post typedefs séparément
        print("🎯 POST My_DataSet...")
        safe_post_typedef({"entityDefs": [typedefs_payload["entityDefs"][0]]})

        print("🎯 POST Column...")
        safe_post_typedef({"entityDefs": [typedefs_payload["entityDefs"][1]]})

        print("🎯 POST Relationships...")
        safe_post_typedef({"relationshipDefs": typedefs_payload["relationshipDefs"]})

        # 🔹 Charger résultats qualité
        result_file_current = "/home/ashahi/PFE/pip/data_quality/results/validation.json"
        tests_current = load_quality_results(result_file_current)

        # 🔹 Chercher le parent
        parent_guid, parent_qn = find_parent_dataset(
            current_name=original_name,
            current_df=df,
            current_hash=hash_value,
            current_tests=tests_current
        )

        # 🔹 Calculer le score de similarité si parent trouvé
        similarity_score = None
        if parent_guid:
            similarity_score = global_similarity(df, df, hash_value, hash_value, tests_current, tests_current)

        # 🔹 Créer le dataset dans Atlas
        dataset_guid = create_dataset(hash_value, original_name, file_path, parent_qn)

        # 🔹 Créer relation versionning si parent
        if parent_guid:
            link_versioning(parent_guid, dataset_guid)

        # 🔹 Créer colonnes associées
        col_guids = create_columns(df, dataset_guid, hash_value)

        return {
            "message": f"My_DataSet + {len(col_guids)} colonnes créés.",
            "dataset_guid": dataset_guid,
            "parent_guid": parent_guid,
            "column_guids": col_guids,
            "similarity_score_with_parent": similarity_score
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur push_atlas: {str(e)}")
