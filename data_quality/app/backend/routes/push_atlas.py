from fastapi import APIRouter, HTTPException
import sys
sys.path.append("/home/ashahi/PFE/pip/data_quality/app/backend")

from session import session_data
from atlas.client import atlas_post, ATLAS_TYPEDEF_URL
from atlas.typedefs import typedefs_payload
from atlas.datasets import find_parent_dataset, create_dataset, link_versioning
from atlas.columns import create_columns

# Import du module similarity
from atlas.similarity import hash_partial, load_quality_results, global_similarity

router = APIRouter()

@router.post("/push-atlas")
def push_atlas():
    try:
        # 🔹 Charger les informations du dataset courant
        df = session_data["df"]
        hash_value = session_data["hash"]
        file_path = session_data["file_path"]
        original_name = session_data["original_name"]

        # 🔹 Charger typedefs sans planter si déjà présents
        try:
            atlas_post(ATLAS_TYPEDEF_URL, typedefs_payload)
        except Exception as e:
            if "409" in str(e):
                pass  # typedefs déjà présents
            else:
                raise

        # 🔹 Charger résultats qualité du dataset courant
        result_file_current = "/home/ashahi/PFE/pip/data_quality/results/validation.json"
        tests_current = load_quality_results(result_file_current)

        # 🔹 Chercher le parent en utilisant le DataFrame et les résultats qualité
        parent_guid, parent_qn = find_parent_dataset(
            current_name=original_name,
            current_df=df,
            current_hash=hash_value,
            current_tests=tests_current
        )

        # 🔹 Calculer le score de similarité si parent trouvé
        similarity_score = None
        if parent_guid:
            # Charger hash et tests parent réels si disponibles
            # Ici, pour l'exemple simplifié, on réutilise le dataset courant
            hash_parent = hash_value
            tests_parent = tests_current
            similarity_score = global_similarity(df, df, hash_value, hash_parent, tests_current, tests_parent)

        # 🔹 Créer le dataset dans Atlas
        dataset_guid = create_dataset(hash_value, original_name, file_path, parent_qn)

        # 🔹 Créer la relation de versioning si parent trouvé
        if parent_guid:
            link_versioning(parent_guid, dataset_guid)

        # 🔹 Créer les colonnes associées au dataset
        col_guids = create_columns(df, dataset_guid, hash_value)

        return {
            "message": f"Dataset + {len(col_guids)} colonnes créés.",
            "dataset_guid": dataset_guid,
            "parent_guid": parent_guid,
            "column_guids": col_guids,
            "similarity_score_with_parent": similarity_score
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
