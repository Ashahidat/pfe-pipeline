from fastapi import APIRouter, HTTPException
from session import session_data
from atlas.client import atlas_post, ATLAS_TYPEDEF_URL
from atlas.typedefs import typedefs_payload
from atlas.datasets import find_parent_dataset, create_dataset, link_versioning
from atlas.columns import create_columns

router = APIRouter()

@router.post("/push-atlas")
def push_atlas():
    try:
        df = session_data["df"]
        hash_value = session_data["hash"]
        file_path = session_data["file_path"]
        original_name = session_data["original_name"]

        atlas_post(ATLAS_TYPEDEF_URL, typedefs_payload)

        parent_guid, parent_qn = find_parent_dataset(original_name, hash_value)

        dataset_guid = create_dataset(hash_value, original_name, file_path, parent_qn)

        if parent_guid:
            link_versioning(parent_guid, dataset_guid)

        col_guids = create_columns(df, dataset_guid, hash_value)

        return {
            "message": f"Dataset + {len(col_guids)} colonnes créés.",
            "dataset_guid": dataset_guid,
            "parent_guid": parent_guid,
            "column_guids": col_guids
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
