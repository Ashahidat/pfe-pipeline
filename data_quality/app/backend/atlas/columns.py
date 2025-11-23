from .client import atlas_post, ATLAS_ENTITY_BULK_URL

def create_columns(df, dataset_guid, dataset_qualified_name):
    entities = []

    for idx, (col_name, dtype) in enumerate(zip(df.columns, df.dtypes)):
        col_guid = f"-col-{idx}"

        entities.append({
            "typeName": "Column",
            "guid": col_guid,
            "attributes": {
                "name": col_name,
                "qualifiedName": f"{dataset_qualified_name}.{col_name}",
                "type": str(dtype),
                "dataset": dataset_guid
            }
        })

    if not entities:
        return []

    res = atlas_post(ATLAS_ENTITY_BULK_URL, {"entities": entities})

    assignments = res.json().get("guidAssignments", {})

    # 🟢 FIX: utiliser ent["guid"], pas l'objet entier
    return [assignments[ent["guid"]] for ent in entities if ent["guid"] in assignments]
