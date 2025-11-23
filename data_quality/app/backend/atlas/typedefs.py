typedefs_payload = {
    "entityDefs": [
        # -------------------------
        # TYPE DATASET PERSONNALISÉ
        # -------------------------
        {
            "name": "My_DataSet",  # ⚡ type personnalisé
            "superTypes": ["DataSet"],  # on étend toujours le type natif
            "attributeDefs": [
                # seulement les attributs personnalisés
                {"name": "filePath", "typeName": "string", "isOptional": True},
                {"name": "rowCount", "typeName": "int", "isOptional": True},
                {"name": "columnCount", "typeName": "int", "isOptional": True},
                {"name": "hashPartial", "typeName": "string", "isOptional": True},
                {"name": "versionComment", "typeName": "string", "isOptional": True},
            ]
        },

        # -------------------------
        # TYPE COLUMN PERSONNALISÉ
        # -------------------------
        {
            "name": "Column",
            "superTypes": ["Asset"],
            "attributeDefs": [
                {"name": "qualifiedName", "typeName": "string", "isOptional": False, "isUnique": True},
                {"name": "name", "typeName": "string", "isOptional": False},
                {"name": "type", "typeName": "string", "isOptional": True},

                # lien vers My_DataSet personnalisé
                {
                    "name": "dataset",
                    "typeName": "My_DataSet",
                    "isOptional": False,
                    "cardinality": "SINGLE"
                }
            ]
        }
    ],

    # -------------------------
    # RELATIONSHIPS
    # -------------------------
    "relationshipDefs": [
        # -------- relations colonnes --------
        {
            "name": "dataset_columns",
            "typeVersion": "1.0",
            "relationshipCategory": "COMPOSITION",
            "endDef1": {
                "type": "My_DataSet",
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

        # -------- relations versionning --------
        {
            "name": "dataset_versioning",
            "typeVersion": "1.0",
            "relationshipCategory": "ASSOCIATION",
            "endDef1": {
                "type": "My_DataSet",
                "name": "previous",
                "isContainer": False,
                "cardinality": "SINGLE"
            },
            "endDef2": {
                "type": "My_DataSet",
                "name": "next",
                "isContainer": False,
                "cardinality": "SINGLE"
            }
        }
    ]
}
