# /home/ashahi/PFE/pip/data_quality/app/utils/atlas/voir_dataset.py

import requests
import logging

AUTH = ("admin", "admin")
HEADERS = {"Content-Type": "application/json"}
ATLAS_REL_URL = "http://localhost:21000/api/atlas/v2/relationship?entityGuid="
ATLAS_ENTITY_URL = "http://localhost:21000/api/atlas/v2/entity/guid/"

logger = logging.getLogger("atlas_check")
logging.basicConfig(level=logging.INFO)

dataset_guid = "22ba49d0-5486-4e88-a6cc-355e7bc2b596"
# 🔹 Récupérer les relations du 
relations = []  # toujours initialisée

res_rel = requests.get(f"{ATLAS_REL_URL}{dataset_guid}", auth=AUTH, headers=HEADERS)
if res_rel.status_code == 200:
    relations = res_rel.json().get("list", [])
    logger.info("✅ Relations trouvées : %d", len(relations))
    for r in relations:
        logger.info("Relation: %s → %s", r.get("end1", {}).get("guid"), r.get("end2", {}).get("guid"))
else:
    logger.warning("⚠️ Impossible de récupérer les relations : %s", res_rel.text)


# 🔹 Récupérer les colonnes du dataset
# 🔹 Récupérer les colonnes du dataset avec GUID
res_ds = requests.get(f"{ATLAS_ENTITY_URL}{dataset_guid}", auth=AUTH, headers=HEADERS)
if res_ds.status_code == 200:
    columns = res_ds.json()["entity"]["relationshipAttributes"].get("columns", [])
    column_guids = [col["guid"] for col in columns]  # <-- ici tu récupères les GUID
    logger.info("✅ Colonnes trouvées : %d", len(column_guids))
    logger.info("GUID des colonnes : %s", column_guids)
else:
    column_guids = []
    logger.warning("⚠️ Impossible de récupérer le dataset : %s", res_ds.text)

# logger.info("Résumé du dataset :")
# logger.info("Nombre de colonnes : %d", len(columns))
# logger.info("Nombre de relations : %d", len(relations))
