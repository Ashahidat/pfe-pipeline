import requests
import logging

# 🔹 Config Atlas
AUTH = ("admin", "admin")
HEADERS = {"Content-Type": "application/json"}
ATLAS_ENTITY_URL = "http://localhost:21000/api/atlas/v2/entity/guid/"

logger = logging.getLogger("atlas_delete")
logging.basicConfig(level=logging.INFO)

# 🔹 Dataset à supprimer
dataset_guid = "22ba49d0-5486-4e88-a6cc-355e7bc2b596"

column_guids = ['4bacabb0-21de-4c22-9b46-b197324e9e82', 'd1742493-2050-4fa0-bd2a-a007b03f4f21', 'aa1949a1-2195-467a-8be9-5a3e101bba47']

def delete_entity(guid: str, entity_type: str):
    """Supprime un dataset ou une colonne par GUID."""
    try:
        url = f"{ATLAS_ENTITY_URL}{guid}"
        res = requests.delete(url, auth=AUTH, headers=HEADERS)
        if res.status_code in [200, 202]:
            logger.info("✅ %s supprimé(e) : %s", entity_type, guid)
        else:
            logger.warning("⚠️ Échec suppression %s %s : %s", entity_type, guid, res.text)
    except Exception as e:
        logger.error("❌ Exception lors de la suppression %s %s : %s", entity_type, guid, e)

# 🔹 1️⃣ Supprimer les colonnes
logger.info("🔹 Suppression des colonnes...")
for col_guid in column_guids:
    delete_entity(col_guid, "Colonne")

# 🔹 2️⃣ Supprimer le dataset
logger.info("🔹 Suppression du dataset...")
delete_entity(dataset_guid, "Dataset")

logger.info("🎯 Suppression terminée !")
