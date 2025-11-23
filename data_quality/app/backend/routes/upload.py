import os
from fastapi import APIRouter, UploadFile, File, HTTPException
from config import spark
from session import session_data
import datetime
import hashlib

# Dossier unique pour stockage permanent
DATA_DIR = "/home/ashahi/PFE/pip/data_quality/data"
os.makedirs(DATA_DIR, exist_ok=True)

router = APIRouter()

# 📤 Upload CSV - Version optimisée sans redondance
@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    try:
        # Stockage permanent dans /data
        permanent_path = os.path.join(DATA_DIR, file.filename)
        
        with open(permanent_path, "wb") as f:
            f.write(await file.read())

        # Lecture Spark UNE SEULE FOIS
        df = spark.read.option("header", True).option("inferSchema", True).csv(permanent_path)
        
        # Hash cohérent avec similarity.py
        from atlas.similarity import hash_partial
        hash_value = hash_partial(df)
        
        # Métadonnées pour éviter les recalculs
        stats = {
            "rowCount": df.count(),
            "columnCount": len(df.columns),
            "columns": list(df.columns)
        }

        # Stockage dans session
        session_data["df"] = df
        session_data["file_path"] = permanent_path
        session_data["hash"] = hash_value
        session_data["original_name"] = file.filename
        session_data["stats"] = stats

        return {
            "message": "Fichier chargé", 
            "columns": df.columns, 
            "hash": hash_value, 
            "original_name": file.filename,
            "stats": stats
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur upload: {str(e)}")

# 👀 Aperçu du dataset
@router.get("/preview")
def preview(n: int = 100):
    df = session_data.get("df")
    if df is None:
        raise HTTPException(status_code=400, detail="Aucun fichier uploadé")
    return df.limit(n).toPandas().to_dict(orient="records")

# 📑 Obtenir les colonnes
@router.get("/get-columns")
async def get_columns():
    df = session_data.get("df")
    if df is None:
        raise HTTPException(status_code=400, detail="Aucun fichier uploadé")
    return {"columns": df.columns}

# 📑 Obtenir colonnes + types Spark
@router.get("/get-schema")
async def get_schema():
    df = session_data.get("df")
    if df is None:
        raise HTTPException(status_code=400, detail="Aucun fichier uploadé")
    return {"schema": [{"name": name, "type": dtype} for name, dtype in df.dtypes]}