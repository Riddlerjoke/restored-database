from typing import List

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorDatabase

from core.deps import get_db
from crud.restore import restore_from_csv
from models.restore import RestoreCSVResponse

router = APIRouter(tags=["restore"])


@router.post("/restore/csv", response_model=RestoreCSVResponse)
async def restore_csv(
    file: UploadFile = File(..., description="Fichier CSV à téléverser"),
    collection: str = Query(..., description="Nom de la collection cible"),
    delimiter: str = Query(
        ",", min_length=1, max_length=1, description="Délimiteur CSV (par défaut ',')"
    ),
    encoding: str = Query("utf-8", description="Encodage du fichier (par défaut utf-8)"),
    drop_existing: bool = Query(
        False, description="Supprimer la collection avant l'insertion (restauration complète)"
    ),
    batch_size: int = Query(1000, ge=1, le=50_000, description="Taille des lots d'insertion"),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> JSONResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Le fichier doit être un CSV")

    try:
        raw = await file.read()
    finally:
        await file.close()

    try:
        inserted_total, fields = await restore_from_csv(
            db,
            raw,
            collection,
            delimiter=delimiter,
            encoding=encoding,
            drop_existing=drop_existing,
            batch_size=batch_size,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'insertion: {e}")

    return JSONResponse(
        status_code=201,
        content={
            "collection": collection,
            "inserted": inserted_total,
            "fields": fields,
        },
    )
