from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from core.deps import get_db

router = APIRouter(tags=["analyse"])


@router.get("/analyse/total")
async def get_total_listings(
    collection: str = Query(
        "listings",
        description="Nom de la collection contenant les annonces",
    ),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Retourne le nombre total de documents (logements) dans la collection.
    """
    try:
        total = await db[collection].count_documents({})
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors du comptage des documents: {e}",
        )

    return {
        "collection": collection,
        "total_listings": total,
    }


@router.get("/analyse/libres")
async def get_available_listings(
    collection: str = Query(
        "listings",
        description="Nom de la collection contenant les annonces",
    ),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Retourne le nombre d'appartements libres.

    Critère :
    - has_availability = True
    - ou valeurs textuelles équivalentes ('t', 'true', 'True') si les données
      ont été importées comme chaînes depuis le CSV.
    """
    # Requête robuste qui gère booléens et chaînes issues du CSV
    query = {
        "has_availability": {
            "$in": [True, "t", "T", "true", "True"]
        }
    }

    try:
        available = await db[collection].count_documents(query)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors du comptage des logements libres: {e}",
        )

    return {
        "collection": collection,
        "available_listings": available,
    }
