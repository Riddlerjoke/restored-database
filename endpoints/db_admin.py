from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from core.deps import get_db
from crud import db_admin as crud_db
from models.common import CollectionsResponse, CountResponse

router = APIRouter(tags=["db-admin"])


@router.get("/collections", response_model=CollectionsResponse)
async def list_collections(db: AsyncIOMotorDatabase = Depends(get_db)) -> CollectionsResponse:
    names = await crud_db.list_collections(db)
    return CollectionsResponse(collections=names)


@router.get("/count", response_model=CountResponse)
async def count_documents(
    collection: str = Query(..., description="Nom de la collection à compter"),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> CountResponse:
    if not collection:
        raise HTTPException(status_code=400, detail="Paramètre 'collection' requis")
    count = await crud_db.count_documents(db, collection)
    return CountResponse(collection=collection, count=count)
