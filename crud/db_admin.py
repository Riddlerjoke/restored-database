from motor.motor_asyncio import AsyncIOMotorDatabase


async def list_collections(db: AsyncIOMotorDatabase) -> list[str]:
    return await db.list_collection_names()


async def count_documents(db: AsyncIOMotorDatabase, collection: str) -> int:
    if collection not in await db.list_collection_names():
        return 0
    return await db[collection].count_documents({})
