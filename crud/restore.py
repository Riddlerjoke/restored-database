import csv
import io
from typing import List, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase


async def restore_from_csv(
    db: AsyncIOMotorDatabase,
    raw_csv_bytes: bytes,
    collection: str,
    *,
    delimiter: str = ",",
    encoding: str = "utf-8",
    drop_existing: bool = False,
    batch_size: int = 1000,
) -> Tuple[int, List[str]]:
    """Restore documents into a Mongo collection from CSV bytes.

    Returns (inserted_total, fields)
    """
    # Optional: drop collection first
    if drop_existing and collection in await db.list_collection_names():
        await db.drop_collection(collection)

    # Decode bytes and parse CSV
    text_stream = io.StringIO(raw_csv_bytes.decode(encoding, errors="replace"))
    reader = csv.DictReader(text_stream, delimiter=delimiter)
    if not reader.fieldnames:
        raise ValueError("En-têtes CSV introuvables")

    inserted_total = 0
    batch: List[dict] = []
    col = db[collection]

    for row in reader:
        # simple cleanup: '' -> None
        doc = {k: (v if v != "" else None) for k, v in row.items()}
        batch.append(doc)
        if len(batch) >= batch_size:
            result = await col.insert_many(batch)
            inserted_total += len(result.inserted_ids)
            batch.clear()

    if batch:
        result = await col.insert_many(batch)
        inserted_total += len(result.inserted_ids)
        batch.clear()

    return inserted_total, list(reader.fieldnames or [])
