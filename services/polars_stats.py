import polars as pl
from core.deps import get_db


async def load_listings_df() -> pl.DataFrame:
    docs = await get_db(["listingparis"].find(
        {},
        {"_id": 0}
    ).to_list(length=None))

    df = pl.from_dicts(docs)
    return df