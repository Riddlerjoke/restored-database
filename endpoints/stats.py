from __future__ import annotations

from typing import Dict, List, Optional

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from core.deps import get_db

router = APIRouter(tags=["stats"])

# Adapte si ta collection s’appelle autrement
COLL = "listing"


# -------------------------
# Helpers (Polars)
# -------------------------

def _truthy_expr(col_name: str) -> pl.Expr:
    """
    Convertit une colonne (bool / int / string) vers un bool, sans cast direct vers Boolean.
    Compatible avec Utf8View.
    """
    s = pl.col(col_name).cast(pl.Utf8, strict=False).str.to_lowercase()

    return (
        s.is_in(["t", "true", "1", "yes", "y"])
        .fill_null(False)
    )


def _neighbourhood_expr() -> pl.Expr:
    """neighbourhood = neighbourhood_cleansed sinon neighbourhood"""
    return (
        pl.coalesce([pl.col("neighbourhood_cleansed"), pl.col("neighbourhood")])
        .cast(pl.Utf8, strict=False)
        .alias("neighbourhood")
    )


def _booking_rate_30_expr() -> pl.Expr:
    """booking_rate_30 = (30 - availability_30) / 30"""
    return (
        ((pl.lit(30) - pl.col("availability_30").cast(pl.Float64, strict=False)) / 30.0)
        .clip(0.0, 1.0)
        .alias("booking_rate_30")
    )


def _require_cols(df: pl.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Champs manquants dans la collection '{COLL}': {missing}",
        )


async def _load_df(
    db: AsyncIOMotorDatabase,
    projection: Optional[Dict[str, int]] = None,
    limit: Optional[int] = None,
) -> pl.DataFrame:
    """Charge Mongo -> Polars DataFrame."""
    if projection is None:
        projection = {"_id": 0}

    cursor = db[COLL].find({}, projection)
    if limit is not None:
        cursor = cursor.limit(limit)

    docs = await cursor.to_list(length=None)
    return pl.from_dicts(docs) if docs else pl.DataFrame()


# -------------------------
# Routes: chiffres
# -------------------------

@router.get("/listings/by-room-type")
async def listings_by_room_type(
    limit: int = Query(200_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Le nombre d’annonces par type de location."""
    df = await _load_df(db, {"_id": 0, "room_type": 1}, limit=limit)
    if df.is_empty():
        return []

    _require_cols(df, ["room_type"])

    out = (
        df
        .with_columns(pl.col("room_type").cast(pl.Utf8, strict=False))
        .group_by("room_type")
        .len()
        .rename({"len": "nb_listings"})
        .sort("nb_listings", descending=True)
    )
    return out.to_dicts()


@router.get("/listings/most-booked")
async def most_booked(
    top: int = Query(20, ge=1, le=200),
    limit: int = Query(200_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Les logements les plus loués (approx fenêtre 30 jours via availability_30)."""
    df = await _load_df(
        db,
        {
            "_id": 0,
            "id": 1,
            "name": 1,
            "room_type": 1,
            "availability_30": 1,
            "number_of_reviews": 1,
            "neighbourhood_cleansed": 1,
            "neighbourhood": 1,
        },
        limit=limit,
    )
    if df.is_empty():
        return []

    _require_cols(df, ["availability_30"])

    out = (
        df
        .with_columns([_booking_rate_30_expr(), _neighbourhood_expr()])
        .sort("booking_rate_30", descending=True)
        .select([
            pl.col("id"),
            pl.col("name"),
            pl.col("room_type"),
            pl.col("neighbourhood"),
            pl.col("availability_30"),
            pl.col("booking_rate_30"),
            pl.col("number_of_reviews"),
        ])
        .head(top)
    )
    return out.to_dicts()


@router.get("/hosts/distinct-count")
async def distinct_hosts(
    limit: int = Query(500_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Le nombre total d’hôtes différents."""
    df = await _load_df(db, {"_id": 0, "host_id": 1}, limit=limit)
    if df.is_empty():
        return {"distinct_hosts": 0}

    _require_cols(df, ["host_id"])

    n = df.select(pl.col("host_id").n_unique()).item()
    return {"distinct_hosts": int(n)}


@router.get("/listings/instant-bookable/count")
async def instant_bookable_count(
    limit: int = Query(500_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Le nombre de locations réservables instantanément."""
    df = await _load_df(db, {"_id": 0, "instant_bookable": 1}, limit=limit)
    if df.is_empty():
        return {"instant_bookable_listings": 0}

    _require_cols(df, ["instant_bookable"])

    count = (
        df
        .with_columns(_truthy_expr("instant_bookable").alias("instant_bookable_bool"))
        .filter(pl.col("instant_bookable_bool"))
        .height
    )
    return {"instant_bookable_listings": int(count)}


@router.get("/hosts/with-more-than")
async def hosts_with_more_than(
    min_listings: int = Query(100, ge=1, le=100000),
    top: int = Query(200, ge=1, le=5000),
    limit: int = Query(2_000_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """changement de logique pour le nombre de listings par host """
    df = await _load_df(db, {"_id": 0, "host_id": 1}, limit=limit)
    if df.is_empty():
        return []

    _require_cols(df, ["host_id"])

    out = (
        df
        .group_by("host_id")
        .len()
        .rename({"len": "nb_listings"})
        .filter(pl.col("nb_listings") > min_listings)
        .sort("nb_listings", descending=True)
        .head(top)
    )
    return out.to_dicts()


@router.get("/hosts/superhosts/distinct-count")
async def distinct_superhosts(
    limit: int = Query(500_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Le nombre de super hôtes différents."""
    df = await _load_df(db, {"_id": 0, "host_id": 1, "host_is_superhost": 1}, limit=limit)
    if df.is_empty():
        return {"distinct_superhosts": 0}

    _require_cols(df, ["host_id", "host_is_superhost"])

    n = (
        df
        .with_columns(_truthy_expr("host_is_superhost").alias("is_superhost"))
        .filter(pl.col("is_superhost"))
        .select(pl.col("host_id").n_unique())
        .item()
    )
    return {"distinct_superhosts": int(n)}


# -------------------------
# Routes: statistiques
# -------------------------

@router.get("/bookings/rate-30d/by-room-type")
async def booking_rate_30d_by_room_type(
    limit: int = Query(500_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Taux de réservation moyen par type de logement (approx fenêtre 30 jours).
    """
    df = await _load_df(db, {"_id": 0, "room_type": 1, "availability_30": 1}, limit=limit)
    if df.is_empty():
        return []

    _require_cols(df, ["room_type", "availability_30"])

    out = (
        df
        .with_columns([
            pl.col("room_type").cast(pl.Utf8, strict=False),
            _booking_rate_30_expr(),
        ])
        .group_by("room_type")
        .agg([
            pl.col("booking_rate_30").mean().alias("avg_booking_rate_30"),
            pl.len().alias("n"),
        ])
        .sort("avg_booking_rate_30", descending=True)
    )
    return out.to_dicts()


@router.get("/reviews/median")
async def median_reviews(
    limit: int = Query(1_000_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Médiane des avis (tous logements)."""
    df = await _load_df(db, {"_id": 0, "number_of_reviews": 1}, limit=limit)
    if df.is_empty():
        return {"median_reviews": None}

    _require_cols(df, ["number_of_reviews"])

    med = df.select(pl.col("number_of_reviews").cast(pl.Float64, strict=False).median()).item()
    return {"median_reviews": None if med is None else float(med)}


@router.get("/reviews/median/by-host-category")
async def median_reviews_by_host_category(
    limit: int = Query(1_000_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Médiane des avis par catégorie d’hôte (superhost vs non)."""
    df = await _load_df(db, {"_id": 0, "number_of_reviews": 1, "host_is_superhost": 1}, limit=limit)
    if df.is_empty():
        return []

    _require_cols(df, ["number_of_reviews", "host_is_superhost"])

    out = (
        df
        .with_columns([
            _truthy_expr("host_is_superhost").alias("is_superhost"),
            pl.col("number_of_reviews").cast(pl.Float64, strict=False),
        ])
        .group_by("is_superhost")
        .agg([
            pl.col("number_of_reviews").median().alias("median_reviews"),
            pl.len().alias("n"),
        ])
        .with_columns(
            pl.when(pl.col("is_superhost"))
            .then(pl.lit("superhost"))
            .otherwise(pl.lit("non_superhost"))
            .alias("host_category")
        )
        .select(["host_category", "median_reviews", "n"])
        .sort("host_category")
    )
    return out.to_dicts()


@router.get("/density/by-neighbourhood")
async def density_by_neighbourhood(
    top: int = Query(100, ge=1, le=5000),
    limit: int = Query(1_000_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Densité de logements par quartier."""
    df = await _load_df(
        db,
        {"_id": 0, "neighbourhood_cleansed": 1, "neighbourhood": 1},
        limit=limit,
    )
    if df.is_empty():
        return []

    if "neighbourhood_cleansed" not in df.columns and "neighbourhood" not in df.columns:
        raise HTTPException(status_code=400, detail="Champs quartier manquants")

    out = (
        df
        .with_columns(_neighbourhood_expr())
        .filter(pl.col("neighbourhood").is_not_null() & (pl.col("neighbourhood") != ""))
        .group_by("neighbourhood")
        .len()
        .rename({"len": "nb_listings"})
        .sort("nb_listings", descending=True)
        .head(top)
    )
    return out.to_dicts()


@router.get("/bookings/rate-30d/top-neighbourhoods")
async def top_neighbourhoods_booking_rate_30d(
    top: int = Query(20, ge=1, le=500),
    min_listings: int = Query(30, ge=1, le=100000),
    limit: int = Query(1_000_000, ge=1, le=2_000_000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Quartiers avec le plus fort taux de réservation (approx 30 jours)."""
    df = await _load_df(
        db,
        {"_id": 0, "availability_30": 1, "neighbourhood_cleansed": 1, "neighbourhood": 1},
        limit=limit,
    )
    if df.is_empty():
        return []

    _require_cols(df, ["availability_30"])

    out = (
        df
        .with_columns([_neighbourhood_expr(), _booking_rate_30_expr()])
        .filter(pl.col("neighbourhood").is_not_null() & (pl.col("neighbourhood") != ""))
        .group_by("neighbourhood")
        .agg([
            pl.col("booking_rate_30").mean().alias("avg_booking_rate_30"),
            pl.len().alias("n"),
        ])
        .filter(pl.col("n") >= min_listings)
        .sort("avg_booking_rate_30", descending=True)
        .head(top)
    )
    return out.to_dicts()
