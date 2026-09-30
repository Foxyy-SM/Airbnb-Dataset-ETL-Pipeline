from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    """One of the seven files Inside Airbnb publishes for a city snapshot."""

    name: str
    relative_path: str
    compressed: bool
    table: str
    kind: str

    @property
    def filename(self) -> str:
        return self.relative_path.rsplit("/", 1)[-1]


# The three detailed extracts are gzip. The four visualisation files are the
# small companions published next to them: summary listings, summary reviews,
# the neighbourhood list, and neighbourhood polygons.
DATASETS: tuple[Dataset, ...] = (
    Dataset("listings_detail", "data/listings.csv.gz", True, "listings", "gzip-csv"),
    Dataset("calendar", "data/calendar.csv.gz", True, "calendars", "gzip-csv"),
    Dataset("reviews_detail", "data/reviews.csv.gz", True, "reviews", "gzip-csv"),
    Dataset(
        "listings_summary",
        "visualisations/listings.csv",
        False,
        "listing_summaries",
        "csv",
    ),
    Dataset(
        "reviews_summary",
        "visualisations/reviews.csv",
        False,
        "review_dates",
        "csv",
    ),
    Dataset(
        "neighbourhoods",
        "visualisations/neighbourhoods.csv",
        False,
        "neighbourhoods",
        "csv",
    ),
    Dataset(
        "neighbourhood_geojson",
        "visualisations/neighbourhoods.geojson",
        False,
        "neighbourhood_geometries",
        "geojson",
    ),
)

_BY_NAME = {dataset.name: dataset for dataset in DATASETS}


def dataset_by_name(name: str) -> Dataset:
    try:
        return _BY_NAME[name]
    except KeyError as exc:
        raise KeyError(name) from exc
