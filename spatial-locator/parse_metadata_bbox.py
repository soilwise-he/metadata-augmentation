from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def parse_metadata(raw_value: str) -> dict[str, Any] | None:
    if raw_value is None:
        return None

    value = raw_value.strip()
    if not value or value.lower() == "null":
        return None

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None

    return parsed if isinstance(parsed, dict) else None


def normalize_bbox(raw_bbox: Any) -> tuple[list[float] | None, str | None]:
    if not isinstance(raw_bbox, list):
        return None, None

    if len(raw_bbox) not in (4, 5):
        return None, None

    coords = raw_bbox[:4]
    crs = raw_bbox[4] if len(raw_bbox) == 5 and isinstance(raw_bbox[4], str) else None

    try:
        west, south, east, north = (float(value) for value in coords)
    except (TypeError, ValueError):
        return None, crs

    if east < west or north < south:
        return None, crs

    return [west, south, east, north], crs


def build_output_row(row: dict[str, str]) -> dict[str, str]:
    metadata = parse_metadata(row.get("metadata", ""))
    bbox, bbox_crs = normalize_bbox(metadata.get("bbox") if metadata else None)

    output_row = dict(row)
    output_row["bbox_status"] = "ok" if bbox else ("missing" if metadata and metadata.get("bbox") is None else "unparsed")
    output_row["bbox"] = "" if not bbox else "[" + ", ".join(str(coord) for coord in bbox) + "]"
    output_row["bbox_crs"] = "" if not bbox_crs else bbox_crs
    output_row["bbox_west"] = "" if not bbox else str(bbox[0])
    output_row["bbox_south"] = "" if not bbox else str(bbox[1])
    output_row["bbox_east"] = "" if not bbox else str(bbox[2])
    output_row["bbox_north"] = "" if not bbox else str(bbox[3])
    output_row["bbox_crs"] = "" if not bbox_crs else bbox_crs
    return output_row


def default_input_path() -> Path:
    return Path(__file__).resolve().with_name("augmented_dataset.csv")


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_with_bbox{input_path.suffix}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse the metadata column in a CSV and expand bbox values into dedicated columns."
    )
    parser.add_argument("--input", type=Path, default=default_input_path(), help="Path to the input CSV file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path for the output CSV file. Defaults to <input>_with_bbox.csv.",
    )
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output or default_output_path(input_path)

    with input_path.open("r", newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        if not reader.fieldnames:
            raise ValueError("Input CSV is missing a header row.")
        if "metadata" not in reader.fieldnames:
            raise ValueError("Input CSV does not contain a 'metadata' column.")

        fieldnames = list(reader.fieldnames)
        for extra_field in ["bbox", "bbox_crs", "bbox_status", "bbox_west", "bbox_south", "bbox_east", "bbox_north", "bbox_crs"]:
            if extra_field not in fieldnames:
                fieldnames.append(extra_field)

        row_count = 0
        bbox_count = 0

        with output_path.open("w", newline="", encoding="utf-8") as target_file:
            writer = csv.DictWriter(target_file, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                output_row = build_output_row(row)
                if output_row["bbox_status"] == "ok":
                    bbox_count += 1
                    writer.writerow(output_row)
                row_count += 1

    print(f"Processed {row_count} rows from {input_path}")
    print(f"Parsed {bbox_count} bbox values into {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())