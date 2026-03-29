from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable

NULL_VALUES = {"", "null", "none", "na", "n/a", "nan"}
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]
DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
]


def detect_value_type(value: str) -> str:
    text = value.strip()
    if text.lower() in NULL_VALUES:
        return "null"

    lowered = text.lower()
    if lowered in {"true", "false"}:
        return "boolean"

    try:
        int(text)
        return "integer"
    except ValueError:
        pass

    try:
        float(text)
        return "float"
    except ValueError:
        pass

    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(text, fmt)
            return "datetime"
        except ValueError:
            continue

    for fmt in DATE_FORMATS:
        try:
            datetime.strptime(text, fmt)
            return "date"
        except ValueError:
            continue

    return "string"


def merge_types(left: str, right: str) -> str:
    if left == right:
        return left
    if left == "null":
        return right
    if right == "null":
        return left

    numeric = {left, right}
    if numeric == {"integer", "float"}:
        return "float"

    temporal = {left, right}
    if temporal == {"date", "datetime"}:
        return "datetime"

    return "string"


def infer_schema(csv_path: Path, sample_rows: int) -> dict[str, str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}

        schema = {col: "null" for col in reader.fieldnames}
        for i, row in enumerate(reader):
            for col in reader.fieldnames:
                detected = detect_value_type(row.get(col, ""))
                schema[col] = merge_types(schema[col], detected)

            if sample_rows > 0 and i + 1 >= sample_rows:
                break

    return schema


def list_csv_files(folder: Path, recursive: bool) -> Iterable[Path]:
    pattern = "**/*.csv" if recursive else "*.csv"
    return sorted(folder.glob(pattern))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="แสดงชื่อไฟล์ CSV และ schema ของแต่ละไฟล์"
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="โฟลเดอร์ที่มีไฟล์ CSV (ค่าเริ่มต้นคือโฟลเดอร์ปัจจุบัน)",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5000,
        help="จำนวนแถวที่ใช้ infer schema ต่อไฟล์ (0 = อ่านทั้งไฟล์)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="ค้นหาไฟล์ CSV ในโฟลเดอร์ย่อยด้วย",
    )
    args = parser.parse_args()

    folder = Path(args.path).resolve()
    csv_files = list(list_csv_files(folder, args.recursive))

    if not csv_files:
        print(f"ไม่พบไฟล์ CSV ใน: {folder}")
        return

    print(f"พบไฟล์ CSV จำนวน {len(csv_files)} ไฟล์ใน: {folder}")
    print("=" * 72)

    for csv_file in csv_files:
        print(f"ชื่อไฟล์: {csv_file.name}")
        schema = infer_schema(csv_file, args.sample_rows)

        if not schema:
            print("schema: (ไฟล์ว่างหรือไม่มี header)")
            print("-" * 72)
            continue

        print("schema:")
        for col, dtype in schema.items():
            print(f"  - {col}: {dtype}")
        print("-" * 72)


if __name__ == "__main__":
    main()
