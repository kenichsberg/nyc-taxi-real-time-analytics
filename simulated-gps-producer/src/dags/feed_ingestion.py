import shutil
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

#TODO move to a common file
dir = os.path.dirname(__file__)
PARTITIONED_DIR = dir / Path("../data/generated/simulated_gps")
STAGING_DIR = dir / Path("../data/staging/")
INGESTION_DIR = dir / Path("../data/ingestion/")

def get_partition_path() -> Path:
    now = datetime.now()
    return PARTITIONED_DIR / f"minute={now.minute}" / f"second={now.second}"


def copy_parquets(
    src_dir: Path,
    staging_dst_dir: Path,
    final_dst_dir: Path
) -> bool:
    if not src_dir.exists():
        print("No files")
        return False

    files: Iterator[Path] = src_dir.glob("*.parquet")
    if not files:
        print("No parquet files")
        return False

    for file in files:
        print("Copying from: ", file)
        staging_dst_file: Path = staging_dst_dir / file.name
        shutil.copy2(file, staging_dst_file)
        os.sync()

        final_dst_file: Path = final_dst_dir / file.name
        os.rename(staging_dst_file, final_dst_file)
        os.sync()
        print("Copied!")
    
    return True


def feed_ingestion_per_second() -> None:
    print("Started to copy parquet files")
    INGESTION_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        partition_path = get_partition_path()
        copy_parquets(partition_path, STAGING_DIR, INGESTION_DIR)
        time.sleep(1)

if __name__ == "__main__":
    feed_ingestion_per_second()
