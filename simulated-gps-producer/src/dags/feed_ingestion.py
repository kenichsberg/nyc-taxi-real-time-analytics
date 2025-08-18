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
FILE_RETENTION_MINUTES = 10
FEED_INTERVAL_SECONDS = 3

def get_partition_path(now: datetime) -> Path:
    return PARTITIONED_DIR / f"minute={now.minute}" / f"second={now.second}"


def copy_parquets_per_src_path(
    src_dir: Path,
    staging_dst_dir: Path,
    final_dst_dir: Path,
    now: datetime
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
        filename: str = str(now.timestamp()) + "_" + file.name
        staging_dst_file: Path = staging_dst_dir / filename
        shutil.copy(file, staging_dst_file)
        os.sync()

        final_dst_file: Path = final_dst_dir / filename
        os.rename(staging_dst_file, final_dst_file)
        os.sync()
        print("Copied!")
    
    return True


def copy_parquets(
    staging_dst_dir: Path,
    final_dst_dir: Path,
    now: datetime,
    backfill_seconds: int
) -> None:
    for i in range(0, backfill_seconds):
        current: datetime = now - timedelta(seconds=i)
        partition_path: Path = get_partition_path(now)
        copy_parquets_per_src_path(
            partition_path,
            staging_dst_dir,
            final_dst_dir,
            current
        )


def cleanup_old_files(
    dir: Path,
    retention_minutes: int,
    now: datetime
) -> None:
    files: Iterator[Path] = dir.glob("*")
    for file in files:
        if file.is_file() and now.timestamp() - file.stat().st_mtime > retention_minutes:
            file.unlink()


def feed_ingestion_per_second() -> None:
    print("Started to copy parquet files")
    INGESTION_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        now: datetime = datetime.now()
        #copy_parquets(
        #    STAGING_DIR,
        #    INGESTION_DIR,
        #    now,
        #    FEED_INTERVAL_SECONDS
        #)
        partition_path: Path = get_partition_path(now)
        copy_parquets_per_src_path(
            partition_path,
            STAGING_DIR,
            INGESTION_DIR,
            now
        )

        cleanup_old_files(
            INGESTION_DIR,
            FILE_RETENTION_MINUTES,
            now
        )

        time.sleep(FEED_INTERVAL_SECONDS)


if __name__ == "__main__":
    print("I'm inside")
    feed_ingestion_per_second()
