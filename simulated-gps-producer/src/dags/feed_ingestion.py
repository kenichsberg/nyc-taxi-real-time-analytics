import shutil
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

#TODO move to a common file
dir = os.path.dirname(__file__)
PARTITIONED_DIR = dir / Path("../data/generated/simulated_gps")
INGESTION_DIR = dir / Path("../data/ingestion/")

def get_partition_path() -> Path:
    now = datetime.now()
    return PARTITIONED_DIR / f"hour={now.hour}" / f"minute={now.minute}" / f"second={now.second}"


def copy_parquets(src_dir: Path, dst_dir: Path) -> bool:
    print("src: ", src_dir)
    print("dst: ", dst_dir)
    if not src_dir.exists():
        print("No files")
        return False

    files: Iterator[Path] = src_dir.glob("*.parquet")
    if not files:
        print("No parquet files")
        return False

    for file in files:
        dst_file: Path = dst_dir / file.name
        print("file: ", file)
        print("dst_file: ", dst_file)
        shutil.copy2(file, dst_file)
        print("Copied!")
    
    return True


def feed_ingestion_per_second() -> None:
    print("Started to copy parquet files")
    INGESTION_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        partition_path = get_partition_path()
        copy_parquets(partition_path, INGESTION_DIR)
        time.sleep(1)

if __name__ == "__main__":
    feed_ingestion_per_second()
