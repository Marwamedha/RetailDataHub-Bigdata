import os
import subprocess
import shutil
from datetime import datetime


def load_to_hdfs(
    local_landing_path,
    hdfs_raw_path,
    hadoop_bin_path="/opt/hadoop/bin/hdfs"
):
    """
    Loads validated CSV batches from local landing zone into HDFS raw layer
    partitioned by date and hour, then archives processed data.
    """

    # Time-based partitions
    full_date = datetime.now().strftime('%Y_%m_%d')
    hour = datetime.now().strftime('%H')

    archive_path = os.path.join(local_landing_path, "archive", full_date)
    os.makedirs(archive_path, exist_ok=True)

    # Scan landing zone
    for folder in os.listdir(local_landing_path):
        full_path = os.path.join(local_landing_path, folder)

        # Skip archive and non-directories
        if folder == "archive" or not os.path.isdir(full_path):
            continue

        # Walk through batch folder
        for _, _, files in os.walk(full_path):

            csv_files = [f for f in files if f.endswith(".csv")]

            # ✅ Data completeness check
            if len(csv_files) != 3:
                print(f"[WARN] Incomplete batch in '{folder}', skipping")
                continue

            print(f"[INFO] Valid batch detected: {folder}")

            # Create HDFS partition
            hdfs_partition = f"{hdfs_raw_path}/{full_date}/{hour}"
            mkdir_cmd = f"{hadoop_bin_path} dfs -mkdir -p {hdfs_partition}"
            subprocess.run(mkdir_cmd, shell=True, check=True)

            # Load CSV files to HDFS
            put_cmd = f"{hadoop_bin_path} dfs -put {full_path}/*.csv {hdfs_partition}"
            subprocess.run(put_cmd, shell=True, check=True)

            print(f"[INFO] Data loaded to HDFS: {hdfs_partition}")

            # Archive processed batch
            shutil.move(full_path, archive_path)
            os.rename(
                os.path.join(archive_path, folder),
                os.path.join(archive_path, hour)
            )

            print(f"[INFO] Archived batch under hour: {hour}")

            # Process only ONE batch per run
            return

    print("[INFO] No valid batches found.")


if __name__ == "__main__":

    # PROJECT ROOT
    PROJECT_ROOT = "/data/RetailDataHub-Bigdata"

    # Local landing zone (from Step 0)
    LOCAL_LANDING_PATH = os.path.join(PROJECT_ROOT, "data", "landing")

    # HDFS raw layer
    HDFS_RAW_PATH = "/project/raw_data"

    load_to_hdfs(
        local_landing_path=LOCAL_LANDING_PATH,
        hdfs_raw_path=HDFS_RAW_PATH
    )
