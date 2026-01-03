import os
import shutil
import time


def move_one_subfolder_per_hour(source_folder, target_folder, delay_seconds=3600):
    """
    Moves one subfolder at a time from source to target
    with a fixed delay (default = 1 hour).
    """

    subfolders = [
        f for f in os.listdir(source_folder)
        if os.path.isdir(os.path.join(source_folder, f))
    ]

    num_subfolders = len(subfolders)

    for i, subfolder in enumerate(subfolders):
        src_path = os.path.join(source_folder, subfolder)
        tgt_path = os.path.join(target_folder, subfolder)

        try:
            shutil.move(src_path, tgt_path)
            print(f"[INFO] Moved '{subfolder}' to landing zone")
        except Exception as e:
            print(f"[ERROR] Failed to move '{subfolder}': {e}")

        # Wait before moving next batch
        if i < num_subfolders - 1:
            print(f"[INFO] Waiting {delay_seconds} seconds before next batch...")
            time.sleep(delay_seconds)


if __name__ == "__main__":

    #  PROJECT PATHS
    PROJECT_ROOT = r"D:\Retail-Big-Data-Project"

    SOURCE_FOLDER = os.path.join(PROJECT_ROOT, "data", "source")
    TARGET_FOLDER = os.path.join(PROJECT_ROOT, "data", "landing")

    move_one_subfolder_per_hour(
        source_folder=SOURCE_FOLDER,
        target_folder=TARGET_FOLDER,
        delay_seconds=3600  # 1 hour simulation
    )

    print("[SUCCESS] All subfolders processed successfully!")
