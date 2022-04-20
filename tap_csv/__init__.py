"""Tap-csv."""
import os


def get_file_paths(file_config: dict) -> list:
    """Return a list of file paths to read.

    This tap accepts file names and directories so it will detect
    directories and iterate files inside.
    """
    file_path = file_config["path"]
    if not os.path.exists(file_path):     
        return []

    prefix = (file_config.get("prefix") or "").lower()

    file_paths = []
    if os.path.isdir(file_path):
        clean_file_path = os.path.normpath(file_path)
        # listdir returns an arbitrary order, sort for incremental sync
        listed_files = sorted(os.listdir(clean_file_path), key=str.lower)

        for filename in listed_files:
            if filename.lower().startswith(prefix):
                file_path = os.path.join(clean_file_path, filename)
                if os.path.isfile(file_path):
                    file_paths.append(file_path)
    else:
        file_paths.append(file_path)

    return file_paths
