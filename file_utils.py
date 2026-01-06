import os
import glob

def list_files(directory, file_spec="*.xlsx", include_subdirs=True):
    """Finds all files in a directory (and subdirectories if specified)."""
    pattern = os.path.join(directory, '**', file_spec) if include_subdirs else os.path.join(directory, file_spec)
    return glob.glob(pattern, recursive=include_subdirs)
