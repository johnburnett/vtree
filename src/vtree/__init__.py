import os


def try_unlink(file_path):
    try:
        os.unlink(file_path)
    except FileNotFoundError:
        pass
