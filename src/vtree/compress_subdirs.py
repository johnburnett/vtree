from collections import namedtuple
import os
import shutil
import subprocess
import sys

from tqdm import tqdm, trange

from . import try_unlink


def compress_subdirs(root_dir_path):
    root_dir_path = os.path.abspath(root_dir_path)

    CompressionInfo = namedtuple("CompressionInfo", [
        'source_dir_name',
        'source_dir_path',
        'parent_dir_name',
        'compressed_file_name',
        'compressed_file_path',
    ])
    compression_infos = []
    with os.scandir(root_dir_path) as dirit:
        for entry in dirit:
            if entry.is_dir():
                source_dir_name = entry.name
                source_dir_path = entry.path
                parent_dir_path = os.path.abspath(os.path.join(source_dir_path, '..'))
                parent_dir_name = os.path.basename(parent_dir_path)
                compressed_file_name = source_dir_name + '.7z'
                compressed_file_path = os.path.join(parent_dir_path, compressed_file_name)
                assert not os.path.exists(compressed_file_path)
                ci = CompressionInfo(**{field: locals()[field] for field in CompressionInfo._fields})
                compression_infos.append(ci)

    progress = tqdm(compression_infos, desc='Compressing folders')
    for ci in progress:
        args = [
            'C:\\Program Files\\7-Zip\\7z.exe',
            'a',
            '-y',       # suppress prompts
            '-bd',      # no progress
            '-snh',     # store hard links
            '-snl',     # store symbolic links
            '-ssp',     # don't touch last-access-time
            f'-mx9',    # max compression
            ci.compressed_file_path,
            ci.source_dir_path,
        ]

        progress_path = os.path.join('...', ci.parent_dir_name, ci.compressed_file_name)
        progress.set_description(f'Compressing "{progress_path}"')
        try:
            assert subprocess.run(args, capture_output=True, check=True)
        except:
            try_unlink(ci.compressed_file_path)
            raise

        progress_path = os.path.join('...', ci.parent_dir_name, ci.source_dir_name)
        progress.set_description(f'Deleting "{progress_path}"')
        shutil.rmtree(ci.source_dir_path)


if __name__ == '__main__':
    try:
        root_dir_path = sys.argv[1]
        assert os.path.isdir(root_dir_path)
    except:
        print('python -m vtree.compress_subdirs <root_dir_path>')
        sys.exit(1)
    else:
        compress_subdirs(root_dir_path)
