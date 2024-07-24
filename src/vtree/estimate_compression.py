import os
import random
import statistics
import subprocess
import sys
import tempfile

from tqdm import tqdm, trange

from . import try_unlink


def estimate_zip_size(root_dir_path, num_trials, files_per_trial, *, compression_methods=(('zip', 5), ('7z', 5), ('7z', 9))):
    all_file_paths = []
    for dir_path, dir_names, file_names in os.walk(root_dir_path):
        # .partial files are from in-progress rclone syncs, which might be running
        # while this script is getting used
        all_file_paths.extend([os.path.join(dir_path, fn) for fn in file_names if not fn.endswith('.partial')])
    all_file_paths.sort()
    assert len(all_file_paths) >= files_per_trial

    methodlevel_to_ratios = {}
    for ii in trange(num_trials, desc=f'Running compression trials across {len(all_file_paths)} files'):
        random.seed(ii)

        sample_file_paths = set()
        while len(sample_file_paths) < files_per_trial:
            update_size = files_per_trial - len(sample_file_paths)
            sample_file_paths.update(random.choices(all_file_paths, k=update_size))
        sample_file_paths = sorted(sample_file_paths)
        assert len(sample_file_paths) == files_per_trial

        try:
            list_file_path = os.path.join(tempfile.gettempdir(), 'estimate_compression.txt')
            with open(list_file_path, 'w', encoding='utf-8') as fp:
                fp.writelines(file_path + '\n' for file_path in sample_file_paths)
            uncompressed_size = sum(os.path.getsize(file_path) for file_path in sample_file_paths)

            for methodlevel in tqdm(compression_methods, desc='Testing compression methods'):
                method, level = methodlevel
                compressed_file_path = os.path.join(tempfile.gettempdir(), f'estimate_compression.{method}')
                try:
                    args = [
                        'C:\\Program Files\\7-Zip\\7z.exe',
                        'a',
                        '-y',       # suppress prompts
                        '-bd',      # no progress
                        '-snh',     # store hard links
                        '-snl',     # store symbolic links
                        '-ssp',     # don't touch last-access-time
                        '-spf2',    # use full paths without drive letter
                        f'-mx{level}',
                        compressed_file_path,
                        f'@{list_file_path}'
                    ]
                    subprocess.run(args, capture_output=True, check=True)
                    compressed_size = os.path.getsize(compressed_file_path)
                    compression_ratio = uncompressed_size / compressed_size
                    methodlevel_to_ratios.setdefault(methodlevel, []).append(compression_ratio)
                finally:
                    try_unlink(compressed_file_path)
        finally:
            try_unlink(list_file_path)

    print(f'{num_trials} compression trials, {files_per_trial} files per trial:')
    for methodlevel in sorted(compression_methods):
        method, level = methodlevel
        print(f'{method} (level {level})')
        compression_ratios = methodlevel_to_ratios[methodlevel]
        print(f'   mean: {statistics.mean(compression_ratios):.2f}x')
        print(f'    min: {min(compression_ratios):.2f}x')
        print(f'    max: {max(compression_ratios):.2f}x')
        print(f'  stdev: {statistics.stdev(compression_ratios):.2f}')


if __name__ == '__main__':
    try:
        root_dir_path = sys.argv[1]
        assert os.path.isdir(root_dir_path)
        num_trials = int(sys.argv[2])
        files_per_trial = int(sys.argv[3])
    except:
        print('python -m vtree.estimate_compression <root_path> <num_trials> <files_per_trial>')
        sys.exit(1)
    else:
        estimate_zip_size(root_dir_path, num_trials, files_per_trial)
