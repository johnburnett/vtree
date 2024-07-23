import os
import random
import statistics
import subprocess
import sys
import tempfile

from tqdm import trange


def estimate_zip_size(root_dir_path, num_trials, files_per_trial):
    all_file_paths = []
    for dir_path, dir_names, file_names in os.walk(root_dir_path):
        # .partial files are from in-progress rclone syncs, which might be running
        # while this script is getting used
        all_file_paths.extend([os.path.join(dir_path, fn) for fn in file_names if not fn.endswith('.partial')])
    all_file_paths.sort()
    assert len(all_file_paths) >= files_per_trial

    compression_ratios = []
    for ii in trange(num_trials, desc=f'Running compression trials across {len(all_file_paths)} files'):
        random.seed(ii)

        sample_file_paths = set()
        while len(sample_file_paths) < files_per_trial:
            update_size = files_per_trial - len(sample_file_paths)
            sample_file_paths.update(random.choices(all_file_paths, k=update_size))
        sample_file_paths = sorted(sample_file_paths)
        assert len(sample_file_paths) == files_per_trial

        list_file_path = os.path.join(tempfile.gettempdir(), 'estimate_compression.txt')
        with open(list_file_path, 'w', encoding='utf-8') as fp:
            fp.writelines(file_path + '\n' for file_path in sample_file_paths)

        zip_file_path = os.path.join(tempfile.gettempdir(), 'estimate_compression.zip')
        try:
            args = [
                'C:\\Program Files\\7-Zip\\7z.exe',
                'a',
                '-snh',     # store hard links
                '-snl',     # store symbolic links
                '-ssp',     # don't touch last-access-time
                '-spf2',    # use full paths without drive letter
                zip_file_path,
                f'@{list_file_path}'
            ]
            subprocess.run(args, capture_output=True, check=True)
            compressed_size = os.path.getsize(zip_file_path)
        finally:
            for file_path in (list_file_path, zip_file_path):
                try:
                    os.unlink(zip_file_path)
                except FileNotFoundError:
                    pass

        uncompressed_size = sum(os.path.getsize(file_path) for file_path in sample_file_paths)
        compression_ratio = uncompressed_size / compressed_size
        compression_ratios.append(compression_ratio)

    print(f'compression ({num_trials} trials, {files_per_trial} files per trial):')
    print(f' mean: {statistics.mean(compression_ratios):.2f}')
    print(f'  min: {min(compression_ratios):.2f}')
    print(f'  max: {max(compression_ratios):.2f}')
    print(f'stdev: {statistics.stdev(compression_ratios):.2f}')

try:
    root_dir_path = sys.argv[1]
    assert os.path.isdir(root_dir_path)
    num_trials = int(sys.argv[2])
    files_per_trial = int(sys.argv[3])
except:
    print('python -m vtree.estimate_compression <root_path> <num_trials> <files_per_trial>')

if __name__ == '__main__':
    estimate_zip_size(root_dir_path, num_trials, files_per_trial)
