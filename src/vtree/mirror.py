import os
import queue
import sys
import threading

from sparse_file import open_sparse


DEFAULT_NUM_WORKER_THREADS = 12

_SPARSE_CHUNKS = list(reversed([b'\x00' * (2**power) for power in range(24)]))


def create_sparse_file(file_path, size):
    '''Create a sparse file on disk.  Attempt at avoiding repeated allocations
    for differently sized files by re-using chunks of zero bytes to fill out
    the file.
    '''
    print(file_path)
    with open(file_path, 'wb') as fp:
        current_size = 0
        for chunk in _SPARSE_CHUNKS:
            while (size - current_size) >= len(chunk):
                fp.write(chunk)
                current_size += len(chunk)

    with open_sparse(file_path, 'ab') as fp:
        assert fp.hole(0, size)


def iter_mirror_tree_paths(source_root_dir, target_root_dir):
    for source_dir_path, source_dir_names, source_file_names in os.walk(source_root_dir):
        source_common_path = os.path.commonpath([source_root_dir, source_dir_path])
        target_dir_path = target_root_dir + source_dir_path[len(source_common_path):]
        for source_file_name in source_file_names:
            source_file_path = os.path.join(source_dir_path, source_file_name)
            target_file_path = os.path.join(target_dir_path, source_file_name)
            yield (source_file_path, target_file_path)


def iter_file_list(file_list_path):
    with open(file_list_path, encoding='utf-8') as fp:
        for line in fp:
            line = line.strip()
            size, path = line.split(' ', 1)
            size = int(size)
            path = path.replace('/', '\\')
            assert not os.path.isabs(path)
            yield (path, size)


def mirror_file_sparse(source_file_path, target_file_path):
    target_dir_path = os.path.dirname(target_file_path)
    os.makedirs(target_dir_path, exist_ok=True)
    source_file_size = os.path.getsize(source_file_path)
    create_sparse_file(target_file_path, source_file_size)


def mirror_tree_sparse(source_root_dir, target_root_dir):
    assert os.path.exists(source_root_dir)
    assert not os.path.exists(target_root_dir)
    for source_file_path, target_file_path in iter_mirror_tree_paths(source_root_dir, target_root_dir):
        target_dir_path = os.path.dirname(target_file_path)
        os.makedirs(target_dir_path, exist_ok=True)
        source_file_size = os.path.getsize(source_file_path)
        create_sparse_file(target_file_path, source_file_size)


def start_work_queue(*, num_threads=DEFAULT_NUM_WORKER_THREADS):
    def worker_proc(work_queue):
        while True:
            proc, args, kwargs = work_queue.get()
            try:
                proc(*args, **kwargs)
            except Exception as ex:
                try:
                    while work_queue.get_nowait():
                        work_queue.task_done()
                except queue.Empty:
                    pass
                raise ex
            finally:
                work_queue.task_done()

    work_queue = queue.Queue()
    for ii in range(num_threads):
        th = threading.Thread(name='worker%d' % ii, target=worker_proc, args=(work_queue,))
        th.daemon = True
        th.start()
    return work_queue


def mirror_rclone_list_sparse(rclone_list_file_path, target_root_dir):
    # slow, one file at a time
    # mirror_tree_sparse(source_root_dir, target_root_dir)

    # faster, crawl source and queue creation of target files across threads
    # work_queue = start_work_queue()
    # for source_target_paths in iter_mirror_tree_paths(source_root_dir, target_root_dir):
    #     work_queue.put((mirror_file_sparse, source_target_paths, {}))
    # work_queue.join()

    # faster still, crawl source using rclone to get sizes/paths, queue creation of
    # target files across threads.  Use the following to create rclone_list_file_path:
    work_queue = start_work_queue()
    for source_file_sub_path, source_file_size in iter_file_list(rclone_list_file_path):
        target_file_path = os.path.join(target_root_dir, source_file_sub_path)
        target_dir_path = os.path.dirname(target_file_path)
        os.makedirs(target_dir_path, exist_ok=True)
        work_queue.put((create_sparse_file, (target_file_path, source_file_size), {}))
    work_queue.join()


if __name__ == '__main__':
    try:
        rclone_list_file_path = sys.argv[1]
        assert os.path.isfile(rclone_list_file_path)
        target_root_dir = sys.argv[2]
    except:
        print('python -m vtree.mirror <rclone_list_file_path> <target_root_dir>')
        sys.exit(1)
    else:
        mirror_rclone_list_sparse(rclone_list_file_path, target_root_dir)
