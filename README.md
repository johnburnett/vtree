# vtree

## vtree.mirror

Create trees of sparse files mirroring a source tree.

Doing this because using something like windirstat on a mounted Google Drive is
slow.  So, finding a faster way of crawling the remote drive (rclone), and then
creating a virtual local tree that windirstat can use instead.

Create rclone ls file via `rclone ls <remote> --fast-list > <rclone_list_file_path>`

Run via `python -m vtree.mirror <rclone_list_file_path> <target_root_dir>`

## vtree.estimate_compression

Estimate how well a file tree will compress by running a number of trials
against a subset of the files.

Run via `python -m vtree.estimate_compression <root_path> <num_trials> <files_per_trial>`

## vtree.compress_subdirs

Compress sub-dirs into individual archives named after the sub-dir names.
