import argparse
from google.cloud.storage import Client, transfer_manager
import os

def upload_chunks_concurrently(
    bucket_name,
    source_filename,
    destination_blob_name,
    chunk_size=32,
    workers=8,
):
	"""Upload a single file, in chunks, concurrently in a process pool."""
	# The ID of your GCS bucket
	# bucket_name = "your-bucket-name"

	# The path to your file to upload
	# source_filename = "local/path/to/file"

	# The ID of your GCS object
	# destination_blob_name = "storage-object-name"

	# The size of each chunk. The performance impact of this value depends on
	# the use case. The remote service has a minimum of 5 MiB and a maximum of
	# 5 GiB.
	# chunk_size = 32 * 1024 * 1024 (32 MiB)

	# The maximum number of processes to use for the operation. The performance
	# impact of this value depends on the use case. Each additional process
	# occupies some CPU and memory resources until finished. Threads can be used
	# instead of processes by passing `worker_type=transfer_manager.THREAD`.
	# workers=8

	if not os.path.exists(source_filename):
		print(f"File '{source_filename}' does not exist.")
		return

	storage_client = Client()
	bucket = storage_client.bucket(bucket_name)
	blob = bucket.blob(destination_blob_name)

	transfer_manager.upload_chunks_concurrently(
			source_filename, blob, chunk_size=chunk_size, max_workers=workers
	)

	print(f"File {source_filename} uploaded to {destination_blob_name}.")


if __name__=="__main__":
	parser = argparse.ArgumentParser(description='Load local file to a GCP Storage bucket')

	parser.add_argument('--bucket_name', required=True, help="ID of your GCS bucket")
	parser.add_argument('--file_name', required=True, help="path to your file to upload")
	parser.add_argument('--object_name', required=False, help="ID of your GCS object")
	parser.add_argument('--chunk_size', required=False, default=32 * 1024 * 1024, help="size of each chunk (max: 5, min: 100)")
	parser.add_argument('--workers', required=False, default=8, help="maximum number of processes to use for the operation")

	args = parser.parse_args()

	bucket_name = args.bucket_name
	file_name = args.file_name
	object_name = args.object_name
	chunk_size = (int(args.chunk_size) * 1024) * 1024
	workers = args.workers

	upload_chunks_concurrently(
		bucket_name=bucket_name,
		source_filename=file_name,
		destination_blob_name=object_name,
		chunk_size=chunk_size,
		workers=workers
	)