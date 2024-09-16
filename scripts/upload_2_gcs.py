from pathlib import Path
import tarfile
from google.cloud import storage
import argparse

proj_id = 'royallepage.ca:api-project-267497502775'
bucket_name = 'ai-tests'
destination_blob_name = 'tmp/analytics_script.tar.gz'
archive_name = 'analytics_script.tar.gz'

# Create a tar.gz file including only .log, .csv, .json files, excluding the script itself
def create_tar_gz(script_dir, exclude_files=['upload_2_gcs.py']):
    archive_path = Path(script_dir) / archive_name
    script_path = Path(script_dir)
    
    # First create the tar
    with tarfile.open(archive_path.with_suffix(''), "w") as tar:
        for path in script_path.glob('*'):
            # Include only .log, .csv, .json files, and exclude specific files
            if path.suffix in ['.log', '.csv', '.json'] and path.name not in exclude_files:
                tar.add(path, arcname=path.name)
    
    # Compress the tar file using gzip
    with open(archive_path.with_suffix(''), 'rb') as f_in:
        with open(archive_path, 'wb') as f_out:
            f_out.write(f_in.read())
    
    # Remove the uncompressed .tar file
    archive_path.with_suffix('').unlink()
    
    print(f"Archive {archive_name} created at {archive_path}.")

# Upload the tar.gz file to Google Cloud Storage
def upload_to_gcs(source_file_path):
    # Create a GCS client
    storage_client = storage.Client(project=proj_id)

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob and upload the file
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

    print(f"File {source_file_path} uploaded to {destination_blob_name} in bucket {bucket_name}.")

if __name__ == "__main__":
    # Setup argument parsing
    parser = argparse.ArgumentParser(description="Create a tar.gz archive and upload it to GCS.")
    parser.add_argument('--script_dir', type=str, required=True, help="The directory where the script files are located.")
    
    args = parser.parse_args()

    # Step 1: Create tar.gz archive in the script directory
    create_tar_gz(args.script_dir)

    # Step 2: Upload to GCS
    archive_path = Path(args.script_dir) / archive_name 
    upload_to_gcs(archive_path)

    # Step 3: Delete the tar.gz file locally after upload
    if archive_path.exists():
        archive_path.unlink()  # Delete the file
        print(f"File {archive_name} deleted from {args.script_dir}.")

