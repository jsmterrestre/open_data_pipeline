from minio import Minio
import pandas as pd
import os
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

class MinioClient:
    def __init__(self):
        self.client = Minio(
            endpoint=f"{os.getenv('MINIO_ENDPOINT', 'localhost')}:{os.getenv('MINIO_PORT', '9000')}",
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False
        )
        self.bucket_name = os.getenv('MINIO_BUCKET', 'datapipeline')
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if it doesn't."""
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def save_dataframe(self, df: pd.DataFrame, filename: str):
        """Save a pandas DataFrame to MinIO as a Parquet file."""
        # Convert DataFrame to Parquet
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Upload to MinIO
        object_name = f"data/{filename}.parquet"
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )

    def load_dataframe(self, filename: str) -> pd.DataFrame:
        """Load a Parquet file from MinIO into a pandas DataFrame."""
        object_name = f"data/{filename}.parquet"
        
        try:
            # Get the object from MinIO
            response = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            
            # Read the Parquet file
            buffer = BytesIO(response.read())
            table = pq.read_table(buffer)
            return table.to_pandas()
            
        except Exception as e:
            raise Exception(f"Error loading file from MinIO: {str(e)}")

    def list_files(self):
        """List all files in the bucket."""
        objects = self.client.list_objects(self.bucket_name, prefix="data/")
        return [obj.object_name for obj in objects] 