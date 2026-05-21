import boto3 
import os 
from dotenv import load_dotenv
from pathlib import Path
import logging as log 

logger = log.getLogger(__name__)
load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name = os.getenv("REGION_NAME")
)

bucket = os.getenv("BUCKET")

def s3_upload(local_path: Path, s3_path: str) -> None:
    
    try:
        s3.upload_file(str(local_path), bucket, s3_path)
        logger.info(f"Upload realizado: {s3_path}")
        
    except Exception as e:
        logger.error(f"Erro ao carregar:{s3_path}: {e}")
        raise 