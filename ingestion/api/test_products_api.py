import requests
import boto3
import json
from datetime import datetime, timezone
import uuid

BASE_URL = "https://dummyjson.com/products"


def fetch_products(limit: int, skip: int):
    response = requests.get(
        BASE_URL,
        params={"limit": limit, "skip": skip},
        timeout=10
    )
    response.raise_for_status()
    return response.json()


def upload_batch_to_minio(products: list):
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    bucket_name = "ecom-bronze"

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    time_part = now.strftime("%H%M%S")
    unique_id = str(uuid.uuid4())

    required_fields = ["id", "title", "price"]

    valid_products = []
    for product in products:
        if all(field in product for field in required_fields):
            product["_metadata"] = {
                "ingested_at": now.isoformat(),
                "source": "dummyjson_api"
            }
            valid_products.append(product)
        else:
            print(f"Skipping invalid record: {product.get('id')}")

    if not valid_products:
        print("No valid products in this batch. Skipping upload.")
        return

    body = "\n".join(json.dumps(p) for p in valid_products)

    file_name = f"products_raw_{time_part}_{unique_id}.json"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"products/ingestion_date={date_partition}/{file_name}",
        Body=body,
        ContentType="application/json",
    )

    print(f"Uploaded batch → {file_name}")


if __name__ == "__main__":
    limit = 50
    skip = 0

    first_batch = fetch_products(limit, skip)
    total = first_batch["total"]

    upload_batch_to_minio(first_batch["products"])
    skip += limit

    while skip < total:
        batch = fetch_products(limit, skip)
        upload_batch_to_minio(batch["products"])
        skip += limit

    print("Bronze ingestion completed.")