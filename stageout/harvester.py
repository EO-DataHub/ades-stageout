import json
import logging
import sys

import botocore.client
import pulsar

logger = logging.getLogger(__name__)


class StageoutHarvester:
    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket: str,
        pulsar_url: str | None,
        output_root: str,
        job_id: str,
        job_url: str,
        prefix: str = "workflow-harvester",
    ) -> None:
        self.s3_client = s3_client
        self.bucket = bucket
        self.pulsar_url = pulsar_url
        self.output_root = output_root
        self.job_id = job_id
        self.job_url = job_url
        self.prefix = prefix

    def harvest_files(self, output_keys: list[str] | None = None, output_files: list[str] | None = None) -> list[str]:
        if output_keys is None:
            return []
        print("Now harvesting files", file=sys.stderr)
        harvested_keys = []
        for file_key in output_keys:
            # Load file from s3
            print(f"Harvesting file {file_key}", file=sys.stderr)
            response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
            file_content = response["Body"].read().decode("utf-8")
            file_dict = json.loads(file_content)
            # Configure new file key
            if file_dict["type"] == "Catalog":
                new_key = f"{self.prefix}/{self.job_id}/{file_key.replace('/', '/catalogs/')}"
            elif file_dict["type"] == "Collection" or file_key.endswith("-annotations.json"):
                file_path, file_name = file_key.rsplit("/", 1)
                new_key = f"{self.prefix}/{self.job_id}/{file_path.replace('/', '/catalogs/')}/collections/{file_name}"
            elif file_dict["type"] == "Feature":
                file_path, collection_name, file_name = file_key.rsplit("/", 2)
                new_key = (
                    f"{self.prefix}/{self.job_id}/{file_path.replace('/', '/catalogs/')}"
                    f"/collections/{collection_name}/items/{file_name}"
                )
            else:
                raise ValueError(f"Unknown file type {file_dict['type']}")

            # Upload file to s3
            print(f"Uploading file to {new_key}", file=sys.stderr)
            try:
                self.s3_client.put_object(Bucket=self.bucket, Key=new_key, Body=json.dumps(file_dict))
                harvested_keys.append(new_key)
                print(f"File {file_key} harvested successfully", file=sys.stderr)
            except Exception as e:
                print(f"Error while uploading file {new_key}: {e}", file=sys.stderr)
        return harvested_keys

    def send_to_ingester(
        self,
        harvest_list: list[str],
        workspace: str,
        topic: str,
        name: str,
        job_id: str,
    ) -> None:
        pulsar_client = pulsar.Client(self.pulsar_url)
        producer = pulsar_client.create_producer(topic=topic, producer_name=f"{name}-{job_id}")
        message_dict = {
            "id": job_id,
            "workspace": workspace,
            "source-uri": self.job_url,
            "bucket_name": self.bucket,
            "updated_keys": [],
            "added_keys": harvest_list,
            "deleted_keys": [],
            "source": f"{self.job_id}/",
            "target": f"catalogs/{self.output_root}/catalogs/",
        }
        try:
            # send message
            msg = json.dumps(message_dict)
            print("Sending message via pulsar ...", file=sys.stderr)
            print(msg, file=sys.stderr)
            producer.send(msg.encode("utf-8"))
        finally:
            producer.close()
            pulsar_client.close()
