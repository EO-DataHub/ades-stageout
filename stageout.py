import json
import os
import shutil
import sys
from datetime import datetime
from typing import List
from urllib.parse import urlparse

import boto3
import botocore
import pulsar
import pystac
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pystac.stac_io import DefaultStacIO, StacIO


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self, s3_endpoint=None, workspace=None, workspace_domain=None):
        self.session = botocore.session.Session()
        self.s3_endpoint = s3_endpoint
        self.workspace = workspace
        self.workspace_domain = workspace_domain
        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            self.s3_client = self.session.create_client(
                service_name="s3",
                use_ssl=True,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                endpoint_url=os.environ["AWS_S3_ENDPOINT"],
                region_name=os.environ["AWS_REGION"],
            )
        else:
            self.s3_client = self.session.create_client("s3")

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        print("parsed scheme", parsed.scheme, file=sys.stderr)
        if parsed.scheme == "s3":
            print(txt, file=sys.stderr)
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=self.s3_endpoint,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        elif parsed.scheme == "https":
            # Construct expected workspace domain path
            expected_workspace_url = f"{self.workspace}.{self.workspace_domain}"
            if parsed.netloc == expected_workspace_url:
                key_suffix = parsed.path.split("/")[
                    3:
                ]  # split by "/" and take path after workspace domain
                key_suffix = "/".join(key_suffix)
                key = f"{self.workspace}/{key_suffix}"
                print("New key is ", key, file=sys.stderr)
                self.s3_client.put_object(
                    Body=txt.encode("UTF-8"),
                    Bucket=self.s3_endpoint,
                    Key=key,
                    ContentType="application/geo+json",
                )
        else:
            super().write_text(dest, txt, *args, **kwargs)


def get_aws_client(endpoint_url: str, region_name: str) -> boto3.client:
    # Configure AWS client
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=endpoint_url,
            region_name=region_name,
        )
    else:
        s3_client = boto3.client("s3")
    return s3_client


def upload_directory(s3_client: boto3.client, local_path: str, bucket: str, s3_path: str):
    base_directory = os.path.basename(local_path)
    for root, _, files in os.walk(local_path):
        # Get path after the base directory
        base_path = root.split(base_directory, 1)[1]
        # If base_path is non-empty need to remove preceeding "/"
        base_path = base_path[1:] if base_path else base_path
        for file in files:
            upload_path_base = os.path.join(s3_path, base_path)
            upload_path = os.path.join(upload_path_base, file)
            s3_client.upload_file(os.path.join(root, file), bucket, upload_path)


def get_current_namespace():
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
            return f.read().strip()
    except IOError:
        # Return default namespace
        return "default"


def get_bucket_from_namespace() -> str:
    # Load the kubernetes config
    config.load_incluster_config()

    # Create a Kubernetes API client
    v1 = client.CoreV1Api()
    try:
        # Read the ConfigMap
        configmap = v1.read_namespaced_config_map(
            name="workspace-config", namespace=get_current_namespace()
        )
        bucket = configmap.data.get("S3_BUCKET_WORKSPACE")
        return bucket
    except ApiException as e:
        print(f"Exception when fetching workspace bucket: {e}", file=sys.stderr)


def send_to_ingester(harvest_list: List[str], bucket: str, workspace: str):
    pulsar_url = os.getenv("PULSAR_URL", "")
    pulsar_client = pulsar.Client(pulsar_url)
    pulsar_topic = "harvested"
    producer = pulsar_client.create_producer(topic=pulsar_topic, producer_name="ades-outputs")
    message_dict = {
        "id": f"ades-outputs/{workspace}",
        "workspace": workspace,
        "repository": "",
        "branch": "main",
        "bucket_name": bucket,
        "updated_keys": [],
        "added_keys": harvest_list,
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
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


def construct_collection_and_upload_assets(
    cat: pystac.Catalog,
    collection: pystac.Collection,
    cat_id: str,
    collection_id: str,
    s3_client: boto3.client,
    s3_data: dict,
) -> pystac.Collection:

    s3_endpoint = s3_data["s3_endpoint"]
    subfolder = s3_data["subfolder"]
    workspace = s3_data["workspace"]
    workspace_domain = s3_data["workspace_domain"]
    bucket = s3_data["bucket"]

    for item in cat.get_items():

        item.set_collection(collection)

        collection.add_item(item)

        for key, asset in item.get_assets().items():
            s3_path = os.path.normpath(
                os.path.join(subfolder, cat_id, collection_id, os.path.basename(asset.href))
            )
            print(
                f"upload {asset.get_absolute_href()} to {s3_endpoint} at {s3_path}",
                file=sys.stderr,
            )
            try:
                s3_client.upload_file(
                    asset.get_absolute_href(),
                    s3_endpoint,
                    s3_path,
                )
            except IsADirectoryError:
                upload_directory(s3_client, asset.get_absolute_href(), s3_endpoint, s3_path)
            except Exception as e:
                print(
                    f"upload {item.id} to {s3_endpoint} at {subfolder} failed {e}",
                    file=sys.stderr,
                )

            # Remove workspace name from s3_path
            prefix = f"{workspace}/"
            if s3_path.startswith(prefix):
                s3_path = s3_path[len(prefix) :]
            # Replace href with workspace-accessible url
            asset.href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{s3_path}"
            print(f"replacing item href with {asset.href}", file=sys.stderr)
            item.add_asset(key, asset)

    return collection


def upload_items(
    collection: pystac.Collection,
    cat_id: str,
    collection_id: str,
    bucket: str,
    subfolder: str,
) -> List[str]:
    files_for_harvesting = []
    for item in collection.get_items():
        # upload item to S3
        s3_path = "s3://" + os.path.normpath(
            os.path.join(
                bucket,
                subfolder,
                cat_id,
                collection_id,
                os.path.basename(item.get_self_href()),
            )
        )
        pystac.write_file(item, dest_href=s3_path)
        print(f"upload {item.id} to {s3_path}", file=sys.stderr)
        files_for_harvesting.append(s3_path.replace(f"s3://{bucket}/", "", 1))

    return files_for_harvesting


def main(
    collection_id,
    cat_url,
    bucket,
    subfolder,
):

    region_name = os.environ["AWS_REGION"]
    endpoint_url = os.environ["AWS_S3_ENDPOINT"]
    s3_endpoint = bucket
    workspace_domain = os.environ["WORKSPACE_DOMAIN"]

    shutil.copytree(cat_url, "/tmp/catalog")
    cat = pystac.read_file(os.path.join("/tmp/catalog", "catalog.json"))

    # Create new catalog id
    cat.id = f"cat_{collection_id}"
    cat_id = cat.id

    # Create new collection id
    collection_id = f"col_{collection_id}"

    s3_client = get_aws_client(endpoint_url, region_name)

    workspace = os.environ["WORKSPACE"]

    # Identify access point
    access_point = get_bucket_from_namespace()
    if access_point:
        subfolder = os.path.join(workspace, subfolder)
        s3_endpoint = access_point
        print(f"Found access point {access_point} in workspace {workspace}", file=sys.stderr)

    def custom_stac_io_factory(s3_endpoint, workspace=workspace, workspace_domain=workspace_domain):
        def create_custom_stac_io():
            return CustomStacIO(
                s3_endpoint=s3_endpoint, workspace=workspace, workspace_domain=workspace_domain
            )

        return create_custom_stac_io

    StacIO.set_default(custom_stac_io_factory(s3_endpoint, workspace, workspace_domain))

    # create a STAC collection for the process
    date = datetime.now().strftime("%Y-%m-%d")

    dates = [
        datetime.strptime(f"{date}T00:00:00", "%Y-%m-%dT%H:%M:%S"),
        datetime.strptime(f"{date}T23:59:59", "%Y-%m-%dT%H:%M:%S"),
    ]

    subfolder_no_workspace = subfolder.replace(f"{workspace}/", "", 1)

    collection = pystac.Collection(
        id=collection_id,
        description="description",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent(intervals=[[min(dates), max(dates)]]),
        ),
        title="Result Collection",
        href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder_no_workspace}/{cat_id}/collection.json",
        stac_extensions=[],
        keywords=["eoepca"],
        license="proprietary",
    )

    for index, link in enumerate(cat.links):
        if link.rel == "root":
            cat.links.pop(index)  # remove root link

    s3_data = {
        "s3_endpoint": s3_endpoint,
        "subfolder": subfolder,
        "workspace": workspace,
        "workspace_domain": workspace_domain,
        "bucket": bucket,
    }

    collection = construct_collection_and_upload_assets(
        cat, collection, cat_id, collection_id, s3_client, s3_data
    )

    collection.update_extent_from_items()

    cat.clear_items()

    cat.add_child(collection)
    print("catalog links")
    print(cat.links, file=sys.stderr)

    # Set hrefs after normalization
    cat.normalize_hrefs(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder_no_workspace}"
    )
    cat_href = cat.get_self_href().replace("/catalog.json", f"/{cat_id}.json")
    cat.set_self_href(cat_href)

    for item in collection.get_items():
        item_href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder_no_workspace}/{cat_id}/{collection_id}/{item.id}.json"
        item.set_self_href(item_href)

    # upload collection to S3
    collection_href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder_no_workspace}/{cat_id}/collection.json"
    collection.set_self_href(collection_href)
    print(f"upload collection.json to {collection_href}", file=sys.stderr)
    pystac.write_file(collection, dest_href=collection_href)

    # upload catalog to S3
    print(f"upload catalog.json to {cat_href}", file=sys.stderr)
    pystac.write_file(cat, dest_href=cat_href)

    files_for_harvesting = upload_items(collection, cat_id, collection_id, bucket, subfolder)
    files_for_harvesting.append(f"{subfolder}/{cat_id}.json")
    files_for_harvesting.append(f"{subfolder}/{cat_id}/collection.json")
    print("Files for harvesting", files_for_harvesting, file=sys.stderr)

    send_to_ingester(files_for_harvesting, bucket, workspace)

    print(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}.json",
        file=sys.stdout,
    )


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: python stageout.py <catalog_url> <bucket> <subfolder> <collection_id>",
            file=sys.stderr,
        )
        sys.exit(1)
    cat_url = sys.argv[1]
    bucket = sys.argv[2]
    subfolder = sys.argv[3]
    collection_id = sys.argv[4]

    main(collection_id, cat_url, bucket, subfolder)
