import json
import logging
import os
import shutil
import sys
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse

import boto3
import botocore
import pystac
import requests
from botocore.exceptions import ClientError
from html_sanitizer import Sanitizer
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pystac.stac_io import DefaultStacIO, StacIO

from .harvester import StageoutHarvester

logger = logging.getLogger(__name__)

OUTPUT_ROOT = os.getenv("OUTPUT_ROOT", "user")


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self, s3_endpoint=None, workspace=None, workspace_domain=None):
        self.session = botocore.session.Session()
        self.s3_endpoint = s3_endpoint
        self.workspace = workspace
        self.workspace_domain = workspace_domain
        if (
            os.getenv("AWS_ACCESS_KEY_ID")
            and os.getenv("AWS_SECRET_ACCESS_KEY")
            and os.getenv("AWS_SESSION_TOKEN")
        ):
            self.s3_client = self.session.create_client(
                service_name="s3",
                use_ssl=True,
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                endpoint_url=os.environ["AWS_S3_ENDPOINT"],
                region_name=os.environ["AWS_REGION"],
                aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
            )
        else:
            self.s3_client = self.session.create_client("s3")

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        print(f"Writing to {dest}", file=sys.stderr)
        # Construct expected workspace domain path
        expected_workspace_url = f"{self.workspace}.{self.workspace_domain}"
        if parsed.scheme == "s3":
            # Output location is an S3 bucket - write directly to S3
            key = parsed.path[1:]
            print(f"Uploading to {key} at {self.s3_endpoint}", file=sys.stderr)
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=self.s3_endpoint,
                Key=key,
                ContentType="application/geo+json",
            )
        elif parsed.scheme == "https" and parsed.netloc == expected_workspace_url:
            # Output location is a link to workspace store - write directly to S3
            # Path is expected to be in the form of https://workspace.domain/files/bucket/key
            key_suffix = parsed.path.split("/")[3:]
            key_suffix = "/".join(key_suffix)
            key = f"{self.workspace}/{key_suffix}"
            print(f"Uploading to {key} at {self.s3_endpoint}", file=sys.stderr)
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=self.s3_endpoint,
                Key=key,
                ContentType="application/geo+json",
            )
        else:
            # Output location is not within the workspace - use default write_text
            super().write_text(dest, txt, *args, **kwargs)


def get_aws_client() -> boto3.client:
    # Configure AWS client
    if (
        os.getenv("AWS_ACCESS_KEY_ID")
        and os.getenv("AWS_SECRET_ACCESS_KEY")
        and os.getenv("AWS_SESSION_TOKEN")
    ):
        s3_client = boto3.client(
            service_name="s3",
            use_ssl=True,
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            endpoint_url=os.environ["AWS_S3_ENDPOINT"],
            region_name=os.environ["AWS_REGION"],
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
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


def create_workflow_catalog(
    workflow_id: str,
    subfolder: str,
    workspace: str,
    workspace_domain: str,
    bucket: str,
    child_cat: pystac.Catalog,
) -> Optional[str]:
    # If workflow results catalog doesn't yet exist, create the catalog and upload to S3
    print(workflow_id)
    catalog = pystac.Catalog(
        id=workflow_id,
        description=f"Catalog for storing data generated by workflow {workflow_id}",
        title=f"{workflow_id} Outputs Catalog",
        stac_extensions=[],
    )
    print("here")
    catalog.add_child(child_cat)
    href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}.json"
    catalog.set_self_href(href)
    print(f"Upload new Catalog {workflow_id} to {href}", file=sys.stderr)
    pystac.write_file(catalog, dest_href=href)
    return f"{workspace}/{subfolder}.json"


def handle_assets(
    item: pystac.Item,
    collection: pystac.Collection,
    cat_id: str,
    s3_client: boto3.client,
    s3_endpoint: str,
    subfolder: str,
    workspace: str,
    workspace_domain: str,
    bucket: str,
):
    for key, asset in item.get_assets().items():
        s3_path = os.path.normpath(
            os.path.join(workspace, subfolder, cat_id, collection.id, os.path.basename(asset.href))
        )
        print(
            f"Upload asset {asset.get_absolute_href()} to {s3_endpoint} at {s3_path}",
            file=sys.stderr,
        )
        try:
            s3_client.upload_file(
                asset.get_absolute_href(),
                s3_endpoint,
                s3_path,
            )
            rewrite_asset_href = True
        except IsADirectoryError:
            upload_directory(s3_client, asset.get_absolute_href(), s3_endpoint, s3_path)
            rewrite_asset_href = True
        except Exception as e:
            print(
                f"Failed to upload asset {asset.get_absolute_href()} to {s3_endpoint} at {subfolder}: {e}",
                file=sys.stderr,
            )
            rewrite_asset_href = False

        # Rewrite asset hrefs
        if rewrite_asset_href:
            # Remove workspace name from s3_path
            prefix = f"{workspace}/"
            if s3_path.startswith(prefix):
                s3_path = s3_path[len(prefix) :]
            # Replace href with workspace-accessible url
            asset.href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{s3_path}"
            print(f"Replacing item href with {asset.href}", file=sys.stderr)
            item.add_asset(key, asset)
    item_href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection.id}/{item.id}.json"
    item.set_self_href(item_href)


def construct_collection_and_upload_assets(
    cat: pystac.Catalog,
    collection: pystac.Collection,
    cat_id: str,
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

        handle_assets(
            item,
            collection,
            cat_id,
            s3_client,
            s3_endpoint,
            subfolder,
            workspace,
            workspace_domain,
            bucket,
        )

    return collection


def upload_items(
    collection: pystac.Collection,
    cat_id: str,
    collection_id: str,
    bucket: str,
    workspace: str,
    subfolder: str,
) -> List[str]:
    files_for_harvesting = []
    for item in collection.get_items():
        # upload item to S3
        s3_path = "s3://" + os.path.normpath(
            os.path.join(
                bucket,
                workspace,
                subfolder,
                cat_id,
                collection_id,
                os.path.basename(item.get_self_href()),
            )
        )
        print(f"Upload {item.id} to {s3_path}", file=sys.stderr)
        pystac.write_file(item, dest_href=s3_path)
        files_for_harvesting.append(s3_path.replace(f"s3://{bucket}/", "", 1))

    return files_for_harvesting


def generate_new_collection(
    collection_id,
    cat,
    s3_client,
    s3_endpoint,
    workspace,
    workspace_domain,
    bucket,
    subfolder,
    cat_id,
    job_id,
    workflow_id,
) -> list[str]:
    # create a STAC collection for the process
    date = datetime.now().strftime("%Y-%m-%d")

    dates = [
        datetime.strptime(f"{date}T00:00:00", "%Y-%m-%dT%H:%M:%S"),
        datetime.strptime(f"{date}T23:59:59", "%Y-%m-%dT%H:%M:%S"),
    ]

    collection = pystac.Collection(
        id=collection_id,
        description="description",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent(intervals=[[min(dates), max(dates)]]),
        ),
        title=f"Result Collection for Job {job_id}",
        href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/collection.json",
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

    collection = construct_collection_and_upload_assets(cat, collection, cat_id, s3_client, s3_data)

    collection.update_extent_from_items()

    cat.clear_items()

    cat.add_child(collection)

    # Upload collection to S3
    collection_href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/collection.json"
    collection.set_self_href(collection_href)
    print(f"Upload collection.json to {collection_href}", file=sys.stderr)
    pystac.write_file(collection, dest_href=collection_href)

    files_for_harvesting = upload_items(
        collection, cat_id, collection_id, bucket, workspace, subfolder
    )
    files_for_harvesting.append(f"{workspace}/{subfolder}/{cat_id}.json")
    files_for_harvesting.append(f"{workspace}/{subfolder}/{cat_id}/collection.json")

    print(
        "Generated new collection and uploaded to S3",
        file=sys.stdout,
    )

    return files_for_harvesting


def upload_annotation(s3_client, s3_endpoint, workspace, subfolder, cat_id, file_name) -> bool:
    try:
        with open(os.path.join("/tmp/outputs", file_name), "r") as file:
            try:
                # Check if file is valid JSON
                _ = json.load(file)
            except json.JSONDecodeError:
                print("Failed to read annotations.json, it is not valid JSON", file=sys.stderr)
                print(file.read(), file=sys.stderr)
                return False
    except FileNotFoundError:
        print(f"Failed to load annotations in {file_name}", file=sys.stderr)
        return False

    # Construct path for annotations in S3
    s3_path = os.path.normpath(
        os.path.join(
            workspace, subfolder, cat_id, file_name
        )  # save annotations at collection level
    )

    # Upload annotations to S3
    print(f"Uploading annotations to {s3_path}", file=sys.stderr)
    try:
        s3_client.upload_file(
            os.path.join("/tmp/outputs", file_name),
            s3_endpoint,
            s3_path,
        )
        print(f"Annotations in {file_name} uploaded successfully", file=sys.stderr)
        return True
    except Exception as e:
        print(f"Failed to upload annotations in {file_name} to {s3_path}: {e}", file=sys.stderr)
        return False


def handle_annotations(
    s3_client, s3_endpoint, workspace, subfolder, catalog_id, job_id
) -> list[str]:
    """
    Handle annotations defined in the annotations.json file
    """

    added_annotations = []
    cat_id = catalog_id
    if not catalog_id:
        cat_id = job_id

    # Identify all annotations files
    for file_name in os.listdir("/tmp/outputs"):
        if file_name.endswith("-annotations.json"):
            if upload_annotation(s3_client, s3_endpoint, workspace, subfolder, cat_id, file_name):
                added_annotations.append(f"{workspace}/{subfolder}/{cat_id}/{file_name}")

    # Send annotations to ingester
    if added_annotations:
        return added_annotations

    return False


def handle_current_collection(
    s3_client,
    s3_endpoint,
    cat,
    collections,
    cat_id,
    workspace_domain,
    bucket,
    workspace,
    subfolder,
    workflow_id,
    job_id,
) -> list[str]:

    files_for_harvesting = []

    # Set hrefs after normalization
    print(
        f"Normalising Catalog hrefs to https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}",
        file=sys.stderr,
    )
    sanitizer = Sanitizer()

    cat.clear_children()

    public_bucket = os.getenv("SPDX_S3_BUCKET")
    hosted_zone = os.getenv("HOSTED_ZONE")

    for collection in collections:
        cat.add_child(collection)

        items = collection.get_items()
        collection.set_self_href(
            f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection.id}.json"
        )

        # Check for license link
        for link in collection.links:
            if link.rel == "license":
                license_href = link.href
                license_filename = os.path.basename(license_href)
                public_path = f"api/catalogue/licences/{workspace}/{license_filename}"
                public_url = f"https://{hosted_zone}/{public_path}"
                # Check if the file already exists in the S3 bucket
                try:
                    s3_client.head_object(Bucket=public_bucket, Key=public_path)
                    # File exists, no need to re-upload
                    new_link = pystac.Link(
                        rel=link.rel,
                        target=public_url,
                        media_type=link.media_type,
                        title=link.title,
                    )
                    collection.links.remove(link)
                    collection.add_link(new_link)
                    print(
                        f"Licence file exists, no need to re-upload {public_url}", file=sys.stderr
                    )
                    continue
                except ClientError:
                    # File does not exist, proceed with upload
                    print(f"File does not exist, proceed with upload {public_url}", file=sys.stderr)
                    pass

                if not license_href.startswith("http"):
                    # It's a relative link, read the file
                    license_path = os.path.join("/tmp/outputs", license_href)
                    with open(license_path, "r") as file:
                        license_content = file.read()
                else:
                    # It's an external URL, download the file
                    # Headers required to mimic a browser request and get around some sites blocking requests
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
                    }
                    response = requests.get(license_href, headers=headers)
                    response.raise_for_status()
                    license_content = response.text

                # Sanitize the license file if it's HTML
                if license_filename.endswith(".html"):
                    license_content = sanitizer.sanitize(license_content)

                # Upload the license file to the public S3 bucket with correct content type
                print(
                    f"Files to be uploaded at {public_url} and on bucket {public_bucket}",
                    file=sys.stderr,
                )
                s3_client.put_object(
                    Bucket=public_bucket,
                    Key=public_path,
                    Body=license_content,
                    ContentType=(
                        "text/html" if license_filename.endswith(".html") else "text/plain"
                    ),
                )
                print(f"License uploaded to {public_path}", file=sys.stderr)
                # Update the license link to point to the new public URL
                new_link = pystac.Link(
                    rel=link.rel, target=public_url, media_type=link.media_type, title=link.title
                )
                collection.links.remove(link)
                collection.add_link(new_link)

        for item in items:
            for index, link in enumerate(item.links):
                if link.rel == "root":
                    item.links.pop(index)  # remove root link

            item.set_collection(collection)

            handle_assets(
                item,
                collection,
                cat_id,
                s3_client,
                s3_endpoint,
                subfolder,
                workspace,
                workspace_domain,
                bucket,
            )

        files_for_harvesting += upload_items(
            collection, cat_id, collection.id, bucket, workspace, subfolder
        )
        # Upload collection to S3
        collection_href = f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection.id}.json"
        print(f"Upload collection {collection.id}.json to {collection_href}", file=sys.stderr)
        pystac.write_file(collection, dest_href=collection_href)
        files_for_harvesting.append(f"{workspace}/{subfolder}/{cat_id}/{collection.id}.json")

    files_for_harvesting.append(f"{workspace}/{subfolder}/{cat_id}.json")

    print("Uploaded existing collections to S3", file=sys.stdout)

    return files_for_harvesting


def handle_stac_outputs(
    s3_client,
    s3_endpoint,
    workspace_domain,
    workspace,
    bucket,
    subfolder,
    job_id,
    workflow_id,
) -> tuple:

    try:
        cat = pystac.read_file(os.path.join("/tmp/outputs", "catalog.json"))
    except FileNotFoundError:
        print("Failed find STAC Catalog at /tmp/catalog", file=sys.stderr)
        return "", False
    except pystac.STACTypeError:
        print("Failed to read STAC Catalog", file=sys.stderr)
        return "", False
    except json.JSONDecodeError as e:
        print(f"Failed to read STAC Catalog: {e}", file=sys.stderr)
        return "", False

    # Create new catalog id
    cat_id = cat.id

    if not cat_id:
        cat_id = f"cat_{job_id}"

    # Create new collection id
    collection_id = "collection"

    # Only need to generate collection if one is not already present in the generated catalog
    collections = list(cat.get_all_collections())

    cat.normalize_hrefs(f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}")

    cat_href = cat.get_self_href().replace("/catalog.json", f"/{cat_id}.json", 1)
    cat.set_self_href(cat_href)

    for index, link in enumerate(cat.links):
        if link.rel == "root":
            cat.links.pop(index)  # remove root link

    if not collections:
        print("No Collections found, generating new collection", file=sys.stderr)
        output_keys = generate_new_collection(
            collection_id,
            cat,
            s3_client,
            s3_endpoint,
            workspace,
            workspace_domain,
            bucket,
            subfolder,
            cat_id,
            job_id,
            workflow_id,
        )
    else:
        # Use collections already present in the catalog
        print("Collections found, using existing collections", file=sys.stderr)
        output_keys = handle_current_collection(
            s3_client,
            s3_endpoint,
            cat,
            collections,
            cat_id,
            workspace_domain,
            bucket,
            workspace,
            subfolder,
            workflow_id,
            job_id,
        )
    # Upload catalog to S3
    print(f"Upload catalog.json to {cat_href}", file=sys.stderr)
    pystac.write_file(cat, dest_href=cat_href)

    file_key = create_workflow_catalog(
        workflow_id, subfolder, workspace, workspace_domain, bucket, cat
    )
    if file_key:
        output_keys.append(file_key)

    return cat_id, output_keys


def main(cat_url: str, bucket: str, subfolder: str, job_id: str, workflow_id: str, job_url: str):

    s3_endpoint = bucket
    workspace_domain = os.environ["WORKSPACE_DOMAIN"]
    workspace = os.environ["WORKSPACE"]

    try:
        shutil.copytree(cat_url, "/tmp/outputs")
    except FileNotFoundError:
        print("Failed find STAC Catalog at /tmp/catalog", file=sys.stderr)
        raise

    # Create S3 client
    s3_client = get_aws_client()

    # Create harvester
    stageout_harvester = StageoutHarvester(
        s3_client, bucket, os.getenv("PULSAR_URL"), OUTPUT_ROOT, job_id, job_url
    )

    # Set custom STAC IO
    def custom_stac_io_factory(s3_endpoint, workspace=workspace, workspace_domain=workspace_domain):
        def create_custom_stac_io():
            return CustomStacIO(
                s3_endpoint=s3_endpoint,
                workspace=workspace,
                workspace_domain=workspace_domain,
            )

        return create_custom_stac_io

    StacIO.set_default(custom_stac_io_factory(s3_endpoint, workspace, workspace_domain))

    cat_id, output_keys = handle_stac_outputs(
        s3_client, s3_endpoint, workspace_domain, workspace, bucket, subfolder, job_id, workflow_id
    )

    output_annotations = handle_annotations(
        s3_client, s3_endpoint, workspace, subfolder, cat_id, job_id
    )

    if not output_keys and not output_annotations:
        print("No outputs generated, stageout failed", file=sys.stderr)
        raise

    # Harvest STAC Outputs
    if output_keys:
        print("STAC outputs found, sending to ingester", file=sys.stdout)
        harvested_keys = stageout_harvester.harvest_files(output_keys)

        # Ingest Outputs
        print(harvested_keys, file=sys.stderr)
        stageout_harvester.send_to_ingester(
            harvested_keys, workspace, "harvested", "ades-outputs-stac", job_id
        )

    # Harvest Annotations
    if output_annotations:
        print("Annotations found, sending to ingester", file=sys.stdout)
        harvested_keys = stageout_harvester.harvest_files(output_annotations)

        # Harvest Annotations
        stageout_harvester.send_to_ingester(
            harvested_keys,
            workspace,
            "harvested-annotations",
            "ades-outputs-annotations",
            job_id,
        )


if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(
            "Usage: python stageout.py <catalog_url> <bucket> <subfolder> ",
            "<job_id> <workflow_id> <owning_workspace>",
            f"you provided {len(sys.argv)} arguments",
            file=sys.stderr,
        )
        sys.exit(1)
    cat_url = sys.argv[1]
    bucket = sys.argv[2]
    subfolder = sys.argv[3]
    job_id = sys.argv[4]
    workflow_id = sys.argv[5]
    job_url = sys.argv[6]

    print(
        f"Running stageout with {cat_url} {bucket} {subfolder} {job_id} {workflow_id} {job_url}",
        file=sys.stderr,
    )

    main(cat_url, bucket, subfolder, job_id, workflow_id, job_url)
