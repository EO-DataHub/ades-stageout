import json
import os
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from stageout.harvester import StageoutHarvester


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "AWS_REGION": "eu-west-2",
            "AWS_S3_ENDPOINT": "mock_endpoint",
            "WORKSPACE_DOMAIN": "test.workspace.domain.com",
            "AWS_ACCESS_KEY_ID": "mock_access_key",
            "AWS_SECRET_ACCESS_KEY": "mock_secret_key",
            "AWS_SESSION_TOKEN": "valid-token",
            "WORKSPACE": "test_workspace",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


@mock_aws
def test_harvest_files(setenvvar):
    s3_client = boto3.client("s3", region_name="eu-west-2")
    bucket = "test-bucket"
    s3_client.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )
    s3_client.put_object(
        Bucket=bucket, Key="workspace/catalog.json", Body=json.dumps({"type": "Catalog"})
    )
    s3_client.put_object(
        Bucket=bucket,
        Key="workspace/catalog/collection.json",
        Body=json.dumps({"type": "Collection"}),
    )
    s3_client.put_object(
        Bucket=bucket,
        Key="workspace/catalog/collection/item.json",
        Body=json.dumps({"type": "Feature"}),
    )

    harvester = StageoutHarvester(
        s3_client=s3_client,
        bucket=bucket,
        pulsar_url="pulsar://localhost:6650",
        output_root="output",
        job_id="job-id",
        job_url="https://test-url/catalogs/user/catalogs/workspace/jobs/job-id",
    )

    output_keys = [
        "workspace/catalog.json",
        "workspace/catalog/collection.json",
        "workspace/catalog/collection/item.json",
    ]
    harvested_keys = harvester.harvest_files(output_keys=output_keys)

    assert len(harvested_keys) == 3
    assert "workflow-harvester/job-id/workspace/catalogs/catalog.json" in harvested_keys
    assert (
        "workflow-harvester/job-id/workspace/catalogs/catalog/collections/collection.json"
        in harvested_keys
    )
    assert (
        "workflow-harvester/job-id/workspace/catalogs/catalog/collections/collection/items/item.json"
        in harvested_keys
    )

    assert s3_client.get_object(
        Bucket=bucket, Key="workflow-harvester/job-id/workspace/catalogs/catalog.json"
    )
    assert s3_client.get_object(
        Bucket=bucket,
        Key="workflow-harvester/job-id/workspace/catalogs/catalog/collections/collection.json",
    )
    assert s3_client.get_object(
        Bucket=bucket,
        Key="workflow-harvester/job-id/workspace/catalogs/catalog/collections/collection/items/item.json",
    )


@mock_aws
def test_harvest_files_unknown_type(setenvvar):
    s3_client = boto3.client("s3", region_name="eu-west-2")
    bucket = "test-bucket"
    s3_client.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )
    s3_client.put_object(
        Bucket=bucket, Key="test/unknown.json", Body=json.dumps({"type": "Unknown"})
    )

    harvester = StageoutHarvester(
        s3_client=s3_client,
        bucket=bucket,
        pulsar_url="pulsar://localhost:6650",
        output_root="output",
        job_id="job-id",
        job_url="https://test-url/catalogs/user/catalogs/workspace/jobs/job-id",
    )

    output_keys = ["test/unknown.json"]
    with pytest.raises(ValueError):
        harvester.harvest_files(output_keys=output_keys)


@mock.patch("pulsar.Client")
def test_send_to_ingester(mock_pulsar_client, setenvvar):
    mock_producer = mock.Mock()
    mock_pulsar_client.return_value.create_producer.return_value = mock_producer

    s3_client = boto3.client("s3", region_name="eu-west-2")
    harvester = StageoutHarvester(
        s3_client=s3_client,
        bucket="test-bucket",
        pulsar_url="pulsar://localhost:6650",
        output_root="output",
        job_id="job-id",
        job_url="https://test-url/catalogs/user/catalogs/workspace/jobs/job-id",
    )

    harvest_list = ["workflow-harvester/job-id/test_workspace/catalogs/catalog.json"]

    expected_message = {
        "id": "job-id",
        "source-uri": "https://test-url/catalogs/user/catalogs/workspace/jobs/job-id",
        "workspace": "test_workspace",
        "bucket_name": "test-bucket",
        "updated_keys": [],
        "added_keys": harvest_list,
        "deleted_keys": [],
        "source": "job-id/",
        "target": "catalogs/output/catalogs/",
    }

    harvester.send_to_ingester(
        harvest_list=harvest_list,
        workspace="test_workspace",
        topic="test_topic",
        name="test_name",
        job_id="job-id",
    )

    mock_producer.send.assert_called_once()
    sent_message = json.loads(mock_producer.send.call_args[0][0].decode("utf-8"))

    assert sent_message == expected_message
