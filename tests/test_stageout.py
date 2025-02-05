import json
import os
from unittest import mock
from unittest.mock import mock_open, patch

import pystac
import pytest
from kubernetes.client.rest import ApiException

from stageout import (
    CustomStacIO,
    construct_collection_and_upload_assets,
    get_aws_client,
    get_bucket_from_namespace,
    get_current_namespace,
    handle_annotations,
    handle_stac_outputs,
    main,
    send_to_ingester,
    upload_annotation,
    upload_directory,
    upload_items,
)


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "AWS_REGION": "mock_region",
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


@patch("botocore.session.Session")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_init_env_var(mock_botocore_session_class, setenvvar):
    """Test the CustomStacIO class given provided environment variables for AWS credentials"""

    # Create mocks
    mock_botocore_session = mock.Mock()
    mock_botocore_session_client = mock.Mock()
    mock_botocore_session.create_client.return_value = mock_botocore_session_client
    mock_botocore_session_class.return_value = mock_botocore_session

    # Configure expected arguments
    expected_args = {
        "service_name": "s3",
        "use_ssl": True,
        "aws_access_key_id": "mock_access_key",
        "aws_secret_access_key": "mock_secret_key",
        "endpoint_url": "mock_endpoint",
        "region_name": "mock_region",
        "aws_session_token": "valid-token",
    }

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO()

    # Assertions
    assert mock_botocore_session.create_client.call_count == 1
    mock_botocore_session.create_client.assert_called_once_with(**expected_args)

    # Check resulting s3_client
    assert test_stac_io.s3_client == mock_botocore_session_client


@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
@patch("botocore.session.Session")
def test_custom_stac_init_service_account(mock_botocore_session_class, setenvvar):
    """Test the CustomStacIO class without provided environment variables for AWS credentials,
    instead using current service account"""

    # Create mocks
    mock_botocore_session = mock.Mock()
    mock_botocore_session_client = mock.Mock()
    mock_botocore_session.create_client.return_value = mock_botocore_session_client
    mock_botocore_session_class.return_value = mock_botocore_session

    # Configure expected arguments
    expected_args = {
        "service_name": "s3",
        "use_ssl": True,
        "aws_access_key_id": "mock_access_key",
        "aws_secret_access_key": "mock_secret_key",
        "endpoint_url": "mock_endpoint",
        "region_name": "mock_region",
        "aws_session_token": "valid-token",
    }

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO()

    # Assertions
    assert mock_botocore_session.create_client.call_count == 1
    mock_botocore_session.create_client.assert_called_once_with(**expected_args)

    # Check resulting s3_client
    assert test_stac_io.s3_client == mock_botocore_session_client


@patch("pystac.stac_io.DefaultStacIO")
@patch("botocore.session.Session")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_write_text_s3(mock_botocore_session_class, mock_default_stacio, setenvvar):

    # Create mocks
    mock_botocore_session = mock.Mock()
    mock_botocore_session_client = mock.Mock()
    mock_botocore_session_client.put_object.return_value = None
    mock_botocore_session.create_client.return_value = mock_botocore_session_client
    mock_botocore_session_class.return_value = mock_botocore_session
    mock_default_stacio.write_text.return_value = None

    # Configure expected arguments
    expected_args = {
        "Body": "test_text".encode("UTF-8"),
        "Bucket": "example/endpoint/for/test",
        "Key": "path",
        "ContentType": "application/geo+json",
    }

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO(s3_endpoint="example/endpoint/for/test")

    # Test write_text
    test_stac_io.write_text(
        "s3://test_dest/path", "test_text", "args_1", "arg_2", kwarg_1="kwarg_1", kwarg_2="kwarg_2"
    )

    # Assertions
    assert mock_botocore_session_client.put_object.call_count == 1
    mock_botocore_session_client.put_object.assert_called_once_with(**expected_args)


@patch("pystac.stac_io.DefaultStacIO")
@patch("botocore.session.Session")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_write_text_workspace_url(
    mock_botocore_session_class, mock_default_stacio, setenvvar
):

    # Create mocks
    mock_botocore_session = mock.Mock()
    mock_botocore_session_client = mock.Mock()
    mock_botocore_session_client.put_object.return_value = None
    mock_botocore_session.create_client.return_value = mock_botocore_session_client
    mock_botocore_session_class.return_value = mock_botocore_session
    mock_default_stacio.write_text.return_value = None

    # Configure expected arguments
    expected_args_1 = {
        "Body": "test_text".encode("UTF-8"),
        "Bucket": "bucket",
        "Key": "workspace/test_dest/path",
        "ContentType": "application/geo+json",
    }
    expected_args_2 = {
        "Body": "test_text".encode("UTF-8"),
        "Bucket": "bucket",
        "Key": "harvested/workspace/test_dest/path",
        "ContentType": "application/geo+json",
    }

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO(
        s3_endpoint="bucket", workspace="workspace", workspace_domain="domain.com"
    )

    # Test write_text
    test_stac_io.write_text(
        "https://workspace.domain.com/files/bucket/test_dest/path",
        "test_text",
        "args_1",
        "arg_2",
        kwarg_1="kwarg_1",
        kwarg_2="kwarg_2",
    )

    # Assertions
    assert mock_botocore_session_client.put_object.call_count == 2
    mock_botocore_session_client.put_object.assert_any_call(**expected_args_1)
    mock_botocore_session_client.put_object.assert_any_call(**expected_args_2)


@patch("pystac.stac_io.DefaultStacIO.write_text")
@patch("botocore.session.Session")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_write_text_super(
    mock_botocore_session_class, mock_default_stacio_write_text, setenvvar
):
    """Test the CustomStacIO write text function with super call, not using S3"""
    # Create mocks
    mock_botocore_session = mock.Mock()
    mock_botocore_session_client = mock.Mock()
    mock_botocore_session_client.put_object.return_value = None
    mock_botocore_session.create_client.return_value = mock_botocore_session_client
    mock_botocore_session_class.return_value = mock_botocore_session
    mock_default_stacio_write_text.return_value = None

    # Configure expected arguments
    expected_args = ("https://test_dest", "test_text", "args_1", "arg_2")

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO()

    # Test write_text
    test_stac_io.write_text(
        "https://test_dest", "test_text", "args_1", "arg_2", kwarg_1="kwarg_1", kwarg_2="kwarg_2"
    )

    # Assertions
    assert mock_default_stacio_write_text.call_count == 1
    mock_default_stacio_write_text.assert_called_once_with(
        *expected_args, kwarg_1="kwarg_1", kwarg_2="kwarg_2"
    )


@patch("os.walk")
@patch("boto3.client")
def test_upload_directory(mock_boto3_client, mock_os_walk):

    # Set test variables
    mock_boto3_client.upload_file.return_value = None
    s3_client = mock_boto3_client
    local_path = "directory/test_path/file_name"
    bucket = "test_bucket"
    s3_path = "test_s3_path"

    # Configure mocks
    mock_os_walk.return_value = [
        ("directory/test_path/file_name", ["dir1", "dir2"], ["file1", "file2"]),
        ("directory/test_path/file_name/dir1", [], ["file3"]),
        ("directory/test_path/file_name/dir2", [], ["file4"]),
    ]

    upload_directory(s3_client, local_path, bucket, s3_path)

    # Assertions
    assert mock_boto3_client.upload_file.call_count == 4

    # Check calls are as expected
    calls = mock_boto3_client.upload_file.call_args_list
    assert calls[0][0] == (
        "directory/test_path/file_name/file1",
        "test_bucket",
        "test_s3_path/file1",
    )
    assert calls[1][0] == (
        "directory/test_path/file_name/file2",
        "test_bucket",
        "test_s3_path/file2",
    )
    assert calls[2][0] == (
        "directory/test_path/file_name/dir1/file3",
        "test_bucket",
        "test_s3_path/dir1/file3",
    )
    assert calls[3][0] == (
        "directory/test_path/file_name/dir2/file4",
        "test_bucket",
        "test_s3_path/dir2/file4",
    )


class MockConfigMapResponse:
    def __init__(self, data):
        self.data = data


@patch("kubernetes.client.CoreV1Api")
@patch("kubernetes.config.load_incluster_config")
def test_get_bucket_from_namespace_success(
    mock_kubernetes_config_load, mock_kubernetes_client_coreV1Api
):

    # Create mocks
    mock_CoreV1Api = mock.Mock()
    mock_CoreV1Api.read_namespaced_config_map.return_value = MockConfigMapResponse(
        {"S3_BUCKET_WORKSPACE": "test_bucket"}
    )
    mock_kubernetes_client_coreV1Api.return_value = mock_CoreV1Api
    mock_kubernetes_config_load.return_value = None

    result = get_bucket_from_namespace()

    # Check result is as expected
    assert result == "test_bucket"


@patch("kubernetes.client.CoreV1Api")
@patch("kubernetes.config.load_incluster_config")
def test_get_bucket_from_namespace_exception(
    mock_kubernetes_config_load, mock_kubernetes_client_coreV1Api, capsys
):

    # Create mocks
    mock_CoreV1Api = mock.Mock()
    mock_CoreV1Api.read_namespaced_config_map.side_effect = ApiException(
        status=400, reason="Some Reason"
    )
    mock_kubernetes_client_coreV1Api.return_value = mock_CoreV1Api
    mock_kubernetes_config_load.return_value = None

    result = get_bucket_from_namespace()

    # Check result is None
    assert not result
    # Check exception was handled
    assert "Exception when fetching workspace bucket" in capsys.readouterr().err


@patch("pulsar.Client")
@patch.dict(os.environ, {"PULSAR_URL": "pulsar://test-url"})
def test_send_to_ingester(mock_pulsar_client):

    # Configure mocks
    mock_pulsar_client_instance = mock.Mock()
    mock_pulsar_client.return_value = mock_pulsar_client_instance
    mock_pulsar_producer = mock.Mock()
    mock_pulsar_producer.send.return_value = None
    mock_pulsar_producer.close.return_value = None
    mock_pulsar_client_instance.create_producer.return_value = mock_pulsar_producer

    # Configure test variables
    test_harvest_list = ["test1", "test2"]
    test_bucket = "test_bucket"
    test_workspace = "test_workspace"
    topic = "harvested-test"
    name = "test_producer"

    expected_message = {
        "id": f"{name}/{test_workspace}",
        "workspace": test_workspace,
        "repository": "",
        "branch": "main",
        "bucket_name": test_bucket,
        "updated_keys": [],
        "added_keys": test_harvest_list,
        "deleted_keys": [],
        "source": test_workspace,
        "target": f"user-datasets/{test_workspace}",
        "job_id": "jobID",
        "workflow_id": "workflowID",
    }

    send_to_ingester(
        test_harvest_list, test_bucket, test_workspace, topic, name, "jobID", "workflowID"
    )

    # Check calls are as expected
    mock_pulsar_client_instance.create_producer.assert_called_once_with(
        topic=topic, producer_name=name
    )
    mock_pulsar_producer.send.assert_called_once_with(json.dumps(expected_message).encode("utf-8"))
    mock_pulsar_producer.close.assert_called_once()


class Asset:
    def __init__(self, href):
        self.href = href
        self.absolute_href = os.path.join("/path/to/asset", href)

    def get_absolute_href(self):
        return self.absolute_href


def test_construct_collection_and_upload_assets():
    """Test construct_collection_and_upload_assets function with assets that are files"""

    # Configure mocks
    mock_catalog = mock.Mock()
    mock_collection = mock.Mock()
    mock_assets_1 = mock.Mock()
    mock_assets_2 = mock.Mock()
    mock_assets_1.items.return_value = [
        ("test_asset_1_item_1", Asset("test_1_href_1")),
        ("test_asset_1_item_2", Asset("test_1_href_2")),
    ]
    mock_assets_2.items.return_value = [
        ("test_asset_2_item_1", Asset("test_2_href_1")),
        ("test_asset_2_item_2", Asset("test_2_href_2")),
    ]
    mock_item_1 = mock.Mock()
    mock_item_2 = mock.Mock()
    mock_item_1.set_collection.return_value = None
    mock_item_2.set_collection.return_value = None
    mock_item_1.get_assets.return_value = mock_assets_1
    mock_item_2.get_assets.return_value = mock_assets_2
    mock_item_1.add_asset.return_value = None
    mock_item_2.add_asset.return_value = None
    mock_catalog.get_items.return_value = [mock_item_1, mock_item_2]
    mock_collection.add_item.return_value = None
    mock_collection.id = "test_collection"

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.return_value = None

    # Configure test variables
    cat_id = "test_catalog"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    s3_data = {
        "s3_endpoint": s3_endpoint,
        "subfolder": subfolder,
        "workspace": workspace,
        "workspace_domain": workspace_domain,
        "bucket": bucket,
    }

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        mock_s3_client,
        s3_data,
    )

    # Check calls are as expected
    # Check upload file call
    assert mock_s3_client.upload_file.call_count == 4
    calls = mock_s3_client.upload_file.call_args_list
    assert calls[0][0] == (
        "/path/to/asset/test_1_href_1",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_1_href_1",
    )
    assert calls[1][0] == (
        "/path/to/asset/test_1_href_2",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_1_href_2",
    )
    assert calls[2][0] == (
        "/path/to/asset/test_2_href_1",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_2_href_1",
    )
    assert calls[3][0] == (
        "/path/to/asset/test_2_href_2",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_2_href_2",
    )

    # Check update href function
    expected_asset_1 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/test_collection/test_1_href_1"
    )
    expected_asset_2 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/test_collection/test_1_href_2"
    )
    expected_asset_3 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/test_collection/test_2_href_1"
    )
    expected_asset_4 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/test_collection/test_2_href_2"
    )

    calls = mock_item_1.add_asset.call_args_list
    assert calls[0][0][0] == ("test_asset_1_item_1")
    assert calls[0][0][1].href == expected_asset_1.href
    assert calls[1][0][0] == ("test_asset_1_item_2")
    assert calls[1][0][1].href == expected_asset_2.href

    calls = mock_item_2.add_asset.call_args_list
    assert calls[0][0][0] == ("test_asset_2_item_1")
    assert calls[0][0][1].href == expected_asset_3.href
    assert calls[1][0][0] == ("test_asset_2_item_2")
    assert calls[1][0][1].href == expected_asset_4.href

    # Check output result
    assert result


def test_construct_collection_and_upload_assets_external_links():
    """Test construct_collection_and_upload_assets function with assets that are files"""

    # Configure mocks
    mock_catalog = mock.Mock()
    mock_collection = mock.Mock()
    mock_assets_1 = mock.Mock()
    mock_assets_1.items.return_value = [
        ("test_asset_1_item_1", Asset("test_1_href_1")),
    ]
    mock_item_1 = mock.Mock()
    mock_item_1.set_collection.return_value = None
    mock_item_1.get_assets.return_value = mock_assets_1
    mock_item_1.add_asset.return_value = None
    mock_catalog.get_items.return_value = [mock_item_1]
    mock_collection.add_item.return_value = None
    mock_collection.id = "test_collection"

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.side_effect = (
        Exception  # generic exception to force code to ignore this asset href
    )

    # Configure test variables
    cat_id = "test_catalog"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    s3_data = {
        "s3_endpoint": s3_endpoint,
        "subfolder": subfolder,
        "workspace": workspace,
        "workspace_domain": workspace_domain,
        "bucket": bucket,
    }

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        mock_s3_client,
        s3_data,
    )

    # Check calls are as expected
    # Check upload file call
    assert mock_s3_client.upload_file.call_count == 1
    calls = mock_s3_client.upload_file.call_args_list
    assert calls[0][0] == (
        "/path/to/asset/test_1_href_1",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_1_href_1",
    )

    # Check add_asset not called as this asset href should be left as is
    mock_item_1.add_asset.assert_not_called()

    # Check output result
    assert result


@patch("stageout.upload_directory")
def test_construct_collection_and_upload_assets_directory(mock_upload_directory):
    """Test construct_collection_and_upload_assets function with assets that are directories"""

    # Configure mocks
    mock_catalog = mock.Mock()
    mock_collection = mock.Mock()
    mock_assets_1 = mock.Mock()
    mock_assets_2 = mock.Mock()
    # Here test_1_href_1 is refering to a directory e.g. a zarr directory
    mock_assets_1.items.return_value = [
        ("test_asset_1_item_1", Asset("test_1_href_1")),
        ("test_asset_1_item_2", Asset("test_1_href_2")),
    ]
    mock_assets_2.items.return_value = [
        ("test_asset_2_item_1", Asset("test_2_href_1")),
        ("test_asset_2_item_2", Asset("test_2_href_2")),
    ]
    mock_item_1 = mock.Mock()
    mock_item_2 = mock.Mock()
    mock_item_1.set_collection.return_value = None
    mock_item_2.set_collection.return_value = None
    mock_item_1.get_assets.return_value = mock_assets_1
    mock_item_2.get_assets.return_value = mock_assets_2
    mock_item_1.add_asset.return_value = None
    mock_item_2.add_asset.return_value = None
    mock_catalog.get_items.return_value = [mock_item_1, mock_item_2]
    mock_collection.add_item.return_value = None
    mock_collection.id = "test_collection"

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.side_effect = IsADirectoryError

    # Configure test variables
    cat_id = "test_catalog"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    s3_data = {
        "s3_endpoint": s3_endpoint,
        "subfolder": subfolder,
        "workspace": workspace,
        "workspace_domain": workspace_domain,
        "bucket": bucket,
    }

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        mock_s3_client,
        s3_data,
    )

    # Check calls are as expected
    assert mock_upload_directory.call_count == 4
    calls = mock_upload_directory.call_args_list
    assert calls[0][0] == (
        mock_s3_client,
        "/path/to/asset/test_1_href_1",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_1_href_1",
    )
    assert calls[1][0] == (
        mock_s3_client,
        "/path/to/asset/test_1_href_2",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_1_href_2",
    )
    assert calls[2][0] == (
        mock_s3_client,
        "/path/to/asset/test_2_href_1",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_2_href_1",
    )
    assert calls[3][0] == (
        mock_s3_client,
        "/path/to/asset/test_2_href_2",
        s3_endpoint,
        f"{workspace}/{subfolder}/{cat_id}/test_collection/test_2_href_2",
    )

    # Check output result
    assert result


def test_construct_collection_and_upload_assets_exception(capsys):

    # Configure mocks
    mock_catalog = mock.Mock()
    mock_collection = mock.Mock()
    mock_assets_1 = mock.Mock()
    mock_assets_2 = mock.Mock()
    # Here test_1_href_1 is refering to a directory e.g. a zarr directory
    mock_assets_1.items.return_value = [
        ("test_asset_1_item_1", Asset("test_1_href_1")),
        ("test_asset_1_item_2", Asset("test_1_href_2")),
    ]
    mock_assets_2.items.return_value = [
        ("test_asset_2_item_1", Asset("test_2_href_1")),
        ("test_asset_2_item_2", Asset("test_2_href_2")),
    ]
    mock_item_1 = mock.Mock()
    mock_item_1.id = "test_item_1"
    mock_item_2 = mock.Mock()
    mock_item_2.id = "test_item_2"
    mock_item_1.set_collection.return_value = None
    mock_item_2.set_collection.return_value = None
    mock_item_1.get_assets.return_value = mock_assets_1
    mock_item_2.get_assets.return_value = mock_assets_2
    mock_item_1.add_asset.return_value = None
    mock_item_2.add_asset.return_value = None
    mock_catalog.get_items.return_value = [mock_item_1, mock_item_2]
    mock_collection.add_item.return_value = None
    mock_collection.id = "test_collection"

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.side_effect = RuntimeError

    # Configure test variables
    cat_id = "test_catalog"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    s3_data = {
        "s3_endpoint": s3_endpoint,
        "subfolder": subfolder,
        "workspace": workspace,
        "workspace_domain": workspace_domain,
        "bucket": bucket,
    }

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        mock_s3_client,
        s3_data,
    )

    # Check exceptions are handled correctly
    error_out = capsys.readouterr().err
    assert f"Upload {mock_item_1.id} to {s3_endpoint} at {subfolder} failed" in error_out
    assert f"Upload {mock_item_2.id} to {s3_endpoint} at {subfolder} failed" in error_out

    # Check output result
    assert result


@patch("pystac.write_file")
def test_upload_items(mock_pystac_write_file):

    # Configure mocks
    mock_pystac_write_file.return_value = None
    mock_collection = mock.Mock()
    mock_item_1 = mock.Mock()
    mock_item_1.get_self_href.return_value = "test_item_1_href"
    mock_item_2 = mock.Mock()
    mock_item_2.get_self_href.return_value = "test_item_2_href"
    mock_collection.get_items.return_value = [mock_item_1, mock_item_2]

    # Configure test variables
    cat_id = "test_catalog"
    collection_id = "test_collection"
    bucket = "test_bucket"
    subfolder = "test_subfolder"
    workspace = "test_workspace"

    result = upload_items(mock_collection, cat_id, collection_id, bucket, workspace, subfolder)

    # Check result is not empty
    assert result
    assert result == [
        f"harvested/{workspace}/{subfolder}/{cat_id}/{collection_id}/test_item_1_href",
        f"harvested/{workspace}/{subfolder}/{cat_id}/{collection_id}/test_item_2_href",
    ]

    # Check calls are as expected
    assert mock_pystac_write_file.call_count == 4
    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_item_1
    assert calls[0][1] == {
        "dest_href": f"s3://{bucket}/{workspace}/{subfolder}/{cat_id}/{collection_id}/test_item_1_href"
    }


@patch("boto3.client")
def test_get_aws_client_with_credentials(mock_boto3_client, setenvvar):
    """Test get_aws_client function with provided AWS credentials"""

    # Configure mocks
    mock_boto3_client.return_value = "example_s3_client"

    result = get_aws_client()

    # Check client is created
    assert result == "example_s3_client"

    # Check calls are as expected
    mock_boto3_client.assert_called_once_with(
        service_name="s3",
        use_ssl=True,
        aws_access_key_id="mock_access_key",
        aws_secret_access_key="mock_secret_key",
        endpoint_url="mock_endpoint",
        region_name="mock_region",
        aws_session_token="valid-token",
    )


@patch.dict(
    os.environ,
    {"AWS_REGION": "test-region", "AWS_S3_ENDPOINT": "test-endpoint"},
)
@patch("boto3.client")
def test_get_aws_client_no_credentials(mock_boto3_client):
    """Test get_aws_client function without provided AWS credentials, instead using service account"""

    # Configure mocks
    mock_boto3_client.return_value = "example_s3_client"

    result = get_aws_client()

    # Check client is created
    assert result == "example_s3_client"

    # Check calls are as expected
    mock_boto3_client.assert_called_once_with("s3")


class Link:
    def __init__(self, rel, href):
        self.rel = rel
        self.href = href
        self.media_type = "application/json"
        self.title = "title"


@patch("builtins.open", new_callable=mock_open, read_data='{"valid": "json"}')
@patch("os.listdir")
@patch("stageout.get_aws_client")
@patch("stageout.send_to_ingester")
@patch("stageout.upload_items")
@patch("stageout.get_bucket_from_namespace")
@patch("stageout.construct_collection_and_upload_assets")
@patch("pystac.write_file")
@patch("pystac.Extent")
@patch("pystac.Collection")
@patch("pystac.read_file")
@patch("shutil.copytree")
def test_main_access_point_no_collection(
    mock_shutil_copytree,
    mock_pystac_read_file,
    mock_pystac_Collection,
    mock_pystac_Extent,
    mock_pystac_write_file,
    mock_construct_collection_and_upload_assets,
    mock_get_bucket_from_namespace,
    mock_upload_items,
    mock_send_to_ingester,
    mock_get_aws_client,
    mock_os_listdir,
    mock_open,
    setenvvar,
):
    """Test main function with no AWS credentials, instead using access point"""

    # Configure test variables
    job_id = "12345678"
    collection_id = f"col_{job_id}"
    cat_url = "test_catalog_url"
    bucket = "bucket-name"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    topic = "harvested"
    name = "ades-outputs-stac"

    # Configure mocks
    mock_boto3_client_instance = mock.Mock()
    mock_get_aws_client.return_value = mock_boto3_client_instance

    mock_os_listdir.return_value = ["catalog.json", "collection-annotations.json"]

    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_all_collections.return_value = []
    mock_catalog.get_self_href.return_value = (
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/catalog.json"
    )

    mock_shutil_copytree.return_value = None

    mock_pystac_read_file.return_value = mock_catalog
    mock_pystac_write_file.return_value = None

    mock_item_1 = mock.Mock()
    mock_item_1.id = "test_item_1"
    mock_item_1.assets = {"asset_key": mock.Mock(href="s3://bucket/path/to/asset1")}
    mock_item_2 = mock.Mock()
    mock_item_2.id = "test_item_2"
    mock_item_2.assets = {"asset_key": mock.Mock(href="s3://bucket/path/to/asset2")}

    mock_collection = mock.Mock()
    mock_collection.update_extent_from_items.return_value = None
    mock_collection.get_self_href.return_value = (
        f"s3://bucket-name/{subfolder}/cat_{job_id}/{collection_id}.json"
    )
    mock_collection.get_items.return_value = [mock_item_1, mock_item_2]
    mock_pystac_Collection.return_value = mock_collection

    mock_pystac_Extent.return_value = "some-extent"
    mock_get_bucket_from_namespace.return_value = "test-access-point/test-bucket"

    mock_construct_collection_and_upload_assets.return_value = mock_collection

    mock_upload_items.return_value = ["harvested/test_item_1", "harvested/test_item_2"]

    mock_send_to_ingester.return_value = None

    main(cat_url, bucket, subfolder, job_id, "test-workflow")

    # Check calls are as expected
    assert mock_collection.update_extent_from_items.call_count == 1
    assert mock_catalog.clear_items.call_count == 1
    mock_catalog.add_child.assert_called_once_with(mock_collection)
    mock_catalog.normalize_hrefs.assert_called_once()
    mock_pystac_Collection.assert_called_once_with(
        id=collection_id,
        description="description",
        extent="some-extent",
        title=f"Result Collection for Job {job_id}",
        href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/collection.json",
        stac_extensions=[],
        keywords=["eoepca"],
        license="proprietary",
    )

    expected_s3_data = {
        "s3_endpoint": "bucket-name",
        "subfolder": subfolder,
        "workspace": "test_workspace",
        "workspace_domain": "test.workspace.domain.com",
        "bucket": bucket,
    }

    mock_construct_collection_and_upload_assets.assert_called_once_with(
        mock_catalog,
        mock_collection,
        f"cat_{job_id}",
        mock_boto3_client_instance,
        expected_s3_data,
    )
    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_collection
    assert calls[0][1] == {
        "dest_href": f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/collection.json"
    }
    assert calls[1][0][0] == mock_catalog
    assert calls[1][1] == {
        "dest_href": f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}.json"
    }
    # Check root link was removed
    assert len(calls[1][0][0].links) == 1

    assert mock_send_to_ingester.call_count == 2
    # Check STAC call
    mock_send_to_ingester.assert_any_call(
        [
            "harvested/test_item_1",
            "harvested/test_item_2",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}.json",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/collection.json",
        ],
        bucket,
        "test_workspace",
        topic,
        name,
        job_id,
        "test-workflow",
    )
    # Check annotations call
    mock_send_to_ingester.assert_any_call(
        [
            f"{workspace}/{subfolder}/cat_{job_id}/collection-annotations.json",
        ],
        bucket,
        "test_workspace",
        "harvested-annotations",
        "ades-outputs-annotations",
        job_id,
        "test-workflow",
    )


@patch("os.listdir")
@patch("stageout.get_aws_client")
@patch("stageout.send_to_ingester")
@patch("stageout.upload_items")
@patch("stageout.get_bucket_from_namespace")
@patch("stageout.construct_collection_and_upload_assets")
@patch("pystac.write_file")
@patch("pystac.Extent")
@patch("pystac.Collection")
@patch("pystac.read_file")
@patch("shutil.copytree")
def test_main_access_point_with_collection(
    mock_shutil_copytree,
    mock_pystac_read_file,
    mock_pystac_Collection,
    mock_pystac_Extent,
    mock_pystac_write_file,
    mock_construct_collection_and_upload_assets,
    mock_get_bucket_from_namespace,
    mock_upload_items,
    mock_send_to_ingester,
    mock_get_aws_client,
    mock_os_listdir,
    setenvvar,
):
    """Test main function with no AWS credentials, instead using access point"""

    # Configure test variables
    job_id = "1234-5678"
    collection_id = f"col_{job_id}"
    workflow_id = "test-workflow"
    cat_url = "test_catalog_url"
    bucket = "test_bucket"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"

    # Configure mocks
    mock_boto3_client_instance = mock.Mock()
    mock_get_aws_client.return_value = mock_boto3_client_instance
    mock_boto3_client_instance.head_object.return_value = None
    mock_link = mock.Mock(rel="root", href="./root-link")

    mock_item_1 = mock.Mock()
    mock_item_1.id = "test_item_1"
    mock_asset_1 = mock.Mock(href="s3://bucket/path/to/asset1")
    mock_item_1.get_assets.return_value = {"asset_key": mock_asset_1}
    mock_item_1.links = [mock_link]
    mock_item_2 = mock.Mock()
    mock_item_2.id = "test_item_2"
    mock_asset_2 = mock.Mock(href="s3://bucket/path/to/asset2")
    mock_item_2.get_assets.return_value = {"asset_key": mock_asset_2}
    mock_item_2.links = [mock_link]

    mock_collection = mock.Mock()
    mock_collection.update_extent_from_items.return_value = None
    mock_collection.get_self_href.return_value = (
        f"s3://bucket-name/{subfolder}/cat_{job_id}/{collection_id}.json"
    )
    mock_collection.id = collection_id
    mock_collection.get_items.return_value = [mock_item_1, mock_item_2]
    mock_collection.links = [Link("root", "test_root_href"), Link("license", "https://test.com")]
    mock_pystac_Collection.return_value = mock_collection

    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_all_collections.return_value = [mock_collection]
    mock_catalog.get_self_href.return_value = (
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/catalog.json"
    )

    mock_shutil_copytree.return_value = None
    mock_os_listdir.return_value = ["catalog.json"]  # no annotations present

    mock_pystac_read_file.return_value = mock_catalog
    mock_pystac_write_file.return_value = None

    mock_pystac_Extent.return_value = "some-extent"
    mock_get_bucket_from_namespace.return_value = "test-access-point/test-bucket"

    mock_construct_collection_and_upload_assets.return_value = mock_collection

    mock_upload_items.return_value = [
        f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_1",
        f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_2",
    ]

    mock_send_to_ingester.return_value = None

    main(cat_url, bucket, subfolder, job_id, "test-workflow")

    # Check calls are as expected
    mock_catalog.set_self_href.assert_called_once_with(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}.json"
    )
    mock_catalog.add_child.assert_called_once_with(mock_collection)
    mock_catalog.normalize_hrefs.assert_called_once()

    mock_collection.set_self_href.assert_called_once_with(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/{collection_id}.json"
    )

    mock_item_1.set_collection.assert_called_once_with(mock_collection)
    mock_item_2.set_collection.assert_called_once_with(mock_collection)

    mock_item_1.set_self_href.assert_called_once_with(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/{collection_id}/{mock_item_1.id}.json"
    )
    mock_item_2.set_self_href.assert_called_once_with(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/{collection_id}/{mock_item_2.id}.json"
    )

    mock_item_1.add_asset.assert_called_once_with("asset_key", mock_asset_1)
    mock_item_2.add_asset.assert_called_once_with("asset_key", mock_asset_2)

    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_collection
    assert calls[0][1] == {
        "dest_href": f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/{collection_id}.json"
    }
    assert calls[1][0][0] == mock_catalog
    assert calls[1][1] == {
        "dest_href": f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}.json"
    }
    # Check root link was removed
    assert len(calls[1][0][0].links) == 1

    assert mock_send_to_ingester.call_count == 1
    mock_send_to_ingester.assert_called_once_with(
        [
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_1",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_2",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}.json",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}.json",
        ],
        bucket,
        "test_workspace",
        "harvested",
        "ades-outputs-stac",
        workflow_id,
        job_id,
    )
    mock_boto3_client_instance.head_object.assert_called_once()


@patch("builtins.open")
def test_get_current_namespace(mock_open):

    # Configure mocks
    mock_open.return_value.__enter__.return_value.read.return_value = " test_namespace "

    result = get_current_namespace()

    # Check result is as expected
    assert result == "test_namespace"


@patch("builtins.open")
def test_get_current_namespace_error(mock_open):

    # Configure mocks
    # Simulate error when opening file
    mock_open.return_value.__enter__.return_value.read.side_effect = IOError

    result = get_current_namespace()

    # Check result is as expected
    assert result == "default"


@patch("os.listdir")
@patch("stageout.get_aws_client")
@patch("stageout.send_to_ingester")
@patch("stageout.upload_items")
@patch("stageout.get_bucket_from_namespace")
@patch("stageout.construct_collection_and_upload_assets")
@patch("pystac.write_file")
@patch("pystac.Extent")
@patch("pystac.Collection")
@patch("pystac.read_file")
@patch("shutil.copytree")
def test_main_no_access_point(
    mock_shutil_copytree,
    mock_pystac_read_file,
    mock_pystac_Collection,
    mock_pystac_Extent,
    mock_pystac_write_file,
    mock_construct_collection_and_upload_assets,
    mock_get_bucket_from_namespace,
    mock_upload_items,
    mock_send_to_ingester,
    mock_get_aws_client,
    mock_os_listdir,
    setenvvar,
):
    """Test main function with AWS credentials"""

    # Configure test variables
    job_id = "12345678"
    collection_id = f"col_{job_id}"
    cat_url = "test_catalog_url"
    bucket = "test_bucket"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    topic = "harvested"
    name = "ades-outputs-stac"

    # Configure mocks
    mock_boto3_client_instance = mock.Mock()
    mock_get_aws_client.return_value = mock_boto3_client_instance

    mock_os_listdir.return_value = ["catalog.json"]

    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_all_collections.return_value = []
    mock_catalog.get_self_href.return_value = (
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/catalog.json"
    )

    mock_shutil_copytree.return_value = None

    mock_pystac_read_file.return_value = mock_catalog
    mock_pystac_write_file.return_value = None

    mock_item_1 = mock.Mock()
    mock_item_1.id = "test_item_1"
    mock_item_1.assets = {"asset_key": mock.Mock(href="s3://bucket/path/to/asset1")}
    mock_item_2 = mock.Mock()
    mock_item_2.id = "test_item_2"
    mock_item_2.assets = {"asset_key": mock.Mock(href="s3://bucket/path/to/asset2")}

    mock_collection = mock.Mock()
    mock_collection.update_extent_from_items.return_value = None
    mock_collection.get_self_href.return_value = (
        f"s3://{bucket}/{subfolder}/cat_{job_id}/{collection_id}.json"
    )
    mock_collection.get_items.return_value = [mock_item_1, mock_item_2]
    mock_pystac_Collection.return_value = mock_collection

    mock_pystac_Extent.return_value = "some-extent"

    mock_get_bucket_from_namespace.return_value = None

    mock_construct_collection_and_upload_assets.return_value = mock_collection

    mock_upload_items.return_value = [
        f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_1",
        f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_2",
    ]

    mock_send_to_ingester.return_value = None

    main(cat_url, bucket, subfolder, job_id, "test-workflow")

    # Check calls are as expected
    assert mock_collection.update_extent_from_items.call_count == 1
    assert mock_catalog.clear_items.call_count == 1
    mock_catalog.add_child.assert_called_once_with(mock_collection)
    mock_catalog.normalize_hrefs.assert_called_once()
    mock_pystac_Collection.assert_called_once_with(
        id=collection_id,
        description="description",
        extent="some-extent",
        title=f"Result Collection for Job {job_id}",
        href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/collection.json",
        stac_extensions=[],
        keywords=["eoepca"],
        license="proprietary",
    )

    expected_s3_data = {
        "s3_endpoint": bucket,
        "subfolder": subfolder,
        "workspace": "test_workspace",
        "workspace_domain": "test.workspace.domain.com",
        "bucket": bucket,
    }

    mock_construct_collection_and_upload_assets.assert_called_once_with(
        mock_catalog,
        mock_collection,
        f"cat_{job_id}",
        mock_boto3_client_instance,
        expected_s3_data,
    )
    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_collection
    assert calls[0][1] == {
        "dest_href": f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/collection.json"
    }
    assert calls[1][0][0] == mock_catalog
    assert calls[1][1] == {
        "dest_href": f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}.json"
    }
    # Check root link was removed
    assert len(calls[1][0][0].links) == 1

    assert mock_send_to_ingester.call_count == 1
    mock_send_to_ingester.assert_called_once_with(
        [
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_1",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/{collection_id}/test_item_2",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}.json",
            f"harvested/{workspace}/{subfolder}/cat_{job_id}/collection.json",
        ],
        "test_bucket",
        "test_workspace",
        topic,
        name,
        job_id,
        "test-workflow",
    )


@patch("builtins.open", new_callable=mock_open, read_data='{"valid": "json"}')
@patch("os.path.join")
@patch("os.path.normpath")
@patch("os.listdir")
def test_upload_annotation_success(mock_os_listdir, mock_normpath, mock_join, mock_open):
    s3_client = mock.Mock()
    s3_client.upload_file.return_value = None
    mock_join.return_value = "/tmp/outputs/annotations.json"
    mock_normpath.return_value = "workspace/subfolder/cat_id/annotations.json"
    mock_os_listdir.return_value = ["file-annotations.json"]
    s3_endpoint = "s3_endpoint"
    workspace = "workspace"
    subfolder = "processing-results"
    cat_id = "cat_id"
    file_name = "annotations.json"

    result = upload_annotation(s3_client, s3_endpoint, workspace, subfolder, cat_id, file_name)
    assert result
    s3_client.upload_file.assert_called_once_with(
        "/tmp/outputs/annotations.json", s3_endpoint, "workspace/subfolder/cat_id/annotations.json"
    )


@patch("builtins.open", new_callable=mock_open, read_data='{"invalid": json}')
@patch("os.path.join")
@patch("os.path.normpath")
@patch("os.listdir")
def test_upload_annotation_invalid_json(mock_os_listdir, mock_normpath, mock_join, mock_open):
    s3_client = mock.Mock()
    s3_client.upload_file.return_value = None
    mock_join.return_value = "/tmp/outputs/annotations.json"
    mock_normpath.return_value = "workspace/subfolder/cat_id/annotations.json"
    mock_os_listdir.return_value = ["file-annotations.json"]
    s3_endpoint = "s3_endpoint"
    workspace = "workspace"
    subfolder = "processing-results"
    cat_id = "cat_id"
    file_name = "annotations.json"

    result = upload_annotation(s3_client, s3_endpoint, workspace, subfolder, cat_id, file_name)
    assert not result
    assert s3_client.upload_file.call_count == 0


@patch("builtins.open", side_effect=FileNotFoundError)
@patch("os.path.join")
@patch("os.path.normpath")
@patch("os.listdir")
def test_upload_annotation_file_not_found(mock_os_listdir, mock_normpath, mock_join, mock_open):
    s3_client = mock.Mock()
    s3_client.upload_file.return_value = None
    mock_join.return_value = "/tmp/outputs/annotations.json"
    mock_normpath.return_value = "workspace/subfolder/cat_id/annotations.json"
    mock_os_listdir.return_value = ["file-annotations.json"]
    s3_endpoint = "s3_endpoint"
    workspace = "workspace"
    subfolder = "processing-results"
    cat_id = "cat_id"
    file_name = "annotations.json"

    result = upload_annotation(s3_client, s3_endpoint, workspace, subfolder, cat_id, file_name)
    assert not result
    assert s3_client.upload_file.call_count == 0


@patch("stageout.upload_annotation")
@patch("stageout.send_to_ingester")
@patch("os.listdir")
def test_handle_annotations_success(mock_os_listdir, mock_send_to_ingester, mock_upload_annotation):
    s3_client = mock.Mock()
    mock_upload_annotation.return_value = True
    mock_os_listdir.return_value = ["file-annotations.json", "file_2-annotations.json"]
    s3_endpoint = "s3_endpoint"
    workspace = "workspace"
    bucket = "bucket"
    subfolder = "processing-results"
    job_id = "job_id"

    added_annotations = [
        f"{workspace}/{subfolder}/cat_{job_id}/file-annotations.json",
        f"{workspace}/{subfolder}/cat_{job_id}/file_2-annotations.json",
    ]

    result = handle_annotations(
        s3_client, s3_endpoint, workspace, bucket, subfolder, job_id, "test-workflow"
    )
    assert result
    mock_send_to_ingester.assert_called_once_with(
        added_annotations,
        bucket,
        workspace,
        "harvested-annotations",
        "ades-outputs-annotations",
        job_id,
        "test-workflow",
    )


@patch("stageout.upload_annotation")
@patch("stageout.send_to_ingester")
@patch("os.listdir")
def test_handle_annotations_no_annotations(
    mock_os_listdir, mock_send_to_ingester, mock_upload_annotation
):
    s3_client = mock.Mock()
    mock_upload_annotation.return_value = True
    mock_os_listdir.return_value = []
    s3_endpoint = "s3_endpoint"
    workspace = "workspace"
    bucket = "bucket"
    subfolder = "processing-results"
    job_id = "job_id"

    result = handle_annotations(
        s3_client, s3_endpoint, workspace, bucket, subfolder, job_id, "test-workflow"
    )
    assert not result
    assert mock_send_to_ingester.call_count == 0


@patch("pystac.read_file")
@patch("os.path.join")
@patch("stageout.CustomStacIO")
@patch("stageout.construct_collection_and_upload_assets")
@patch("stageout.upload_items")
@patch("stageout.send_to_ingester")
@patch("pystac.write_file")
def test_handle_stac_outputs_no_collection_success(
    mock_write_file,
    mock_send_to_ingester,
    mock_upload_items,
    mock_construct_collection,
    mock_CustomStacIO,
    mock_os_path_join,
    mock_read_file,
):
    # Configure mocks
    s3_client = mock.Mock()
    s3_endpoint = "s3_endpoint"
    workspace_domain = "workspace_domain"
    workspace = "workspace"
    bucket = "bucket"
    subfolder = "subfolder"
    job_id = "job_id"

    # Mock the pystac.read_file to return a mock catalog
    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_all_collections.return_value = []
    mock_catalog.get_self_href.return_value = (
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/catalog.json"
    )
    mock_os_path_join.return_value = "/tmp/outputs/catalog.json"
    mock_read_file.return_value = mock_catalog

    # Mock the collection construction and upload
    mock_collection = mock.Mock()
    mock_item = mock.Mock()
    mock_item.id = "item_id"
    mock_item.set_self_href.return_value = None
    mock_collection.get_items.return_value = [mock_item]
    mock_construct_collection.return_value = mock_collection

    # Mock the upload_items function
    mock_upload_items.return_value = []

    result = handle_stac_outputs(
        s3_client,
        s3_endpoint,
        workspace_domain,
        workspace,
        bucket,
        subfolder,
        job_id,
        "test-workflow",
    )
    assert result
    mock_read_file.assert_called_once_with("/tmp/outputs/catalog.json")
    mock_write_file.assert_any_call(
        mock_collection,
        dest_href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/collection.json",
    )
    mock_write_file.assert_any_call(
        mock_catalog,
        dest_href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}.json",
    )
    mock_send_to_ingester.assert_called_once()


@patch("pystac.read_file")
@patch("os.path.join")
@patch("stageout.CustomStacIO")
@patch("stageout.construct_collection_and_upload_assets")
@patch("stageout.upload_items")
@patch("stageout.send_to_ingester")
@patch("pystac.write_file")
def test_handle_stac_outputs_with_collection_success(
    mock_write_file,
    mock_send_to_ingester,
    mock_upload_items,
    mock_construct_collection,
    mock_CustomStacIO,
    mock_os_path_join,
    mock_read_file,
):
    # Configure mocks
    s3_client = mock.Mock()
    s3_endpoint = "s3_endpoint"
    workspace_domain = "workspace_domain"
    workspace = "workspace"
    bucket = "bucket"
    subfolder = "subfolder"
    job_id = "1234-5678"

    # Mock the collection construction and upload
    mock_collection = mock.Mock()
    mock_item = mock.Mock()
    mock_item.id = "item_id"
    mock_assets_1 = mock.Mock()
    mock_assets_1.items.return_value = [
        ("test_asset_item", Asset("test_href_1")),
    ]
    mock_item.set_self_href.return_value = None
    mock_item.links = [Link("self", "test_self_href")]
    mock_item.get_assets.return_value = mock_assets_1
    mock_collection.get_items.return_value = [mock_item]
    mock_construct_collection.return_value = mock_collection
    mock_collection.links = [Link("root", "test_root_href"), Link("license", "https://test.com")]

    # Mock the pystac.read_file to return a mock catalog
    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_all_collections.return_value = [mock_collection]
    mock_catalog.get_self_href.return_value = (
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/catalog.json"
    )
    mock_os_path_join.return_value = "/tmp/outputs/catalog.json"
    mock_read_file.return_value = mock_catalog

    # Mock the upload_items function
    mock_upload_items.return_value = []

    result = handle_stac_outputs(
        s3_client,
        s3_endpoint,
        workspace_domain,
        workspace,
        bucket,
        subfolder,
        job_id,
        "test-workflow",
    )
    assert result
    mock_read_file.assert_called_once_with("/tmp/outputs/catalog.json")
    # mock_write_file.assert_any_call(
    #     mock_collection,
    #     dest_href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}/collection.json",
    # )
    mock_write_file.assert_any_call(
        mock_catalog,
        dest_href=f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/cat_{job_id}.json",
    )
    mock_send_to_ingester.assert_called_once()


@patch("pystac.read_file")
def test_handle_stac_outputs_file_not_found(mock_read_file):
    s3_client = mock.Mock()
    mock_read_file.side_effect = FileNotFoundError
    s3_endpoint = "s3_endpoint"
    workspace_domain = "workspace_domain"
    workspace = "workspace"
    bucket = "bucket"
    subfolder = "subfolder"
    job_id = "job_id"

    result = handle_stac_outputs(
        s3_client,
        s3_endpoint,
        workspace_domain,
        workspace,
        bucket,
        subfolder,
        job_id,
        "test-workflow",
    )
    assert not result
    mock_read_file.assert_called_once_with("/tmp/outputs/catalog.json")


@patch("pystac.read_file")
def test_handle_stac_outputs_stac_type_error(mock_read_file):
    s3_client = mock.Mock()
    mock_read_file.side_effect = pystac.STACTypeError(
        bad_dict={"id": "test"}, expected=pystac.Catalog
    )
    s3_endpoint = "s3_endpoint"
    workspace_domain = "workspace_domain"
    workspace = "workspace"
    bucket = "bucket"
    subfolder = "subfolder"
    job_id = "job_id"

    result = handle_stac_outputs(
        s3_client,
        s3_endpoint,
        workspace_domain,
        workspace,
        bucket,
        subfolder,
        job_id,
        "test-workflow",
    )
    assert not result
    mock_read_file.assert_called_once_with("/tmp/outputs/catalog.json")
