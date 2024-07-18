import json
import os
from unittest import mock
from unittest.mock import patch

from kubernetes.client.rest import ApiException

from stageout import (
    CustomStacIO,
    construct_collection_and_upload_assets,
    get_aws_client,
    get_bucket_from_workspace,
    get_current_namespace,
    main,
    send_to_ingester,
    upload_directory,
    upload_items,
)


@patch("botocore.session.Session")
@patch("os.environ")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_init_env_var(mock_environ, mock_botocore_session_class, monkeypatch):
    # Set up mock environment variables
    mock_environ.__getitem__.side_effect = lambda key: {
        "AWS_REGION": "mock_region",
        "AWS_S3_ENDPOINT": "mock_endpoint",
        "WORKSPACE_DOMAIN": "mock_domain",
        "AWS_ACCESS_KEY_ID": "mock_access_key",
        "AWS_SECRET_ACCESS_KEY": "mock_secret_key",
    }.get(key, "")

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
    }

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO()

    # Assertions
    assert mock_botocore_session.create_client.call_count == 1
    mock_botocore_session.create_client.assert_called_once_with(**expected_args)

    # Check resulting s3_client
    assert test_stac_io.s3_client == mock_botocore_session_client


@patch("botocore.session.Session")
@patch("os.environ")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_init_service_account(mock_environ, mock_botocore_session_class, monkeypatch):
    # Set up mock environment variables
    mock_environ.__getitem__.side_effect = lambda key: {
        "AWS_REGION": "mock_region",
        "AWS_S3_ENDPOINT": "mock_endpoint",
        "WORKSPACE_DOMAIN": "mock_domain",
        "AWS_ACCESS_KEY_ID": "",
        "AWS_SECRET_ACCESS_KEY": "",
    }.get(key, "")

    # Create mocks
    mock_botocore_session = mock.Mock()
    mock_botocore_session_client = mock.Mock()
    mock_botocore_session.create_client.return_value = mock_botocore_session_client
    mock_botocore_session_class.return_value = mock_botocore_session

    # Configure expected arguments
    expected_args = "s3"

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO()

    # Assertions
    assert mock_botocore_session.create_client.call_count == 1
    mock_botocore_session.create_client.assert_called_once_with(expected_args)

    # Check resulting s3_client
    assert test_stac_io.s3_client == mock_botocore_session_client


@patch("pystac.stac_io.DefaultStacIO")
@patch("botocore.session.Session")
@patch("os.environ")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_write_text_s3(
    mock_environ, mock_botocore_session_class, mock_default_stacio, monkeypatch
):
    # Set up mock environment variables
    mock_environ.__getitem__.side_effect = lambda key: {
        "AWS_REGION": "mock_region",
        "AWS_S3_ENDPOINT": "mock_endpoint",
        "WORKSPACE_DOMAIN": "mock_domain",
        "AWS_ACCESS_KEY_ID": "",
        "AWS_SECRET_ACCESS_KEY": "",
    }.get(key, "")

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
        "Bucket": "bucket",
        "Key": "path",
        "ContentType": "application/geo+json",
    }

    # Instantiate CustomStacIO
    test_stac_io = CustomStacIO()

    # Test write_text
    test_stac_io.write_text(
        "s3://test_dest/path", "test_text", "args_1", "arg_2", kwarg_1="kwarg_1", kwarg_2="kwarg_2"
    )

    # Assertions
    assert mock_botocore_session_client.put_object.call_count == 1
    mock_botocore_session_client.put_object.assert_called_once_with(**expected_args)


@patch("pystac.stac_io.DefaultStacIO.write_text")
@patch("botocore.session.Session")
@patch("os.environ")
@patch("sys.argv", ["stageout.py", "catalog_url", "bucket", "subfolder", "collection_id"])
def test_custom_stac_write_text_super(
    mock_environ, mock_botocore_session_class, mock_default_stacio_write_text, monkeypatch
):
    # Set up mock environment variables
    mock_environ.__getitem__.side_effect = lambda key: {
        "AWS_REGION": "mock_region",
        "AWS_S3_ENDPOINT": "mock_endpoint",
        "WORKSPACE_DOMAIN": "mock_domain",
        "AWS_ACCESS_KEY_ID": "",
        "AWS_SECRET_ACCESS_KEY": "",
    }.get(key, "")

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
def test_get_bucket_from_workspace_success(
    mock_kubernetes_config_load, mock_kubernetes_client_coreV1Api
):

    # Create mocks
    mock_CoreV1Api = mock.Mock()
    mock_CoreV1Api.read_namespaced_config_map.return_value = MockConfigMapResponse(
        {"S3_BUCKET_WORKSPACE": "test_bucket"}
    )
    mock_kubernetes_client_coreV1Api.return_value = mock_CoreV1Api
    mock_kubernetes_config_load.return_value = None

    result = get_bucket_from_workspace("test_workspace")

    # Check result is as expected
    assert result == "test_bucket"


@patch("kubernetes.client.CoreV1Api")
@patch("kubernetes.config.load_incluster_config")
def test_get_bucket_from_workspace_exception(
    mock_kubernetes_config_load, mock_kubernetes_client_coreV1Api, capsys
):

    # Create mocks
    mock_CoreV1Api = mock.Mock()
    mock_CoreV1Api.read_namespaced_config_map.side_effect = ApiException(
        status=400, reason="Some Reason"
    )
    mock_kubernetes_client_coreV1Api.return_value = mock_CoreV1Api
    mock_kubernetes_config_load.return_value = None

    result = get_bucket_from_workspace("test_workspace")

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

    expected_message = {
        "id": f"ades-outputs/{test_workspace}",
        "workspace": test_workspace,
        "repository": "",
        "branch": "main",
        "bucket_name": test_bucket,
        "updated_keys": [],
        "added_keys": test_harvest_list,
        "deleted_keys": [],
        "source": test_workspace,
        "target": f"user-datasets/{test_workspace}",
    }

    send_to_ingester(test_harvest_list, test_bucket, test_workspace)

    # Check calls are as expected
    mock_pulsar_producer.send.assert_called_once_with(json.dumps(expected_message).encode("utf-8"))
    mock_pulsar_producer.close.assert_called_once()


class Asset:
    def __init__(self, href):
        self.href = href
        self.absolute_href = os.path.join("/path/to/asset", href)

    def get_absolute_href(self):
        return self.absolute_href


def test_construct_collection_and_upload_assets():

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

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.return_value = None

    # Configure test variables
    cat_id = "test_catalog"
    collection_id = "test_collection"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        collection_id,
        mock_s3_client,
        s3_endpoint,
        subfolder,
        workspace,
        workspace_domain,
        bucket,
    )

    # Check calls are as expected
    # Check upload file call
    assert mock_s3_client.upload_file.call_count == 4
    calls = mock_s3_client.upload_file.call_args_list
    assert calls[0][0] == (
        "/path/to/asset/test_1_href_1",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_1_href_1",
    )
    assert calls[1][0] == (
        "/path/to/asset/test_1_href_2",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_1_href_2",
    )
    assert calls[2][0] == (
        "/path/to/asset/test_2_href_1",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_2_href_1",
    )
    assert calls[3][0] == (
        "/path/to/asset/test_2_href_2",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_2_href_2",
    )

    # Check update href function
    expected_asset_1 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection_id}/test_1_href_1"
    )
    expected_asset_2 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection_id}/test_1_href_2"
    )
    expected_asset_3 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection_id}/test_2_href_1"
    )
    expected_asset_4 = Asset(
        f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}/{cat_id}/{collection_id}/test_2_href_2"
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


@patch("stageout.upload_directory")
def test_construct_collection_and_upload_assets_directory(mock_upload_directory):

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

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.side_effect = IsADirectoryError

    # Configure test variables
    cat_id = "test_catalog"
    collection_id = "test_collection"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        collection_id,
        mock_s3_client,
        s3_endpoint,
        subfolder,
        workspace,
        workspace_domain,
        bucket,
    )

    # Check calls are as expected
    assert mock_upload_directory.call_count == 4
    calls = mock_upload_directory.call_args_list
    assert calls[0][0] == (
        mock_s3_client,
        "/path/to/asset/test_1_href_1",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_1_href_1",
    )
    assert calls[1][0] == (
        mock_s3_client,
        "/path/to/asset/test_1_href_2",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_1_href_2",
    )
    assert calls[2][0] == (
        mock_s3_client,
        "/path/to/asset/test_2_href_1",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_2_href_1",
    )
    assert calls[3][0] == (
        mock_s3_client,
        "/path/to/asset/test_2_href_2",
        s3_endpoint,
        f"{subfolder}/{cat_id}/{collection_id}/test_2_href_2",
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

    mock_s3_client = mock.Mock()
    mock_s3_client.upload_file.side_effect = RuntimeError

    # Configure test variables
    cat_id = "test_catalog"
    collection_id = "test_collection"
    s3_endpoint = "s3://test-endpoint/path"
    subfolder = "test_subfolder"
    workspace = "test_workspace"
    workspace_domain = "test.workspace.domain.com"
    bucket = "test_bucket"

    result = construct_collection_and_upload_assets(
        mock_catalog,
        mock_collection,
        cat_id,
        collection_id,
        mock_s3_client,
        s3_endpoint,
        subfolder,
        workspace,
        workspace_domain,
        bucket,
    )

    # Check exceptions are handled correctly
    error_out = capsys.readouterr().err
    assert f"upload {mock_item_1.id} to {s3_endpoint} at {subfolder} failed" in error_out
    assert f"upload {mock_item_2.id} to {s3_endpoint} at {subfolder} failed" in error_out

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

    result = upload_items(mock_collection, cat_id, collection_id, bucket, subfolder)

    # Check result is not empty
    assert result
    assert result == [
        f"{subfolder}/{cat_id}/{collection_id}/test_item_1_href",
        f"{subfolder}/{cat_id}/{collection_id}/test_item_2_href",
    ]

    # Check calls are as expected
    assert mock_pystac_write_file.call_count == 2
    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_item_1
    assert calls[0][1] == {
        "dest_href": f"s3://{bucket}/{subfolder}/{cat_id}/{collection_id}/test_item_1_href"
    }


@patch("boto3.client")
def test_get_aws_client_with_credentials(mock_boto3_client):

    # Configure test variables
    aws_access_key_id = "test_access_key"
    aws_secret_access_key = "test_secret_key"

    # Configure mocks
    mock_boto3_client.return_value = "example_s3_client"

    result = get_aws_client(
        aws_access_key_id, aws_secret_access_key, "test_endpoint", "test-region"
    )

    # Check client is created
    assert result == "example_s3_client"

    # Check calls are as expected
    mock_boto3_client.assert_called_once_with(
        "s3",
        aws_access_key_id="test_access_key",
        aws_secret_access_key="test_secret_key",
        endpoint_url="test_endpoint",
        region_name="test-region",
    )


@patch("boto3.client")
def test_get_aws_client_no_credentials(mock_boto3_client):

    # Configure mocks
    mock_boto3_client.return_value = "example_s3_client"

    result = get_aws_client(None, None, "test_endpoint", "test-region")

    # Check client is created
    assert result == "example_s3_client"

    # Check calls are as expected
    mock_boto3_client.assert_called_once_with("s3")


class Link:
    def __init__(self, rel, href):
        self.rel = rel
        self.href = href


@patch.dict(
    os.environ,
    {"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "WORKSPACE": "test_workspace"},
)
@patch("stageout.get_aws_client")
@patch("stageout.send_to_ingester")
@patch("stageout.upload_items")
@patch("stageout.get_bucket_from_workspace")
@patch("stageout.construct_collection_and_upload_assets")
@patch("pystac.write_file")
@patch("pystac.Extent")
@patch("pystac.Collection")
@patch("pystac.read_file")
@patch("shutil.copytree")
def test_main_access_point(
    mock_shutil_copytree,
    mock_pystac_read_file,
    mock_pystac_Collection,
    mock_pystac_Extent,
    mock_pystac_write_file,
    mock_construct_collection_and_upload_assets,
    mock_get_bucket_from_workspace,
    mock_upload_items,
    mock_send_to_ingester,
    mock_get_aws_client,
):

    # Configure test variables
    collection_id = "test_collection"
    cat_url = "test_catalog_url"
    bucket = "test_bucket"
    subfolder = "test_subfolder"
    aws_access_key = "test_access_key"
    aws_secret_access_key = "test_secret_key"
    endpoint_url = "s3://test-endpoint"
    aws_region = "test_region"
    s3_endpoint = "s3://test-endpoint/path"
    workspace_domain = "test.workspace.domain.com"

    # Configure mocks
    mock_boto3_client_instance = mock.Mock()
    mock_get_aws_client.return_value = mock_boto3_client_instance

    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_self_href.return_value = "path/to/catalog/catalog.json"

    mock_shutil_copytree.return_value = None

    mock_pystac_read_file.return_value = mock_catalog
    mock_pystac_write_file.return_value = None

    mock_collection = mock.Mock()
    mock_collection.update_extent_from_items.return_value = None
    mock_collection.get_self_href.return_value = (
        f"s3://eodhp-dev-workspaces/{subfolder}/cat_{collection_id}/{collection_id}.json"
    )
    mock_pystac_Collection.return_value = mock_collection

    mock_pystac_Extent.return_value = "some-extent"
    mock_get_bucket_from_workspace.return_value = "test-access-point/test-bucket"

    mock_construct_collection_and_upload_assets.return_value = mock_collection

    mock_upload_items.return_value = ["test_item_1", "test_item_2"]

    mock_send_to_ingester.return_value = None

    main(
        collection_id,
        cat_url,
        bucket,
        subfolder,
        aws_access_key,
        aws_secret_access_key,
        endpoint_url,
        aws_region,
        s3_endpoint,
        workspace_domain,
    )

    # Check calls are as expected
    assert mock_collection.update_extent_from_items.call_count == 1
    assert mock_catalog.clear_items.call_count == 1
    mock_catalog.add_child.assert_called_once_with(mock_collection)
    mock_catalog.normalize_hrefs.assert_called_once_with(
        f"s3://eodhp-dev-workspaces/test_workspace/{subfolder}"
    )
    mock_pystac_Collection.assert_called_once_with(
        id=f"col_{collection_id}",
        description="description",
        extent="some-extent",
        title="Result Collection",
        href=f"s3://eodhp-dev-workspaces/test_workspace/{subfolder}/cat_{collection_id}/col_{collection_id}.json",
        stac_extensions=[],
        keywords=["eoepca"],
        license="proprietary",
    )
    mock_construct_collection_and_upload_assets.assert_called_once_with(
        mock_catalog,
        mock_collection,
        f"cat_{collection_id}",
        f"col_{collection_id}",
        mock_boto3_client_instance,
        "test-access-point/test-bucket",
        f"test_workspace/{subfolder}",
        "test_workspace",
        workspace_domain,
        "eodhp-dev-workspaces",
    )
    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_collection
    assert calls[0][1] == {
        "dest_href": f"s3://eodhp-dev-workspaces/test_workspace/{subfolder}/cat_{collection_id}/test_collection.json"
    }
    assert calls[1][0][0] == mock_catalog
    assert calls[1][1] == {"dest_href": f"path/to/catalog/cat_{collection_id}.json"}
    # Check root link was removed
    assert len(calls[1][0][0].links) == 1

    assert mock_send_to_ingester.call_count == 1
    mock_send_to_ingester.assert_called_once_with(
        [
            "test_item_1",
            "test_item_2",
            f"test_workspace/{subfolder}/cat_{collection_id}/{collection_id}.json",
            f"path/to/catalog/cat_{collection_id}.json",
        ],
        "eodhp-dev-workspaces",
        "test_workspace",
    )


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


@patch.dict(
    os.environ,
    {"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "WORKSPACE": "test_workspace"},
)
@patch("stageout.get_aws_client")
@patch("stageout.send_to_ingester")
@patch("stageout.upload_items")
@patch("stageout.get_bucket_from_workspace")
@patch("stageout.construct_collection_and_upload_assets")
@patch("pystac.write_file")
@patch("pystac.Extent")
@patch("pystac.Collection")
@patch("pystac.read_file")
@patch("shutil.copytree")
@patch("boto3.client")
def test_main_no_access_point(
    mock_boto3_client,
    mock_shutil_copytree,
    mock_pystac_read_file,
    mock_pystac_Collection,
    mock_pystac_Extent,
    mock_pystac_write_file,
    mock_construct_collection_and_upload_assets,
    mock_get_bucket_from_workspace,
    mock_upload_items,
    mock_send_to_ingester,
    mock_get_aws_client,
):

    # Configure test variables
    collection_id = "test_collection"
    cat_url = "test_catalog_url"
    bucket = "test_bucket"
    subfolder = "test_subfolder"
    aws_access_key = "test_access_key"
    aws_secret_access_key = "test_secret_key"
    endpoint_url = "s3://test-endpoint"
    aws_region = "test_region"
    s3_endpoint = "s3://test-endpoint/path"
    workspace_domain = "test.workspace.domain.com"

    # Configure mocks
    mock_boto3_client_instance = mock.Mock()
    mock_get_aws_client.return_value = mock_boto3_client_instance

    mock_catalog = mock.Mock()
    mock_catalog.id = "test_catalog"
    mock_catalog.links = [Link("root", "test_root_href"), Link("self", "test_self_href")]
    mock_catalog.clear_items.return_value = None
    mock_catalog.add_child.return_value = None
    mock_catalog.normalize_hrefs.return_value = None
    mock_catalog.get_self_href.return_value = "path/to/catalog/catalog.json"

    mock_shutil_copytree.return_value = None

    mock_pystac_read_file.return_value = mock_catalog
    mock_pystac_write_file.return_value = None

    mock_collection = mock.Mock()
    mock_collection.update_extent_from_items.return_value = None
    mock_collection.get_self_href.return_value = (
        f"s3://{bucket}/{subfolder}/cat_{collection_id}/{collection_id}.json"
    )
    mock_pystac_Collection.return_value = mock_collection

    mock_pystac_Extent.return_value = "some-extent"

    mock_get_bucket_from_workspace.return_value = None

    mock_construct_collection_and_upload_assets.return_value = mock_collection

    mock_upload_items.return_value = ["test_item_1", "test_item_2"]

    mock_send_to_ingester.return_value = None

    main(
        collection_id,
        cat_url,
        bucket,
        subfolder,
        aws_access_key,
        aws_secret_access_key,
        endpoint_url,
        aws_region,
        s3_endpoint,
        workspace_domain,
    )

    # Check calls are as expected
    assert mock_collection.update_extent_from_items.call_count == 1
    assert mock_catalog.clear_items.call_count == 1
    mock_catalog.add_child.assert_called_once_with(mock_collection)
    mock_catalog.normalize_hrefs.assert_called_once_with(f"s3://{bucket}/{subfolder}")
    mock_pystac_Collection.assert_called_once_with(
        id=f"col_{collection_id}",
        description="description",
        extent="some-extent",
        title="Result Collection",
        href=f"s3://{bucket}/{subfolder}/cat_{collection_id}/col_{collection_id}.json",
        stac_extensions=[],
        keywords=["eoepca"],
        license="proprietary",
    )
    mock_construct_collection_and_upload_assets.assert_called_once_with(
        mock_catalog,
        mock_collection,
        f"cat_{collection_id}",
        f"col_{collection_id}",
        mock_boto3_client_instance,
        s3_endpoint,
        subfolder,
        "test_workspace",
        workspace_domain,
        bucket,
    )
    calls = mock_pystac_write_file.call_args_list
    assert calls[0][0][0] == mock_collection
    assert calls[0][1] == {
        "dest_href": f"s3://{bucket}/{subfolder}/cat_{collection_id}/test_collection.json"
    }
    assert calls[1][0][0] == mock_catalog
    assert calls[1][1] == {"dest_href": f"path/to/catalog/cat_{collection_id}.json"}
    # Check root link was removed
    assert len(calls[1][0][0].links) == 1

    assert mock_send_to_ingester.call_count == 1
    mock_send_to_ingester.assert_called_once_with(
        [
            "test_item_1",
            "test_item_2",
            f"{subfolder}/cat_{collection_id}/{collection_id}.json",
            f"path/to/catalog/cat_{collection_id}.json",
        ],
        "test_bucket",
        "test_workspace",
    )
