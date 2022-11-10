import json
import os
import warnings
from typing import IO, Dict, Optional, Union, cast

from uritools import SplitResult

from minimalkv.fsspecstore import FSSpecStore, FSSpecStoreEntry

try:
    from gcsfs import GCSFileSystem
    from google.auth.credentials import Credentials

    has_gcsfs = True
except ImportError:
    has_gcsfs = False


def _get_project_from_credentials(
    credentials: Union[str, dict, Credentials]
) -> Optional[str]:
    if isinstance(credentials, str):
        with open(credentials) as file:
            credentials = json.load(file)

    if isinstance(credentials, dict):
        return credentials.get("project_id")

    if credentials.hasattr("project_id"):
        return credentials.project_id

    return None


class GoogleCloudStore(FSSpecStore):
    """A store using ``Google Cloud storage`` as a backend.

    See ``https://cloud.google.com/storage``.
    """

    def __init__(
        self,
        bucket_name: str,
        credentials: Optional[Union[str, dict, Credentials]] = None,
        create_if_missing: bool = True,
        bucket_creation_location: str = "EUROPE-WEST3",
        project: Optional[str] = None,
    ):
        """
        Create a new GoogleCloudStore.

        The `credentials` parameter can be
        - a path to a JSON file containing the credentials, e.g. a service account key
        - a dictionary containing the credentials
        - a google.auth.credentials.Credentials object

        If `credentials` is None, the application default credentials will be used.
        The credentials are passed on to gcsfs, see here_ for their documentation.

        .. _here: https://gcsfs.readthedocs.io/en/latest/#credentials

        Parameters
        ----------
        bucket_name: str
            The name of the bucket to store the entries in.
        credentials: Optional[Union[str, dict, Credentials]]
            The credentials to use for the store. See above for details.
        create_if_missing: bool
            If True, the bucket will be created if it does not exist.
        bucket_creation_location: str
            The location to create the bucket in if it does not exist, e.g. "EUROPE-WEST3".
            Only relevant if create_if_missing is True.
        project: Optional[str]
            The project the bucket is in or should be created in.
            If None, gcsfs will try to infer the project from the credentials.
        """
        if project is None and credentials is not None:
            try:
                project = _get_project_from_credentials(credentials)
            except Exception as error:
                warnings.warn(
                    f"""
                    Could not infer the project name from the credentials.
                    You set create_if_missing to `{create_if_missing}`.
                    You might not be able to create a new bucket for this store.

                    This was caused by the following error:
                    {error}
                    """
                )

        self._credentials = credentials
        self.bucket_name = bucket_name
        self.create_if_missing = create_if_missing
        self.bucket_creation_location = bucket_creation_location
        self.project_name = project

        super().__init__(prefix=f"{bucket_name}/", mkdir_prefix=create_if_missing)

    def _create_filesystem(self) -> "GCSFileSystem":
        if not has_gcsfs:
            raise ImportError("Cannot find optional dependency gcsfs.")

        # If self._credentials is None, gcsfs will try to
        # find application default credentials.
        return GCSFileSystem(
            project=self.project_name,
            token=self._credentials,
            access="read_write",
            default_location=self.bucket_creation_location,
        )

    def _open(self, key: str) -> IO:
        from google.cloud.exceptions import NotFound

        if self._prefix_exists is False:
            raise NotFound(f"Could not find bucket: {self.bucket_name}")
        return cast(IO, FSSpecStoreEntry(super()._open(key)))

    def _get_file(self, key: str, file: IO) -> str:
        from google.cloud.exceptions import NotFound

        if self._prefix_exists is False:
            raise NotFound(f"Could not find bucket: {self.bucket_name}")
        return super()._get_file(key, file)

    def __eq__(self, other):
        """Assert that two stores are equal."""
        return (
            isinstance(other, GoogleCloudStore)
            and isinstance(self._credentials, other._credentials.__class__)
            and self.bucket_name == other.bucket_name
            and self.create_if_missing == other.create_if_missing
            and self.bucket_creation_location == other.bucket_creation_location
            and self.project_name == other.project_name
        )

    @classmethod
    def from_url(cls, url: str) -> "GoogleCloudStore":
        """
        Create a ``GoogleCloudStore`` from a URL.

        URl format:
        ``gcs://[credentials@]bucket_name[?<query_args>]``

        **Positional arguments:**

        ``credentials``:
        Optional: A service account to use instead of the default credentials.
        Encode the service account key JSON object as a base64 string.
        See here_ for how to obtain such a JSON object.

        Encode the credentials into base64 like this::

            from pathlib import Path
            import base64
            json_as_bytes = Path(<path_to_json>).read_bytes()
            json_b64_encoded = base64.urlsafe_b64encode(json_as_bytes).decode()

        ``bucket_name``: Name of the bucket the blobs are stored in.

        **Query arguments**:

        ``project``: The name of the GCStorage project. If a credentials JSON is passed then it contains the project name
        and this parameter will be ignored.

        ``create_if_missing``: [optional] Create new bucket to store blobs in if ``bucket_name`` doesn't exist yet.
        (default: ``True``).

        ``bucket_creation_location``: [optional] If a new bucket is created (``create_if_missing=True``),
        the location it will be created in.
        If ``None`` then GCloud uses a default location.

        **Notes**:

        If the scheme is ``hgcs``, an ``HGoogleCloudStore`` is returned which allows ``/`` in key names.

        .. _here: https://cloud.google.com/iam/docs/creating-managing-service-account-keys

        Parameters
        ----------
        url
            URL to create store from.

        Returns
        -------
        store
            GoogleCloudStore created from URL.
        """
        from minimalkv import get_store_from_url

        store = get_store_from_url(url, store_cls=cls)
        if not isinstance(store, cls):
            raise ValueError(f"Expected {cls}, got {type(store)}")
        return store

    @classmethod
    def from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "GoogleCloudStore":
        """
        Build a GoogleCloudStore from a parsed URL.

        See :func:`from_url` for details on the expected format of the URL.

        Parameters
        ----------
        parsed_url: SplitResult
            The parsed URL.
        query: Dict[str, str]
            Query parameters from the URL.

        Returns
        -------
        store : GoogleCloudStore
            The created GoogleCloudStore.
        """
        params = {"bucket_name": parsed_url.gethost()}

        if "project" in query:
            params["project"] = query["project"]

        # Decode credentials
        credentials_base64_string: str = parsed_url.getuserinfo()
        if credentials_base64_string is not None:
            # Get as bytes
            credentials_base64_bytes: bytes = credentials_base64_string.encode()
            import base64

            credentials_bytes: bytes = base64.urlsafe_b64decode(
                credentials_base64_bytes
            )
            # Load as JSON
            credentials_dict = json.loads(credentials_bytes)

            if "project_id" in credentials_dict:
                params["project"] = credentials_dict["project_id"]

            from google.oauth2.service_account import Credentials

            params["credentials"] = Credentials.from_service_account_info(
                credentials_dict,
                scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
            )

        if "project" not in params:
            params["project"] = (
                os.environ.get("CLOUDSDK_PROJECT")
                or os.environ.get("CLOUDSDK_CORE_PROJECT")
                or os.environ.get("GCP_PROJECT")
                or os.environ.get("GCLOUD_PROJECT")
                or os.environ.get("GOOGLE_CLOUD_PROJECT")
            )

        params["create_if_missing"] = (
            query.get("create_if_missing", "true").lower() == "true"
        )

        if "bucket_creation_location" in query:
            params["bucket_creation_location"] = query["bucket_creation_location"]

        return cls(**params)
