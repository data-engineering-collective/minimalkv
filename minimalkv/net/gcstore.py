import json
import warnings
from typing import IO, Dict, cast

from google.oauth2.service_account import Credentials
from uritools import SplitResult

from minimalkv.fsspecstore import FSSpecStore, FSSpecStoreEntry

try:
    from gcsfs import GCSFileSystem

    has_gcsfs = True
except ImportError:
    has_gcsfs = False


class GoogleCloudStore(FSSpecStore):
    """A store using ``Google Cloud storage`` as a backend.

    See ``https://cloud.google.com/storage``.
    """

    def __init__(
        self,
        credentials,
        bucket_name: str,
        create_if_missing: bool = True,
        bucket_creation_location: str = "EUROPE-WEST3",
        project=None,
    ):
        if isinstance(credentials, str):
            # Parse JSON from path to extract project name
            # The project name is required to create new buckets
            try:
                with open(credentials) as f:
                    credentials_dict = json.load(f)
                    project = project or credentials_dict["project_id"]
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as error:
                warnings.warn(
                    f"""
                    Could not get the project name from the credentials file.
                    You set create_if_missing to {create_if_missing}.
                    You will not be able to create a new bucket for this store.

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
        return (
            isinstance(other, GoogleCloudStore)
            and super().__eq__(other)
            and self._credentials == other._credentials
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
        ``gcs://credentials@bucket_name[?<query_args>]``

        **Positional arguments:**

        ``credentials``: A service account JSON object encoded as base64.
        See here_ for how to obtain such a JSON object.

        Get the encoded credentials as a string like this::

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

        # Decode credentials
        credentials = parsed_url.getuserinfo()
        if credentials is not None:
            # Get as bytes
            credentials = credentials.encode()
            # Decode base64
            import base64

            credentials = base64.urlsafe_b64decode(credentials)
            # Load as JSON
            credentials_dict = json.loads(credentials)
            credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
            )
            params["project"] = credentials_dict["project_id"]
            params["credentials"] = credentials

        params["create_if_missing"] = (
            query.get("create_if_missing", "true").lower() == "true"
        )
        params["bucket_creation_location"] = query.get("bucket_creation_location", None)

        return cls(**params)
