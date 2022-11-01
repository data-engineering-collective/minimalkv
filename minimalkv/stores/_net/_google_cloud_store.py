import json
import warnings
from typing import IO, cast
from urllib.parse import ParseResult

from minimalkv.stores._fsspec_store import FSSpecStore, FSSpecStoreEntry

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

    def from_parsed_url(cls, parsed_url: ParseResult) -> "GoogleCloudStore":
        """
        * ``"gcs"``: Returns a ``minimalkv.net.gcstore.GoogleCloudStore``.  Parameters are
          ``"credentials"``, ``"bucket_name"``, ``"bucket_creation_location"``, ``"project"`` and ``"create_if_missing"`` (default: ``True``).

          - ``"credentials"``: either the path to a credentials.json file or a *google.auth.credentials.Credentials* object
          - ``"bucket_name"``: Name of the bucket the blobs are stored in.
          - ``"project"``: The name of the GCStorage project. If a credentials JSON is passed then it contains the project name
            and this parameter will be ignored.
          - ``"create_if_missing"``: [optional] Create new bucket to store blobs in if ``"bucket_name"`` doesn't exist yet. (default: ``True``).
          - ``"bucket_creation_location"``: [optional] If a new bucket is created (create_if_missing=True), the location it will be created in.
            If ``None`` then GCloud uses a default location.
        * ``"hgcs"``: Like ``"gcs"`` but "/" are allowed in the keynames.

        credentials_b64 = userinfo
        params = {"type": scheme, "bucket_name": host}
        params["credentials"] = base64.urlsafe_b64decode(credentials_b64.encode())
        if "bucket_creation_location" in query:
            params["bucket_creation_location"] = query.pop("bucket_creation_location")[
                0
            ]
        return params
        """
        pass
