import json
from contextlib import contextmanager
from typing import IO, Optional, Tuple, cast

from google.cloud.exceptions import NotFound

from minimalkv.fsspecstore import FSSpecStore, FSSpecStoreEntry
from minimalkv.net._net_common import LAZY_PROPERTY_ATTR_PREFIX, lazy_property


@contextmanager
def map_gcloud_exceptions(
    key: Optional[str] = None, error_codes_pass: Tuple[str, ...] = ()
):
    """
    Map Google Cloud specific exceptions to the minimalkv-API.

    This function exists so the gcstore module can be imported
    without needing to install google-cloud-storage (as we lazily
    import the google library)

    Parameters
    ----------
    key : str, optional, default = None
        Key to be mentioned in KeyError message.
    error_codes_pass : tuple of str
        Errors to be passed.

    """
    from google.cloud.exceptions import GoogleCloudError, NotFound

    try:
        yield
    except NotFound:
        if "NotFound" in error_codes_pass:
            pass
        else:
            raise KeyError(key)
    except GoogleCloudError:
        if "GoogleCloudError" in error_codes_pass:
            pass
        else:
            raise IOError


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
            try:
                with open(credentials) as f:
                    credentials_dict = json.load(f)
                    project = project or credentials_dict["project_id"]
            except (FileNotFoundError, json.JSONDecodeError) as error:
                print(
                    f"Could not get the project name from the credentials file: {error}"
                )

        self._credentials = credentials
        self.bucket_name = bucket_name
        self.create_if_missing = create_if_missing
        self.bucket_creation_location = bucket_creation_location
        self.project_name = project

        super().__init__(
            self._fs, prefix=f"{bucket_name}/", mkdir_prefix=create_if_missing
        )

    def _open(self, key: str) -> IO:
        if not self._prefix_exists:
            raise NotFound(f"Could not find bucket: {self.bucket_name}")
        return cast(IO, FSSpecStoreEntry(super()._open(key)))

    # These exist to allow the store to be pickled even though some properties don't support pickling.
    # We make pickling work by omitting the lazy properties from __getstate__
    # and just (re)creating them when they're used (again).
    # `_bucket` and `_client` are used for testing only.
    # All actual operations are performed using `_fs`.
    @lazy_property
    def _bucket(self):
        if self.create_if_missing and not self._client.lookup_bucket(self.bucket_name):
            return self._client.create_bucket(
                bucket_or_name=self.bucket_name, location=self.bucket_creation_location
            )
        else:
            # will raise an error if bucket not found
            return self._client.get_bucket(self.bucket_name)

    @lazy_property
    def _client(self):
        from google.cloud.storage import Client

        if type(self._credentials) == str:
            return Client.from_service_account_json(self._credentials)
        else:
            return Client(credentials=self._credentials, project=self.project_name)

    @lazy_property
    def _fs(self):
        from gcsfs import GCSFileSystem

        return GCSFileSystem(
            project=self.project_name,
            token=self._credentials,
            access="read_write",
            default_location=self.bucket_creation_location,
        )

    # Skips lazy properties.
    # These will be recreated after unpickling through the lazy_property decorator
    def __getstate__(self):  # noqa D
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith(LAZY_PROPERTY_ATTR_PREFIX)
        }
