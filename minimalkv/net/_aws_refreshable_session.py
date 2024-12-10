# Copyright (c) QuantCo 2024-2024
# SPDX-License-Identifier: LicenseRef-QuantCo

from collections.abc import Coroutine
from datetime import datetime
from logging import getLogger
from typing import Any, Callable, Optional, TypedDict

from aiobotocore.credentials import (
    AioRefreshableCredentials,
    CredentialResolver,
    create_assume_role_refresher,
)
from aiobotocore.session import AioSession
from botocore.credentials import CredentialProvider
from dateutil import tz  # type: ignore[import-untyped]

logger = getLogger(__name__)


class BotoCredentials(TypedDict):
    access_key: str
    secret_key: str
    token: str
    expiry_time: str


class RefreshableAssumeRoleProvider(CredentialProvider):
    """A credential provider that loads self-refreshing credentials using 'sts.assume_role'.

    The credentials are obtained in the '_refresh' method using the given `sts_session`
    and 'assume_role_params' to assume a role.
    Whenever a refresh is needed, the credentials are obtained
    again by assuming the role again through the `sts_session`.

    This means that if the given `sts_session` is expired, the refreshable credentials
    also won't be able to refresh themselves.
    """

    METHOD = "refreshable-assume-role"

    def __init__(
        self,
        sts_session: AioSession,
        assume_role_params: dict,
        advisory_timeout: Optional[int] = None,
        mandatory_timeout: Optional[int] = None,
    ):
        self.sts_session = sts_session
        self._assume_role_params = assume_role_params
        self.advisory_timeout = advisory_timeout
        self.mandatory_timeout = mandatory_timeout

        super().__init__()

    async def load(self) -> AioRefreshableCredentials:
        refresh = self.create_refresh()

        refreshable_credentials = AioRefreshableCredentials.create_from_metadata(
            metadata=await refresh(),
            refresh_using=refresh,
            method=self.METHOD,
            advisory_timeout=self.advisory_timeout,
            mandatory_timeout=self.mandatory_timeout,
        )

        return refreshable_credentials

    def create_refresh(self) -> Callable[[], Coroutine[Any, Any, BotoCredentials]]:
        async def _refresh() -> BotoCredentials:
            logger.info("Refreshing credentials")
            sts_client = self.sts_session.create_client("sts")
            refresh = create_assume_role_refresher(sts_client, self._assume_role_params)

            credentials: BotoCredentials = await refresh()
            to_zone = tz.tzlocal()
            local_expiry_time = (
                datetime.strptime(credentials["expiry_time"], "%Y-%m-%dT%H:%M:%S%Z")
                .replace(tzinfo=tz.tzutc())
                .astimezone(to_zone)
            )
            logger.info(
                f"""Refreshed credentials with access key '{credentials["access_key"]}' and expiry time '{local_expiry_time}'."""
            )
            return credentials

        return _refresh


def create_aio_session_w_refreshable_credentials(
    access_key: str,
    secret_key: str,
    token: Optional[str],
    assume_role_params: dict,
    advisory_timeout: Optional[int] = None,
    mandatory_timeout: Optional[int] = None,
):
    """Create an aio session that knows how to refresh its credentials using 'sts.assume_role'.

    The 'aws_credentials' are used to assume a role (specified by assume_role_params)
    and if refresh is needed, the session will use the credentials to assume the role
    again.

    This means that if the given 'aws_credentials' are expired, the returned session
    also won't be able to refresh its credentials.
    """
    # This session is used to provide the refresh mechanism
    sts_session = AioSession()
    sts_session.set_credentials(
        access_key=access_key,
        secret_key=secret_key,
        token=token,
    )

    # Actual session with refreshable credentials
    session = AioSession()
    resolver = CredentialResolver(
        providers=[
            RefreshableAssumeRoleProvider(
                sts_session, assume_role_params, advisory_timeout, mandatory_timeout
            )
        ]
        # Only offer 1 way of retrieving credentials
        # => Do not need to worry about "side-effects" anymore
    )
    session.register_component(
        "credential_provider", resolver
    )  # "registering" replaces the current credential provider with our resolver

    return session
