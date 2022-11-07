from typing import Optional

from uritools import SplitResult


def get_username(split_result: SplitResult) -> Optional[str]:
    userinfo = split_result.getuserinfo()
    if not userinfo:
        return None
    return userinfo.split(":")[0]


def get_password(split_result: SplitResult) -> Optional[str]:
    userinfo = split_result.getuserinfo()
    if not userinfo or ":" not in userinfo:
        return None
    return userinfo.split(":")[1]
