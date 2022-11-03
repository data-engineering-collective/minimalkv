from typing import Optional

from uritools import SplitResult


def get_username(split_result: SplitResult) -> Optional[str]:
    if not split_result.getuserinfo():
        return None
    return split_result.getuserinfo().split(':')[0]


def get_password(split_result: SplitResult) -> Optional[str]:
    if ':' not in split_result.getuserinfo():
        return None
    return split_result.getuserinfo().split(':')[1]
