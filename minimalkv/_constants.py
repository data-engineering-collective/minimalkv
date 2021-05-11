import re

FOREVER = "forever"
NOT_SET = "not_set"

VALID_NON_NUM = r"""\`\!"#$%&'()+,-.<=>?@[]^_{}~"""
VALID_KEY_REGEXP = "^[%s0-9a-zA-Z]+$" % re.escape(VALID_NON_NUM)
"""This regular expression tests if a key is valid. Allowed are all
alphanumeric characters, as well as ``!"`#$%&'()+,-.<=>?@[]^_{}~``."""

VALID_KEY_RE = re.compile(VALID_KEY_REGEXP)
"""A compiled version of :data:`~minimalkv._constants.VALID_KEY_REGEXP`."""

VALID_NON_NUM_EXTENDED = VALID_NON_NUM + r"/ "
VALID_KEY_REGEXP_EXTENDED = "^[%s0-9a-zA-Z]+$" % re.escape(VALID_NON_NUM_EXTENDED)
"""This regular expression tests if a key is valid when the extended keyspace mixin is used.
Allowed are all alphanumeric characters, as well as ``!"`#$%&'()+,-.<=>?@[]^_{}~/``. and spaces"""
VALID_KEY_RE_EXTENDED = re.compile(VALID_KEY_REGEXP_EXTENDED)
"""A compiled version of :data:`~minimalkv._constants.VALID_KEY_REGEXP_EXTENDED`."""
