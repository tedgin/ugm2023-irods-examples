# -*- coding: utf-8 -*-

"""Support functions and constants for the iRODS Python Rule Engine."""

import json


"""indicates that a file was created"""
FILE_CREATE = '1'

"""indicates that a file was opened for reading"""
FILE_OPEN_READ = '2'

"""indicates that a file was opened for writing"""
FILE_OPEN_WRITE = '3'


"""
indicates that a replica's open mode is 'a' no create, i.e., write-only, where
writes append
"""
OPEN_FLAG_A = '1'

"""
indicates that a replica's open mode is 'a' create, i.e., write-only, where the
replica need not exist and writes append
"""
OPEN_FLAG_A_CREATE = '65'

"""
indicates that a replica's open mode is 'a+' create, i.e., read-write, where
the replica need not exist and writes append
"""
OPEN_FLAG_AP_CREATE = '66'

"""indicates that a replica's open mode is 'r', i.e., read-only"""
OPEN_FLAG_R = '0'

"""
indicates that a replica's open mode is 'r+' or 'a+' no create, i.e.,
read-write, where writes append
"""
OPEN_FLAG_RP = '2'

"""
indicates that a replicas's open mode is 'w' no create, i.e., write-only,
where the replica is truncated
"""
OPEN_FLAG_W = '513'

"""
indicates that a replica's open mode is 'w' create, i.e., write-only, where the
replica need not exist, but if it does, it will be truncated
"""
OPEN_FLAG_W_CREATE = '577'

"""
indicates that a replica's open mode is 'w+' no create, i.e., read-write, where
the replica is truncated
"""
OPEN_FLAG_WP = '514'

"""
indicates that a replica's open mode is 'w+' create, i.e., read-write, where
the replica need not exist, but if it does, it will be truncated
"""
OPEN_FLAG_WP_CREATE = '578'


def replica_truncated(open_flags):
    """Determine if a data object was truncted on open.

    Parameters:
        open_flags  the open flag set
    """
    return open_flags in [
        OPEN_FLAG_W, OPEN_FLAG_W_CREATE, OPEN_FLAG_WP, OPEN_FLAG_WP_CREATE]


"""indicates that a rule succeeded"""
SUCCESS = 0


def has_key(kv_map, key):
    """Test to see if the given KeyValMap contains the given key."""
    try:
        kv_map[key]
        return True
    except KeyError:
        return False


def value(kv_map, key):
    """Retrieve the value of the given key from the given KeyValueMap.

    If the key isn't found, it returns the empty string
    """
    try:
        return str(kv_map[key])
    except KeyError:
        return ""


def split_path(data_path):
    """Split a data object path into parent collection path and data name.

    Given the absolute path to a data object, it returns a tuple containing the
    absolute path to the object's collection and the object's name.
    """
    return tuple(data_path.rsplit('/', 1))


def default_resc():
    """Return the default_resource_name from server_config.json."""
    with open('/etc/irods/server_config.json') as f:
        server_config = json.load(f)
        return server_config['default_resource_name']


def root_resc(resc_hier):
    """Determine the root resource from a resource hierarchy."""
    res = resc_hier.split(';', 1)
    return res[0]
