# -*- coding: utf-8 -*-

"""CyVerse-wide service account policy.

A service account has a user type of 'ds-service'. It has no home or trash
collection, and it is not a member of the public group.
"""

import irods_errors  # type: ignore
import session_vars  # type: ignore

import irods_extra


_SVC_TYPE = 'ds-service'


def acCreateUser(_, cb, rei):
    """Create a service type account."""
    user_type = session_vars.get_map(rei).get('other_user').get('user_type')

    if user_type == _SVC_TYPE:
        res = cb.msiCreateUser()

        if not res['status']:
            cb.msiRollback()
            return res['code']

        cb.msiCommit()
        return irods_extra.SUCCESS

    return irods_errors.RULE_ENGINE_CONTINUE
