# -*- coding: utf-8 -*-

"""CyVerse-wide service account policy."""

import irods_errors  # type: ignore
import session_vars  # type: ignore

import irods_extra


_SVC_TYPE = 'service'


def acCreateUser(_, cb, rei):
    """Create a service type account.

    A service type account doens't have a home or trash collection, and it
    doesn't belong to the public group.
    """
    user_type = session_vars.get_map(rei).get('other_user').get('user_type')

    if user_type == _SVC_TYPE:
        res = cb.msiCreateUser()

        if not res['status']:
            cb.msiRollback()
            return res['code']

        cb.msiCommit()
        return irods_extra.SUCCESS

    return irods_errors.RULE_ENGINE_CONTINUE
