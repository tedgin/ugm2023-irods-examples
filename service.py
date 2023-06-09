# -*- coding: utf-8 -*-

"""CyVerse-wide service account policy."""

import irods_extra
import session_vars  # type: ignore


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

        res = cb.msiCommit()

    return irods_extra.SUCCESS
