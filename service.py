# -*- coding: utf-8 -*-

"""CyVerse-wide service account policy.

A service account has a user type of 'ds-service'. It has no home or trash
collection, and it is not a member of the public group.
"""

import irods_errors  # type: ignore
import session_vars  # type: ignore

import irods_extra
import rule


_SVC_TYPE = 'ds-service'


@rule.make()
def acCreateUser(ctx):
    """Create a service type account."""
    user_type = session_vars.get_map(ctx.rei).get('other_user').get('user_type')

    if user_type == _SVC_TYPE:
        res = ctx.msiCreateUser()

        if not res['status']:
            ctx.msiRollback()
            return res['code']

        ctx.msiCommit()
        return irods_extra.SUCCESS

    return irods_errors.RULE_ENGINE_CONTINUE
