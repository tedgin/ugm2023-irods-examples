# -*- coding: utf-8 -*-

"""CyVerse data residency policy"""

import genquery  # type: ignore
import session_vars  # type: ignore

import irods_extra


_HOST_COLL_ATTR = 'ipc::hosted-collection'

_REPL_RESC_ATTR = 'cyverse::replica-resource'

_RESIDENCY_FORCE = 'forced'
_RESIDENCY_PREF = 'preferred'


def get_repl_resc(resc, cb):
    cond = "RESC_NAME = '{}' and META_RESC_ATTR_NAME = '{}'".format(
        resc, _REPL_RESC_ATTR)

    for rec in genquery.Query(cb, 'META_RESC_ATTR_VALUE', cond):
        return rec

    return None


def acSetRescSchemeForCreate(_, cb, rei):
    """Select the resource selection scheme for a new data object's replica.

    Use the value of the ipc::hosted-collection resource AVU to determine which
    resource to use for the replica of a new data object. If the unit is
    'forced', the caller is not allowed to override the chosen resource.
    """
    data_path = session_vars.get_map(rei).get('data_object').get('object_path')
    residency = _RESIDENCY_PREF
    resc = irods_extra.default_resc()

    cols = (
        'ORDER_DESC(META_RESC_ATTR_VALUE)',
        'META_RESC_ATTR_UNITS',
        'RESC_NAME')

    for rec in genquery.Query(
        cb, cols, "META_RESC_ATTR_NAME = '{}'".format(_HOST_COLL_ATTR)
    ):
        if data_path.startswith(rec[0]):
            residency = rec[1].lower()
            resc = rec[2]

            # Since the results are sorted lexicographically by hosted
            # collection path in descending order, the first match is the most
            # specific match. We can stop looking.
            break

    ret = cb.msiSetDefaultResc(
        resc, 'forced' if residency == _RESIDENCY_FORCE else 'preferred')

    if not ret['status']:
        msg = 'failed to set resource scheme to {} {} ({})'.format(
            resc, residency, ret['code'])

        cb.writeLine('serverLog', msg)

    return ret['code']
