# -*- coding: utf-8 -*-

"""CyVerse data residency policy

This module uses AVUs to set the default data object create and replicate
schemes.

the scheme for data object create depends on the collections it belongs to. To
have the data objects beling to a given collection stored on a certain
resource, Adding an 'ipc::hosted-collection' AVU to that resource with the
value set to the collection's path. The units control the scheme's option.
Setting it to 'forced' means the resource choice cannot be overridden. Setting
it to any other value means the choice can be overridden.

The scheme for data object replicate depends on the resource holding its good
replica. A replication resource is bound to another resource, its primary, so
the choice where a data object is to be replicated depends on the resource
holding its good replica. To bind a replication resource to its primary, set an
'ipc::replica-resource' AVU on the primary resource where the value is the name
of the replication resource. The units control the scheme's option. Setting it
to 'forced' means the resource choice cannot be overridden. Setting it to any
other value means the choice can be overridden.
"""

import genquery  # type: ignore
import session_vars  # type: ignore

import irods_extra

from yoda import rule


_HOST_COLL_ATTR = 'ipc::hosted-collection'

_REPL_RESC_ATTR = 'ipc::replica-resource'

_RESIDENCY_FORCE = 'forced'
_RESIDENCY_PREF = 'preferred'


def _query_create_scheme(ctx, data_path):
    resc = irods_extra.default_resc()
    residency = _RESIDENCY_PREF

    cols = (
        'ORDER_DESC(META_RESC_ATTR_VALUE)',
        'META_RESC_ATTR_UNITS',
        'RESC_NAME')

    for rec in genquery.Query(
        ctx.callback,
        cols,
        "META_RESC_ATTR_NAME = '{}'".format(_HOST_COLL_ATTR)
    ):
        if data_path.startswith(rec[0]):
            residency = rec[1].lower()
            resc = rec[2]

            # Since the results are sorted lexicographically by hosted
            # collection path in descending order, the first match is the most
            # specific match. We can stop looking.
            break

    return resc, residency


def _query_repl_scheme(ctx, resc):
    cond = "RESC_NAME = '{}' and META_RESC_ATTR_NAME = '{}'".format(
        resc, _REPL_RESC_ATTR)

    for rec in genquery.Query(
        ctx.callback, ('META_RESC_ATTR_VALUE', 'META_RESC_ATTR_UNITS'), cond
    ):
        return rec

    return None


def _set_default_scheme(ctx, scheme):
    ret = ctx.msiSetDefaultResc(
        scheme[0], 'forced' if scheme[1] == _RESIDENCY_FORCE else 'preferred')

    return ret['code']


def get_repl_resc(ctx, resc):
    # TODO document
    scheme = _query_repl_scheme(ctx, resc)
    return scheme[0] if scheme else None


@rule.make()
def acSetRescSchemeForCreate(ctx):
    """Set the resource selection scheme for a new data object's replica."""
    scheme = _query_create_scheme(
        ctx,
        session_vars.get_map(ctx.rei).get('data_object').get('object_path'))

    rc = _set_default_scheme(ctx, scheme)

    if rc < irods_extra.SUCCESS:
        msg = 'failed to set create resource scheme to {} {} ({})'.format(
            scheme[0], scheme[1], rc)

        ctx.writeLine('serverLog', msg)

    return rc


# TODO test
@rule.make()
def acSetRescSchemeForRepl(ctx):
    """Set the resource selection scheme for relication."""
    data_path = session_vars.get_map(ctx.rei).get('data_object').get('object_path')
    coll_path, data_name = irods_extra.split_path(data_path)

    cols = ('DATA_RESC_HIER', 'DATA_REPL_STATUS')
    cond = "COLL_NAME = '{}'and DATA_NAME = '{}'".format(coll_path, data_name)

    cur_rescs = [
        (irods_extra.root_resc(rec[0]), rec[1])
        for rec in genquery.Query(ctx.callback, cols, cond)]

    cur_resc = None
    if len(cur_rescs) == 1:
        cur_resc = cur_rescs[0][0]
    elif cur_rescs[0][1] != '1':
        cur_resc = cur_rescs[1][0]
    elif cur_rescs[1][1] != '1':
        cur_resc = cur_rescs[0][0]

    repl_scheme = None
    if cur_resc:
        repl_scheme = _query_repl_scheme(ctx, cur_resc)

    if not repl_scheme:
        repl_scheme = _query_create_scheme(ctx, data_path)

    rc = _set_default_scheme(ctx, repl_scheme)

    if rc < irods_extra.SUCCESS:
        msg = 'Failed tpo set replica resource scheme to {} {} ({})'.format(
            repl_scheme[0], repl_scheme[1], rc)

        ctx.writeLine('serverLog', msg)

    return rc
