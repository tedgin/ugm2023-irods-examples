# -*- coding: utf-8 -*-

"""CyVerse replication logic.

This module ensure that each data object has two up-to-date replicas as long as
this data object has no replicas with with an invalid checksum.

This rule base uses the resource set by the "default_resource_name" field in
/etc/irods/server_config.json as the resource used to store the first replica
of a newly created data object. It is also know as the primary resource.  To
determine the resource to use for the second replica, the rule base uses the
value of the AVU "cyverse::replica-resource" that is attached to the primary
resource. If this AVU is not present, no replication will occur.

NOTE: This rule base assumes that every replica has a checksum, so the
corresponding checksum rules should be successfully executed first.
"""

import genquery  # type: ignore
import irods_errors  # type: ignore
import json

from yoda import rule

from .. import irods_extra
from .. import throttle

import residency


def _query_data_id(ctx, data_path):
    [coll_path, data_name] = irods_extra.split_path(data_path)
    for rec in genquery.Query(
        ctx.callback,
        'DATA_ID',
        "COLL_NAME = '{}' and DATA_NAME = '{}'".format(coll_path, data_name)
    ):
        return rec

    return None


def _query_data_path(ctx, data_id):
    for rec in genquery.Query(
        ctx.callback,
        ('COLL_NAME', 'DATA_NAME'),
        "DATA_ID = '{}'".format(data_id),
        genquery.AS_LIST
    ):
        return "{}/{}".format(*rec)

    return None


def _resolve_replicate_issue(ctx, code, data_path, dest_resc):
    if (
        code in [
            irods_errors.CAT_NOT_ROWS_FOUND, irods_errors.CAT_UNKNOWN_FILE]
    ):
        ctx.writeLine(
            'serverLog',
            "failed to replicate {}, no longer exists".format(data_path))
        return irods_extra.SUCCESS
    elif code == irods_errors.SYS_NOT_ALLOWED:
        # Replicas already up to date
        return irods_extra.SUCCESS
    elif code == irods_errors.SYS_RESC_DOES_NOT_EXIST:
        msg_fmt = (
            "failed to replicate {}, destination resource {} no longer exists"
        )

        ctx.writeLine('serverLog', msg_fmt.format(data_path, dest_resc))
        return irods_extra.SUCCESS
    elif code == irods_errors.USER_CHKSUM_MISMATCH:
        msg_fmt = "failed to replicate {}, source replica has bad checksum"
        ctx.writeLine('serverLog', msg_fmt.format(data_path))
        return irods_extra.SUCCESS
    else:
        ctx.writeLine(
            'serverLog',
            "Failed to replicate {}, trying again later".format(data_path))
        return code


def _replicate_with_path(ctx, data_path, dest_resc):
    ret = ctx.msiDataObjRepl(
        data_path, "backupRescName={}++++verifyChksum=".format(dest_resc), 0)

    if not ret['status']:
        return _resolve_replicate_issue(ctx, ret['code'], data_path, dest_resc)

    return irods_extra.SUCCESS


def _sync_replicas_with_path(ctx, data_path):
    ret = ctx.msiDataObjRepl(
        data_path, "all=++++updateRepl=++++verifyChksum=", 0)

    if not ret['status']:
        return _resolve_replicate_issue(ctx, ret['code'], data_path, None)

    return irods_extra.SUCCESS


@rule.make([1,2])
def replicate(ctx, data_id, dest_resc):
    """Replicate a data object.

    NOTE: This is intended to be called by DelayExec.
    """
    data_path = _query_data_path(ctx, data_id)

    return (
        _replicate_with_path(ctx, data_path, dest_resc) if data_path
        else irods_extra.SUCCESS)


@rule.make([1])
def sync_replicas(ctx, data_id):
    """Update all data object replicas to current version.

    NOTE: This is intended to be called by DelayExec.
    """
    data_path = _query_data_path(ctx, data_id)

    return (
        _sync_replicas_with_path(ctx, data_path) if data_path
        else irods_extra.SUCCESS)


def _sched_repl_task(ctx, task):
    cond_fmt = (
        "<INST_NAME>irods_rule_engine_plugin-python-instance</INST_NAME>"
        "<PLUSET>{}s</PLUSET><EF>8h REPEAT UNTIL SUCCESS</EF>")

    ctx.delayExec(cond_fmt.format(throttle.next_delay()), task, "")


def _sched_replication(ctx, data_id, dest_resc):
    _sched_repl_task(
        ctx, "callback.replicate('{}', '{}')".format(data_id, dest_resc))


def _try_replicate(ctx, data_path, dest_resc):
    if _replicate_with_path(ctx, data_path, dest_resc) < irods_extra.SUCCESS:
        _sched_replication(ctx, _query_data_id(ctx, data_path), dest_resc)


def _sched_sync_replicas(ctx, data_id):
    _sched_repl_task(ctx, "callback.sync_replicas('{}')".format(data_id))


def _try_sync_replicas(ctx, data_path):
    if _sync_replicas_with_path(ctx, data_path) < irods_extra.SUCCESS:
        _sched_sync_replicas(ctx, _query_data_id(ctx, data_path))


@rule.make(inputs=[0,1,2,3])
def async_api_bulk_data_obj_put_post(
    ctx, _instance, _comm, bulk_opr_inp_json, _  # pyright: ignore
):
    """Ensure bulk uploaded data objects have two good replicas.

    Every replica created by bulk upload, gets replicated, and every one that
    gets overriden, gets its peer replica updated. If the first attempt at
    replicating or updating fails, schedule the task to be retried every 8
    hours until it succeeds.
    """
    boi = json.loads(bulk_opr_inp_json)
    opts = boi['condInput']

    repl_resc = residency.get_repl_resc(
        ctx, irods_extra.root_resc(irods_extra.value(opts, 'resc_hier')))

    objs = [
        boi['attriArray']['sqlResult'][0]['row'][r]
        for r in range(boi['attriArray']['rowCnt'])]

    if irods_extra.has_key(opts, 'forceFlag'):  # noqa
        for obj in objs:
            (coll_path, data_name) = irods_extra.split_path(obj)

            cond = "COLL_NAME = '{}' and DATA_NAME = '{}'".format(
                coll_path, data_name)

            for rec in genquery.Query(
                ctx.callback, ('DATA_CREATE_TIME', 'DATA_MODIFY_TIME'), cond
            ):
                if rec[0] == rec[1]:
                    if repl_resc:
                        _try_replicate(ctx, obj, repl_resc)
                else:
                    _try_sync_replicas(ctx, obj)
    elif repl_resc:
        for obj in objs:
            _try_replicate(ctx, obj, repl_resc)

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2,3])
def pep_api_data_obj_copy_post(ctx, _instance, _comm, data_obj_copy_inp, _):  # pyright: ignore
    """Ensure data object created or modified by copying has two good replicas.

    A replica created this way gets asynchrounously replicated. One modified
    this way, gets its peer replica asynchronously updated.
    """
    dest_obj = data_obj_copy_inp.destDataObjInp
    dest_opts = dest_obj.condInput
    dest_path = str(dest_obj.objPath)

    if irods_extra.value(dest_opts, 'openType') == irods_extra.FILE_CREATE:
        repl_resc = residency.get_repl_resc(
            ctx,
            irods_extra.root_resc(irods_extra.value(dest_opts, 'resc_hier')))

        if repl_resc:
            _sched_replication(ctx, _query_data_id(ctx, dest_path), repl_resc)
    else:
        _sched_sync_replicas(ctx, _query_data_id(ctx, dest_path))

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2,3], outputs=[4])
def pep_api_data_obj_put_post(ctx, _instance, _comm, data_obj_inp, _):  # pyright: ignore
    """Ensure data object created or modified by upload has two good replicas.

    A replica created this way gets asynchrounously replicated. One modified
    this way, gets its peer replica asynchronously updated.
    """
    opts = data_obj_inp.condInput
    path = str(data_obj_inp.objPath)

    if irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE:
        repl_resc = residency.get_repl_resc(
            ctx, irods_extra.root_resc(irods_extra.value(opts, 'resc_hier')))

        if repl_resc:
            _sched_replication(ctx, _query_data_id(ctx, path), repl_resc)
    else:
        _sched_sync_replicas(ctx, _query_data_id(ctx, path))

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2])
def pep_api_phy_path_reg_post(ctx, _instance, _, phy_path_reg_inp):  # pyright: ignore
    """Ensure replic added through registration has an up-to-date peer replica.

    If registration created a data object, replicate the original replica. If
    registration added a replica to an existing data object, update the peer
    replica.
    """
    opts = phy_path_reg_inp.condInput
    path = str(phy_path_reg_inp.objPath)

    if irods_extra.has_key(opts, 'regRepl'):  # noqa
        _sched_sync_replicas(ctx, _query_data_id(ctx, path))
    else:
        repl_resc = residency.get_repl_resc(
            ctx, irods_extra.root_resc(irods_extra.value(opts, 'resc_hier')))

        if repl_resc:
            _sched_replication(ctx, _query_data_id(ctx, path), repl_resc)

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2])
def pep_api_touch_post(ctx, _instance, _, json_input):  # pyright: ignore
    """Ensure replica created through touching, gets replicated."""
    inp = json.loads(str(json_input.buf))
    opts = inp['options']

    if (
        not opts['no_create'] and
        'replica_number' not in opts or
        'leaf_resource_name' not in opts
    ):
        coll_path, data_name = irods_extra.split_path(inp['logical_path'])

        cols = (
            "DATA_ID",
            'DATA_CREATE_TIME',
            'DATA_MODIFY_TIME',
            'DATA_RESC_HIER')

        cond = "COLL_NAME = '{}' and DATA_NAME = '{}'".format(
            coll_path, data_name)

        for rec in genquery.Query(ctx.callback, cols, cond):
            if rec[1] == rec[2]:
                repl_resc = residency.get_repl_resc(
                    ctx, irods_extra.root_resc(rec[3]))

                if repl_resc:
                    _sched_replication(ctx, rec[0], repl_resc)

    return irods_extra.SUCCESS


# NOTE: Ideally, this would be a map from data objects to property sets, since
# multiple data objects can be uploaded concurrently using
# data_obj_create/data_obj_open + data_obj_write + data_obj_close or
# replica_open + data_obj_write + data_obj_close. Unfortunately, for the
# data_obj_write doesn't know the data object's Id or path, and data_obj_open
# doesn't know the data objects l1descInx, so there's know way to be sure that
# data object handled by a given data_obj_open call is the same as that being
# handled by a subsequent data_obj_write call.
__write_props = {}


@rule.make(inputs=[0,1,2])
def pep_api_data_obj_create_post(_ctx, _instance, _, data_obj_inp):  # pyright: ignore
    """Ensure data object added through creation has two up-to-date replicas.

    The work will be done in data_obj_close. To pass the required information
    along to data_obj_close, the following entries ared added to the global
    dict __write_props.

    'data_path'  the absolute path to the data object
    'resc_hier'  the storage resource of the original replica
    'created'    True (indicates the data object was created)
    'modified'   False (indicates the data object wasn't modified after
                 creation)
    """
    opts = data_obj_inp.condInput
    __write_props['data_path'] = str(data_obj_inp.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')
    __write_props['created'] = True
    __write_props['modified'] = False
    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2])
def pep_api_data_obj_open_post(_ctx, _instance, _, data_obj_inp):  # pyright: ignore
    """Ensure data object editted by writing has two up-to-date replicas.

    If a data object was created, replica its original replica. If a data
    object was modified, ensure the unmodified replica is updated. The work
    will be done in data_obj_write and data_obj_close. To pass the required
    information along to these rules, the following entries are added to the
    global dict __write_props.

    'data_path'  the absolute path to the data object
    'resc_hier'  the storage resource of the created or modified replica
    'created'    whether or not the data object was created
    'modified'   whether or not the data object has been modified
    """
    flags = str(data_obj_inp.openFlags)
    opts = data_obj_inp.condInput
    __write_props['data_path'] = str(data_obj_inp.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')

    if flags == irods_extra.OPEN_FLAG_R:
        __write_props['created'] = False
        __write_props['modified'] = False
    else:
        __write_props['created'] = (
            irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE)

        __write_props['modified'] = irods_extra.replica_truncated(flags)

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2], outputs=[3])
def pep_api_replica_open_post(_ctx, _instance, _, data_obj_inp):  # pyright: ignore
    """Ensure data object editted by replica API has two up-to-date replicas.

    If a data object was created, replica its original replica. If a data
    object was modified, ensure the unmodified replica is updated. The work
    will be done in data_obj_write and replica_close. To pass the required
    information along to these rules, the following entries are added to the
    global dict __write_props.

    'data_path'  the absolute path to the data object
    'resc_hier'  the storage resource of the created or modified replica
    'created'    whether or not the data object was created
    'modified'   whether or not the data object has been modified
    """
    flags = str(data_obj_inp.openFlags)
    opts = data_obj_inp.condInput
    __write_props['data_path'] = str(data_obj_inp.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')

    if flags == irods_extra.OPEN_FLAG_R:
        __write_props['created'] = False
        __write_props['modified'] = False
    else:
        __write_props['created'] = (
            irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE)

        __write_props['modified'] = irods_extra.replica_truncated(flags)

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2,3])
def pep_api_data_obj_write_post(
    _ctx, _instance, _comm, _data_obj_write_inp, _  # pyright: ignore
):
    """See data_obj_open and replica_opne for more details."""
    __write_props['modified'] = True


@rule.make(inputs=[0,1,2])
def pep_api_data_obj_close_post(ctx, _instance, _comm, _):  # pyright: ignore
    """See data_obj_create and data_obj_open for more details."""
    if __write_props['created']:
        repl_resc = residency.get_repl_resc(
            ctx, irods_extra.root_resc(__write_props['resc_hier']))

        if repl_resc:
            _sched_replication(
                ctx,
                _query_data_id(ctx, __write_props['data_path']),
                repl_resc)
    elif __write_props['modified']:
        _sched_sync_replicas(
            ctx, _query_data_id(ctx, __write_props['data_path']))

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2])
def pep_api_data_obj_close_finally(_ctx, _instance, _comm, _):  # pyright: ignore
    """Reset __write_props in case it's needed again in the current session."""
    __write_props.clear()
    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2])
def pep_api_replica_close_post(ctx, _instance, _, json_input):  # pyright: ignore
    """See replica_open for details."""
    if __write_props['created']:
        repl_resc = residency.get_repl_resc(
            ctx, irods_extra.root_resc(__write_props['resc_hier']))

        if repl_resc:
            _sched_replication(
                ctx,
                _query_data_id(ctx, __write_props['data_path']),
                repl_resc)
    elif __write_props['modified']:
        _sched_sync_replicas(
            ctx, _query_data_id(ctx, __write_props['data_path']))

    return irods_extra.SUCCESS


@rule.make(inputs=[0,1,2])
def pep_api_replica_close_post(ctx, _instance, _comm, _):  # pyright: ignore
    """Reset __write_props in case it's needed again in this session."""
    __write_props.clear()
    return irods_extra.SUCCESS
