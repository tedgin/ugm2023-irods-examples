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

import json

import genquery  # type: ignore
import irods_errors  # type: ignore
import irods_extra
import throttle


_REPL_RESC_ATTR = 'cyverse::replica-resource'


def _query_data_id(data_path, cb):
    [coll_path, data_name] = irods_extra.split_path(data_path)
    for rec in genquery.Query(
        cb,
        'DATA_ID',
        "COLL_NAME = '{}' and DATA_NAME = '{}'".format(coll_path, data_name)
    ):
        return rec

    return None


def _query_data_path(data_id, cb):
    for rec in genquery.Query(
        cb,
        ('COLL_NAME', 'DATA_NAME'),
        "DATA_ID = '{}'".format(data_id),
        genquery.AS_LIST
    ):
        return "{}/{}".format(*rec)

    return None


def _query_repl_resc(resc, cb):
    cond = "RESC_NAME = '{}' and META_RESC_ATTR_NAME = '{}'".format(
        resc, _REPL_RESC_ATTR)

    for rec in genquery.Query(cb, 'META_RESC_ATTR_VALUE', cond):
        return rec

    return None


def _resolve_replicate_issue(code, data_path, dest_resc, cb):
    if (
        code in [
            irods_errors.CAT_NOT_ROWS_FOUND, irods_errors.CAT_UNKNOWN_FILE]
    ):
        cb.writeLine(
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

        cb.writeLine('serverLog', msg_fmt.format(data_path, dest_resc))
        return irods_extra.SUCCESS
    elif code == irods_errors.USER_CHKSUM_MISMATCH:
        msg_fmt = "failed to replicate {}, source replica has bad checksum"
        cb.writeLine('serverLog', msg_fmt.format(data_path))
        return irods_extra.SUCCESS
    else:
        cb.writeLine(
            'serverLog',
            "Failed to replicate {}, trying again later".format(data_path))
        return code


def _replicate_with_path(data_path, dest_resc, cb):
    ret = cb.msiDataObjRepl(
        data_path, "backupRescName={}++++verifyChksum=".format(dest_resc), 0)

    if not ret['status']:
        return _resolve_replicate_issue(ret['code'], data_path, dest_resc, cb)

    return irods_extra.SUCCESS


def _sync_replicas_with_path(data_path, cb):
    ret = cb.msiDataObjRepl(
        data_path, "all=++++updateRepl=++++verifyChksum=", 0)

    if not ret['status']:
        return _resolve_replicate_issue(ret['code'], data_path, None, cb)

    return irods_extra.SUCCESS


def replicate(rule_args, cb, _):
    """Replicate a data object.

    NOTE: This is intended to be called by DelayExec.
    """
    data_id = rule_args[0]
    dest_resc = rule_args[1]
    data_path = _query_data_path(data_id, cb)

    return (
        _replicate_with_path(data_path, dest_resc, cb) if data_path
        else irods_extra.SUCCESS)


def sync_replicas(rule_args, cb, _):
    """Update all data object replicas to current version.

    NOTE: This is intended to be called by DelayExec.
    """
    data_id = rule_args[0]
    data_path = _query_data_path(data_id, cb)

    return (
        _sync_replicas_with_path(data_path, cb) if data_path
        else irods_extra.SUCCESS)


def _sched_repl_task(task, cb):
    cond_fmt = (
        "<INST_NAME>irods_rule_engine_plugin-python-instance</INST_NAME>"
        "<PLUSET>{}s</PLUSET><EF>8h REPEAT UNTIL SUCCESS</EF>")

    cb.delayExec(cond_fmt.format(throttle.next_delay()), task, "")


def _sched_replication(data_id, dest_resc, cb):
    _sched_repl_task(
        "callback.replicate('{}', '{}')".format(data_id, dest_resc), cb)


def _try_replicate(data_path, dest_resc, cb):
    if _replicate_with_path(data_path, dest_resc, cb) < irods_extra.SUCCESS:
        _sched_replication(_query_data_id(data_path, cb), dest_resc, cb)


def _sched_sync_replicas(data_id, cb):
    _sched_repl_task("callback.sync_replicas('{}')".format(data_id), cb)


def _try_sync_replicas(data_path, cb):
    if _sync_replicas_with_path(data_path, cb) < irods_extra.SUCCESS:
        _sched_sync_replicas(_query_data_id(data_path, cb), cb)


def acSetRescSchemeForCreate(_rule_args, cb, _):
    """Use the default resource as primary replica."""
    ret = cb.msiSetDefaultResc(irods_extra.default_resc(), 'forced')
    return ret['code']


def async_api_bulk_data_obj_put_post(rule_args, cb, _):
    """Ensure bulk uploaded data objects have two good replicas.

    Every replica created by bulk upload, gets replicated, and every one that
    gets overriden, gets its peer replica updated. If the first attempt at
    replicating or updating fails, schedule the task to be retried every 8
    hours until it succeeds.
    """
    boi = json.loads(rule_args[2])
    opts = boi['condInput']

    repl_resc = _query_repl_resc(
        irods_extra.root_resc(irods_extra.value(opts, 'resc_hier')), cb)

    objs = [
        boi['attriArray']['sqlResult'][0]['row'][r]
        for r in range(boi['attriArray']['rowCnt'])]

    if irods_extra.has_key(opts, 'forceFlag'):  # noqa
        for obj in objs:
            (coll_path, data_name) = irods_extra.split_path(obj)

            cond = "COLL_NAME = '{}' and DATA_NAME = '{}'".format(
                coll_path, data_name)

            for rec in genquery.Query(
                cb, ('DATA_CREATE_TIME', 'DATA_MODIFY_TIME'), cond
            ):
                if rec[0] == rec[1]:
                    if repl_resc:
                        _try_replicate(obj, repl_resc, cb)
                else:
                    _try_sync_replicas(obj, cb)
    elif repl_resc:
        for obj in objs:
            _try_replicate(obj, repl_resc, cb)

    return irods_extra.SUCCESS


def pep_api_data_obj_copy_post(rule_args, cb, _):
    """Ensure data object created or modified by copying has two good replicas.

    A replica created this way gets asynchrounously replicated. One modified
    this way, gets its peer replica asynchronously updated.
    """
    doci = rule_args[2]
    dest_obj = doci.destDataObjInp
    dest_opts = dest_obj.condInput
    dest_path = str(dest_obj.objPath)

    if irods_extra.value(dest_opts, 'openType') == irods_extra.FILE_CREATE:
        repl_resc = _query_repl_resc(
            irods_extra.root_resc(irods_extra.value(dest_opts, 'resc_hier')),
            cb)

        if repl_resc:
            _sched_replication(_query_data_id(dest_path, cb), repl_resc, cb)
    else:
        _sched_sync_replicas(_query_data_id(dest_path, cb), cb)

    return irods_extra.SUCCESS


def pep_api_data_obj_put_post(rule_args, cb, _):
    """Ensure data object created or modified by upload has two good replicas.

    A replica created this way gets asynchrounously replicated. One modified
    this way, gets its peer replica asynchronously updated.
    """
    doi = rule_args[2]
    opts = doi.condInput
    path = str(doi.objPath)

    if irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE:
        repl_resc = _query_repl_resc(
            irods_extra.root_resc(irods_extra.value(opts, 'resc_hier')), cb)

        if repl_resc:
            _sched_replication(_query_data_id(path, cb), repl_resc, cb)
    else:
        _sched_sync_replicas(_query_data_id(path, cb), cb)

    return irods_extra.SUCCESS


def pep_api_phy_path_reg_post(rule_args, cb, _):
    """Ensure replic added through registration has an up-to-date peer replica.

    If registration created a data object, replicate the original replica. If
    registration added a replica to an existing data object, update the peer
    replica.
    """
    ppri = rule_args[2]
    opts = ppri.condInput
    path = str(ppri.objPath)

    if irods_extra.has_key(opts, 'regRepl'):  # noqa
        _sched_sync_replicas(_query_data_id(path, cb), cb)
    else:
        repl_resc = _query_repl_resc(
            irods_extra.root_resc(irods_extra.value(opts, 'resc_hier')), cb)

        if repl_resc:
            _sched_replication(_query_data_id(path, cb), repl_resc, cb)

    return irods_extra.SUCCESS


def pep_api_touch_post(rule_args, cb, _):
    """Ensure replica created through touching, gets replicated."""
    json_inp = rule_args[2]
    inp = json.loads(str(json_inp.buf))
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

        for rec in genquery.Query(cb, cols, cond):
            if rec[1] == rec[2]:
                repl_resc = _query_repl_resc(irods_extra.root_resc(rec[3]), cb)

                if repl_resc:
                    _sched_replication(rec[0], repl_resc, cb)

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


def pep_api_data_obj_create_post(rule_args, *_):
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
    doi = rule_args[2]
    opts = doi.condInput
    __write_props['data_path'] = str(doi.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')
    __write_props['created'] = True
    __write_props['modified'] = False
    return irods_extra.SUCCESS


def pep_api_data_obj_open_post(rule_args, *_):
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
    doi = rule_args[2]
    flags = str(doi.openFlags)
    opts = doi.condInput
    __write_props['data_path'] = str(doi.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')

    if flags == irods_extra.OPEN_FLAG_R:
        __write_props['created'] = False
        __write_props['modified'] = False
    else:
        __write_props['created'] = (
            irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE)

        __write_props['modified'] = irods_extra.replica_truncated(flags)

    return irods_extra.SUCCESS


def pep_api_replica_open_post(rule_args, *_):
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
    doi = rule_args[2]
    flags = str(doi.openFlags)
    opts = doi.condInput
    __write_props['data_path'] = str(doi.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')

    if flags == irods_extra.OPEN_FLAG_R:
        __write_props['created'] = False
        __write_props['modified'] = False
    else:
        __write_props['created'] = (
            irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE)

        __write_props['modified'] = irods_extra.replica_truncated(flags)

    return irods_extra.SUCCESS


def pep_api_data_obj_write_post(*_):
    """See data_obj_open and replica_opne for more details."""
    __write_props['modified'] = True


def pep_api_data_obj_close_post(_rule_args, cb, _):
    """See data_obj_create and data_obj_open for more details."""
    if __write_props['created']:
        repl_resc = _query_repl_resc(
            irods_extra.root_resc(__write_props['resc_hier']), cb)

        if repl_resc:
            _sched_replication(
                _query_data_id(__write_props['data_path'], cb), repl_resc, cb)
    elif __write_props['modified']:
        _sched_sync_replicas(
            _query_data_id(__write_props['data_path'], cb), cb)

    return irods_extra.SUCCESS


def pep_api_data_obj_close_finally(*_):
    """Reset __write_props in case it's needed again in the current session."""
    __write_props.clear()
    return irods_extra.SUCCESS


def pep_api_replica_close_post(_rule_args, cb, _):  # pyright: ignore
    """See replica_open for details."""
    if __write_props['created']:
        repl_resc = _query_repl_resc(
            irods_extra.root_resc(__write_props['resc_hier']), cb)

        if repl_resc:
            _sched_replication(
                _query_data_id(__write_props['data_path'], cb), repl_resc, cb)
    elif __write_props['modified']:
        _sched_sync_replicas(
            _query_data_id(__write_props['data_path'], cb), cb)

    return irods_extra.SUCCESS


def pep_api_replica_close_finally(*_):
    """Reset __write_props in case it's needed again in this session."""
    __write_props.clear()
    return irods_extra.SUCCESS
