# -*- coding: utf-8 -*-

"""CyVerse checksum logic.

This module ensures that every replica of every data object has a checksum.
"""

import json

import genquery  # type: ignore

import irods_extra


def _ensure_replicas_checksum(cb, data_path, resc_hier=""):
    if resc_hier == '':
        ret = cb.msiDataObjChksum(data_path, "ChksumAll=", '')

        if not ret['status']:
            msg_fmt = "Failed to generate checksum for the replicas of {} ({}}"
            msg = msg_fmt.format(data_path, ret['code'])
            cb.writeLine('serverLog', msg)

        return ret
    else:
        res = irods_extra.SUCCESS
        (coll_path, data_name) = irods_extra.split_path(data_path)

        cond_fmt = (
            "COLL_NAME = '{}' and "
            "DATA_NAME = '{}' and "
            "DATA_RESC_HIER = '{}'")

        cond = cond_fmt.format(coll_path, data_name, resc_hier)

        for rec in genquery.Query(cb, 'DATA_REPL_NUM', cond):
            ret = cb.msiDataObjChksum(data_path, "replNum={}".format(rec), '')

            if not ret['status']:
                msg_fmt = (
                    "Failed to generate checksums for the replicas of {} "
                    "on {} "
                    "({})")

                msg = msg_fmt.format(data_path, resc_hier, ret['code'])
                cb.writeLine('serverLog', msg)

                if res == irods_extra.SUCCESS:
                    res = ret['code']

        return res


def async_api_bulk_data_obj_put_post(rule_args, cb, _):
    """Ensure every replica created or updated by bulk upload has a checksum.

    If neither BulkOpInp.regChksum nor BulkOpInp.verifyChksum exist, calculate
    the checksum of replica on BulkOpInp.resc_hier for each entry of
    BulkOpInp.logical_path.
    """
    boi = json.loads(rule_args[2])
    opts = boi['condInput']
    res = irods_extra.SUCCESS

    if (
        not irods_extra.has_key(opts, 'regChksum') and  # noqa
        not irods_extra.has_key(opts, 'verifyChksum')
    ):
        objs = [
            boi['attriArray']['sqlResult'][0]['row'][r]
            for r in range(boi['attriArray']['rowCnt'])]

        resc = irods_extra.value(opts, 'resc_hier')

        for obj in objs:
            ret = _ensure_replicas_checksum(cb, obj, resc)

            if ret < irods_extra.SUCCESS and res == irods_extra.SUCCESS:
                res = ret

    return res


def pep_api_data_obj_copy_post(rule_args, cb, _):
    """Ensure every replica created or updated by copying has a checksum.

    If neither DataObjCopyInp.regChksum nor DataObjCopyInp.verifyChksum exist,
    calculate the checksum of DataObjCopyInp.dst_obj_path on
    DataObjCopyInp.dst_resc_hier.
    """
    doci = rule_args[2]
    dest_obj = doci.destDataObjInp
    dest_opts = dest_obj.condInput

    if (
        irods_extra.has_key(dest_opts, 'regChksum') or  # noqa
        irods_extra.has_key(dest_opts, 'verifyChksum')
    ):
        return irods_extra.SUCCESS

    return _ensure_replicas_checksum(
        cb, str(dest_obj.objPath), irods_extra.value(dest_opts, 'resc_hier'))


def pep_api_data_obj_put_post(rule_args, cb, _):
    """Ensure every replica created or updated by uploading has a checksum.

    If neither DataObjInp.regChksum nor DataObjInp.verifyChksum exist,
    calculate the checksum of DataObjInp.obj_path on DataObjInp.resc_hier.
    """
    doi = rule_args[2]
    opts = doi.condInput

    if (
        irods_extra.has_key(opts, 'regChksum') or  # noqa
        irods_extra.has_key(opts, 'verifyChksum')
    ):
        return irods_extra.SUCCESS

    return _ensure_replicas_checksum(
        cb, str(doi.objPath), irods_extra.value(opts, 'resc_hier'))


def pep_api_phy_path_reg_post(rule_args, cb, _):
    """Ensure every replica added through registration has a checksum.

    If none of PhyPathRegInp.regRepl, PhyPathRegInp.regChksum, or
    PhyPathRegInp.verifyChksum are set, calculate the checksum of replica of
    PhyPathRegInp.obj_path on PhyPathRegInp.resc_hier.
    """
    ppri = rule_args[2]
    opts = ppri.condInput

    if (
        irods_extra.has_key(opts, 'regRepl') or  # noqa
        irods_extra.has_key(opts, 'regChksum') or
        irods_extra.has_key(opts, 'verifyChksum')
    ):
        return irods_extra.SUCCESS

    return _ensure_replicas_checksum(
        cb, str(ppri.objPath), irods_extra.value(opts, 'resc_hier'))


def pep_api_touch_post(rule_args, cb, _):
    """Ensure every replica created through touching has a checksum.

    Check to see if JsonInput.buf.options.no_create is false. If it is, check
    to see if neither options.replica_number nor options.leaf_resource_name is
    set. If that's the case, check to see if the data object's 0 replica has a
    checksum. If it doesn't compute its checksum.
    """
    json_inp = rule_args[2]
    inp = json.loads(str(json_inp.buf))
    opts = inp['options']

    if (
        opts['no_create'] or
        'replica_number' in opts or
        'leaf_resource_name' in opts
    ):
        return irods_extra.SUCCESS

    data_path = inp['logical_path']
    (coll_path, data_name) = irods_extra.split_path(data_path)
    cond = "COLL_NAME = '{}' and DATA_NAME = '{}'".format(coll_path, data_name)

    for rec in genquery.Query(
        cb, ("DATA_CHECKSUM", "DATA_RESC_HIER"), cond
    ):
        return (
            _ensure_replicas_checksum(cb, data_path, rec[1])
            if rec[0] == ''
            else irods_extra.SUCCESS)


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
    """Ensure every data object added through creation has a checksum.

    Always compute the checksum. Store the path to the data object and the
    selected resource hierarchy for its replica in __write_props using the keys
    'data_path' and 'resc_hier', respectively. Also, set the key
    'needs_checksum' to True. If needed, data_obj_close will use these keys to
    compute the checksum of the indicated replica.
    """
    doi = rule_args[2]
    opts = doi.condInput
    __write_props['data_path'] = str(doi.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')
    __write_props['needs_checksum'] = True
    return irods_extra.SUCCESS


def pep_api_data_obj_open_post(rule_args, *_):
    """Ensure every data object created or modified by opening has a checksum.

    A checksum can only be computed after the replica has been modified, so
    this needs to happen in data_obj_close. Only when a change occurs does a
    checksum need to be computed. A change won't occur if the open mode is
    'r'. If the mode isn't 'r', store the data object's path and the resource
    holding its replica for use by data_obj_close. A change definitely occurs
    when a replica is created or truncated, so if this happens, store a flag to
    let data_obj_close know that it needs to perform a checksum. If a replica
    is written to, it has also been modified, so  data_obj_write stores a flag
    to let data_obj_close know this has happened.
    """
    doi = rule_args[2]
    flags = str(doi.openFlags)
    opts = doi.condInput
    __write_props['data_path'] = str(doi.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')

    if flags == irods_extra.OPEN_FLAG_R:
        __write_props['needs_checksum'] = False
    else:
        __write_props['needs_checksum'] = (
            irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE or
            irods_extra.replica_truncated(flags))

    return irods_extra.SUCCESS


def pep_api_replica_open_post(rule_args, *_):
    """Ensure a replica created or modified through replica API has checksum.

    When replica_open is called, store DataObjInp.destRescName and
    DataObjInp.obj_path in __write_props. If a data object is created or
    truncated, or if data_obj_write is called, it is assumed the data object
    has been modified. When replica_close is called, if the data object was
    modified and if JsonInput.buf.compute_checksum isn't true, it will
    compute the checksum of obj_path on destRescName.
    """
    doi = rule_args[2]
    flags = str(doi.openFlags)
    opts = doi.condInput
    __write_props['data_path'] = str(doi.objPath)
    __write_props['resc_hier'] = irods_extra.value(opts, 'resc_hier')

    if flags == irods_extra.OPEN_FLAG_R:
        __write_props['needs_checksum'] = False
    else:
        __write_props['needs_checksum'] = (
            irods_extra.value(opts, 'openType') == irods_extra.FILE_CREATE or
            irods_extra.replica_truncated(flags))

    return irods_extra.SUCCESS


def pep_api_data_obj_write_post(*_):
    """See data_obj_open and replica_opne for more details."""
    __write_props['needs_checksum'] = True
    return irods_extra.SUCCESS


def pep_api_data_obj_close_post(_rule_args, cb, _):  # pyright: ignore
    """See data_obj_create and data_obj_open for more details."""
    if 'data_path' not in __write_props:
        return irods_extra.SUCCESS

    if not __write_props['needs_checksum']:
        return irods_extra.SUCCESS

    return _ensure_replicas_checksum(
        cb, __write_props['data_path'], __write_props['resc_hier'])


def pep_api_data_obj_close_finally(*_):
    """Reset __write_props in case it's needed again in the current session."""
    __write_props.clear()
    return irods_extra.SUCCESS


def pep_api_replica_close_post(rule_args, cb, _):
    """See replica_open for details."""
    json_inp = rule_args[2]

    if __write_props['needs_checksum']:
        inp = json.loads(str(json_inp.buf))

        if 'compute_checksum' not in inp or not inp['compute_checksum']:
            return _ensure_replicas_checksum(
                cb, __write_props['data_path'], __write_props['resc_hier'])

    return irods_extra.SUCCESS


def pep_api_replica_close_finally(*_):
    """Reset __write_props in case it's needed again in this session."""
    __write_props.clear()
    return irods_extra.SUCCESS
