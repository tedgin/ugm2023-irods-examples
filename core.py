# -*- coding: utf-8 -*-

"""CyVerse-wide data policy."""

import inspect
import json

import irods_errors  # type: ignore
import irods_extra
import throttle

import checksum
import replication
import residency
import service

# required for delayExec executions
from replication import replicate, sync_replicas  # pyright: ignore


_RE_DONE = 0


def _compose_rules(
    rule_bases, rule_args, callback, rei,
    success_resp=irods_errors.RULE_ENGINE_CONTINUE,
    fail_resp=_RE_DONE,
    cont_on_fail=False
):
    rule_name = inspect.currentframe().f_back.f_code.co_name
    resp = success_resp

    for base in rule_bases:
        res = getattr(base, rule_name)(rule_args, callback, rei)

        if res < irods_extra.SUCCESS:
            if resp == success_resp:
                resp = fail_resp if fail_resp else res

            if not cont_on_fail:
                break

    return resp


def acCreateUser(*args):
    """Compose rules that bind to the PEP acCreateUser.

    Execute all the rules that bind to this PEP in the order provided. If all
    of them succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule
    engines can apply policy.
    """
    return _compose_rules([service], *args, fail_resp=None, cont_on_fail=True)


def acSetRescSchemeForCreate(*args):
    """Compose rules that bind to the PEP acSetRescSchemeForCreate.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return its error code to the rule engine. If all
    of them succeed, return 0 (DONE) to the rule engine to let it know that no
    other rule engines need to be tried.
    """
    return _compose_rules(
        [residency], *args, success_resp=_RE_DONE, fail_resp=None)


def acSetRescSchemeForRepl(*args):
    """Compose rules that bind to the PEP acSetRescSchemeForCrate."""
    return _compose_rules([residency], *args)


def pep_api_data_obj_copy_post(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_copy_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return 0 (DONE) to the rule engine to let it know
    that no other rule engines should be tried. If all rules succeed, return
    RULE_ENGINE_CONTINUE, so that subsequent rule engines can apply policy.
    """
    return _compose_rules([checksum, replication], *args)


def pep_api_data_obj_put_post(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_put_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return 0 (DONE) to the rule engine to let it know
    that no other rule engines should be tried. If all rules succeed, return
    RULE_ENGINE_CONTINUE, so that subsequent rule engines can apply policy.
    """
    return _compose_rules([checksum, replication], *args)


def pep_api_phy_path_reg_post(*args):
    """Compose rules that bind to the PEP pep_api_phy_path_reg_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return 0 (DONE) to the rule engine to let it know
    that no other rule engines should be tried. If all rules succeed, return
    RULE_ENGINE_CONTINUE, so that subsequent rule engines can apply policy.
    """
    return _compose_rules([checksum, replication], *args)


def pep_api_touch_post(*args):
    """Compose rules that bind to the PEP pep_api_touch_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return 0 (DONE) to the rule engine to let it know
    that no other rule engines should be tried. If all rules succeed, return
    RULE_ENGINE_CONTINUE, so that subsequent rule engines can apply policy.
    """
    return _compose_rules([checksum, replication], *args)


#
# NOTE: data_obj_create and data_obj_create_and_stat are used in conjunction
#       with data_obj_close.
#


def pep_api_data_obj_create_post(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_create_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    first one fails, and return its error code to the rule engine. If all rules
    succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule engines can
    apply policy.
    """
    return _compose_rules([checksum, replication], *args, fail_resp=None)


# NOTE: data_obj_create_and_stat isn't used by iCommands or any official API,
#       so it's not implemented.
# def pep_api_data_obj_create_and_stat_post(rule_args,callback, rei):


# NOTE: data_obj_open, data_obj_write, and data_obj_close are used together


def pep_api_data_obj_open_post(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_open_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    first one fails, and return its error code to the rule engine. If all rules
    succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule engines can
    apply policy.
    """
    return _compose_rules([checksum, replication], *args, fail_resp=None)


# NOTE: replica_open, data_obj_write, and replica_closed are used together


def pep_api_replica_open_post(*args):
    """Compose rules that bind to the PEP pep_api_replica_open_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    first one fails, and return its error code to the rule engine. If all rules
    succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule engines can
    apply policy.
    """
    return _compose_rules([checksum, replication], *args, fail_resp=None)


def pep_api_data_obj_write_post(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_write_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    first one fails, and return its error code to the rule engine. If all rules
    succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule engines can
    apply policy.
    """
    return _compose_rules([checksum, replication], *args, fail_resp=None)


def pep_api_data_obj_close_post(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_close_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return 0 (DONE) to the rule engine to let it know
    that no other rule engines should be tried. If all rules succeed, return
    RULE_ENGINE_CONTINUE, so that subsequent rule engines can apply policy.
    """
    return _compose_rules([checksum, replication], *args)


def pep_api_data_obj_close_finally(*args):
    """Compose rules that bind to the PEP pep_api_data_obj_close_finally.

    Execute the rules that bind to this PEP in the order provided. If all rules
    succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule engines can
    apply policy. Otherwise, return the error code of the first rule that
    failed.
    """
    return _compose_rules(
        [checksum, replication], *args,
        fail_resp=irods_errors.RULE_ENGINE_CONTINUE,
        cont_on_fail=True)


def pep_api_replica_close_post(*args):
    """Compose rules that bind to the PEP pep_api_replica_close_post.

    Execute the rules that bind to this PEP in the order provided. Stop when
    the first one fails, and return 0 (DONE) to the rule engine to let it know
    that no other rule engines should be tried. If all rules succeed, return
    RULE_ENGINE_CONTINUE, so that subsequent rule engines can apply policy.
    """
    return _compose_rules([checksum, replication], *args)


def pep_api_replica_close_finally(*args):
    """Compose rules that bind to the PEP pep_api_replica_close_finally.

    Execute the rules that bind to this PEP in the order provided. If all rules
    succeed, return RULE_ENGINE_CONTINUE, so that subsequent rule engines can
    apply policy. Otherwise, return the error code of the first rule that
    failed.
    """
    return _compose_rules(
        [checksum, replication], *args,
        fail_resp=irods_errors.RULE_ENGINE_CONTINUE,
        cont_on_fail=True)


def async_api_bulk_data_obj_put_post(*args):
    """See bulk_data_obj_put for details."""
    return _compose_rules(
        [checksum, replication], *args, success_resp=_RE_DONE)


# NOTE: https://github.com/irods/irods/issues/7110  bulk_data_obj_put is broken
#       when the forceFlag is set. It's scheduled to be fixed in iRODS 4.3.1.
def pep_api_bulk_data_obj_put_post(rule_args, cb, _):
    """Compose rules that bind to the PEP pep_api_bulk_data_obj_put_post.

    Asynchronously, execute these rules in the order provided in the function
    async_api_bulk_data_obj_put_post. Stop when the first one fails, and return
    its error code to the rule engine. If all rules succeed, return 0 (DONE) to
    the rule engine.

    When writing a rule that should bind to this PEP, name it
    async_api_bulk_data_obj_put_post so that the
    async_api_bulk_data_obj_put_post function in this module will call it. This
    name was chosen for the rule that should bind to this PEP, because the
    first argument passed to them has all the same information, but it contains
    native Python types instead of wrapped, foreign types. The information in
    the argument needs to be accessed differently. In order to reduce the risk
    of confusion, the name async_api_bulk_data_obj_put_post was chosen for
    these rules.

    The async_api_bulk_data_obj_put_post rule should take three arguments. The
    second and third are the callback and REI values that the Python rule
    engine passes to all rules. The first argument, has the information
    contained in the list of arguments passed synchronously to
    pep_api_bulk_data_obj_put_post but with foreign objects replaced by Python
    dicts and then JSON-serialized. More descriptively,

    async_api_bulk_data_obj_put_post(rule_args, callback, rei)

    rule_args = [
        str,       # Instance
        str,       # JSON serialized PluginContext
        str,       # JSON serialized BulkObjInp
        str        # BulkObjInpByteBuf
    ]

    BulkOprInp => {
        'objPath':     str,
        'attriArray':  {},   # GenQueryOut
        'condInput':   {}    # KeyValPair
    }

    GenQueryOut => {
        rowCnt:         int,
        attriCnt:       int,
        continueInx:    int,
        totalRowCount:  int,
        sqlResult:      [ {} ]  # array of SqlResult
    }

    SqlResult => {
        attriInx:  int,
        len:       int,
        row:       [ str ]
    }

    KeyValPair => { str:  str, ... }

    PluginContext => {
        'api_index':                            str,
        'auth_scheme':                          str,
        'client_addr':                          str,
        'connect_count':                        str,
        'option':                               str,
        'proxy_auth_info_auth_flag':            str,
        'proxy_auth_info_auth_scheme':          str,
        'proxy_auth_info_auth_str':             str,
        'proxy_auth_info_flag':                 str,
        'proxy_auth_info_host':                 str,
        'proxy_auth_info_ppid':                 str,
        'proxy_rods_zone':                      str,
        'proxy_sys_uid':                        str,
        'proxy_user_name':                      str,
        'proxy_user_other_info_user_comments':  str,
        'proxy_user_other_info_user_create':    str,
        'proxy_user_other_info_user_info':      str,
        'proxy_user_other_info_user_modify':    str,
        'proxy_user_type':                      str,
        'socket':                               str,
        'status':                               str,
        'user_auth_info_auth_flag':             str,
        'user_auth_info_auth_scheme':           str,
        'user_auth_info_auth_str':              str,
        'user_auth_info_flag':                  str,
        'user_auth_info_host':                  str,
        'user_auth_info_ppid':                  str,
        'user_rods_zone':                       str,
        'user_sys_uid':                         str,
        'user_user_name':                       str,
        'user_user_other_info_user_comments':   str,
        'user_user_other_info_user_create':     str,
        'user_user_other_info_user_info':       str,
        'user_user_other_info_user_modify':     str,
        'user_user_type':                       str
    }
    """
    instance = rule_args[0]
    comm = rule_args[1]
    bulk_opr_inp = rule_args[2]
    bulk_opr_inp_b_buf = rule_args[3]
    attris = bulk_opr_inp.attriArray

    boi_map = {
        'objPath': str(bulk_opr_inp.objPath),
        'attriArray': {
            'rowCnt': attris.rowCnt,
            'attriCnt': attris.attriCnt,
            'continueInx': attris.continueInx,
            'totalRowCount': attris.totalRowCount,
            'sqlResult': [],
        },
        'condInput': {}
    }

    for a in range(attris.attriCnt):
        boi_map['attriArray']['sqlResult'].append({
            'attriInx': attris.sqlResult[a].attriInx,
            'len': attris.sqlResult[a].len,
            'row': [attris.sqlResult[a].row(r) for r in range(attris.rowCnt)]})

    for i in range(rule_args[2].condInput.len):
        boi_map['condInput'][str(rule_args[2].condInput.key[i])] = (
            str(rule_args[2].condInput.value[i]))

    cond_fmt = (
        "<INST_NAME>irods_rule_engine_plugin-python-instance</INST_NAME>"
        "<PLUSET>{}s</PLUSET><EF>0s REPEAT 0 TIMES</EF>")

    cond = cond_fmt.format(throttle.next_delay())

    rule_fmt = (
        "callback.async_api_bulk_data_obj_put_post('{}', '{}', '{}', '{}')")

    rule = rule_fmt.format(
        str(instance),
        json.dumps(comm.map()),
        json.dumps(boi_map),
        str(bulk_opr_inp_b_buf.buf))

    cb.delayExec(cond, rule, "")
    return irods_errors.RULE_ENGINE_CONTINUE


# NOTE: bulk_data_obj_reg isn't used by iCommands or any official API, so it's
#       implemented.
# def pep_api_bulk_data_obj_reg_post(rule_arg, callback, _):
