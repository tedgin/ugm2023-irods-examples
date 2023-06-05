# -*- coding: utf-8 -*-

"""Provides a way an incrementally increasing delay.

This is useful for reducing the risk of a single user from monopolizing the
delay rule server.
"""


__delay_time = -1


def next_delay():
    """Return the next delay time in seconds."""
    global __delay_time
    __delay_time += 1
    return __delay_time
