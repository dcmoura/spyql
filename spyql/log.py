import logging
import asciichartpy as colors
import sys
import json

from spyql.utils import quote_ifstr

error_on_warning = False  # overrided by command line arg


def colored(val, color):
    """
    Creates string representation of `val` for color printing
    """
    return color + str(val) + colors.reset


def mk_user_msg(level, level_color, message, code=None, code_color=colors.lightyellow):
    return (
        colored(level, level_color)
        + f"\t{message}"
        + f"{': ' + colored(code, code_color) if code else ''}"
    )


def user_error(message, exception, code=None, vars=None):
    """
    Reports an error, throwing the original exception
    Prints a custom message.
    Prints the data that originated the exception (if available).
    """
    sys.tracebacklimit = 0
    logging.error(mk_user_msg("ERROR", colors.lightred, message, code))
    if vars and vars.get("input_row_number") and vars.get("_values"):
        logging.error(
            f"\tat data row #{vars['input_row_number']}: "
            + colored(vars["_values"], colors.darkgray)
        )
    raise exception from None


def user_warning(message, exception=None, code=None):
    """
    Reports a warning.
    Prints a custom message.
    If `error_on_warning` is True, raises an exception instead.
    """
    if error_on_warning:
        if exception:
            raise exception
        raise Exception(message)
    logging.warning(mk_user_msg("WARNING", colors.lightyellow, message, code))


def user_info(message, code=None):
    """
    Reports (verbose) information.
    """
    logging.info(mk_user_msg("INFO", colors.green, message, code, colors.yellow))


def user_debug(message, code=None):
    """
    Reports (verbose) information.
    """
    logging.debug(
        mk_user_msg("DEBUG", colors.lightcyan, message, code, colors.lightgray)
    )


def user_debug_dict(message, adict):
    """
    Reports (debug) information, printing a dict as a pretty json.
    """
    user_debug(message, json.dumps(adict, indent=4))


def user_warning4func(message, exception, *args, **kwargs):
    user_warning(
        message,
        exception,
        ",".join(
            [quote_ifstr(v) for v in args]
            + [f"{k}={quote_ifstr(v)}" for k, v in kwargs.items()]
        ),
    )


def conversion_warning(atype, exception, *args, **kwargs):
    user_warning4func(
        f"could not convert string to {atype}, returning NULL",
        exception,
        *args,
        **kwargs,
    )
