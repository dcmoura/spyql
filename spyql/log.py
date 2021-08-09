import logging
import asciichartpy as colors
import sys
import json

error_on_warning = False

def user_error(message, exception, code = None, vars = None):
    """
    Reports an error, throwing the original exception
    Prints a custom message.
    Prints the data that originated the exception (if available).
    """
    sys.tracebacklimit = 0
    logging.error(f"{colors.lightred}ERROR{colors.reset}\t{message}{': ' + colors.lightyellow + str(code) + colors.reset if code else ''}")
    if vars and vars['input_row_number'] and vars['_values']:
        logging.error(f"\tat data row #{vars['input_row_number']}: {colors.darkgray}{vars['_values']}{colors.reset}")
    raise exception from None

def user_warning(message, exception = None, code = None):
    """
    Reports a warning.
    Prints a custom message.
    If `error_on_warning` is True, raises an exception instead.
    """
    if error_on_warning:
        if exception:
            raise exception
        raise Exception(message)
    logging.warning(f"{colors.lightyellow}WARNING{colors.reset}\t{message}{': ' + colors.lightyellow + str(code) + colors.reset if code else ''}")

def user_info(message, code = None):
    """
    Reports (verbose) information.
    """
    logging.info(f"{colors.green}INFO{colors.reset}\t{message}{': ' + colors.yellow + str(code) + colors.reset if code else ''}")

def user_debug(message, code = None):
    """
    Reports (verbose) information.
    """
    logging.debug(f"{colors.lightcyan}DEBUG{colors.reset}\t{message}{': ' + colors.lightgray + str(code) + colors.reset if code else ''}")

def user_debug_dict(message, adict):
    """
    Reports (debug) information, printing a dict as a pretty json.
    """
    user_debug(message, json.dumps(adict, indent=4))


def quote_ifstr(s):
    return f"'{s}'" if isinstance(s, str) else s

def user_warning4func(message, exception, *args, **kwargs):
    user_warning(
        message,
        exception,
        ','.join(
            [quote_ifstr(v) for v in args] +
            [f"{k}={quote_ifstr(v)}" for k, v in kwargs.items()]
        )
    )

def conversion_warning(atype, exception, *args, **kwargs):
    user_warning4func(f"could not convert string to {atype}, returning NULL", exception, *args, **kwargs)
