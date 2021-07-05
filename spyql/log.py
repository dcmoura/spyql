import logging
import asciichartpy as colors
import sys

error_on_warning = False

def user_error(message, exception, code = None, vars = None):
    """
    Reports an error, throwing the original exception
    Prints a custom message.
    Prints the data that originated the exception (if available).
    """
    sys.tracebacklimit = 0    
    logging.error(f"{colors.lightred}ERROR{colors.reset}\t{message}{': ' + colors.lightyellow + code + colors.reset if code else ''}")
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
    logging.warning(f"{colors.lightyellow}WARNING{colors.reset}\t{message}{': ' + colors.lightyellow + code + colors.reset if code else ''}")
