import logging
import asciichartpy as colors
import sys

def user_error(exception, message = "", code = None, vars = None):
    """
    Reports an error, throwing the original exception
    Prints a custom message.
    Prints the data that originated the exception (if available).
    """
    sys.tracebacklimit = 0
    logging.error(f"\n{colors.lightred}ERROR!{colors.reset}\t{message}{': ' + colors.yellow + code + colors.reset if code else ''}")
    if vars and vars['input_row_number'] and vars['_values']:
        logging.error(f"\tat data row #{vars['input_row_number']}: {colors.darkgray}{vars['_values']}{colors.reset}")                    
    raise exception from None            