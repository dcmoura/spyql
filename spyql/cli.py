from spyql.query import Query
import spyql.utils
import spyql.log
import click


def parse_options(ctx, param, options):
    options = [opt.split("=", 1) for opt in options]
    for opt in options:
        if len(opt) < 2:
            raise click.BadParameter(
                f"bad format for option '{opt[0]}', format must be 'option=value'"
            )
    return {kv[0]: spyql.utils.try2eval(kv[1], globals()) for kv in options}


@click.command()
@click.argument("query")
@click.option(
    "-I",
    "input_opt",
    type=click.UNPROCESSED,
    callback=parse_options,
    multiple=True,
    help=(
        "Set input options in the format 'option=value'. Example: -Idelimiter=,"
        " -Iheader=False"
    ),
)
@click.option(
    "-O",
    "output_opt",
    type=click.UNPROCESSED,
    callback=parse_options,
    multiple=True,
    help=(
        "Set output options in the format 'option=value'. Example: -Odelimiter=,"
        " -Oheader=False"
    ),
)
@click.option(
    "--unbuffered",
    "-u",
    is_flag=True,
    help="Force output to be unbuffered.",
)
@click.option(
    "--verbose",
    "-v",
    "verbose",
    default=0,
    help=(
        "Set verbose level: -2 to supress errors and warnings; -1 to supress warnings;"
        " 0 to only show errors and warnings (default); 1 to show additional info"
        " messages; 2 to show additional debug messages."
    ),
)
@click.option(
    "-W",
    "warning_flag",
    type=click.Choice(["default", "error"]),
    default="default",
    help=(
        "Set if warnings are turned into errors or if warnings do not halt execution"
        " (default)."
    ),
)
@click.version_option(version=spyql.__version__)
def main(query, warning_flag, verbose, unbuffered, input_opt, output_opt):
    """
    Tool to run a SpyQL QUERY over text data.
    For more info visit: https://github.com/dcmoura/spyql

    \b
    [ IMPORT python_module [ AS identifier ] [, ...] ]
    SELECT [ DISTINCT | PARTIALS ]
        [ * | python_expression [ AS output_column_name ] [, ...] ]
        [ FROM csv | spy | text | python_expression | json [ EXPLODE path ] ]
        [ WHERE python_expression ]
        [ GROUP BY output_column_number | python_expression  [, ...] ]
        [ ORDER BY output_column_number | python_expression
            [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
        [ LIMIT row_count ]
        [ OFFSET num_rows_to_skip ]
        [ TO csv | json | spy | sql | pretty | plot ]
    """

    out = Query(
        query,
        input_opt,
        output_opt,
        unbuffered,
        warning_flag,
        verbose,
        default_to_clause="CSV",
    )()

    spyql.log.user_info(f"Output Meta: {out}")


if __name__ == "__main__":
    main()
