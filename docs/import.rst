Modules & UDFs
---------------------------------------------------

By default, SPyQL imports some commonly used modules:

* everything from the ``math`` module
* ``datetime``\ , ``date`` and ``timezone`` from the ``datetime`` module
* the ``re`` module

SPyQL queries support a single import statement at the beginning of the query where several modules can be imported (e.g. ``IMPORT numpy AS np, sys SELECT ...``\ ). Note that the python syntax ``from module import identifier`` is not supported in queries.

In addition, you can create a python file that is loaded before executing queries. Here you can define imports, functions, variables, etc using regular python code. Everything defined in this file is available to all your spyql queries. The file should be located at ``XDG_CONFIG_HOME/spyql/init.py``. If the environment variable ``XDG_CONFIG_HOME`` is not defined, it defaults to ``HOME/.config`` (e.g. ``/Users/janedoe/.config/spyql/init.py``\ ).
