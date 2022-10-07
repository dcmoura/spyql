=======
History
=======


0.7.0 (2022-10-08)
------------------
* Added support to key-value lookup (equi joins)
* Added dict aggregation 
* Fixed set aggregation when returning empty set


0.6.0 (2022-04-06)
------------------
* Added ORJSON processor for faster queries on JSON data
* Added ORJSON writer for faster JSON output
* Improved efficiency of the JSON writer (based on the standard library)
* Added JSON querying benchmark


0.5.0 (2022-03-28)
------------------
* Added Python API
* Added input/output options to FROM/TO clauses
* Added file path as input/output option
* Added access by attribute on dicts
* Changed row metadata to allow unified access to different input formats
* Renamed NullSafeDict to qdict


0.4.1 (2022-01-31)
------------------
* Added profiling module
* Improved efficiency of JSON reading


0.4.0 (2021-12-02)
------------------
* Added data type inference when reading CSVs
* Added meta layer to access column names and values
* Improved handling of NULLs on pretty and plot writers


0.3.0 (2021-11-18)
------------------
* Added aggretation/window functions
* Added GROUP BY clause
* Added SELECT DISTINCT modifier
* Added SELECT PARTIALS modifier for running analytical/window queries


0.2.0 (2021-10-23)
------------------

* Added IMPORT clause
* Added ORDER BY clause
* Added support for init file


0.1.0 (2021-08-19)
------------------

* First release on PyPI.
