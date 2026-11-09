"""Read-only extracts of Lakefront Rails history.

Step 1 of the pipeline work wraps these functions in an Airflow DAG.
The functions do not open a connection until a caller asks them to.
"""

__version__ = "0.1.0"
