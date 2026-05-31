"""Infrastructure zone — the ONLY place pyspark/delta/SDK may be imported.

Importing this package does NOT pull in Spark; the Spark-backed modules
(repository_delta, sources, adapters, uc, vector, observability) import pyspark
lazily inside their own module bodies, so the pandas-only pieces
(repository_memory, encoders) work with no cluster present.
"""
