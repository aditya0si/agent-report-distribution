"""Agent-wise report distribution pipeline.

A daily, agent-scoped CSV report is produced from a large policy/claim dataset and delivered to
each field agent as a short-lived S3 pre-signed link. The same package runs in three places:

* ``agent_reports.ingest``        - synthetic-but-realistic dataset generator (runs anywhere)
* ``agent_reports.emr.jobs``      - PySpark aggregation (EMR Serverless / EMR on EC2)
* ``agent_reports.lambda_handlers`` - orchestrator / dispatcher / presign / chunker Lambdas

The free-tier path replaces the Spark step with ``lambda_handlers.chunker`` and is what the
offline test suite and ``scripts/e2e_local.py`` exercise end to end under moto.
"""

__version__ = "1.0.0"

__all__ = ["__version__"]
