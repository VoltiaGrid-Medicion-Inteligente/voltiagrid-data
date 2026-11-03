"""Business rules RN-01 to RN-10 in plain Python, one function per rule (traceability matrix, US-05).

They are the readable statement of each rule and what tests/rules/test_acceptance.py checks against the CA
scenarios. The Spark jobs in jobs/ apply the same rules to millions of rows; tests/rules/test_rules_match_spark.py
runs both on the same scenarios, so the two never drift apart.
"""
