"""ctrlm_core — shared core for the Control-M -> Airflow converter.

Modules:
  model     typed IR + partition models (THE contract, see docs/impl-contracts.md)
  parser    DEFTABLE XML -> Deftable IR
  desugar   folder-level conditions/schedules -> job-level (synthetic start/end nodes)
  schedule  day-pattern normalization + cron helpers + ODATE-clock helpers
  graph     condition matching -> CtmGraph (E edges + wiring set)
  cuts      shared cut phases: cyclic extraction, hub cuts, pattern cuts
  stats     partition statistics shared by both strategies
  emit      Jinja2 codegen: PartitionResult -> Airflow DAG .py files
  pipeline  orchestration: parse -> desugar -> normalize -> graph -> partition -> emit
"""
