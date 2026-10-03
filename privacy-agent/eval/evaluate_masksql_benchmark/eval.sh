python eval/evaluate_masksql_benchmark.py \
  --gold-file data/masksql/1_input.json \
  --pred-file outputs/masksql_output.json \
  --db-dir data/masksql/databases \
  --out-file outputs/masksql_eval.json \
  --compare-mode masksql_exact