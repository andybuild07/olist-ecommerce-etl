# olist-ecommerce-etl
Every night a scheduler picks up the files that landed in object storage, converts them to Parquet, cleans and joins them, and loads a small star schema that a dashboard reads.
