# jobs/common/io.py
import yaml

with open("configs/config.yml") as f:
    cfg = yaml.safe_load(f)


def read_postgres(spark, query=None):
    reader = (
        spark.read.format("jdbc")
        .option("url", cfg["postgres"]["url"])
        .option("user", cfg["postgres"]["user"])
        .option("password", cfg["postgres"]["password"])
        .option("driver", "org.postgresql.Driver")
    )

    if query:
        reader = reader.option("query", query)
    else:
        reader = reader.option("dbtable", cfg["postgres"]["table"])

    return reader.load()


def write_delta(dataframe, table_name, path, mode="overwrite"):
    dataframe.write.format("delta").mode(mode).option("path", path).saveAsTable(
        table_name
    )
    dataframe.sparkSession.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'"
    )
