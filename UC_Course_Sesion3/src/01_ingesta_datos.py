import sys
import logging
import argparse
from pyspark.sql.functions import input_file_name, current_timestamp

# Inicializa logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="Nombre de archivo")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    parser.add_argument("--write_mode", required=True, help="Formato de escritura de la Delta Table de destino")
    parser.add_argument("--partition_by", required=False, help="Columnas para particionar la Delta Table destino, separadas por coma")

    args = parser.parse_args()
    
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.input_file)
        .withColumn("ingest_time", current_timestamp())
    )

    row_count = df.count()
    logger.info(f"Número de filas leídas: {row_count}")

    # Comprobar si el archivo no tiene filas
    if df.count() == 0:
        logger.warning("El archivo no contiene filas. Se aborta el proceso.")
        raise Exception("El archivo no contiene filas para procesar.")

    if args.partition_by != '':
        partition_columns = args.partition_by.split(",")
        df.write.format("delta").mode(args.write_mode).partitionBy(*partition_columns).saveAsTable(args.output_table)
    else:
        df.write.format("delta").mode(args.write_mode).saveAsTable(args.output_table)
        
    logger.info("Ingesta completada exitosamente.")

if __name__ == "__main__":
    main()