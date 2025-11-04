import sys
import argparse
import json
from pyspark.sql.functions import col, count, isnan, when, to_date, min as spark_min, max as spark_max
from pyspark.sql.functions import lit, trim


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Ruta de la Delta tabla de entrada")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    parser.add_argument("--config_path", required=True, help="Ruta del archivo JSON con la configuración")
    parser.add_argument("--table_name", required=True, help="Nombre de la tabla en la configuración JSON")
    parser.add_argument("--write_mode", required=False, default="overwrite", choices=["overwrite","append"])
    parser.add_argument("--quarantine_table", required=True, default="")
    
    args = parser.parse_args()

    df = spark.table(args.input_table)

    with open(args.config_path, 'r') as f:
        config = json.load(f)

    table_config = config.get(args.table_name, {})

    claves_primarias = table_config.get("pk", [])
    columnas_numericas = table_config.get("numeric_ranges", {})
    columna_fecha = table_config.get("date_non_future", "")
    columnas_completas = table_config.get("expected_columns", [])
    nullable_map = {col_def["name"]: col_def.get("nullable", True)
                    for col_def in table_config.get("columns", [])}
    type_map = {col_def["name"]: col_def.get("type", "string")
                for col_def in table_config.get("columns", [])}
    string_cols = {name for name, t in type_map.items() if t.lower() == "string"}


    # 0. Seleccionamos las columnas que queremos
    df = df.select([col(c) for c in columnas_completas])
    total_filas = df.count()

    # 5. Validaciones fila a fila y envío a cuarentena (estilo desplegado y simple)


    # Creamos una columna de control para marcar las filas con errores
    df = df.withColumn("_is_bad", lit(False))

    # 5.1 Nulos en claves primarias
    if claves_primarias:
        for col_name in claves_primarias:
            df = df.withColumn(
                "_is_bad",
                when(col(col_name).isNull() | col("_is_bad"), True).otherwise(col("_is_bad"))
            )

    # 5.2 Duplicados por claves primarias
    if claves_primarias:
        df_dupes = (
            df.groupBy(claves_primarias)
              .count()
              .filter(col("count") > 1)
              .select(*claves_primarias)
              .withColumn("_dup_marker", lit(True))
        )

        df = (
            df.join(df_dupes, claves_primarias, "left")
              .withColumn(
                  "_is_bad",
                  when(col("_dup_marker").isNotNull() | col("_is_bad"), True).otherwise(col("_is_bad"))
              )
              .drop("_dup_marker")
        )

    # 5.3 Columnas numéricas fuera de rango o nulas (si no son nullable)
    for col_name, (min_val, max_val) in columnas_numericas.items():
        # Si la columna no es nullable
        if not nullable_map.get(col_name, True):
            df = df.withColumn(
                "_is_bad",
                when(col(col_name).isNull() | isnan(col(col_name)) | col("_is_bad"), True)
                .otherwise(col("_is_bad"))
            )

        # Si está fuera del rango definido
        df = df.withColumn(
            "_is_bad",
            when(
                (col(col_name).isNotNull()) &
                ((col(col_name) < lit(min_val)) | (col(col_name) > lit(max_val))) |
                col("_is_bad"),
                True
            ).otherwise(col("_is_bad"))
        )

    # 5.4 Columnas no numéricas: nulas o vacías "" o solo espacios (si no son nullable)
    for col_name in df.columns:
        if col_name not in columnas_numericas:
            es_nullable = nullable_map.get(col_name, True)

            # Solo aplicamos chequeo de blancos si la columna es STRING
            if (not es_nullable) and (col_name in string_cols):
                df = df.withColumn(
                    "_is_bad",
                    when(
                        col(col_name).isNull() | (trim(col(col_name)) == "") | col("_is_bad"),
                        True
                    ).otherwise(col("_is_bad"))
                )
            # Si no es STRING pero no es nullable, solo chequeamos NULL
            elif (not es_nullable) and (col_name not in string_cols):
                df = df.withColumn(
                    "_is_bad",
                    when(col(col_name).isNull() | col("_is_bad"), True).otherwise(col("_is_bad"))
                )

    # 5.5 Columna de fecha mal formateada (si aplica)
    if columna_fecha:
        df = df.withColumn("_fecha_parseada", to_date(col(columna_fecha)))
        df = df.withColumn(
            "_is_bad",
            when(col("_fecha_parseada").isNull() | col("_is_bad"), True).otherwise(col("_is_bad"))
        ).drop("_fecha_parseada")

    # 5.6 Separar datos válidos y no válidos
    df_quarantine = df.filter(col("_is_bad")).drop("_is_bad")
    df_valid = df.filter(~col("_is_bad")).drop("_is_bad")

    # 5.7 Escritura final
    df_valid.write.format("delta").mode(args.write_mode).saveAsTable(args.output_table)
    if args.quarantine_table:
        df_quarantine.write.format("delta").mode("append").saveAsTable(args.quarantine_table)

    print("Validación completada. Datos válidos escritos en tabla destino y errores enviados a cuarentena.\n")


if __name__ == "__main__":
    main()
