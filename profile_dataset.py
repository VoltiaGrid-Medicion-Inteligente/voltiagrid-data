import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, countDistinct, input_file_name
from schemas.f1 import f1_schema
from pathlib import Path

def main():
    spark = SparkSession.builder \
        .appName("VoltiaGrid-F1-Profiling") \
        .getOrCreate()

    # Obtener las rutas completas de los archivos CSV
    carpeta_raw = Path("data/raw").resolve()

    archivos_csv = sorted(
        str(archivo.resolve())
        for archivo in carpeta_raw.glob("block_*.csv")
    )

    if not archivos_csv:
        raise FileNotFoundError(
            f"No se encontraron archivos CSV en {carpeta_raw}"
        )

    print(f"Archivos encontrados: {len(archivos_csv)}")
    print("Leyendo los datos de Kaggle...")

    df = spark.read.csv(
        archivos_csv,
        header=True,
        schema=f1_schema
    )

    # --- T-03.1: Requerimientos de perfilamiento ---

    # A) Filas por bloque
    print("\n--- Filas por bloque ---")
    df.groupBy(input_file_name()).count().show(truncate=False)

    # B) Hogares (LCLid) distintos
    hogares_distintos = df.select("LCLid").distinct().count()
    print(f"\nHogares distintos: {hogares_distintos}")

    # C) Rango de fechas
    rango_fechas = df.select(
        min("tstp").alias("fecha_minima"), 
        max("tstp").alias("fecha_maxima")
    ).collect()[0]
    print(f"\nRango de fechas: {rango_fechas['fecha_minima']} a {rango_fechas['fecha_maxima']}")

    # D) Nulos, incluidos los "Null" escritos como texto en energy(kWh/hh)
    nulos = df.filter(col("energy(kWh/hh)").isNull() | (col("energy(kWh/hh)") == "Null")).count()
    print(f"\nCantidad de valores nulos o 'Null': {nulos}")

    # E) Valores mínimos y máximos
    # Excluimos los "Null" y casteamos a float temporalmente para sacar min y max matemáticos
    df_limpio = df.filter((col("energy(kWh/hh)").isNotNull()) & (col("energy(kWh/hh)") != "Null"))
    df_numerico = df_limpio.withColumn("energy_num", col("energy(kWh/hh)").cast("double"))
    
    min_max = df_numerico.select(
        min("energy_num").alias("min_energy"), 
        max("energy_num").alias("max_energy")
    ).collect()[0]
    
    print(f"\nValor Mínimo de energía: {min_max['min_energy']}")
    print(f"Valor Máximo de energía: {min_max['max_energy']}")

    spark.stop()

if __name__ == "__main__":
    main()