#!/bin/bash
#SBATCH --job-name=wangMendel #Nombre del script
#SBATCH --partition=spark #Cola Spark
#SBATCH --output=salidaPrubaSpark_%j.out #Fichero con la salidas
#SBATCH --error=salidaPruebaSpark_%j.err #Errores con los errores
#SBATCH --mail-type=END,FAIL      # Notificación cuando el trabajo termina o falla (todavía no implementado)
#SBATCH --mail-user=jmj00007@ujaen.es #E-Mail para avisos
echo "Ejecutando la prueba"
./wang-mendel-spark.sh #Ejecución a incorporar en la cola