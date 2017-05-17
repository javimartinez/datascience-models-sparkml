$SPARK_HOME/bin/spark-submit \
  --class com.jmartinez.datascience.models.sparkml.examples.WangMendelAlgorithmExampleShuttle \
  --master spark://login:7077 \
  --deploy-mode cluster \  # can be client for client mode
  --conf "spark.executor.instances=2"
  --conf "spark.executor.cores=3" 
  --conf "spark.executor.memory=3G"
  /home/invitado/jmj00007/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar \
 /home/invitado/jmj00007/shuttle-5-fold/ shuttle /home/invitado/jmj00007/result-shuttle