scp /Users/Javi/development/data/data.zip invitado@simidat-apps.ujaen.es:/home/invitado/jmj00007

zip sparkml.zip /Users/Javi/development/datascience-models-sparkml/target/scala-2.11/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar

scp /Users/Javi/development/datascience-models-sparkml/target/scala-2.11/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar  invitado@simidat-apps.ujaen.es:/home/invitado/jmj00007


//to turing 

scp /home/invitado/jmj00007/data.zip simidat@turing.ujaen.es:/home/simidat/jmj000017

scp /home/invitado/jmj00007/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar simidat@turing.ujaen.es:/home/simidat/jmj000017


$SPARK_HOME/bin/spark-submit \
  --class com.jmartinez.datascience.models.sparkml.examples.OneRClassifierExample \
  --master spark://login.turing.ceatic.ujaen.es:6066 \
  --deploy-mode cluster \
  --num-executors 1 \
  /home/simidat/jmj000017/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar \
  spark://login.turing.ceatic.ujaen.es:6066 /home/simidat/jmj000017/data /home/simidat/jmj000017/resultOneR oneR 10

   --conf "spark.executor.memory=10g"\



