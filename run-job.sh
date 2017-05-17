#!/usr/bin/env bash

if [ "$#" -ne 1 ];
then
echo 'One argument is necessary!!!'
exit 0
else

#sbt clean assembly

#docker exec -it spark-master /bin/bash ./bin/spark-submit \
# --master spark://master:7077 \
# --class com.jmartinez.datascience.models.sparkml.examples.OneRClassifierExample \
#/workspace/target/scala-2.11/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar $1

docker exec -it spark-master /bin/bash ./bin/spark-submit \
 --master spark://master:7077 \
 --class com.jmartinez.datascience.models.sparkml.examples.WangMendelAlgortimExampleShuttle \
/workspace/target/scala-2.11/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar $1
fi


#  --deploy-mode cluster \

# entrar al docker

#docker exec -it spark-master /bin/bash

