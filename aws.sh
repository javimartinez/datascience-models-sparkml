#!/usr/bin/env bash



aws s3 cp /Users/Javi/development/datascience-models-sparkml/target/scala-2.11/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar \
s3://datascience-model-sparkml


aws emr create-cluster --name "DataScienceModels"  \
--release-label emr-5.5.0 \
--applications Name=Spark \
--ec2-attributes KeyName=spark-cluster-emr \
--instance-type m3.xlarge \
--instance-count 2 \
--log-uri s3://spark--cluster--logs \
--steps Type=Spark,Name="WangMendelAlgorithm",ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,--master,yarn-cluster,--class,com.jmartinez.datascience.models.sparkml.examples.WangMendelAlgorithmExample,s3://datascience-model-sparkml/datascience-models-sparkml-assembly-0.0.1-SNAPSHOT.jar,s3://datasets-tfg/poker.dat] --use-default-roles

--master yarn-cluster ???
