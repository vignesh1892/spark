#!/bin/bash
gcloud dataproc jobs submit pyspark --cluster=cluster-iz-dplr --region=us-central1 /home/vigneshkumar180992/.git/spark/Usecase4_GcpGcsReadWritehive_cloud.py
if [ $? -ne 0 ]
then
echo "`date` error occured in the Pyspark job" > /tmp/gcp_pyspark_schedule.log
else
echo "`date` Pyspark job is completed successfully" > /tmp/gcp_pyspark_schedule.log
fi
echo "`date` gcloud pyspark ETL script is completed" >> /tmp/gcp_pyspark_schedule.log
