#!/usr/bin/env bash


######################################################################################################################
#                               Sql automation Spark Job                                                             #
######################################################################################################################


appName=${1}
className=${2}
master=${3}
queueName=${4}
jobConfig=${5}
sqlFile1=${6}
sqlFile2=${7}
jarLocation=${8}
loggingScript=${9}
localLogPath=${10}
env=${11}




jobRunDate=$(date '+%Y-%m-%d')
configFileName=$(basename "$jobConfig")
logfileName=$(basename ${configFileName} .conf)



#--------------------------------------------------------------------------------------------------------------
# Logging Script : Sourcing Logging utility script having functions : infoLog, errorLog, debugLog, warningLog
#--------------------------------------------------------------------------------------------------------------

logFileNameVar="${logfileName}"
source ${loggingScript} ${localLogPath} ${logFileNameVar} ${jobRunDate}




#-----------------------------------------------------------------------------------------------------------
#Script parameter list
#-----------------------------------------------------------------------------------------------------------
infoLog "App Name                             : ${appName}"
infoLog "Class Name                           : ${className}"
infoLog "Master                               : ${master}"
infoLog "Queue Name                           : ${queueName}"
infoLog "jobConfig                            : ${jobConfig}"
infoLog "sqlFile1                             : ${sqlFile1}"
infoLog "sqlFile2                             : ${sqlFile2}"
infoLog "configFileName                       : ${configFileName}"
infoLog "Jar location                         : ${jarLocation}"
infoLog "Logging script                       : ${loggingScript}"
infoLog "Logging path                         : ${localLogPath}"
infoLog "Job run date                         : ${jobRunDate}"



export SPARK_MAJOR_VERSION=2;
spark-submit \
--verbose \
--name ${appName} \
--master ${master}  \
--deploy-mode client \
--conf spark.executor.instances=2 \
--conf spark.executor.memory=5G \
--conf spark.executor.cores=2 \
--conf spark.driver.memory=5G \
--conf spark.yarn.queue=${queueName} \
--conf "spark.driver.extraJavaOptions=-Dconfig.file=${jobConfig} -Ddate=${jobRunDate}" \
--files /etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/yarn-site.xml,/usr/hdp/current/spark2-client/conf/hive-site.xml,${jobConfig},${sqlFile1},${sqlFile2} \
--class ${className} ${jarLocation} ${env}  >> ${local_log_file} 2>&1



if [[ $? != 0 ]];then
   infoLog "Error occurred while running Sql Automation"
   exit 1
else
   infoLog "Sql Automation Successfully completed"
fi




