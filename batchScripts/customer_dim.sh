#!/bin/bash


deploy_env=$1

if [[ deploy_env == 'sales_dev' || deploy_env == 'sales_prod' || deploy_env == 'sales_dev_cloud' || deploy_env == 'sales_prod_cloud' ]]; then
    echo "Executing for parameter "$deploy_env
else
    echo "Invalid argument received"
    echo "Valid parameters are : sales_dev,sales_prod,sales_dev_cloud,sales_prod_cloud"
    exit -1
fi


#location of config file

config_loc="/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json"

#get list of tables

        table_list=$(cat /customerSegment/src/main/scala/com/mycompany/config/customerConfig.json | jq  ".$deploy_env.tables | keys[]")


        #get S3 path location & file existennce

        file_path=$(for i in $table_list; do cat $config_loc | jq -r ".$deploy_env.tables.$i.table_location"; done;)


        file_missing_counter=0

        for i in $file_path; do exists=$(aws s3 ls $i);
        if [[ -z "$exists" ]];
        then echo "$i bucket does not exit";
           file_missing_counter=$((file_missing_counter+1));
           else
                echo " $i bucket exit";
       fi ;
       done


#exit in case file not found.

      if [[ $file_missing_counter -gt 0 ]]; then
            echo "file not found" ; exit 1;
      fi


#submit spark program

      spark-submit --class com.mycompany.drivercode.customerDriverProgram /customerSegment/target/scala-2.11/-customersegment_2.11-0.1.jar


