#!/bin/bash

#location of config file

config_loc="/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json"

#get list of tables

        table_list=$(cat /customerSegment/src/main/scala/com/mycompany/config/customerConfig.json | jq  '.sales_dev_cloud.tables | keys[]')


        #get S3 path location & file existennce

        file_path=$(for i in $table_list; do cat $config_loc | jq -r ".sales_dev_cloud.tables.$i.table_location"; done;)


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

      spark-submit --class com.mycompany.drivercode.monthSalesDriverProgram /customerSegment/target/scala-2.11/-customersegment_2.11-0.1.jar


