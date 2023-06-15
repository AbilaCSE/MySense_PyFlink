### BELOW IS A STARTER TEMPLATE AND WORKING CODE THAT READS DATA FROM INOUT AND OUTPUTS only the fields: cid, type and heartrates (already in json format) ###

### MODIFY THE CODE BELOW TO COMPLETE THE TASK A and B ###

import os
import datetime

from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udtf, AggregateFunction
from pyflink.table import DataTypes
from pyflink.table.window import Tumble

env = StreamExecutionEnvironment.get_execution_environment()

table_env = StreamTableEnvironment.create(stream_execution_environment=env)


def get_input_table_ddl_task_B(input_table_name_taskB):
    return   """CREATE TABLE {0} (
                cid STRING,
                `type` STRING,
                heartrate INT,
                hr_time TIMESTAMP(3),
                WATERMARK FOR hr_time as hr_time - INTERVAL '10' MINUTES
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/output/taskA_result',
            'format' = 'json'
            )""".format(input_table_name_taskB)


def get_output_table_ddl_task_B(output_table_name_taskB):
    return   """CREATE TABLE {0} (
                 cid STRING,
                 time_frame_start TIMESTAMP,
                 average_heart_rate INT
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/output/taskB_result',
            'format' = 'json'
            )""".format(output_table_name_taskB)



def main():

    # tables names
    input_table_name_taskB = "input_table_name_taskB"
    output_table_name_taskB = "output_table_name_taskB"

    ### drop tables if they exist

    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(input_table_name_taskB))
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(output_table_name_taskB))

    # create the tables for input and output by executing the DDL
    table_env.execute_sql(get_input_table_ddl_task_B(input_table_name_taskB))
    table_env.execute_sql(get_output_table_ddl_task_B(output_table_name_taskB))

    # Read from table to begin transformations
    tab1 = table_env.from_path(input_table_name_taskB)

    tumble_window = Tumble.over(lit(10).minutes) \
                    .on (col("hr_time")) \
                    .alias ("w")

    resultB = tab1.window(tumble_window) \
    .group_by(col('cid'), col('w')) \
    .select(col('cid'), col('w').start.alias('time_start'), col("heartrate").avg.alias('avg_h'))
   
    resultB.execute().print() #print in console
    final_result_B=resultB.execute_insert(output_table_name_taskB) #write to target file
    final_result_B.wait()





if __name__ == "__main__":
    main()
