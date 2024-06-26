USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

--01 - CREATE THE IAM USER AND GET PROGRAMATIC ACCESS KEYS IN AWS CONSOLE.
--     https://aws.amazon.com/console/

--02 - CREATE THE BUCKET AND UPLOAD THE FILES IN THE DATA DIRECTORY FROM THE COURSE RESOURCES.

--03 - CREATE THE PLURALSIGHT_DATM DATABASE.
CREATE OR REPLACE DATABASE pluralsight_datm;

--04 - CREATE THE RAW SCHEMA.
CREATE OR REPLACE SCHEMA pluralsight_datm.raw;

--05 - CREATE THE STAGE, UPDATE THE URL, AWS_KEY_ID, AND AWS_SECRET_KEY WITH THE APPROPRIATE VALUES.
CREATE OR REPLACE STAGE my_s3_stage
  URL='s3://***/'
  CREDENTIALS = (AWS_KEY_ID='***' AWS_SECRET_KEY='***')
  FILE_FORMAT = (TYPE = 'CSV');

--06 LIST THE FILES IN THE STAGE TO MAKE SURE THAT SNOWFLAKE CAN CONNECT TO YOUR BUCKET.
LIST @my_s3_stage;

--07 CREATE THE employee_data TABLE
CREATE OR REPLACE TABLE pluralsight_datm.raw.employee_data
(
    employee_id NUMBER(19,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    age VARCHAR(16777216),
    gender VARCHAR(20),
    department VARCHAR(16777216),
    salary VARCHAR(16777216),
    hobbies VARIANT
);

--08 RUN THE COPY INTO COMMAND
COPY INTO pluralsight_datm.raw.employee_data
FROM @my_s3_stage/employee_data.tsv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = '\t');

--09 QUERY THE DATA FROM THE NEW TABLE
SELECT * FROM pluralsight_datm.raw.employee_data;
