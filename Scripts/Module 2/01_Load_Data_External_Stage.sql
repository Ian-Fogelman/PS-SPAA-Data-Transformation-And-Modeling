USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

--01 - CREATE THE IAM USER AND GET PROGRAMATIC ACCESS KEYS IN AWS CONSOLE.
--     https://aws.amazon.com/console/

-- 02 - CREATE THE BUCKET AND UPLOAD THE FILES IN THE DATA DIRECTORY FROM THE COURSE RESOURCES.

-- 03 - CREATE THE PLURALSIGHT_DATM DATABASE.
CREATE OR REPLACE DATABASE pluralsight_datm;

-- 04 - CREATE THE RAW SCHEMA.
CREATE OR REPLACE SCHEMA pluralsight_datm.raw;

-- 05 - CREATE THE STAGE, UPDATE THE URL, AWS_KEY_ID, AND AWS_SECRET_KEY WITH THE APPROPRIATE VALUES.
CREATE OR REPLACE STAGE my_s3_stage
  URL='s3://***/'
  CREDENTIALS = (AWS_KEY_ID='***' AWS_SECRET_KEY='***')
  FILE_FORMAT = (TYPE = 'CSV');

-- 06 LIST THE FILES IN THE STAGE TO MAKE SURE THAT SNOWFLAKE CAN CONNECT TO YOUR BUCKET.
LIST @my_s3_stage;

-- 07 CREATE THE employee_data TABLE
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

-- 08 RUN THE COPY INTO COMMAND
COPY INTO pluralsight_datm.raw.employee_data
FROM @my_s3_stage/employee_data.tsv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = '\t');

-- 09 QUERY THE DATA FROM THE NEW TABLE
SELECT * FROM pluralsight_datm.raw.employee_data;


-- 10 CREATE AND LOAD THE REMAINING TABLES

-- 10A CREATE THE employee_engagement_survey TABLE
CREATE OR REPLACE TABLE pluralsight_datm.raw.employee_engagement_survey
(
    employee_id NUMBER(19,0),
    name VARCHAR(16777216),
    gender VARCHAR(20),
    age VARCHAR(16777216),
    department VARCHAR(16777216),
    job_title VARCHAR(16777216),
    satisfaction_score integer,
    work_life_balance_score integer,
    career_growth_score integer,
    communication_score integer,
    teamwork_score integer
);

-- 10B RUN THE COPY INTO COMMAND
COPY INTO pluralsight_datm.raw.employee_engagement_survey
FROM @my_s3_stage/employee_engagement_survey.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

SELECT * FROM employee_engagement_survey;

-- 10C CREATE THE employee_learning_management TABLE
CREATE OR REPLACE TABLE pluralsight_datm.raw.employee_learning_management
(
    employee_id NUMBER(19,0),
    course_id VARCHAR(200),
    course_name VARCHAR(200),
    completion_date date,
    completion_status VARCHAR(200)
);

-- 10D RUN THE COPY INTO COMMAND
COPY INTO pluralsight_datm.raw.employee_learning_management
FROM @my_s3_stage/employee_learning_management.parquet
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME ='CASE_INSENSITIVE';


-- 10E CREATE THE customers_transactions TABLE
CREATE OR REPLACE TABLE pluralsight_datm.raw.customers_transactions
(
    transaction_id NUMBER(19,0),
    customer_id VARCHAR(16777216),
    date VARCHAR(16777216),
    time VARCHAR(16777216),
    product_name VARCHAR(20),
    category VARCHAR(16777216),
    quantity Integer,
    price FLOAT
);

-- 10F RUN THE COPY INTO COMMAND
COPY INTO pluralsight_datm.raw.customers_transactions
FROM @my_s3_stage/customers_transactions.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-- 10G CREATE THE customer_survey_data TABLE
CREATE OR REPLACE TABLE pluralsight_datm.raw.customer_survey_data
(
    RecordID NUMBER(19,0),
    Name VARCHAR(16777216),
    Age VARCHAR(16777216),
    Gender VARCHAR(16777216),
    Education VARCHAR(20),
    Employment VARCHAR(16777216),
    Income VARCHAR(16777216),
    MaritalStatus VARCHAR(20),
    City VARCHAR(100),
    Satisfaction INTEGER,
    Recommendation INTEGER
);

-- 10H RUN THE COPY INTO COMMAND
COPY INTO pluralsight_datm.raw.customer_survey_data
FROM @my_s3_stage/customer_survey_data.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


--VIEW ALL DATA

SELECT * FROM EMPLOYEE_DATA
SELECT * FROM EMPLOYEE_ENGAGEMENT_SURVEY
SELECT * FROM EMPLOYEE_LEARNING_MANAGEMENT
SELECT * FROM CUSTOMERS_TRANSACTIONS
SELECT * FROM CUSTOMER_SURVEY_DATA
