-- 01 - SET THE ROLE, VIRTUAL WARE HOUSE AND DATABASE
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE PLURALSIGHT_DATM;

-- 02 - RUN THE CLONE COMMAND
CREATE OR REPLACE SCHEMA TIMETRAVEL CLONE STAGING;

-- VIEW THE DATA IN THE TIME TRAVEL SCHEMA
SELECT * FROM PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA;

-- YOU CAN ENABLE TIME TRAVEL AT THE TABLE, SCHEMA, OR DATABASE LEVEL
-- LOOK FOR THE "retention_time" COLUMN OF THE VARIOUS QUERY RESULTS

-- 03 - CHECK IF TIME TRAVEL IS ENABLED
-- TO SEE IF TIME TRAVEL IS ENABLED ON A TABLE:

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    RETENTION_TIME,
    LAST_ALTERED,
    LAST_DDL,
    CREATED
FROM 
    INFORMATION_SCHEMA.TABLES 
WHERE 
    TABLE_NAME = 'EMPLOYEE_DATA';

-- TO SEE IF TIME TRAVEL IS ENABLED ON A SCHEMA:
SHOW SCHEMAS LIKE 'TIMETRAVEL';

-- TO SEE IF TIME TRAVEL IS ENABLE ON A DATABASE:
SHOW DATABASES LIKE 'PLURALSIGHT_DATM';

-- 03 - ENABLE TIME TRAVEL:

-- ENABLE TIME TRAVEL:
-- TO ENABLE TIME TRAVEL ON A TABLE
ALTER TABLE PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- CHECK THE RETENTION PERIOD AGAIN FOR THE "EMPLOYEE_DATA" TABLE:
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    RETENTION_TIME,
    LAST_ALTERED,
    LAST_DDL,
    CREATED
FROM 
    INFORMATION_SCHEMA.TABLES 
WHERE 
    TABLE_NAME = 'EMPLOYEE_DATA';
    
-- TO ENABLE TIME TRAVEL FOR A SCHEMA:
--ALTER SCHEMA TIMETRAVEL SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- TO ENABLE TIME TRAVEL FOR AN ENTIRE DATABASE, NOT THIS WILL ENABLE TIMETRAVEL FOR 30 DAYS FOR ALL SCHEMAS AND ALL TABLES:
--ALTER DATABASE PLURALSIGHT SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- YOU CAN CREATE ALSO SPECIFY THE TIME PERIOD AT THE TIME OF TABLE CREATION
/*
CREATE OR REPLACE TABLE TEST (
    id INT,
    name STRING
)
DATA_RETENTION_TIME_IN_DAYS = 7;
*/

-- 04 - UPDATE THE EMPLOYEE_DATA TABLE FOR EMPLOYEE_ID 1 JOHN DOE:
UPDATE PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA
SET AGE = 36, SALARY = 75000, DEPARTMENT = 'Finance'
WHERE EMPLOYEE_ID = 1;

-- 05 - QUERY THE DATA WITH TIME TRAVEL:

-- VIEW THE DATA AS IT WAS, 1 MINUTE AGO (60 = NUMBER OF SECONDS), WITHOUT THE UPDATE:
SELECT * FROM PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA BEFORE (OFFSET => - 60);

-- VIEW THE EMPLOYEE DATA AS IT IS LIVE, WITH THE UPDATED SALARY, AGE, AND DEPARTMENT:
SELECT * FROM PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA;

-- VIEW THE EMPLOYEE DATA AS IT WAS BEFORE A SPECIFIC DATE:
SELECT * FROM PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA AT(TIMESTAMP => 'Sat, 15 June 2024 17:25:00 -0500'::timestamp_tz);

-- 05 - TURN OFF TIME TRAVEL:

-- TO SET TIME TRAVEL BACK TO THE DEFAULT VALUE:
ALTER TABLE PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- TO COMPLETLY TURN OFF TIME TRAVEL FOR THE TABLE:
ALTER TABLE PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA SET DATA_RETENTION_TIME_IN_DAYS = 0;

-- ATTEMPT TO QUERY THE TABLE WITH TIME TRAVEL TURNED OFF RESULTS IN AN ERROR
-- Time travel data is not available for table EMPLOYEE_DATA. The requested time is either beyond the allowed time travel period or before the object creation time.

SELECT * FROM PLURALSIGHT_DATM.TIMETRAVEL.EMPLOYEE_DATA BEFORE (OFFSET => - 60);