CREATE EXTERNAL TABLE employees_temp (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary DOUBLE,
    project STRING,
    join_date STRING,
    department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/employee/';


LOAD DATA INPATH '/user/hive/warehouse/employee/employees.csv' 
INTO TABLE employees_temp;

CREATE TABLE employees (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary DOUBLE,
    project STRING,
    join_date STRING
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE employees PARTITION(department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department
FROM employees_temp;

MSCK REPAIR TABLE employees;

CREATE EXTERNAL TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/department/';

LOAD DATA INPATH '/user/hive/warehouse/department/departments.csv' 
INTO TABLE departments;

SELECT * FROM employees 
WHERE year(TO_DATE(join_date, 'yyyy-MM-dd')) > 2015;

SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department;

SELECT * FROM employees WHERE project = 'Alpha';

SELECT job_role, COUNT(*) AS num_employees
FROM employees
GROUP BY job_role;

SELECT e.*
FROM employees e
JOIN (SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department) dept_avg
ON e.department = dept_avg.department
WHERE e.salary > dept_avg.avg_salary;

SELECT department, COUNT(*) AS num_employees
FROM employees
GROUP BY department
ORDER BY num_employees DESC
LIMIT 1;


SELECT * FROM employees
WHERE emp_id IS NULL OR name IS NULL OR age IS NULL OR job_role IS NULL OR salary IS NULL 
   OR project IS NULL OR join_date IS NULL OR department IS NULL;

CREATE TABLE employees_clean AS
SELECT * FROM employees
WHERE emp_id IS NOT NULL 
AND name IS NOT NULL 
AND age IS NOT NULL 
AND job_role IS NOT NULL 
AND salary IS NOT NULL 
AND project IS NOT NULL 
AND join_date IS NOT NULL 
AND department IS NOT NULL;

SELECT e.*, d.location
FROM employees e
JOIN departments d
ON e.department = d.department_name;

SELECT emp_id, name, department, salary,
RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;

SELECT emp_id, name, department, salary
FROM (
    SELECT emp_id, name, department, salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees
) ranked_employees
WHERE rank <= 3;

