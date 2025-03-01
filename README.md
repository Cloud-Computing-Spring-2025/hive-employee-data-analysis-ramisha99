# HadoopHiveHue

# 🏢 Hive Employee & Department Data Analysis

## 📌 **Project Overview**
This project involves analyzing employee and department data using **Apache Hive**. The goal is to:
- Load employee and department data from CSV files into Hive.
- Transform and store data in a partitioned Hive table.
- Execute SQL queries for insights such as salary analysis, employee filtering, ranking, and joins.
- Store query results in text files and push them to GitHub.

## 🔄 **1. Loading Data into Hive**
### **Create Temporary Table for Employees**
```sql
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


LOAD DATA INPATH '/user/hive/warehouse/employee/employees.csv' INTO TABLE employees_temp;
Create Table for Departments

CREATE EXTERNAL TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/department/';
sql
Copy
Edit
LOAD DATA INPATH '/user/hive/warehouse/department/departments.csv' INTO TABLE departments;
🔄 2. Transform & Move Data to Partitioned Table

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

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

INSERT OVERWRITE TABLE employees PARTITION(department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department
FROM employees_temp;

MSCK REPAIR TABLE employees;

🔍 3. Queries and Execution


1️⃣ Retrieve Employees Who Joined After 2015

SELECT * FROM employees 
WHERE YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(join_date, 'yyyy-MM-dd'))) > 2015;
Run:



2️⃣ Find the Average Salary Per Department

SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department;

3️⃣ Identify Employees Working on the 'Alpha' Project

SELECT * FROM employees WHERE project = 'Alpha';


4️⃣ Count the Number of Employees in Each Job Role

SELECT job_role, COUNT(*) AS num_employees
FROM employees
GROUP BY job_role;

5️⃣ Retrieve Employees Earning Above the Department’s Average Salary

SELECT e.*
FROM employees e
JOIN (SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department) dept_avg
ON e.department = dept_avg.department
WHERE e.salary > dept_avg.avg_salary;

6️⃣ Find the Department with the Highest Number of Employees

SELECT department, COUNT(*) AS num_employees
FROM employees
GROUP BY department
ORDER BY num_employees DESC
LIMIT 1;

7️⃣ Check for Null Values in Any Column

SELECT * FROM employees
WHERE emp_id IS NULL OR name IS NULL OR age IS NULL OR job_role IS NULL OR salary IS NULL 
   OR project IS NULL OR join_date IS NULL OR department IS NULL;

#To exclude them from further analysis:

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


8️⃣ Join Employees and Departments (With Locations)

SELECT e.*, d.location
FROM employees e
JOIN departments d
ON e.department = d.department_name;

9️⃣ Rank Employees by Salary in Each Department

SELECT emp_id, name, department, salary,
RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;

🔟 Find the Top 3 Highest-Paid Employees Per Department

SELECT emp_id, name, department, salary
FROM (
    SELECT emp_id, name, department, salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees
) ranked_employees
WHERE rank <= 3;

🔗 4. Push Everything to GitHub
After saving .hql files and .txt output files, push them to GitHub:

bash
Copy
Edit
git init
git add .
git commit -m "Added Hive queries and output files"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
