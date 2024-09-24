CREATE TABLE Dim_tbl_training_development (
 Training_id serial PRIMARY key not null,
 TrainingName varchar(255),
 StartDate DATE,
 EndDate DATE
);

CREATE TABLE Dim_tbl_emp_training_developmnet (
 Emp_td_id serial PRIMARY key not null,
 EmployeeName varchar(255),
 TrainingName varchar(255),
 StartDate DATE,
 EndDate DATE,
 Status varchar(255),
 training_id int,
 CONSTRAINT fk_dim_tbl_emp_training_development FOREIGN KEY(training_id) REFERENCES Dim_tbl_training_development(Training_id)
);

CREATE TABLE Dim_tbl_emp_training_performance (
 Emp_tp_id serial PRIMARY key not null,
 EmployeeName varchar(255),
 ReviewPeriod varchar(255),
 Rating float,
 Comments varchar(255)
);

CREATE TABLE Dim_tbl_job_salary (
 Job_id serial PRIMARY key not NULL,
 Title varchar(255),
 Department varchar(255),
 Salary int
);

CREATE TABLE Dim_tbl_emp ( 
 Employee_id int,
 EmployeeName varchar(255),
 Gender varchar(255),
 Age int,
 Department varchar(255),
 Title varchar(255),
 PRIMARY KEY (Employee_id)
);

CREATE TABLE Dim_tbl_emp_training_performance_review (
 TPR_id serial PRIMARY key not NULL,
 TD_id int,  
 TP_id int,
 CONSTRAINT fk_fact_emp_training_dev_p FOREIGN KEY(TD_id) REFERENCES Dim_tbl_emp_training_developmnet(Emp_td_id),
 CONSTRAINT fk_fact_emp_training_performance FOREIGN KEY(TP_id) REFERENCES Dim_tbl_emp_training_performance(Emp_tp_id)
);

CREATE TABLE Dim_tbl_emp_salary (
 EmpSalary_id  serial PRIMARY key not NULL,
 EmployeeName varchar(255),
 Gender varchar(255),
 Age int,
 Department varchar(255),
 Title varchar(255),
 OverTimePay int,
 PaymentDate Date,
 Work_id int,
 Person_id int,
 CONSTRAINT fk_fact_emp_job FOREIGN KEY(Work_id) REFERENCES Dim_tbl_job_salary(Job_id),
 CONSTRAINT fk_fact_emp_sal FOREIGN KEY(Person_id) REFERENCES Dim_tbl_emp(Employee_id)
);

CREATE TABLE Fact_tbl_HR (
 EmpSalary_id int,
 TPR_id int,
 CONSTRAINT fk_fact_hr_salary FOREIGN KEY(EmpSalary_id) REFERENCES Dim_tbl_emp_salary(EmpSalary_id),
 CONSTRAINT fk_fact_hr_training FOREIGN KEY(TPR_id) REFERENCES Dim_tbl_emp_training_performance_review(TPR_id)
);