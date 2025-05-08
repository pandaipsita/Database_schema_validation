CREATE TABLE contractor_management.Contractor (
    contractor_id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    address VARCHAR(255),
    department_id INT
);

CREATE TABLE contractor_management.Department (
    department_id INT PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100)
);

CREATE TABLE contractor_management.Attendance (
    attendance_id INT PRIMARY KEY,
    contractor_id INT,
    attendance_date DATE,
    status VARCHAR(20)
);

CREATE TABLE contractor_management.LeaveRequests (
    leave_id INT PRIMARY KEY,
    contractor_id INT,
    leave_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    status VARCHAR(20)
);

CREATE TABLE contractor_management.Payroll (
    payroll_id INT PRIMARY KEY,
    contractor_id INT,
    payment_date DATE,
    amount DECIMAL(10,2)
);

CREATE TABLE contractor_management.PerformanceReviews (
    review_id INT PRIMARY KEY,
    contractor_id INT,
    review_date DATE,
    score INT,
    comments TEXT
);

CREATE TABLE contractor_management.TrainingRecords (
    training_id INT PRIMARY KEY,
    contractor_id INT,
    course_name VARCHAR(100),
    completed_date DATE
);

CREATE TABLE contractor_management.AssetsAssigned (
    asset_id INT PRIMARY KEY,
    contractor_id INT,
    asset_name VARCHAR(100),
    assigned_date DATE
);

CREATE TABLE contractor_management.EmergencyContacts (
    contact_id INT PRIMARY KEY,
    contractor_id INT,
    contact_name VARCHAR(100),
    relation VARCHAR(50),
    phone_number VARCHAR(20)
);

CREATE TABLE contractor_management.TransactionHistory_Contractor (
    transaction_id INT PRIMARY KEY,
    contractor_id INT,
    action_type VARCHAR(50),
    -- Only 'Payroll' should exist
    description TEXT,
    timestamp DATE
);