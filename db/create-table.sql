\c test_warehouse;

CREATE TABLE dim_staff (
    staff_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    location TEXT,
    email_address TEXT
);

SELECT * FROM dim_staff;