INSERT INTO temp_json
(application_id, set_of_books_id, period_name, last_update_date, last_updated_by, closing_status, start_date, end_date, year_start_date, quarter_num)
VALUES
(1, 101, 'January 2024', '2024-01-31', 'John Doe', 'Closed', '2024-01-01', '2024-01-31', '2024-01-01', 1),
(2, 101, 'February 2024', '2024-02-29', 'Jane Smith', 'Open', '2024-02-01', '2024-02-29', '2024-01-01', 1),
(3, 101, 'March 2024', '2024-03-31', 'Alice Johnson', 'Closed', '2024-03-01', '2024-03-31', '2024-01-01', 1),
(4, 101, 'April 2024', '2024-04-30', 'Bob Brown', 'Open', '2024-04-01', '2024-04-30', '2024-01-01', 2),
(5, 101, 'May 2024', '2024-05-31', 'Chris Lee', 'Closed', '2024-05-01', '2024-05-31', '2024-01-01', 2);
