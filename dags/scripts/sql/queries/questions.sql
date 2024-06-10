--How many users were active on a given day (they made a deposit or withdrawal)
SELECT COUNT(DISTINCT user_id) AS active_users
FROM transaction_fact t
LEFT JOIN date_dim d ON t.date_id = d.date_id
WHERE d.date = DATE '2023-08-23'


--Identify users who haven't made a deposit
SELECT u.user_id
FROM user_dim u
LEFT JOIN transaction_fact t
ON u.user_id = t.user_id AND t.transaction_type = 'deposit'
WHERE t.user_id IS NULL;


--When was the last time a user made a login
SELECT user_id, MAX(event_timestamp) AS last_login_date
FROM login_fact
GROUP BY user_id;
