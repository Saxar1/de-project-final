-- Шаг 1: Создание таблиц transactions и currencies
CREATE TABLE ST23052702__STAGING.transactions (
    operation_id VARCHAR(60),
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(30),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP
);

CREATE TABLE ST23052702__STAGING.currencies (
    date_update DATE,
    currency_code VARCHAR(3),
    currency_code_with VARCHAR(3),
    currency_with_div DECIMAL(10, 4)
);

-- Шаг 2: Создание проекций по датам для таблиц transactions и currencies
CREATE PROJECTION ST23052702__STAGING.transactions_date_projection (
    transaction_dt,
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount
) AS
SELECT
    transaction_dt,
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount
FROM ST23052702__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;

CREATE PROJECTION ST23052702__STAGING.currencies_date_projection (
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
) AS
SELECT
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
FROM ST23052702__STAGING.currencies
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES;

-- Шаг 3: Создание таблицы global_metrics в витрине
CREATE TABLE ST23052702__DWH.global_metrics (
    date_update DATE,
    currency_from VARCHAR(3),
    amount_total DECIMAL(10, 2),
    cnt_transactions INT,
    avg_transactions_per_account DECIMAL(10, 2),
    cnt_accounts_make_transactions INT
);

-- Шаг 4: Обновление витрины global_metrics ежедневно инкрементом
INSERT INTO ST23052702__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT
    DATEADD('day', -1, CURRENT_DATE) AS date_update,
    t.currency_from,
    SUM(t.amount) AS amount_total,
    COUNT(t.transaction_id) AS cnt_transactions,
    COUNT(t.transaction_id) / COUNT(DISTINCT t.user_id) AS avg_transactions_per_account,
    COUNT(DISTINCT t.user_id) AS cnt_accounts_make_transactions
FROM ST23052702__STAGING.transactions t
GROUP BY t.currency_from;