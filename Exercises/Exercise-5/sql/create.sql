DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    customer_id INT, 
    first_name VARCHAR(40) NOT NULL, 
    last_name VARCHAR(40), 
    address_1 VARCHAR(100), 
    address_2 VARCHAR(50), 
    city VARCHAR(50), 
    state VARCHAR(50), 
    zip_code VARCHAR(12), 
    join_date DATE,
    CONSTRAINT PK_accounts PRIMARY KEY (customer_id)
);
CREATE INDEX IDX_accounts_first_name ON accounts (first_name);


CREATE TABLE products (
    product_id INT,
    product_code INT NOT NULL UNIQUE,
    product_description VARCHAR(100),
    CONSTRAINT PK_products PRIMARY KEY (product_id)
);
CREATE INDEX IDX_products_product_code ON products(product_code);


CREATE TABLE transactions (
    transaction_id VARCHAR(27), 
    transaction_date DATE NOT NULL, 
    product_id INT NOT NULL, 
    product_code INT, 
    product_description VARCHAR(100), 
    quantity INT NOT NULL, 
    account_id INT,
    CONSTRAINT PK_transactions PRIMARY KEY (transaction_id),
    CONSTRAINT FK_accounts FOREIGN KEY (account_id) REFERENCES accounts(customer_id),
    CONSTRAINT FK_products_product_id FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT FK_products_product_code FOREIGN KEY (product_code) REFERENCES products(product_code)
);
CREATE INDEX IDX_transactions_transaction_date ON transactions(transaction_date);