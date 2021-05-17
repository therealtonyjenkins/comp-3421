/*
    Database Name: crypto_port
    Author: Troy Jennings
    Date: April 27, 2021
*/

-- create database
SELECT 'Dropping and create crypto_port database' AS '';
DROP DATABASE IF EXISTS crypto_port;
CREATE DATABASE crypto_port;
USE crypto_port;

-- create user table
SELECT 'Create user' AS '';
DROP TABLE IF EXISTS user;
CREATE TABLE user (
    uuid CHAR(36) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email_address VARCHAR(50),
    PRIMARY KEY (uuid)
);

-- create portfolio table
SELECT 'Create portfolio' AS '';
DROP TABLE IF EXISTS portfolio;
CREATE TABLE portfolio (
    uuid CHAR(36) NOT NULL,
    user_id CHAR(36) NOT NULL,
    name VARCHAR(140),
    balance DECIMAL(10, 2),
    PRIMARY KEY (uuid),
    FOREIGN KEY (user_id) REFERENCES user (uuid)
);

-- create token table
SELECT 'Create token' AS '';
DROP TABLE IF EXISTS token;
CREATE TABLE token (
    uuid CHAR(36) NOT NULL,
    symbol VARCHAR(10),
    name VARCHAR(50),
    PRIMARY KEY (uuid)
);

-- create position table
SELECT 'Create position' AS '';
DROP TABLE IF EXISTS position;
CREATE TABLE position (
    uuid CHAR(36) NOT NULL,
    portfolio_id CHAR(36) NOT NULL,
    token_id CHAR(36) NOT NULL,
    volume DECIMAL(15, 6),
    PRIMARY KEY (uuid),
    FOREIGN KEY (portfolio_id) REFERENCES portfolio (uuid),
    FOREIGN KEY (token_id) REFERENCES token (uuid)
);

-- create portfolio table
SELECT 'Create position_transaction' AS '';
DROP TABLE IF EXISTS position_transaction;
CREATE TABLE position_transaction (
    portfolio_id CHAR(36) NOT NULL,
    position_id CHAR(36) NOT NULL,
    position_date DATETIME,
    action VARCHAR(10),
    PRIMARY KEY (portfolio_id, position_id),
    FOREIGN KEY (portfolio_id) REFERENCES portfolio (uuid),
    FOREIGN KEY (position_id) REFERENCES position (uuid)
);

-- create transaction table
SELECT 'Create transaction' AS '';
DROP TABLE IF EXISTS transaction;
CREATE TABLE transaction (
    uuid CHAR(36) NOT NULL,
    token_id CHAR(36) NOT NULL,
    transaction_datetime DATETIME,
    volume DECIMAL(20, 11),
    price DECIMAL(20, 11),
    PRIMARY KEY (uuid),
    FOREIGN KEY (token_id) REFERENCES token (uuid)
);

-- create intraday_price table
SELECT 'Create intraday_price' AS '';
DROP TABLE IF EXISTS intraday_price;
CREATE TABLE intraday_price (
    uuid CHAR(36) NOT NULL,
    token_id CHAR(36) NOT NULL,
    price_datetime DATETIME,
    open_price DECIMAL(20, 11),
    close_price DECIMAL(20, 11),
    trade_volume DECIMAL(20, 11),
    PRIMARY KEY (uuid),
    FOREIGN KEY (token_id) REFERENCES token (uuid)
);

-- describes
DESCRIBE user;
DESCRIBE portfolio;
DESCRIBE position;
DESCRIBE position_transaction;
DESCRIBE token;
DESCRIBE transaction;
DESCRIBE intraday_price;

SELECT 'Seeding test data' AS '';

-- seed user data
load data local infile '/Users/troyjennings/Documents/masters/comp-3421/week4/users_2021-04-28.csv'
    into table user
    fields terminated by '|'
    lines terminated by '\n';

-- seed portfolio data
load data local infile '/Users/troyjennings/Documents/masters/comp-3421/week4/portfolio_2021-04-28.csv'
    into table portfolio
    fields terminated by '|'
    lines terminated by '\n';

-- seed token data
load data local infile '/Users/troyjennings/Documents/masters/comp-3421/week4/assets_2021-04-28.csv'
    into table token
    fields terminated by '|'
    lines terminated by '\n';

-- get results
SELECT count(*) from user;
SELECT count(*) from portfolio;
SELECT count(*) from token;

SHOW WARNINGS;
