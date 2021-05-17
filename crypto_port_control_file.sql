/*
    Database Name: crypto_port
    Author: Troy Jennings
    Date: April 28, 2021
*/

-- user database
USE crypto_port;

SET GLOBAL local_infile = 'ON';

SELECT 'Seeding test data' AS '';

-- -- seed user data
-- load data local infile '/Users/troyjennings/Documents/masters/comp-3421/week4/users_2021-04-28.csv'
--     into table user
--     fields terminated by '|'
--     lines terminated by '\n';

-- -- seed portfolio data
-- load data local infile '/Users/troyjennings/Documents/masters/comp-3421/week4/portfolio_2021-04-28.csv'
--     into table portfolio
--     fields terminated by '|'
--     lines terminated by '\n';

-- seed token data
load data local infile '/Users/troyjennings/Documents/masters/comp-3421/week4/assets_2021-05-05.csv'
    into table token
    fields terminated by '|'
    lines terminated by '\n';

-- get results
SELECT count(*) from user;
SELECT count(*) from portfolio;
SELECT count(*) from token;
