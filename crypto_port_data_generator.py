import requests
import sys
from faker import Faker
import uuid
import csv
import random
import time
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timezone
from collections import defaultdict

messari_api_key = '2c56ef31-37a2-498c-8396-d4baf854686f'
coinapi_api_key = 'C580D8B6-886D-4CB6-8611-D76DE9FCB5AF'

class MessariClient():
    def __init__(self, token):
        self.token = token
        self.host = 'https://data.messari.io'
        self.headers = { 'x-messari-api-key': self.token }

    def get_assets(self, session, page, limit):
        '''Makes a request to the Messari API to get asset data.'''
        print(f'Retrieving {limit} assets for page {page}')
        # assets v2 path
        asset_path = '/api/v2/assets'
        # define fieldset for request
        payload = {
            'page': page,
            'sort': 'id',
            'limit': limit,
            'fields': ','.join(['id', 'symbol', 'slug'])
        }
        # make request
        r = session.get(self.host + asset_path, headers=self.headers, params=payload)
        # formulate custom response
        response = {}
        if r.status_code == 200:
            response['status'] = 'OK'
            response['data'] = r.json()['data']
            return response
        else:
            print(f'ERROR: Request {asset_path} returned error {r.status_code}')
            # print(f'ERROR: URL: {r.url}')
            response['status'] = 'ERR'
            response['data'] = {}
            return response

    def get_token_timeseries(self, session, page, limit, token_slug, metric_id):
        '''Makes a request to the Messari API to get asset timeseries data.'''
        print(f'Retrieving {limit} timeseries points for page {page}')
        # assets v2 path
        timeseries_path = f'/api/v1/assets/{token_slug}/metrics/{metric_id}/time-series'
        # define fieldset for request
        payload = {
            'start': page,
            'sort': 'id',
            'limit': limit,
            'fields': ','.join(['id', 'symbol', 'name', 'slug', 'metrics/market_data'])
        }
        # make request
        r = session.get(self.host + timeseries_path, headers=self.headers, params=payload)
        # formulate custom response
        response = {}
        if r.status_code == 200:
            response['status'] = 'OK'
            response['data'] = r.json()['data']
            return response
        else:
            print(f'ERROR: Request {asset_path} returned error {r.status_code}')
            # print(f'ERROR: URL: {r.url}')
            response['status'] = 'ERR'
            response['data'] = {}
            return response

class CoinAPIClient():
    def __init__(self, token):
        self.token = token
        self.host = 'https://rest.coinapi.io/'
        self.headers = {'X-CoinAPI-Key' : self.token}
    
    def get_historical_token_trades(self, session, symbol, limit, timestart):
        pass

class MySQLClient():
    def __init__(self):
        try:
            conn = mysql.connector.connect(
                user= 'root', 
                password= '#Adm1nistr@t0r', 
                host= 'localhost', 
                database= 'crypto_port', 
                allow_local_infile= True
            )
            self.conn = conn
            print('Setup connection to [localhost]')
        # error handling
        except Exception as error:
            print(error)
            sys.exit(1)       

    def select_database(self):
        """Selects the crypto_port database for using."""
        db_query = ('USE crypto_port')
        data = ()
        self.execute_query(db_query, data)
    
    def execute_query(self, query, data):
        """Executes a query with parameters.
        
        Arguments
        ---------
        query : str; required
            The query string to execute.
        data : tuple; required
            The query parameters to pass into parameterized queries.
        """

        # attempt the query execution
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, data)
            result = cursor.fetchall()
            # commit for data inserts
            self.conn.commit()
            return result

        # exception handling
        except mysql.connector.Error as error:
            print(error)
            if (self.conn.is_connected()):
                cursor.close()
                self.conn.close()
            sys.exit(1)

    def get_random_users(self, num_users):
        """Selects a specified number of randomly selected users.
        
        Arguments
        ---------
        num_users : int; required
            The number of random users to select.
        """

        # get users
        user_query = (
            'SELECT u.uuid, u.first_name, u.last_name FROM user AS u '
            'ORDER BY RAND() LIMIT %s'
        )
        user_data = (num_users, )
        # print(f'\tGetting {num_users} random users')
        return self.execute_query(user_query, user_data)

    def insert_positions(self, user_id, token_id, volume):
        """Inserts positions into a user's portfolio and records a position
        transaction for the insert.
        
        Arguments
        ---------
        user_id : str; required
            The uuid record for a given user.
        token_id : str; required
            The uuid record for a token.
        volume : decimal; required
            The number of token(s) to purchase.
        """

        # 1. get user's portfolio
        portfolio_query = (
            'SELECT p.uuid FROM portfolio AS p '
            'WHERE p.user_id = %s'
        )
        portfolio_data = (user_id, )
        # print(f'\tGetting portfolio for user with id [{user_id}]')
        portfolio_id = self.execute_query(portfolio_query, portfolio_data)[0][0]

        # 2. creation position
        position_query = (
            'INSERT INTO position VALUES (%s, %s, %s, %s)'
        )
        # create new id for the position
        position_id = str(uuid.uuid4())
        position_data = (position_id, portfolio_id, token_id, volume)
        self.execute_query(position_query, position_data)
        # print(f'\t\tCreated new position [{position_id}]')
        
        # 3. create position_transaction
        transaction_query = (
            'INSERT INTO position_transaction VALUES (%s, %s, %s, %s)'
        )
        # create timestamp
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
        transaction_data = (portfolio_id, position_id, timestamp, 'buy')
        self.execute_query(transaction_query, transaction_data)
        # print(f'\t\tCreated new position_transaction')

        # 4. update portfolio balance
        portfolio_query = (
            'UPDATE portfolio AS p '
            'SET p.balance = %s'
            'WHERE p.user_id = %s'
        )
        portfolio_data = (volume, user_id)
        # print(f'\tGetting portfolio for user with id [{user_id}]')
        self.execute_query(portfolio_query, portfolio_data)

    def close_connection(self):
        """Closes an active database connection"""
        self.conn.close()
        print('Closed connection to [localhost]')

def main():
    # # setup faker client
    # faker_client = Faker()
    # Faker.seed(0)

    # # process users
    # export_users_and_portfolios(faker_client, 20000)

    # # setup Messari client and session
    # messari_client = MessariClient(messari_api_key)
    # session = requests.Session()

    # # process assets
    # export_assets(messari_client, session)

    add_buy_positions(100, 10, 10)
    
    print('Script completed')

def export_transaction_timeline(client, session, token):
    pass

def export_assets(client, session):
    """Executes a query against the Messari API for assets and writes asset data to CSV."""
    # track pagecount and totals for assets
    asset_page = 1
    asset_count = 0

    # open file for writing
    filename = 'assets_' + str(datetime.now(timezone.utc)).replace(' ', '-')[0:10] + '.csv'
    with open(filename, 'w') as file:
        # setup dictwriter writer
        fieldnames = [
            'id', 
            'symbol', 
            'slug', 
        ]
        csvwriter = csv.DictWriter(file, fieldnames=fieldnames, quoting=csv.QUOTE_NONE, delimiter='|')
        
        # paginate through assets
        while True:
            # request assets
            asset_data = client.get_assets(session, asset_page, 500)

            if asset_data['status'] == 'OK':
                if asset_data['data']:
                    # asset response status is OK and data exists
                    for asset in asset_data['data']:
                        if asset['symbol']:
                            # collect data then dump it to file
                            data = {
                                'id': asset['id'],
                                'symbol': asset['symbol'],
                                'slug': asset['slug'],
                            }
                            # write asset to file
                            csvwriter.writerow(data)
                            # increment asset counter
                            asset_count += 1
                # data is empty; exit loop
                else:
                    print(asset_data['data'])
                    break
            # status is anything other than OK; exit loop
            else:
                break
            
            # increment page counter
            asset_page += 1
        print(f'SUCCESS: {asset_count} assets collected.')

def export_users_and_portfolios(client, count):
    """Randomly generates user data and writes data to CSV."""

    # track totals for users and portfolios
    user_count = 0
    portfolio_count = 0
    # domains for generating email address
    domains = ['gmail.com', 'yahoo.com', 'msn.com', 'aol.com', 'mozilla.com', 'hotmail.com']

    # setup writer files and fieldnames
    user_filename = 'users_' + str(datetime.now(timezone.utc)).replace(' ', '-')[0:10] + '.csv'
    user_fieldnames = [ 'uuid', 'first_name', 'last_name', 'email_address' ]
    portfolio_filename = 'portfolio_' + str(datetime.now(timezone.utc)).replace(' ', '-')[0:10] + '.csv'
    portfolio_fieldnames = [ 'uuid', 'user_id', 'name', 'balance' ]

    # open file for writing
    with open(user_filename, 'w') as user_file:
        user_writer = csv.DictWriter(user_file, fieldnames=user_fieldnames, quoting=csv.QUOTE_NONE, delimiter='|')
        user_writer.writeheader()
        with open(portfolio_filename, 'w') as portfolio_file:
            portfolio_writer = csv.DictWriter(portfolio_file, fieldnames=portfolio_fieldnames, quoting=csv.QUOTE_NONE, delimiter='|')
            portfolio_writer.writeheader()

            for i in range(0, count):
                # randomly select a number within the range of domains
                index = random.randrange(0, len(domains)-1, 1)
                # store user and portfolio data
                user_row = {}
                portfolio_row = {}

                first = client.first_name()
                last = client.last_name()
                user_id = uuid.uuid4()
                port_id = uuid.uuid4()

                user_row['uuid'] = str(user_id)
                user_row['first_name'] = first
                user_row['last_name'] = last
                user_row['email_address'] = first + '.' + last + '@' + domains[index]

                portfolio_row['uuid'] = str(port_id)
                portfolio_row['user_id'] = str(user_id)
                portfolio_row['name'] = 'Default Portfolio'
                portfolio_row['balance'] = 0

                # write rows
                user_writer.writerow(user_row)
                portfolio_writer.writerow(portfolio_row)
                user_count += 1
                portfolio_count += 1

    print(f'SUCCESS: {user_count} users generated with {portfolio_count} portfolios.')

def add_buy_positions(num_users= 1, num_tokens= 1, max_transactions= 1):
    """Seeds position and position_transaction data for users.

    Arguments
    ---------
    num_users : int 
        The number of users for which data should be seeded.
    num_tokens : int
    max_transactions : int 
        The ceiling for number of purchase transactions to generate per user.
    """
    # counters
    total_positions = 0

    # cache for asset data and names
    preferred_assets = [
        'BTC'
        'ETH',
        'TRX',
        'ADA',
        'DOGE',
        'XLM',
        'LTC',
        'BCH',
        'DASH',
        'DOT',
        'XMR',
        'NEO'
    ]
    assets = {}
    asset_names = []
    # read in assets to collections
    try:
        with open('./data/assets_2021-05-05.csv', 'r') as asset_file:
            csv_reader = csv.reader(asset_file, delimiter='|')
            for row in csv_reader:
                if row[1] in preferred_assets:
                    # stores new item in assets as { asset_name: asset_id }
                    assets[row[1]] = row[0].strip().replace('\n', '').replace('\r', '')
                    # stores the name
                    asset_names.append(row[1].strip().replace('\n', '').replace('\r', ''))
    except Error as e:
        print(e)
    
    # create transactions by token
    for _ in range(num_tokens):
        # get the key for a randomly selected token
        token_name = asset_names[random.randint(0, len(asset_names)-1)]
        token_key = assets[token_name]
        print(f'\tSeeding data for token [{token_name}]')
        
        # get random users
        # setup MySQL client
        sql = MySQLClient()
        # select the database
        sql.select_database()

        # get single element tuples (uuid, )
        for user in sql.get_random_users(num_users):
            user_positions = 0
            # create positions and position transactions
            username = user[1] + ' ' + user[2]
            # print(f'\t\tSeeding data for user [{username}]')

            # create a random number of transactions for the user
            for _ in range(random.randint(1, max_transactions)):
                sql.insert_positions(
                    user[0], # user uuid
                    token_key.strip().replace('\n', '').replace('\r', ''), # token id
                    round(random.expovariate(6),6) # random volume
                )
                total_positions += 1
                user_positions += 1
            # print(f'\t\t\t{user_positions} positions created for {username}')
        # force close the mysql connection
        sql.close_connection()

    print(f'{total_positions} positions created across {num_users} users across {num_tokens} tokens')

if __name__ == '__main__':
    main()