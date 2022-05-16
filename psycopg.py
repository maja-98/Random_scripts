import psycopg2


def connect_database(events):
    conn = psycopg2.connect(
        database=events['database'],
        user=events['username'],
        password=events['password'],
        host=events['host'],
        port=events['port']
        )

    conn.autocommit = True
    return conn


def create_tables(events):
    #creating tables in database
    conn=connect_database(events)
    cursor = conn.cursor()

    sql ='''CREATE TABLE IF NOT EXISTS Customer(
       customer_id INT PRIMARY KEY NOT NULL,
       first_name VARCHAR(20) NOT NULL,
       last_name VARCHAR(20) NOT NULL,
       date_of_birth DATE NOT NULL
    )'''
    cursor.execute(sql)

    sql ='''CREATE TABLE IF NOT EXISTS Transactions(
       txn_id INT PRIMARY KEY  NOT NULL,
       customer_id INT REFERENCES Customer(customer_id),
       txn_type VARCHAR(10) NOT NULL,
       txn_amount INT,
       transaction_date DATE NOT NULL
    )'''
    cursor.execute(sql)
    #insert_values(cursor)
    conn.close()

#for testing purpose added
def insert_values(cursor):
    try:
        sql ='''INSERT INTO Customer(customer_id,first_name,last_name,date_of_birth)
        VALUES(3,'TEST','ABCD','1996-12-22') 
        '''
        cursor.execute(sql)
    except Exception as e:
        print(e)
        pass
    try:
        sql ='''INSERT INTO Transactions(txn_id,customer_id,txn_type,txn_amount,transaction_date)
        VALUES(1249,'3','CREDIT',2500,'2021-05-10') 
        '''
        cursor.execute(sql)
        
    except Exception as e:
        print(e)
        pass
    
    cursor.execute('select * from customer')
    print(cursor.fetchall())
    cursor.execute('select * from transactions')
    print(cursor.fetchall())

    
def calculate_savings(events,context):
    try:
        date = events['date']
        year,month,day=list(map(int,date.split('-')))
        conn=connect_database(events)
        cursor=conn.cursor()
        sql='''SELECT a.customer_id,a.txn_type,a.txn_amount,a.transaction_date,b.date_of_birth
               from Transactions a
               LEFT JOIN Customer b on a.customer_id=b.customer_id
               WHERE a.transaction_date <= date '
               '''+date+'''
                '
               '''
        
        
        cursor.execute(sql)
        result = cursor.fetchall()
        customer_transactions={}
        for value in result:
            customer_id,txn_type,txn_amount,transaction_date,date_of_birth=value
            
            
            if customer_id not in customer_transactions:
                #age is not accurate. require some more calculations
                customer_transactions[customer_id] = {'age':(year-date_of_birth.year),'savings':0}
                
            if txn_type=='CREDIT':
                    customer_transactions[customer_id]['savings']+=txn_amount
            else:
                    customer_transactions[customer_id]['savings']-=txn_amount

        data={}
        for values in customer_transactions:
            value = customer_transactions[values]
            if value['age'] not in data:
                data[value['age']]=[value['savings']]
            else:
                data[value['age']].append( value['savings'])
        for val in data:
            data[val]=round(sum(data[val])/len(data[val]))
        
        payload={
            "status":200,
            "data":data
            }
        return payload
    except Exception as e:
        payload= {
            "statusCode":400,
            "message":str(e)
            }
        print(payload)
        return payload



    
events={
    "database":'postgres',
    "port":5432,
    "host":'127.0.0.1',
    "username":'postgres',
    'password':'password',
    'date': '2022-05-13'
    }
create_tables(events)

payload=calculate_savings(events,{})
print(payload)

