import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="MFG0001")
@udf.function()
def z_mfg_sales_trans(sqlDB: fn.FabricSqlConnection) -> str:
    
    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()

    # test_data = [('A116260', 'Rollo Hazley', 'AU', 'rhazleyv@jigsy.com', 28429777.729999993, 82), ('A128083', 'Dorolice Hugett', 'TH', 'dhugett1d@simplemachines.org', 11033436.700000001, 47)]
    
    # Create the table if it doesn't exist
    create_table_query = """
        IF OBJECT_ID(N'dbo.sales_txn', N'U') IS NULL
        CREATE TABLE dbo.sales_txn (
            customer_code NVARCHAR(100) PRIMARY KEY,
            fullname NVARCHAR(100),
            country NVARCHAR(10),
            email NVARCHAR(100),
            total_spend_amt FLOAT,
            total_order INT
            );
    """
    cursor.execute(create_table_query)
    
    my_query = "EXEC sp_mfg_join_table;"
    cursor.execute(my_query)

    # Fetch all results
    results = []
    for row in cursor.fetchall():
        results.append(row)

    # Masking process
    def mask_email(email: str) -> str:
        try:
            cust_name, domain = email.split("@")
            updated_email = cust_name[:2] + "*" * max(1, len(cust_name) - 2)
            return f"{updated_email}@{domain}"
        except Exception:
            return "masked@unknown.com"

    masked_data = []
    for row in results:
        row = list(row)
        row[3] = mask_email(row[3])  
        masked_data.append(tuple(row))

    # Insert data into the table
    insert_query = "INSERT INTO dbo.sales_txn (customer_code, fullname, country, email, total_spend_amt, total_order) VALUES (?, ?, ?, ?, ?, ?);"
    cursor.executemany(insert_query, masked_data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()  

    return "Customer Sales Transaction table for Sales Department was created (if necessary) and data was added to this table"
