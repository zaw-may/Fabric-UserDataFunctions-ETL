import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="MFG0001")
@udf.function()
def z_mfg_sales_trans(sqlDB: fn.FabricSqlConnection) -> str:
    
    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
    
    # results = [('A116260', 'Rollo Hazley', 'AU', 'rhazleyv@jigsy.com', 28429777.729999993, 82), ('A128083', 'Dorolice Hugett', 'TH', 'dhugett1d@simplemachines.org', 11033436.700000001, 47), ('A109374', 'Jethro Fend', 'ID', 'jfend3@accuweather.com', 2486433.8799999994, 31), ('A113414', 'Berk Dwane', 'TH', 'bdwanek@independent.co.uk', 1968404.1600000001, 25), ('A136773', 'Avie Kenworthy', 'TH', 'akenworthy2o@seattletimes.com', 4092061.93, 24), ('A121286', 'Delainey Bollin', 'ID', 'dbollin18@intel.com', 4128961.0500000007, 22), ('A136318', 'Lothaire Folkes', 'TH', 'lfolkes2j@slashdot.org', 1294051.51, 19), ('A143014', 'Susannah Sandhill', 'TH', 'ssandhill4m@google.com.hk', 1660645.7899999998, 17), ('A133464', 'Hershel Kaveney', 'PH', 'hkaveney21@scribd.com', 2230145.66, 14), ('A146271', 'Eleonore Goomes', 'TH', 'egoomes5v@phpbb.com', 2573189.3200000003, 14), ('A132739', 'Jennica Angear', 'ID', 'jangear1t@cpanel.net', 2071865.6800000002, 12), ('A101708', 'Anallise Camings', 'IT', 'acamings0@un.org', 3365420.03, 12), ('A117187', 'Nomi Huddle', 'TH', 'nhuddlex@google.pl', 771564.87, 12), ('A120010', 'Maible Goggey', 'TH', 'mgoggey13@amazon.co.uk', 310561.4, 12), ('A143183', 'Rolf Blackford', 'TH', 'rblackford4q@printfriendly.com', 1055884.49, 11), ('A144005', 'Chelsae Stapleford', 'NZ', 'cstapleford52@huffingtonpost.com', 849673.37, 11), ('A144814', 'Ettie Gofton', 'ID', 'egofton5c@washington.edu', 697630.27, 10), ('A136354', 'Jabez Bart', 'TH', 'jbart2k@paypal.com', 356365.08999999997, 10), ('A119895', 'Miof mela Ratledge', 'ID', 'mratledge12@shinystat.com', 1071172.87, 10), ('A110039', 'Winni Clue', 'PH', 'wclue5@furl.net', 1403322.76, 10), ('A110168', 'Neils Convery', 'MY', 'nconvery6@etsy.com', 7229225.569999999, 9), ('A132459', 'Sheila-kathryn Hicks', 'TH', 'shicks1q@51.la', 1291155.8299999998, 9), ('A137820', 'Pavla Prinne', 'ID', 'pprinne34@pbs.org', 1258355.1400000001, 9), ('A140740', 'Easter Caselli', 'MY', 'ecaselli3q@chicagotribune.com', 406770.39, 9), ('A141795', 'Netty Jeanneau', 'ID', 'njeanneau42@networkadvertising.org', 1616403.2799999998, 9), ('A144841', 'Stuart Wehner', 'TH', 'swehner5d@hexun.com', 187076.0, 8), ('A113527', 'Diego Gregson', 'ID', 'dgregsonl@soup.io', 4681504.37, 8), ('A120810', 'Vasili Luxon', 'TH', 'vluxon15@addtoany.com', 2165219.0, 8), ('A146573', 'Sonia Hassur', 'TH', 'shassur61@vinaora.com', 297576.29, 8), ('A134212', 'Loralee Regitz', 'TH', 'lregitz25@hud.gov', 178053.1, 7), ('A141927', 'Kory Ronchetti', 'TH', 'kronchetti45@1und1.de', 236263.0, 7), ('A141949', 'Siusan Cargen', 'TH', 'scargen47@mysql.com', 2483659.67, 7), ('A143052', 'Moe Rangle', 'TH', 'mrangle4n@upenn.edu', 779649.0, 7), ('A141705', 'Harley Kew', 'ID', 'hkew3z@rediff.com', 1625355.53, 7), ('A137333', 'Keelia Nethercott', 'PH', 'knethercott2w@census.gov', 1542289.6600000001, 7), ('A138266', 'Arel Poller', 'ID', 'apoller3a@t.co', 569459.06, 6), ('A139867', 'Cinda McGauhy', 'TH', 'cmcgauhy3g@indiatimes.com', 1189460.0, 6), ('A134814', 'Sidonnie Francesco', 'ID', 'sfrancesco2a@digg.com', 406749.08, 6), ('A133042', 'Adriane Americi', 'ID', 'aamerici1x@marriott.com', 333162.57, 6), ('A148512', 'Nicolais Pigny', 'ID', 'npigny6g@chronoengine.com', 3940038.0300000003, 6)]

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