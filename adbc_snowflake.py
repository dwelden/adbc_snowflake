# https://arrow.apache.org/adbc/0.5.1/python/quickstart.html
# python pip install adbc_driver_manager adbc_driver_snowflake pyarrow

import adbc_driver_snowflake.dbapi

snowflake_uri = 
"""
user[:password]@account/database/schema[?param1=value1&paramN=valueN]
user[:password]@account/database[?param1=value1&paramN=valueN]
user[:password]@host:port/database/schema?account=user_account[&param1=value1&paramN=valueN]
host:port/database/schema?account=user_account[&param1=value1&paramN=valueN]
 """

with adbc_driver_snowflake.dbapi.connect(snowflake_uri) as conn:
    cursor = conn.cursor()

    # Row-oriented fetch
    cursor.execute("SELECT 1, 2.0, 'Hello, world!'")
    cursor.fetchone()

    # Arrow fetch
    cursor.execute("SELECT 1, 2.0, 'Hello, world!'")
    cursor.fetch_arrow_table()

    # Parameterized query
    cursor.execute("SELECT ? + 1 AS the_answer", parameters=(41,))
    cursor.fetch_df()

    # Bind values
    cursor.executemany("INSERT INTO example VALUES ($1, $2)", [(1,2),(3,4)])

    # Bind Arrow data
    data = pyarrow.record_batch(
        [
            [5,6],
            [7,8],
        ],
    )
    cursor.executemany("INSERT INTO example VALUES ($1, $2)", data)

    # Ingest bulk data
    import pyarrow
    table = pyarrow.table([[1, 2], ["a", None]], names=["ints", "strs"])
    cursor.adbc_ingest("sample", table)
    cursor.execute("SELECT COUNT(DISTINCT ints) FROM sample")
    cursor.fetchall()

    # Append existing table
    table = pyarrow.table([[2, 3], [None, "c"]], names=["ints", "strs"])
    cursor.adbc_ingest("sample", table, mode="append")

    # Get database/driver metadata
    conn.adbc_get_info()["vendor_name"]
    conn.adbc_get_info()["driver_name"]

    # Query catalogs, schemas, tables, and columns
    info = conn.adbc_get_objects().read_all().to_pylist()
    main_catalog = info[0]
    schema = main_catalog["catalog_db_schemas"][0]
    tables = schema["db_schema_tables"]
    tables[0]["table_name"]
    [column["column_name"] for column in tables[0]["table_columns"]]

    # Get Arrow schema of a table
    conn.adbc_get_table_schema("sample")

# Driver manager
import adbc_driver_manager

with adbc_driver_manager.AdbcDatabase(driver="adbc_driver_snowflake") as db:
    with adbc_driver_manager.AdbcConnection(db) as conn:
        pass

