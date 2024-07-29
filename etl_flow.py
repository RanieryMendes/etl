import pandas as pd
# import sqlalchemy as db
import sqlalchemy as db
from prefect import flow

path_data = "/Users/ranierymendes/Documents/summer_2024/goal/etl/data/h+_sport_orders.xlsx"
# engine_conn = db.create_engine("postgresql://postgres:3EgyY8eE8s7xNexG@fancifully-optimum-sleeper.data-1.use1.tembo.io:5432/postgres")
# engine = create_engine("postgresql://postgres:3EgyY8eE8s7xNexG@fancifully-optimum-sleeper.data-1.use1.tembo.io:5432/postgres")

@flow(log_prints=True)
def main()->None:
    """
    Cleans up dataset to feed into data warehouse
    """
    conn = db.create_engine("postgresql://postgres:3EgyY8eE8s7xNexG@fancifully-optimum-sleeper.data-1.use1.tembo.io:5432/postgres")

    print("Load excel")
    orders = pd.read_excel(path_data, sheet_name='data')
    orders = orders[["OrderID", "Date", "TotalDue", "Status", "CustomerID", "SalespersonID"]]
    print("Attempt to connect...")
    # con = engine.connect()
#     print("Connected")
#     connection_fairy = con.connection

# # typically to run statements one would get a cursor() from this
# # object
#     cursor_obj = connection_fairy #.cursor()
    orders.to_sql("orders", con=conn, if_exists="replace", index=False)
    # con.close()
    print("Connection closed")
    print("Data cleaned and uploaded!")



if __name__ == "__main__":
    main()
