import pandas as pd
# import sqlalchemy as db
import sqlalchemy as db
from prefect import flow, task


path_data = "/Users/ranierymendes/Documents/summer_2024/goal/etl/data/h+_sport_products.xlsx"

@flow(log_prints=True)
def main()->None:
    """
    Cleans up dataset to feed into data warehouse
    """

    print("Load excel")
    products = pd.read_excel(path_data, sheet_name='Sheet1')
    print("Transform data")
    products = transform_data(products)
    conn = db.create_engine("postgresql://postgres:3EgyY8eE8s7xNexG@fancifully-optimum-sleeper.data-1.use1.tembo.io:5432/postgres")
    products.to_sql("products", con=conn, if_exists="replace", index=False)
    print("Data cleaned and uploaded!")

@task
def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    df["Price"]  = round(df["Price"] * 0.92,2)
    return df

if __name__ == "__main__":
    main()
