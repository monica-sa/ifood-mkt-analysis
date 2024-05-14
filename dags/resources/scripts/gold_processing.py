import pandas as pd
from sqlalchemy import create_engine

def aggregate_campaign_data(db_connection_string, table_name='ifood_table_silver', agg_table_name='ifood_table_campaign_agg'):
    engine = create_engine(db_connection_string)
    
    campaigns = ['AcceptedCmp1', 'AcceptedCmp2', 'AcceptedCmp3', 'AcceptedCmp4', 'AcceptedCmp5', 'AcceptedCmpOverall']
    
    # Start building the SQL query
    campaign_queries = []
    for campaign in campaigns:
        query = f"""
        SELECT
            '{campaign}' AS campaign,
            CASE 
                WHEN "{campaign}" = 1 THEN 'Accepted'
                ELSE 'Not Accepted'
            END AS acceptance,
            COUNT(*) AS total_customers,
            AVG("Income") AS avg_income,
            SUM("MntTotal") AS total_spend,
            AVG("TotalPurchases") AS avg_purchases,
            AVG("Age") AS avg_age,
            AVG("MntWines") AS avg_wines,
            AVG("MntFruits") AS avg_fruits,
            AVG("MntMeatProducts") AS avg_meat,
            AVG("MntFishProducts") AS avg_fish,
            AVG("MntSweetProducts") AS avg_sweet,
            AVG("MntGoldProds") AS avg_gold,
            AVG("MntRegularProds") AS avg_regular,
            AVG("NumDealsPurchases") AS avg_deals,
            AVG("NumCatalogPurchases") AS avg_catalog,
            AVG("NumStorePurchases") AS avg_store,
            AVG("NumWebPurchases") AS avg_web
        FROM
            {table_name}
        GROUP BY
            1,2
        """
        campaign_queries.append(query)
    
    # Combine all campaign queries
    full_query = " UNION ALL ".join(campaign_queries)
    
    # Execute the query and fetch the results
    with engine.connect() as conn:
        result = conn.execute(full_query)
        agg_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    # Write the aggregated DataFrame to a new table
    agg_df.to_sql(agg_table_name, engine, if_exists='replace', index=False)

if __name__ == "__main__":
    db_connection_string = 'postgresql+psycopg2://airflow:airflow@postgres_container:5432/airflow'
    aggregate_campaign_data(db_connection_string)