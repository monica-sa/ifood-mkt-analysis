import pandas as pd
from sqlalchemy import create_engine

def assign_spender_group(row):
    if row['MntTotal'] < 500:
        return 'Low'
    elif row['MntTotal'] <= 1000:
        return 'Medium'
    elif row['MntTotal'] <= 2000:
        return 'High'
    return 'VeryHigh'

def assign_income_group(row):
    if row['Income'] < 35196:
        return 'Low'
    elif row['Income'] <= 68281:
        return 'Middle'
    return 'High'

def get_education(row):
    education_levels = ['PhD', 'Master', 'Graduation', '2nCycle', 'Basic']
    for level in education_levels:
        if row[f'education{level}'] == 1:
            return level
    return 'Unknown'

def get_marital_status(row):
    statuses = ['Divorced', 'Married', 'Single', 'Together', 'Widow']
    for status in statuses:
        if row[f'marital{status}'] == 1:
            return status
    return 'Unknown'


def process_silver_data():
    # Load data
    file_path = '/opt/airflow/dags/resources/data/bronze/ifood_df_bronze.csv'
    cleaned_df = pd.read_csv(file_path)
    
    # Basic transformations
    cleaned_df["Dependents"] = cleaned_df["Kidhome"] + cleaned_df["Teenhome"]
    cleaned_df["TotalPurchases"] = cleaned_df[["NumDealsPurchases", "NumWebPurchases", 
                                               "NumCatalogPurchases", "NumStorePurchases"]].sum(axis=1)

    
    # RFM scoring
    r_labels, f_labels, m_labels = [4, 3, 2, 1], [1, 2, 3, 4], [1, 2, 3, 4]
    cleaned_df['R_Score'] = pd.qcut(cleaned_df['Recency'], 4, labels=r_labels)
    cleaned_df['F_Score'] = pd.qcut(cleaned_df['TotalPurchases'], 4, labels=f_labels)
    cleaned_df['M_Score'] = pd.qcut(cleaned_df['MntTotal'], 4, labels=m_labels)
    cleaned_df['RFM_Segment'] = cleaned_df[['R_Score', 'F_Score', 'M_Score']].apply(lambda x: ''.join(x.values.astype(str)), axis=1)
    cleaned_df['RFM_Score'] = cleaned_df[['R_Score', 'F_Score', 'M_Score']].sum(axis=1)


    # Demographic and behavioral segmentation 
    cleaned_df.loc[cleaned_df['Age'].between(0, 30, inclusive='both'), 'AgeGroup'] = 1
    cleaned_df.loc[cleaned_df['Age'].between(31, 45, inclusive='both'), 'AgeGroup'] = 2
    cleaned_df.loc[cleaned_df['Age'].between(46, 60, inclusive='both'), 'AgeGroup'] = 3
    cleaned_df.loc[cleaned_df['Age'].between(61, 74, inclusive='both'), 'AgeGroup'] = 4 
    cleaned_df.loc[cleaned_df['Age'].gt(75), 'AgeGroup'] = 5

    # Groups
    cleaned_df['SpenderGroup'] = cleaned_df.apply(assign_spender_group, axis=1)
    cleaned_df['IncomeGroup'] = cleaned_df.apply(assign_income_group, axis=1)
    cleaned_df['Education'] = cleaned_df.apply(get_education, axis=1)
    cleaned_df['MaritalStatus'] = cleaned_df.apply(get_marital_status, axis=1)

    # Preferred channel
    purchase_channels = cleaned_df[['NumDealsPurchases', 'NumCatalogPurchases', 'NumStorePurchases', 'NumWebPurchases']]
    # Identifying the preferred channel for each customer
    cleaned_df['PreferredChannel'] = purchase_channels.idxmax(axis=1)
    channel_mapping = {
        'NumDealsPurchases': 'Deals',
        'NumCatalogPurchases': 'Catalog',
        'NumStorePurchases': 'Store',
        'NumWebPurchases': 'Web'
    }
    cleaned_df['PreferredChannel'] = cleaned_df['PreferredChannel'].map(channel_mapping)
    
    # # Additional flags
    cleaned_df['isGraduated'] = cleaned_df[['educationMaster', 'educationPhD', 'educationGraduation']].max(axis=1)
    cleaned_df['isCouple'] = cleaned_df[['maritalMarried', 'maritalTogether']].max(axis=1) 
    cleaned_df['hasChild'] = (cleaned_df['Dependents'] > 0).astype(int)
    cleaned_df['isSingleParenting'] = ((cleaned_df['isCouple'] == 0) & (cleaned_df['hasChild'] == 1)).astype(int)
    cleaned_df['isChurn'] = (cleaned_df['Recency'] > 74).astype(int)  

    ## guarantee column name standardization
    cleaned_df.columns = cleaned_df.columns.str.replace(' ', '').str.replace('_', '')


    # Output
    cleaned_df.to_csv('/opt/airflow/dags/resources/data/silver/ifood_df_silver.csv', index=False)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres_container:5432/airflow')
    cleaned_df.to_sql('ifood_table_silver', engine, if_exists='replace', index=False)

if __name__ == "__main__":
    process_silver_data()