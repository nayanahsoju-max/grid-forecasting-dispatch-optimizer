import pandas as pd
import numpy as np
from sqlalchemy import create_engine
DB_URI="postgresql://energy_user:energy_pass@localhost:5432/energy_db"
engine=create_engine(DB_URI)
def load_data():
    weather=pd.read_sql("SELECT * FROM raw.weather",engine)
    gen=pd.read_sql("SELECT * FROM raw.generation",engine)
    weather["timestamp"]=pd.to_datetime(weather["timestamp"]).dt.floor("h")
    gen["timestamp"]=pd.to_datetime(gen["timestamp"]).dt.floor("h")
    df=pd.merge_asof(weather.sort_values("timestamp"),gen.sort_values("timestamp"),on="timestamp",direction="nearest")
    return df
def create_features(df):
    df["hour"]=df["timestamp"].dt.hour
    df["solar_lag_1"]=df["solar"].shift(1)
    df["wind_lag_1"]=df["wind"].shift(1)
    df["wind_rolling_3"]=df["wind"].rolling(3).mean()
    df["temp_rolling_3"]=df["temperature"].rolling(3).mean()
    df=df.bfill().ffill()
    return df
def save_features(df):
    df.to_sql("engineered_features",engine,schema="features",if_exists="replace",index=False)
    print("Features saved",len(df))
if __name__=="__main__":
    df=load_data()
    df=create_features(df)
    save_features(df)