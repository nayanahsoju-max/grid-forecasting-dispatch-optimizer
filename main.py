from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine
app=FastAPI()
engine=create_engine("postgresql://energy_user:energy_pass@localhost:5432/energy_db")
@app.get("/forecast")
def get_forecast():
    df=pd.read_sql("SELECT * FROM analytics.forecast ORDER BY timestamp",engine)
    return df.to_dict(orient="records")