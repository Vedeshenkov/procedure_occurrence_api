# main.py
import uvicorn
import os
from fastapi import FastAPI, HTTPException
from typing import Optional
from google.cloud import bigquery
from pydantic import BaseModel


#bigquery setup
path = os.path.join(os.path.dirname(__file__),"virtual-bonito-382111-dbc51f57aeb1.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=path
client = bigquery.Client()


#FastApi setup
tags_metadata = [
    {
        "name": "count",
        "description": " Endpoint to get the total count of rows from the procedure_occurrence table.",
    },
    {
        "name": "person_count",
        "description": "Endpoint to get count of persons for the last N procedure_dat.",
    },
]


app = FastAPI(
    title="My API",
    description="This is a sample API",
    version="0.1.0",
    openapi_tags=tags_metadata,
)


class PersonCountRequest(BaseModel):
    last_n_dates: Optional[int] = 7

class CountResponse(BaseModel):
    count: int
    

        

# Endpoint to get the total count of rows from the procedure_occurrence table
@app.get("/procedure_occurrence/count",
         response_model=CountResponse,
         tags=["count"])
async def get_procedure_count():
    """
    Endpoint to get count of rows   from the procedure_occurrence table
    
    Args:
        None
        
    Returns:
        TotalCountResponse: response body containing total row count 
    """
    
    query = """
        SELECT COUNT(*) AS count 
        FROM  `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
    """
    query_job = client.query(query)
    results = query_job.result()
    count = None
    for row in results:
        count = row.count
    if count is None:
         raise HTTPException(status_code=404, detail="No data found in procedure_occurrence table")
    return {"count": count}



# Endpoint to get count of persons for the last N procedure_dat
@app.post("/procedure_occurrence/person_count",
         response_model=CountResponse,
         tags=[ "person_count"])
async def get_person_count(personalcountrequest: PersonCountRequest):
    """
    Endpoint to get count of persons for the last N procedure_dat.
    
    Args:
        request (personalcountrequest): request body containing the last_n_dates parameter
        
    Returns:
        PersonCountResponse: response body containing the count of persons
    """
    query = f"""
       SELECT COUNT(DISTINCT person_id) AS count
       FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
       WHERE procedure_dat IN (
           SELECT procedure_dat 
           FROM (
               SELECT DISTINCT procedure_dat 
               FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
               ORDER BY procedure_dat DESC 
               LIMIT {personalcountrequest.last_n_dates}
               )
           )
    """
    query_job = client.query(query)
    results = query_job.result()
    count = None
    for row in results:
        count = row.count
    if count is None:
        raise HTTPException(status_code=404, detail=f"No data found in procedure_occurrence table for the last {personalcountrequest.last_n_dates} days")
    return {"count": count}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
