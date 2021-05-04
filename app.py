from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Covid(BaseModel):
    date: str
    state: str
    city: str
    place_type: str
    confirmed: str
    deaths: str
    order_for_place: str
    is_last: str
    estimated_population_2019: str
    estimated_population: str
    city_ibge_code: str
    confirmed_per_100k_inhabitants: str
    death_rate: str



@app.get("/")
async def root():
    return {"message": "Hello World"}