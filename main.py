from fastapi import FastAPI, Request, HTTPException, Header, Depends
from pymongo import MongoClient
from bson import ObjectId
from urllib.parse import quote_plus
from pydantic import BaseModel, conlist
from redis import Redis
from functools import wraps
import os
from datetime import datetime, timedelta
from typing import List, Optional

# Redis client initialization
redis_url = "redis://red-coj91s2cn0vc73dqves0:6379"
if redis_url:
    redis_client = redis.Redis.from_url(redis_url)
else:
    redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Rate limiter configuration
MAX_REQUESTS_PER_DAY = 100  # Set the desired limit

# Rate limiter function
def rate_limiter(user_id: str = Header(...)):
    current_date = datetime.now().date()
    key = f"rate_limit:{user_id}:{current_date}"
    previous_date = current_date - timedelta(days=1)
    previous_key = f"rate_limit:{user_id}:{previous_date}"
    # Get the current number of requests for the user and the current date
    current_requests = redis_client.get(key)
    if current_requests is None:
        # If the key doesn't exist, initialize it to 1
        redis_client.set(key, 1, ex=timedelta(days=1))
        current_requests = 1
    else:
        current_requests = int(current_requests)
        
        # Check if the rate limit has been exceeded
        if current_requests >= MAX_REQUESTS_PER_DAY:
            raise HTTPException(status_code=429, detail="Too many requests")
        
        # Increment the request count
        redis_client.incr(key)
        current_requests += 1
    
    # Set a TTL (Time-To-Live) for the key to automatically expire the next day
    expiration_time = int(timedelta(days=1).total_seconds())
    redis_client.expire(key, expiration_time)
    
    # Remove the previous day's rate limiting data
    redis_client.delete(previous_key)
    return current_requests

# Schemas
class Address(BaseModel):
    city: str
    country: str

class StudentCreate(BaseModel):
    name: str
    age: int
    address: Address

class Student(BaseModel):
    name: str
    age: int
    address: Address

class StudentUpdate(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None
    address: Optional[Address] = None

class StudentList(BaseModel):
    data: List[Student]

# MongoDB connection details
username = "sonu123"
password = "Sonu@46312"
cluster_url = "cluster0.a6ui5uo.mongodb.net"
db_name = "sample_mflix"
escaped_username = quote_plus(username)
escaped_password = quote_plus(password)

# Construct the updated MongoDB connection string
connection_string = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster_url}/{db_name}?retryWrites=true&w=majority&appName=Cluster0"

def connect_to_mongodb():
    client = MongoClient(connection_string, connect=False)
    db = client[db_name]
    return db

db = connect_to_mongodb()
app = FastAPI()

@app.post("/students", status_code=201, dependencies=[Depends(rate_limiter)])
async def create_student(student_data: dict, current_requests=Depends(rate_limiter)):
    result = db["students"].insert_one(student_data)
    return {"id": str(result.inserted_id)}

@app.get("/students", status_code=200, dependencies=[Depends(rate_limiter)])
async def list_students(country: str = None, age: int = None, current_requests=Depends(rate_limiter)):
    query = {}
    if country:
        query["address.country"] = country
    if age is not None:
        query["age"] = {"$gte": age}
    projection = {"_id": 0, "name": 1, "age": 1}
    students = list(db["students"].find(query, projection))
    return {"data": students}

@app.get("/students/{id}", status_code=200, dependencies=[Depends(rate_limiter)])
async def get_student(id: str, current_requests=Depends(rate_limiter)):
    student = db["students"].find_one({"_id": ObjectId(id)}, {"_id": 0})
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return student

@app.patch("/students/{id}", status_code=204, dependencies=[Depends(rate_limiter)])
async def update_student(id: str, student_data: dict, current_requests=Depends(rate_limiter)):
    result = db["students"].update_one({"_id": ObjectId(id)}, {"$set": student_data})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Student not found")

@app.delete("/students/{id}", status_code=200, dependencies=[Depends(rate_limiter)])
async def delete_student(id: str, current_requests=Depends(rate_limiter)):
    result = db["students"].delete_one({"_id": ObjectId(id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Student not found")

@app.get("/", dependencies=[Depends(rate_limiter)])
async def read_root(current_requests=Depends(rate_limiter)):
    return {"message": "Hello, Welcome to Library Management System!"}
