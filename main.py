from fastapi import FastAPI,HTTPException
from pymongo import MongoClient
from bson import ObjectId
from urllib.parse import quote_plus
from pydantic import BaseModel, conlist
from typing import List, Optional

#schemas
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


username = "sonu123"
password = "Sonu@46312"
cluster_url = "cluster0.a6ui5uo.mongodb.net"
db_name = "sample_mflix"

escaped_username = quote_plus(username)
escaped_password = quote_plus(password)

# Construct the updated MongoDB connection string
connection_string = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster_url}/{db_name}?retryWrites=true&w=majority&appName=Cluster0&ssl=true&ssl_cert_reqs=CERT_NONE"
def connect_to_mongodb():
    client = MongoClient(connection_string)
    db = client[db_name]
    return db


db = connect_to_mongodb()

app = FastAPI()

@app.post("/students", status_code=201)
async def create_student(student_data: dict):
    result = db["students"].insert_one(student_data)
    return {"id": str(result.inserted_id)}

@app.get("/students", status_code=200)
async def list_students(country: str = None, age: int = None):
    query = {}
    if country:
        query["address.country"] = country
    if age is not None:
        query["age"] = {"$gte": age}
    projection = {"_id": 0, "name": 1, "age": 1}
    students = list(db["students"].find(query, projection))
    return {"data": students}

@app.get("/students/{id}", status_code=200)
async def get_student(id: str):
    student = db["students"].find_one({"_id": ObjectId(id)}, {"_id": 0})
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return student

@app.patch("/students/{id}", status_code=204)
async def update_student(id: str, student_data: dict):
    result = db["students"].update_one({"_id": ObjectId(id)}, {"$set": student_data})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Student not found")



@app.delete("/students/{id}", status_code=200)
async def delete_student(id: str):
    result = db["students"].delete_one({"_id": ObjectId(id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Student not found")


@app.get("/")
async def read_root():
    return {"message": "Hello, Welcome to Libarary Management System!!"}

