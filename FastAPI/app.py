from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import time 
from fastapi.middleware.cors import CORSMiddleware 


class Todo(BaseModel): #prevents api type errors
    id: Optional[int] = None
    task: str
    is_completed: bool = False


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # can set so only my website can make a request
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

todos = []

@app.post("/todos")
async def add_todos(todo: Todo):
    todo.id = len(todos) + 1 
    todos.append(todo)    
    return todo
 
@app.get("/todos")
async def read_todos():
    return todos

@app.get("/todos/{id}")
async def read_todos(id: int):
    for todos in todos:
        if todo.id == id:
            return todo
    raise HTTPException(status_code=404,detail='Item not found')

@app.middleware('http')
async def log_middleware(request,call_next):
    print("before route")
    start_time = time()
    response = await call_next(request)
    print('After route')
    end_time = time()
    process_time = end_time - start_time
    print(f"Request {request.url} processed in {process_time} in sec")
    return response