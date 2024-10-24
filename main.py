from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from redis import Redis
import json
import jsonschema
from json.decoder import JSONDecodeError

app = FastAPI()
redis = Redis(host='localhost', port=6379, db=0)

# Clear all data in Redis when the server starts
@app.on_event("startup")
async def startup_event():
    redis.flushdb()

# JSON Schema for validation
schema = {
    "type": "object",
    "properties": {
        "planCostShares": {
            "type": "object",
            "properties": {
                "deductible": {"type": "number"},
                "_org": {"type": "string"},
                "copay": {"type": "number"},
                "objectId": {"type": "string"},
                "objectType": {"type": "string"}
            },
            "required": ["deductible", "_org", "copay", "objectId", "objectType"],
            "additionalProperties": False
        },
        "linkedPlanServices": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "linkedService": {
                        "type": "object",
                        "properties": {
                            "_org": {"type": "string"},
                            "objectId": {"type": "string"},
                            "objectType": {"type": "string"},
                            "name": {"type": "string"}
                        },
                        "required": ["_org", "objectId", "objectType", "name"],
                        "additionalProperties": False
                    },
                    "planserviceCostShares": {
                        "type": "object",
                        "properties": {
                            "deductible": {"type": "number"},
                            "_org": {"type": "string"},
                            "copay": {"type": "number"},
                            "objectId": {"type": "string"},
                            "objectType": {"type": "string"}
                        },
                        "required": ["deductible", "_org", "copay", "objectId", "objectType"],
                        "additionalProperties": False
                    },
                    "_org": {"type": "string"},
                    "objectId": {"type": "string"},
                    "objectType": {"type": "string"}
                },
                "required": ["linkedService", "planserviceCostShares", "_org", "objectId", "objectType"],
                "additionalProperties": False
            }
        },
        "_org": {"type": "string"},
        "objectId": {"type": "string"},
        "objectType": {"type": "string"},
        "planType": {"type": "string"},
        "creationDate": {"type": "string"}
    },
    "required": ["planCostShares", "linkedPlanServices", "_org", "objectId", "objectType", "planType", "creationDate"],
    "additionalProperties": False
}

@app.post("/v1/plan")
async def create_plan(request: Request):
    try:
        data = await request.json()
    except JSONDecodeError:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON and cannot be empty.")
    
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")
    
    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid data in request body")
    
    object_id = data.get("objectId")
    
    if redis.hexists("plan", object_id):
        raise HTTPException(status_code=409, detail="Plan with this objectId already exists.")
    
    # Store the data in Redis
    redis.hset("plan", object_id, json.dumps(data))
    
    return JSONResponse(content={"message": "Plan successfully created.", "objectId": object_id}, status_code=201)

@app.get("/v1/plan/{object_id}")
async def get_plan(object_id: str, request: Request):
    if_none_match = request.headers.get("If-None-Match")
    
    plan_data = redis.hget("plan", object_id)
    if not plan_data:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    plan = json.loads(plan_data)
    etag = hash(json.dumps(plan))
    
    if if_none_match and int(if_none_match) == etag:
        return Response(status_code=304)

    response = JSONResponse(content=plan)
    response.headers["ETag"] = str(etag)
    return response

@app.delete("/v1/plan/{object_id}")
async def delete_plan(object_id: str):
    if not redis.hexists("plan", object_id):
        raise HTTPException(status_code=404, detail="Plan not found")
    
    redis.hdel("plan", object_id)
    return Response(status_code=204)