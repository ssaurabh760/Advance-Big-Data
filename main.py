from fastapi import FastAPI, HTTPException, Request, Depends, Header
from fastapi.responses import JSONResponse, Response
from redis import Redis
import json
import jsonschema
from json.decoder import JSONDecodeError
import jwt
from jwt import PyJWKClient
import hashlib

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
        "planStatus": {"type": "string"},
        "creationDate": {"type": "string"}
    },
    "required": ["planCostShares", "linkedPlanServices", "_org", "objectId", "objectType", "planStatus", "creationDate"],
    "additionalProperties": False
}

# Clear all data in Redis when the server starts
@app.on_event("startup")
async def startup_event():
    redis.flushdb()

# Validate Google Bearer token
GOOGLE_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs"
CLIENT_ID = "132133612061-krvs9s1lp0udoijkafote2fo1k60ebtv.apps.googleusercontent.com"  # Replace with your actual client ID
NGROK_URL = "https://41a7-155-33-133-27.ngrok-free.app"
async def verify_google_token(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    
    token = authorization.split(" ")[1] if "Bearer " in authorization else None
    if not token:
        raise HTTPException(status_code=401, detail="Invalid token format")

    try:
        jwks_client = PyJWKClient(GOOGLE_JWKS_URL)
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        payload = jwt.decode(token, signing_key.key, algorithms=["RS256"], audience=[CLIENT_ID, NGROK_URL], issuer="https://accounts.google.com")
        return payload
    except (jwt.exceptions.InvalidTokenError, jwt.exceptions.PyJWKClientError):
        raise HTTPException(status_code=401, detail="Invalid authentication token")

# Create Plan (POST)
@app.post("/v1/plan")
async def create_plan(request: Request, user_info: dict = Depends(verify_google_token)):
    try:
        data = await request.json()
    except JSONDecodeError:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON and cannot be empty.")
    
    if not data:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")
    
    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        raise HTTPException(status_code=400, detail="Invalid data in request body")
    
    object_id = data.get("objectId")
    
    if redis.hexists("plan", object_id):
        raise HTTPException(status_code=409, detail="Plan with this objectId already exists.")
    
    # Store the data in Redis
    redis.hset("plan", object_id, json.dumps(data))
    # Generate ETag
    etag = hashlib.md5(json.dumps(data).encode()).hexdigest()
    
    # Create response
    response = JSONResponse(
        content={"message": "Plan successfully created.", "objectId": object_id},
        status_code=201
    )
    
    # Add ETag to response header
    response.headers["ETag"] = f'"{etag}"'
    
    return response

# Get Plan (GET)
@app.get("/v1/plan/{object_id}")
async def get_plan(object_id: str, request: Request, user_info: dict = Depends(verify_google_token)):
    if_none_match = request.headers.get("If-None-Match")
    
    plan_data = redis.hget("plan", object_id)
    if not plan_data:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    plan = json.loads(plan_data)
    # Generate ETag using MD5 hash
    etag = hashlib.md5(json.dumps(plan).encode()).hexdigest()
    
    if if_none_match and if_none_match.strip('"') == etag:
        return Response(status_code=304)

    response = JSONResponse(content=plan)
    response.headers["ETag"] = f'"{etag}"'
    return response


@app.patch("/v1/plan/{object_id}")
async def patch_plan(object_id: str, request: Request, user_info: dict = Depends(verify_google_token)):
    try:
        existing_data = redis.hget("plan", object_id)
        if not existing_data:
            raise HTTPException(status_code=404, detail="Plan not found")
        
        existing_data = json.loads(existing_data)

        # Generate ETag for existing data
        existing_etag = hashlib.md5(json.dumps(existing_data).encode()).hexdigest()

        # Check If-Match header
        if_match = request.headers.get("If-Match")
        if not if_match:
            raise HTTPException(status_code=412, detail="Precondition Failed: If-Match header is required")
        if if_match:
            if if_match.strip('"') != existing_etag:
                raise HTTPException(status_code=412, detail="Precondition Failed: ETag mismatch")

        # Get the update data from the request
        update_data = await request.json()

        if 'objectId' in update_data and update_data['objectId'] != object_id:
            raise HTTPException(status_code=400, detail="Changing the main object ID is not allowed")

        # Deep merge function to handle nested updates
        # Function to deep merge dictionaries
        def deep_merge(source, update):
            for key, value in update.items():
                if isinstance(value, dict):
                    source[key] = deep_merge(source.get(key, {}), value)
                elif isinstance(value, list):
                    # Handle linkedPlanServices specially
                    if key == "linkedPlanServices":
                        existing_ids = set(item['objectId'] for item in source.get(key, []))
                        for new_item in value:
                            if new_item['objectId'] not in existing_ids:
                                source.setdefault(key, []).append(new_item)
                                existing_ids.add(new_item['objectId'])
                    else:
                        source[key] = value
                else:
                    source[key] = value
            return source

        # Apply updates
        updated_data = deep_merge(existing_data, update_data)
        
        # Validate updated data against JSON schema
        jsonschema.validate(instance=updated_data, schema=schema)

        # Store the updated data in Redis
        redis.hset("plan", object_id, json.dumps(updated_data))

        # Generate new ETag for the updated data
        new_etag = hashlib.md5(json.dumps(updated_data).encode()).hexdigest()

        # Create response with ETag
        response = JSONResponse(
            content=updated_data,
            status_code=200
        )
        
        # Add new ETag to response header
        response.headers["ETag"] = f'"{new_etag}"'

        return response

    except jsonschema.exceptions.ValidationError:
        raise HTTPException(status_code=400, detail="Invalid data in request body")
# Delete Plan (DELETE)
@app.delete("/v1/plan/{object_id}")
async def delete_plan(object_id: str, user_info: dict = Depends(verify_google_token)):
    if not redis.hexists("plan", object_id):
        raise HTTPException(status_code=404, detail="Plan not found")
    
    redis.hdel("plan", object_id)
    return Response(status_code=204)
