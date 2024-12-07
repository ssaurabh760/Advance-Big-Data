from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from redis import Redis

consumer = KafkaConsumer('plan_operations',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=("elastic", "EQC7rE-sOVAiTGiw7Sx-"),
    verify_certs=False
)
def process_message(message):
    print(f"Consumed message: {json.dumps(message, indent=4)}")
    operation = message['operation']
    data = message['data']
    
    if operation == 'create' or operation == 'update':
        # Index main plan (no routing needed as it's the root)
        plan_doc = {
            "objectId": data["objectId"],
            "objectType": data["objectType"],
            "_org": data["_org"],
            "planStatus": data["planStatus"],
            "creationDate": data["creationDate"],
            "plan_join": {
                "name": "plan"
            }
        }
        es.index(index='plans', id=data["objectId"], body=plan_doc)
        
        # Index planCostShares (route to plan)
        plan_cost_shares = {
            **data["planCostShares"],
            "plan_join": {
                "name": "planCostShares",
                "parent": data["objectId"]
            }
        }
        es.index(
            index='plans',
            id=data["planCostShares"]["objectId"],
            routing=data["objectId"],  # route to plan
            body=plan_cost_shares
        )
        
        # Process linkedPlanServices
        for service in data["linkedPlanServices"]:
            linked_plan_service_id = service["objectId"]
            
            # Index linkedPlanServices (route to plan)
            linked_plan_service = {
                "objectId": linked_plan_service_id,
                "objectType": service["objectType"],
                "_org": service["_org"],
                "plan_join": {
                    "name": "linkedPlanServices",
                    "parent": data["objectId"]
                }
            }
            es.index(
                index='plans',
                id=linked_plan_service_id,
                routing=data["objectId"],  # route to plan
                body=linked_plan_service
            )
            
            # Index linkedService (route to linkedPlanServices)
            linked_service = {
                **service["linkedService"],
                "plan_join": {
                    "name": "linkedService",
                    "parent": linked_plan_service_id
                }
            }
            es.index(
                index='plans',
                id=service["linkedService"]["objectId"],
                routing=linked_plan_service_id,  # route to linkedPlanServices
                body=linked_service
            )
            
            # Index planserviceCostShares (route to linkedPlanServices)
            service_cost_shares = {
                **service["planserviceCostShares"],
                "plan_join": {
                    "name": "planserviceCostShares",
                    "parent": linked_plan_service_id
                }
            }
            es.index(
                index='plans',
                id=service["planserviceCostShares"]["objectId"],
                routing=linked_plan_service_id,  # route to linkedPlanServices
                body=service_cost_shares
            )
            
        print(f"Message processed: {operation} completed for objectId {data['objectId']}")

    elif operation == 'delete':
        try:
            plan_id = data["objectId"]
            
            # Step 1: First get all linkedPlanService IDs for routing
            linked_services_query = {
                "query": {
                    "term": {
                        "plan_join.name": "linkedPlanServices"
                    }
                },
                "_source": ["objectId"]
            }
            
            # Get all linkedPlanService documents
            linked_services_result = es.search(
                index='plans',
                body=linked_services_query,
                size=100
            )
            
            # Store all linkedPlanService IDs
            linked_service_ids = [hit['_source']['objectId'] for hit in linked_services_result['hits']['hits']]
            print(f"Found linkedPlanService IDs: {linked_service_ids}")
            
            # Step 2: Delete all child documents using their specific routing
            for linked_service_id in linked_service_ids:
                try:
                    # Delete children with this specific routing
                    child_query = {
                        "query": {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "plan_join.parent": linked_service_id
                                        }
                                    }
                                ]
                            }
                        }
                    }
                    
                    es.delete_by_query(
                        index='plans',
                        body=child_query,
                        routing=linked_service_id,
                        refresh=True,
                        conflicts="proceed"
                    )
                    print(f"Deleted children with routing {linked_service_id}")
                except Exception as e:
                    print(f"Error deleting children for {linked_service_id}: {str(e)}")
            
            # Step 3: Delete all documents with plan routing
            plan_level_query = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "_id": plan_id
                                }
                            },
                            {
                                "term": {
                                    "plan_join.parent": plan_id
                                }
                            }
                        ]
                    }
                }
            }
            
            es.delete_by_query(
                index='plans',
                body=plan_level_query,
                routing=plan_id,
                refresh=True,
                conflicts="proceed"
            )
            
            # Step 4: Final cleanup - explicit deletion
            final_cleanup_query = {
                "query": {
                    "match_all": {}
                }
            }
            
            cleanup_result = es.search(
                index='plans',
                body=final_cleanup_query,
                size=100
            )
            
            for hit in cleanup_result['hits']['hits']:
                try:
                    routing = hit.get('_routing', hit['_id'])
                    es.delete(
                        index='plans',
                        id=hit['_id'],
                        routing=routing,
                        refresh=True
                    )
                    print(f"Cleaned up document {hit['_id']} with routing {routing}")
                except Exception as e:
                    print(f"Error in final cleanup for {hit['_id']}: {str(e)}")
            
            print(f"Successfully deleted plan {plan_id} and all related documents")
            
        except Exception as e:
            print(f"Error during delete operation: {str(e)}")
            raise e
        

def run_consumer():
    print("Kafka Consumer is running...")
    for message in consumer:
        process_message(message.value)

if __name__ == "__main__":
    run_consumer()