

from .core.yadtq_broker import MessageBroker 
from .core.yadtq_result_db import ResultStore 

def create_yadtq(kafka_servers=['localhost:9092'], 
                 redis_host='localhost', redis_port=6379):
    """Create YADTQ broker and result store instances."""
   
    broker = MessageBroker(kafka_servers)
    
   
    result_store = ResultStore(redis_host, redis_port)
    
    
    return broker, result_store
