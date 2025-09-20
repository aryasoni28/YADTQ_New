import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from datetime import datetime
import redis
from threading import Thread

class MessageBroker:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'yadtq_tasks'
        self._ensure_topic_exists()

    def _ensure_topic_exists(self):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            if self.topic not in admin_client.list_topics():
                topic = NewTopic(name=self.topic, num_partitions=3, replication_factor=1)
                admin_client.create_topics([topic])
        except Exception as e:
            print(f"Warning: Could not create topic: {e}")

    def get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda p: json.dumps(p).encode('utf-8')
        )

    def get_consumer(self, group_id):
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=False
        )


class ResultStore:
    def __init__(self, host='localhost', port=6379):
        self.redis_client = redis.Redis(host=host, port=port)

    def set_task_status(self, task_id, status, result=None, worker_id=None):
        mapping = {
            'status': status,
            'timestamp': datetime.utcnow().isoformat()
        }
        if result is not None:
            mapping['result'] = str(result)
        if worker_id is not None:
            mapping['worker_id'] = worker_id

        self.redis_client.hset(task_id, mapping=mapping)

    def get_task_status(self, task_id):
        task_info = self.redis_client.hgetall(task_id)
        if not task_info:
            return None
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in task_info.items()}

    def update_worker_heartbeat(self, worker_id):
        self.redis_client.hset(
            f'worker:{worker_id}',
            mapping={
                'last_heartbeat': datetime.utcnow().isoformat(),
                'status': 'active'
            }
        )
        self.redis_client.expire(f'worker:{worker_id}', 30) 


class WorkerAssignment:
    def __init__(self, group_id, message_broker, result_store):
        self.consumer = message_broker.get_consumer(group_id)
        self.result_store = result_store

    def assign_task(self):
        for message in self.consumer:
            task = message.value
            task_id = task['task_id']
            worker_id = task['worker_id']
            self.result_store.set_task_status(task_id, 'processing', worker_id=worker_id)
            try:
                self.execute_task(task)
                
                self.result_store.set_task_status(task_id, 'success', result='Task completed', worker_id=worker_id)
            except Exception as e:
               
                self.result_store.set_task_status(task_id, 'failed', result=str(e), worker_id=worker_id)
                self.reassign_task(task_id)  # Reassign failed task

    def execute_task(self, task):
        
        try:
            print(f"Executing task: {task['task_id']}")
            if task['task_id'] == "fail":
                raise Exception("Task failed during execution.")
        except Exception as e:
                self.result_store.set_task_status(task_id, 'failed', result=str(e), worker_id=worker_id)
                self.reassign_task(task_id)  # Reassign failed task
            
              

    def execute_task(self, task):
        print(f"Executing task: {task['task_id']}")
        if task['task_id'] == "fail":
            raise Exception("Task failed during execution.")

    def reassign_task(self, task_id):
        print(f"Reassigning failed task: {task_id}")


class Heartbeat:
    def __init__(self, result_store, worker_id, interval=10):
        self.result_store = result_store
        self.worker_id = worker_id
        self.interval = interval

    def send_heartbeat(self):
        while True:
            self.result_store.update_worker_heartbeat(self.worker_id)
            print(f"Heartbeat sent by worker: {self.worker_id}")
            time.sleep(self.interval)  # Send heartbeat every `interval` seconds


class Logger:
    def __init__(self, log_file='worker.log'):
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')
        self.logger = logging.getLogger()

    def log(self, message):
        self.logger.info(message)



if __name__ == "__main__":
    message_broker = MessageBroker()
    result_store = ResultStore()
    logger = Logger()
    worker_id = "worker_01"
    heartbeat = Heartbeat(result_store, worker_id)
    heartbeat_thread = Thread(target=heartbeat.send_heartbeat)
    heartbeat_thread.start()

   
    worker_assignment = WorkerAssignment("worker_group_01", message_broker, result_store)
    worker_assignment.assign_task()

    logger.log("Worker process started.")

