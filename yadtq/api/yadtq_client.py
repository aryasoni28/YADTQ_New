import logging
from yadtq.core.yadtq_task import Task  
from typing import Any, Dict
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class TaskClient:
    def __init__(self, broker, result_store):
        self._broker = broker
        self._result_store = result_store
        self._producer = self._broker.get_producer()

    def submit(self, task_name: str, *args, **kwargs) -> str:
        
        task = Task.create(task_name, *args, **kwargs)
        
        
        self._result_store.set_task_status(task.task_id, 'queued')
        
        # Send task to queue
        self._producer.send(self._broker.topic, task.to_dict())
        self._producer.flush()
        
        logger.info(f"Task {task.task_id} submitted successfully.")  # Log success

        return task.task_id

    def get_result(self, task_id: str) -> Dict[str, Any]:
        
        result = self._result_store.get_task_status(task_id)
        if not result:
            return {'status': 'not_found'}
        return result

    def wait_for_result(self, task_id: str, timeout: int = None) -> Dict[str, Any]:
        start_time = time.time()
        while True:
            result = self.get_result(task_id)
            if result['status'] in ['success', 'failed', 'queued', 'processing']:
                return result
            if timeout and time.time() - start_time > timeout:
                return {'status': 'timeout'}
            time.sleep(0.5)


def main():
    broker = None 
    result_store = None  

    
    client = TaskClient(broker, result_store)

    task_name = 'example_task'
    args = ['arg1', 'arg2']
    kwargs = {'key1': 'value1'}

    try:
        
        task_id = client.submit(task_name, *args, **kwargs)
        logger.info(f"Task submitted with task ID: {task_id}")

       
        result = client.wait_for_result(task_id, timeout=60)
        logger.info(f"Task result: {result}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()

