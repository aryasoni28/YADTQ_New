import threading
from typing import Dict, Callable
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class TaskWorker:
    
    def __init__(self, worker_id: str, task_handlers: Dict[str, Callable], broker, result_store, max_retries: int = 3):
        self.worker_id = worker_id
        self.task_handlers = task_handlers
        self._broker = broker
        self._result_store = result_store
        self._consumer = self._broker.get_consumer('yadtq_worker_group')  
        self._running = False
        self._heartbeat_thread = None
        self.max_retries = max_retries

    def _send_heartbeat(self):
        while self._running:
            try:
                self._result_store.update_worker_heartbeat(self.worker_id)
                logger.debug(f"{self.worker_id} sent a heartbeat.")
            except Exception as e:
                logger.error(f"{self.worker_id} failed to send heartbeat: {e}")
            time.sleep(1)

    def _process_task(self, task_data):
        task_id = task_data.get('task_id')
        task_name = task_data.get('task_name')
        args = task_data.get('args', [])
        kwargs = task_data.get('kwargs', {})

        logger.info(f"{self.worker_id} received task {task_id}: {task_name} with args {args}, kwargs {kwargs}")
       
        self._result_store.set_task_status(task_id, 'processing', worker_id=self.worker_id)

        retries = 0
        while retries <= self.max_retries:
            try:
                handler = self.task_handlers.get(task_name)
                if not handler:
                    raise ValueError(f"No handler found for task: {task_name}")
                
                result = handler(*args, **kwargs)
                
               
                self._result_store.set_task_status(task_id, 'success', result=result)
                logger.info(f"{self.worker_id} successfully completed task {task_id}: {result}")
                return  
            
            except Exception as e:
                retries += 1
                error_message = str(e)
                logger.error(f"{self.worker_id} failed task {task_id}: {error_message} (Retry {retries}/{self.max_retries})")
                
                if retries > self.max_retries:
                    self._result_store.set_task_status(task_id, 'failed', result=error_message)
                    logger.error(f"{self.worker_id} task {task_id} failed after {self.max_retries} retries.")
                    return  

                time.sleep(2)  
    def _poll_tasks(self):
        
        while self._running:
            try:
                messages = self._consumer.poll(timeout_ms=1000)
                if not messages:
                    logger.debug(f"{self.worker_id} found no new messages.")
                    continue
                
                for topic_partition, batch in messages.items():
                    for message in batch:
                        task_data = message.value
                        self._process_task(task_data)  
                        self._consumer.commit() 
            
            except Exception as e:
                logger.error(f"{self.worker_id} encountered an error while polling tasks: {e}")
                time.sleep(2)  

    def start(self):
        self._running = True

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._send_heartbeat, daemon=True)
        self._heartbeat_thread.start()

        logger.info(f"{self.worker_id} has started and is ready to process tasks.")
        try:
            self._poll_tasks()  # Start polling for tasks
        except KeyboardInterrupt:
            logger.warning(f"{self.worker_id} received shutdown signal.")
        finally:
            self.stop()

    def stop(self):
        self._running = False
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1)
        logger.info(f"{self.worker_id} has stopped.")

