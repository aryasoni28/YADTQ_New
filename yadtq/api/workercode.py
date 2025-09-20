#!/usr/bin/env python3

from yadtq import create_yadtq
from yadtq.api.yadtq_worker import TaskWorker
import threading
import time


def add(a, b):
    time.sleep(2)  
    return a + b

def multiply(a, b):
    time.sleep(2) 
    return a * b
def sub(a, b):
    time.sleep(2)
    return a - b
def divide(a, b):
    time.sleep(2)
    return a / b

task_handlers = {
    'add': add,
    'multiply': multiply,
    'sub': sub,

}

def run_worker(worker_id, broker, result_store):
    worker = TaskWorker(worker_id, task_handlers, broker, result_store)
    worker.start()

def main():
  
    broker, result_store = create_yadtq()

    
    worker_threads = []
    for i in range(3):  
        thread = threading.Thread(
            target=run_worker,
            args=(f"worker_{i}", broker, result_store)
        )
        thread.daemon = True
        thread.start()
        worker_threads.append(thread)

    
    print("Workers are running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down workers.")

if __name__ == "__main__":
    main()

