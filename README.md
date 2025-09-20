# Yet Another Distributed Task Queue (YADTQ)

A lightweight distributed task queue system built with Python, Kafka, and Redis.

Overview
YADTQ is a distributed task processing system that allows you to submit tasks to a queue and have them processed by multiple workers. It provides:

Distributed task processing with multiple workers

Persistent task storage with Redis

Kafka-based message brokering for reliable task delivery

Task status tracking and result retrieval

Automatic retry mechanism for failed tasks

Worker heartbeat monitoring

Architecture
text
Client -> Submit Task -> Kafka Broker -> Workers -> Process Task -> Store Results in Redis
Components
Client: Submits tasks and checks results

Broker: Kafka-based message queue for task distribution

Workers: Process tasks and store results

Result Store: Redis-based storage for task status and results

Installation
Prerequisites
Python 3.8+

Kafka (running on localhost:9092 by default)

Redis (running on localhost:6379 by default)

Dependencies
Install required Python packages:

bash
pip install kafka-python redis
Usage
1. Start the Infrastructure
Make sure Kafka and Redis are running:

bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
kafka-server-start.sh config/server.properties

# Start Redis
redis-server
2. Run Workers
Start worker processes to handle tasks:

bash
python workercode.py
This will start 3 worker threads that can process tasks.

3. Submit Tasks
Run the client to submit tasks:

bash
python clientcode.py
The client will submit tasks and periodically check their status until all are completed.

Configuration
Default Settings
Kafka servers: localhost:9092

Redis host: localhost

Redis port: 6379

Task topic: yadtq_tasks

Custom Configuration
Modify the create_yadtq() call in your code to use different servers:

python
broker, result_store = create_yadtq(
    kafka_servers=['your-kafka-server:9092'],
    redis_host='your-redis-host',
    redis_port=6379
)
Available Tasks
The system currently supports these task types:

add(a, b): Add two numbers

multiply(a, b): Multiply two numbers

sub(a, b): Subtract b from a

divide(a, b): Divide a by b (handles division by zero)

Adding Custom Tasks
To add a new task type:

Define the task function in workercode.py:

python
def custom_task(param1, param2):
    # Your task logic here
    return result
Add it to the task_handlers dictionary:

python
task_handlers = {
    'add': add,
    'multiply': multiply,
    'sub': sub,
    'divide': divide,
    'custom_task': custom_task,  # Add your custom task
}
Submit the task from the client:

python
task_id = client.submit('custom_task', param1_value, param2_value)
API Reference
TaskClient
submit(task_name, *args, **kwargs): Submit a task for processing

get_result(task_id): Get the current status of a task

wait_for_result(task_id, timeout): Wait for a task to complete

TaskWorker
Workers automatically:

Poll for new tasks

Process tasks with retry mechanism

Update task status in Redis

Send heartbeats to indicate they're alive

Monitoring
Task Status
Tasks can have these statuses:

queued: Task is in the queue waiting for processing

processing: Task is being processed by a worker

success: Task completed successfully

failed: Task failed after all retry attempts

Worker Status
Worker status is tracked in Redis with heartbeats. Workers that don't send heartbeats for 30 seconds are considered offline.

Error Handling
Tasks are automatically retried up to 3 times

Division by zero errors are caught and handled gracefully

Network issues with Kafka or Redis are handled with retries

File Structure

text

yadtq/

├── api/                 # Client and worker APIs

│   ├── yadtq_client.py # Task submission and result retrieval

│   └── yadtq_worker.py # Worker implementation

├── core/               # Core components

│   ├── yadtq_broker.py     # Kafka message broker

│   ├── yadtq_result_db.py  # Redis result storage

│   └── yadtq_task.py       # Task data structure

├── __init__.py         # Package initialization

clientcode.py          # Example client

workercode.py          # Example worker implementation

Troubleshooting
Common Issues
Kafka connection errors: Ensure Kafka is running and accessible

Redis connection errors: Ensure Redis is running and accessible

Task not processed: Check that workers are running and connected

Logging
Both client and worker components output logs with timestamps for debugging.
