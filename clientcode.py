#!/usr/bin/env python3

from yadtq import create_yadtq
from yadtq.api.yadtq_client import TaskClient
import time

def main():
   
    broker, result_store = create_yadtq()

   
    client = TaskClient(broker, result_store)

    
    tasks = [
        ('add', (5, 3)),
        ('multiply', (4, 6)),
        ('sub', (2, 1)),
        ('divide', (12, 0)),
#        ('add1', (5, 5)),
 #       ('multiply1', (14, 16)),
  #      ('sub1', (12, 11)),
   #     ('divide1', (12, 16)),
       # ('add2', (56, 43)),
   #     ('multiply2', (74, 96)),
    #    ('sub2', (22, 13)),
     #   ('divide2', (112, 6)),
      #  ('add3', (54, 83)),
       # ('multiply3', (94, 34)),
#        ('sub3', (212, 112)),
 #       ('divide3', (142, 46)),
  #      ('add4', (54, 38)),
   #     ('multiply4', (28, 25)),
    #    ('sub4', (20, 16)),
     #   ('divide4', (145, 60))
    ]

    task_ids = [] 

    
    for task_name, args in tasks:
        task_id = client.submit(task_name, *args)
        task_ids.append(task_id)
        print(f"Submitted {task_name}{args} with ID: {task_id}")

  
    results = {}

    while task_ids:
        for task_id in task_ids[:]: 
            try:
                result = client.get_result(task_id)  
                status = result.get('status')
                
                # Print the current status of the task
                print(f"Task {task_id} is currently: {status}")
                
                
                if status in ['success', 'failed']:
                    results[task_id] = result 
                    task_ids.remove(task_id)  
            except Exception as e:
                print(f"Error checking task {task_id}: {e}")

        time.sleep(1)  

   
    print("\nFinal Results:")
    for task_id, result in results.items():
        print(f"Task {task_id} completed with status: {result['status']}, result: {result.get('result', 'N/A')}")

    print("\nAll tasks completed.")

if __name__ == "__main__":
    main()

