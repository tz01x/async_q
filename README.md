# AsyncQ
A simple worker task queue with async support
## Installation
```bash
pip install async_q
```

## Features
1. Submit asynchronously task 
2. Task routing
3. Distributed async worker process

## Use Case: Asynchronous Task Queue for I/O-Bound Task Processing

### Overview
In scenarios where you need to execute I/O-bound tasks asynchronously within a single application, the "async_q" library provides a straightforward solution. This use case illustrates how to create and utilize the library for such purposes.

### Prerequisites
Before using the "async_q" library, ensure you have Python 3.8 and Redis (Version 5.0 to current) installed, along with the required dependencies.


### Setting Up the Application

#### 1. Creating the Async Task Queue App
Begin by creating an instance of the `AsyncTaskQueue` to set up your application. In this example, we will name our app "async_q_app" and configure it to use Redis as the message broker.

```python
# main_app.py

from async_q import AsyncTaskQueue, RedisBuilder

# Define Async Task Queue App
async_q_app = AsyncTaskQueue(
    redis_builder=RedisBuilder(
        port='6379',
    )
)
```

#### 2. Defining the Task Function
Next, define a task function that will be submitted to the queue for processing. In this example, we have a task function named `my_task`. This function simulates I/O waiting with a specified delay.

```python
# my_task.py

import logging
import asyncio
from async_q import submit_task

# For initializing the app
from main_app import async_q_app

async def my_task(idx, delay=2, *args, **kwargs):
    # Simulate I/O waiting
    await asyncio.sleep(delay)

    logging.info(f'{idx} has finished the task. Task ID: {kwargs.get("task_id")}')
```

#### 3. Submitting Tasks
To submit tasks for processing, you can use the `submit_task` function. In this example, we submit 20 tasks to be processed by the queue.

```python
if __name__ == '__main__':
    for i in range(20):
        submit_task(my_task, kwargs={'idx': i, 'delay': 10})
```

In the previous example, the `submit_task` function also includes a `queue` argument with the default value set to `default` .  This offers flexibility in case you want to specify a different queue for a particular task, but if no queue is provided, it will use the default value of `default`.

```python
if __name__ == '__main__':
     for i in range(20):
         # Submit a task with an explicit queue name
         submit_task(my_task, kwargs={'idx': i, 'delay': 10}, queue_name='default')
         # Retry up to 3 times (delays 1s, 2s, 4s, capped at backoff_max) before moving to the DLQ
         submit_task(my_task, kwargs={'idx': i}, max_retries=3, backoff_base=1.0, backoff_factor=2.0, backoff_max=60.0)
```

### Running Worker Processes



#### 4. Initializing Worker Processes

In order to efficiently handle incoming tasks, it's crucial to set up and launch worker processes. The level of concurrency can be precisely determined by employing the `-c` flag. You can now provide the app in multiple ways:

- File path form (original):

```bash
python -m async_q -a main_app.py:async_q_app -c 5
```

Installing the package also provides an `async_q` command, equivalent to `python -m async_q`:

```bash
async_q -a main_app.py:async_q_app -c 5
```

- Dotted module path (portable across machines):

```bash
python -m async_q -a main_app:async_q_app -c 5
```

- Environment variable (omit `-a`):

```bash
# Bash
export ASYNC_Q_APP=main_app:async_q_app
python -m async_q -c 5

# Windows PowerShell
$env:ASYNC_Q_APP='main_app:async_q_app'
python -m async_q -c 5
```

By default, these workers will process tasks from the `default` queue. You can assign specific workers to other queues as needed:

```bash
python -m async_q -a main_app:async_q_app -c 5 -q mail_ps
```

By making these adjustments, you can tailor the behavior of your worker processes to suit your application's specific requirements.

#### 5. Submitting Tasks for Processing
With the worker processes running, you can now submit tasks for processing. Use the following command to execute the `my_task.py` script, which submits tasks to the queue.

```bash
$ python my_task.py
```

### Running multiple workers

Run as many worker processes per queue as you like (containers, pods, terminals). Each worker keeps its in-flight tasks in its own Redis list and refreshes a heartbeat every 10s by default (`heartbeat_interval`). If a worker dies, a peer moves its in-flight tasks back to the queue within ~40s with the defaults (`heartbeat_ttl` 30s plus one reaper pass).

- **Delivery is at-least-once.** A task can run twice if its worker dies mid-task or loses Redis for longer than the heartbeat TTL. Make tasks idempotent.
- **Long tasks are fine.** Liveness is per worker, not per task, so a 2-hour task on a healthy worker is never taken away — as long as it doesn't block the event loop. Synchronous CPU work or `time.sleep` stops the heartbeat, and peers will reclaim (and re-run) that worker's tasks; run blocking code in a thread or process.
- **Graceful shutdown.** On SIGTERM/Ctrl+C a worker stops fetching, waits up to `--shutdown-timeout` seconds (default 30) for running tasks, then re-queues the rest. Raise it for long tasks, and set your orchestrator's grace period (e.g. Kubernetes `terminationGracePeriodSeconds`) a bit higher.
- **Redis Cluster.** Use `RedisBuilder(host=..., port=..., cluster=True)`. Every queue/worker key of a queue shares one hash slot (`async_task:{<queue>}` …).
- **Tuning.** `AsyncTaskQueue(redis_builder, status_ttl=86400, heartbeat_interval=10, heartbeat_ttl=30, shutdown_timeout=30)`.

**Upgrading from 0.2.x:** key names changed. New workers move tasks from the old keys automatically, and keep draining the old queue key for producers that are still on 0.2.x. Stop all 0.2.x **workers** before starting 0.3 workers.

### Result
The worker processes will asynchronously process the submitted tasks with the specified delays. You can monitor the progress and completion of tasks through log messages generated by the `my_task` function. The "Async Queue" library is suitable for I/O-bound workloads that benefit from asynchronous processing within a single application.

## Todo
1. Test and check back compatibility

