import threading
import sys
import traceback

from queue import Queue
from time import sleep
from importlib import import_module
from lib.logger import Logger
#from config import log_path, kafka_servers, kafka_compression_codec
from config import config

# threads
threads = []

# queues
producer_queue = Queue()

def thread(target,thread_qt,**kwargs):
  for i in range(thread_qt):
    kwargs['id'] = i
    t = threading.Thread(target=target,kwargs=kwargs,daemon=True)
    t.kill_received = False
    threads.append(t)
    t.start()

try:
  # run param
  run = sys.argv[1]

  # app config & logger
  run_config = getattr(import_module(f'run.{run}.config',package='config'),'config')
  print(f'run config {run_config}')
  config = {**config,**run_config}
  log_path = f"{config['log_path']}/{run}.log"
  logger = Logger(f'main-{run}',log_path)

  # run import & execution
  run = import_module(f'run.{run}.main')
  run = run.main(
    config,                  # config
    thread,                  # thread def
    threads,                 # threads list
    producer_queue,          # queue
    Logger,                  # logger class
    log_path                 
  )
except IndexError:
  print(f"Incorrect usage, usage: main.py runner")
  sys.exit(2)
except ImportError as e:
  print(f"Can't import module: {e}")
  traceback.print_exc()
  sys.exit(2)
except Exception as e:
  print(f'Some error stuff: {e}')
  traceback.print_exc()
  sys.exit(2)

while True in [t.isAlive() for t in threads]:
  try:
    sleep(1)
  except (KeyboardInterrupt,SystemExit):
    for t in threads:
      t.kill_received = True

if run is not None:
  run.stop()
