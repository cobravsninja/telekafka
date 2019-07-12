import threading
from lib.kafka_producer import KafkaProducer
from queue import Empty

# sends data from queue to kafka
class OutgoingKafka():

  def __init__(self,**kwargs):
    self.producer_queue = kwargs['producer_queue']
    self.logger = kwargs['logger']
    self.producer = KafkaProducer(kwargs['kafka_servers'],kwargs['kafka_compression_codec'],self.logger)

    self.logger.info(f"starting producer {kwargs['id']}")
    while not threading.currentThread().kill_received:
      try:
        msg = self.producer_queue.get(timeout=1)
        self.logger.info(f'received msg through queue {msg}')
        self.producer.send_msg(msg['topic'],str(msg['key']),msg['msg'])
      except Empty:
        pass
      except Exception as e:
        self.logger.error(f'some error stuff - {e}')

    self.logger.info('outgoing kafka has been stopped')
    self.producer.flush()
