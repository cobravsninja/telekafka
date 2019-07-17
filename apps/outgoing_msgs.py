import threading
import json
from lib.kafka_consumer import KafkaConsumer

# sends messages from kafka to telegram
class OutgoingMsgs():

  def __init__(self,**kwargs):
    self.bot = kwargs['bot']
    self.logger = kwargs['logger']
    self.consumer = KafkaConsumer(
      logger=self.logger,
      kafka_topics=kwargs['kafka_outgoing_msgs_topics'],
      kafka_servers=kwargs['kafka_servers'],
      kafka_group=kwargs['kafka_group']
    )

    while not threading.currentThread().kill_received:
      msg = self.consumer.get_data()
      if msg is None:
        continue
         
      if 'error' in msg:
        self.logger.error(f"some error stuff {msg['error']}")
        continue
       
      self.logger.info(f'received msg {msg}')
      msg = json.loads(msg['msg'])
      self.bot.retry_message(chat_id=msg['chat_id'],text=msg['msg'])
