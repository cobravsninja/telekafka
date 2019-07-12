from confluent_kafka import Consumer, KafkaError

class KafkaConsumer():

  def __init__(self,**kwargs):
    self.logger        = kwargs['logger']
    self.kafka_topics  = kwargs['kafka_topics']
    self.kafka_servers = kwargs['kafka_servers']
    self.kafka_group   = kwargs['kafka_group']
    self.c = Consumer({
        'bootstrap.servers': self.kafka_servers,
        'group.id': self.kafka_group
        #'auto.offset.reset': 'earliest',
    })

    self.logger.info(f'subscribing to topics {self.kafka_topics}')
    self.c.subscribe(self.kafka_topics)

  def get_data(self):
    msg = self.c.poll(1.0)

    if msg is None:
      return None

    if msg.error():
      return {'error': msg.error()}

    self.logger.info(f'partition {msg.partition()}, offset {msg.offset()}')
    message = None if msg.value() is None else msg.value().decode('utf-8')

    return {
      'key':        msg.key().decode('utf-8'),
      'msg':        message,
      'topic':      msg.topic()
    }

  def close(self):
    self.c.close()
