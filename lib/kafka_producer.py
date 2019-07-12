from confluent_kafka import Producer

class KafkaProducer():

  def __init__(self,kafka_servers,kafka_compression_codec,logger):
    self.logger = logger
    self.producer = Producer({
      'bootstrap.servers': kafka_servers,
      'compression.codec': kafka_compression_codec,
      'max.in.flight.requests.per.connection': 1,
      'acks': 'all'
    })

  def send_msg(self,topic,key,msg):
    self.logger.info(f'sending msg to topic {topic} (key {key}): {msg}')
    self.producer.poll(0)
    self.producer.produce(topic,key=key,value=msg,
      callback=self.delivery_report)
    self.producer.flush()

  def delivery_report(self,err,msg):
    if err is not None:
        self.logger.error(f'Message delivery failed: {err}')
    else:
        self.logger.info(f'Message delivered to {msg.topic()} partition {msg.partition()}, offset {msg.offset()}')

  def flush(self):
    self.producer.flush()
