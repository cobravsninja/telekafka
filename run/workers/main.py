from apps.outgoing_kafka import OutgoingKafka
from apps.worker import Worker

def main(config,thread,threads,producer_queue,Logger,log_path):

  # logger & thread app thread_qt params
  # sends data from queue to kafka
  logger = Logger('OutgoingKafka',log_path)
  thread(OutgoingKafka,3,
    producer_queue=producer_queue,
    logger=logger,
    kafka_servers=config['kafka_servers'],
    kafka_compression_codec=config['kafka_compression_codec']
  )
  thread(Worker,3,
    producer_queue=producer_queue,
    logger=logger,
    kafka_servers=config['kafka_servers'],
    kafka_group=config['kafka_group'],
    kafka_instagram_search_topic=config['kafka_instagram_search_topic'],
    kafka_google_search_topic=config['kafka_google_search_topic'],
    kafka_instagram_search_reply_topic=config['kafka_instagram_search_reply_topic'],
    kafka_google_search_reply_topic=config['kafka_google_search_reply_topic'],
    kafka_outgoing_msgs_topic=config['kafka_outgoing_msgs_topic'],
    uas=config['uas'],
  )
