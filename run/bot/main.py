from apps.bot import Bot
from apps.outgoing_kafka import OutgoingKafka
from apps.outgoing_msgs import OutgoingMsgs
from apps.downloader import Downloader

def main(config,thread,threads,producer_queue,Logger,log_path):

  logger = Logger('bot',log_path)
  # we can pass bot to our apps, so no need to use queue
  bot_object = Bot(logger=logger,config=config,producer_queue=producer_queue)
  bot = bot_object.get_bot()

  # logger & thread app thread_qt params
  # sends data from queue to kafka
  logger = Logger('OutgoingKafka',log_path)
  thread(OutgoingKafka,3,
    producer_queue=producer_queue,
    logger=logger,
    kafka_servers=config['kafka_servers'],
    kafka_compression_codec=config['kafka_compression_codec']
  )
  # sends messages from kafka to telegram
  thread(OutgoingMsgs,3,
    bot=bot,
    kafka_outgoing_msgs_topics=[config['kafka_outgoing_msgs_topic']], # should be list
    kafka_servers=config['kafka_servers'],
    kafka_group=config['kafka_group'],
    logger=logger
  )
  thread(Downloader,3,
    bot=bot,
    kafka_bot_urls_topics=[config['kafka_bot_urls_topic']], # should be list
    kafka_servers=config['kafka_servers'],
    kafka_group=config['kafka_group'],
    uas=config['uas'],
    logger=logger
  )

  return bot_object.get_updater()
