from apps.outgoing_kafka import OutgoingKafka
from apps.pipitor.search import Search

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

  # django setup, etc.
  import os
  import django
  os.environ["DJANGO_SETTINGS_MODULE"] = 'apps.pipitor.pipitor.settings'
  django.setup()
  import apps.pipitor.telega.models as models

  #'kafka_topics': ['bot_google_requests2','bot_instagram_requests2','bot_instagram_search_reply2','bot_google_search_reply2']
  # this guy has dozen of params, this is why I decided pass config directly, pity
  thread(Search,3,
    config=config,
    producer_queue=producer_queue,
    logger=logger,
    models=models,
    kafka_topics=[config['kafka_google_requests_topic'],config['kafka_instagram_requests_topic'],
      config['kafka_instagram_search_reply_topic'],config['kafka_google_search_reply_topic']] # should be list
  )
