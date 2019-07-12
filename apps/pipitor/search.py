import threading
import json
import time
from random import randint
from datetime import datetime
from lib.kafka_consumer import KafkaConsumer

class Search():
  def __init__(self,**kwargs):
    self.logger = kwargs['logger']
    config = kwargs['config'] # pass all config params
    self.consumer = KafkaConsumer(
      logger=self.logger,
      kafka_topics=kwargs['kafka_topics'],
      kafka_servers=config['kafka_servers'],
      kafka_group=config['kafka_group']
    )
    self.models                             = kwargs['models']
    self.producer_queue                     = kwargs['producer_queue']
    self.outgoing                           = config['kafka_outgoing_msgs_topic']
    self.kafka_google_requests_topic        = config['kafka_google_requests_topic']
    self.kafka_instagram_requests_topic     = config['kafka_instagram_requests_topic']
    self.kafka_google_search_reply_topic    = config['kafka_google_search_reply_topic']
    self.kafka_instagram_search_reply_topic = config['kafka_instagram_search_reply_topic']
    self.kafka_google_search_topic          = config['kafka_google_search_topic']
    self.kafka_instagram_search_topic       = config['kafka_instagram_search_topic']
    self.kafka_bot_urls_topic               = config['kafka_bot_urls_topic']

    while not threading.currentThread().kill_received:
      msg = self.consumer.get_data()
      if msg is None:
        continue

      if 'error' in msg:
        self.logger.error(f"some error stuff {msg['error']}")
        continue

      self.logger.info(f'received msg {msg}')

      if msg['topic'] == self.kafka_instagram_requests_topic or msg['topic'] == self.kafka_google_requests_topic:
        self.search_request(msg)
        continue

      if msg['topic'] == self.kafka_instagram_search_reply_topic or msg['topic'] == self.kafka_google_search_reply_topic:
        self.reply_request(msg)
        continue
    
    # kill received, closing connection
    self.consumer.close()

  def search_request(self,msg):
    self.logger.info(f'search request msg {msg}')
    topic    = msg['topic']
    model    = self.get_model(msg['topic'])[0]
    json_msg = json.loads(msg['msg'])
    
    try:
      record = model.objects.get(query=json_msg['query'],chat_id=json_msg['chat_id'])
      if record.status is True:
        self.outgoing_queue_put(
          self.outgoing,randint(1000,100000),
          json_msg['chat_id'],
          f'''"{json_msg['query']}" {self.search} search query already active, sorry'''
        )
        return
      else:
        record.status = True
        record.save()
        self.get_urls(msg,record)
        record.status = False
        record.save()
        return  
    except Exception as e:
      self.logger.error('object not found, etc.')
          
    record = model(query=json_msg['query'],chat_id=json_msg['chat_id'])
    record.save()
    
    self.producer_queue.put({'topic':self.topic,'key':record.id,'msg':json_msg['query']})
  
  def reply_request(self,msg):
    model = self.get_model(msg['topic'])[0]
    try:
      query = model.objects.get(pk=msg['key'])
    except Exception as e:
      self.logger.error(f'error while retreiving object - {e}')
      return

    if msg['msg'] == 'error':
      self.logger.error(f"error msg received, id {msg['key']}")
      self.outgoing_queue_put(self.outgoing,randint(1000,100000),query.chat_id,
        f'some error occurred while performing "{query.query}" {self.search} query, sorry'
      )
      query.delete()
      return

    if msg['msg'] is None:
      self.outgoing_queue_put(self.outgoing,randint(1000,100000),query.chat_id,
        f'no search results while performing "{query.query}" {self.search} query, sorry'
      )
      query.delete()
      return

    if msg['msg'] == 'done':
      self.get_urls(msg,query)
      query.status = False
      query.save()
      return

    model = self.get_model(msg['topic'])[1]
    record = model(url=msg['msg'],active=query)
    record.save()

  def outgoing_queue_put(self,topic,key,chat_id,msg):
    self.logger.info(f'sending msg {msg} to {topic}')
    self.producer_queue.put({
      'topic': topic,
      'key':   key,
      'msg':   json.dumps({'chat_id':chat_id,'msg':msg})
    })

  def get_model(self,topic):
    if topic == self.kafka_google_requests_topic or topic == self.kafka_google_search_reply_topic:
      model       = 'ActiveGoogleSearch' 
      model_url   = 'GoogleUrl'
      self.search = 'google'
      self.topic  = self.kafka_google_search_topic
    else:
      model       = 'ActiveInstagramSearch'
      model_url   = 'InstagramUrl'
      self.search = 'instagram'
      self.topic  = self.kafka_instagram_search_topic
    
    return [getattr(self.models,model),getattr(self.models,model_url)]

  def get_urls(self,msg,query):
    url_model = self.get_model(msg['topic'])[1]
    data = url_model.objects.filter(active=query,status=True).values('id')[:3]
    if len(data) == 0:
      query.delete()
      self.search_request(msg)
      return
    
    for i in data:
      i = url_model.objects.get(pk=i['id'])
      i.status = False
      i.requested = datetime.now()
      i.save()
      self.producer_queue.put({
        'topic': self.kafka_bot_urls_topic,
        'key':   i.id,
        'msg':   json.dumps({
          'url':i.url,
          'chat_id':query.chat_id,
          'search': self.search,
          'query':query.query
        })
      })
