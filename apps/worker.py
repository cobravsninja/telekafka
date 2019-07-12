import threading
import urllib.request, urllib.error, urllib.parse
import json
from lib.instagram import InstagramScraper
from bs4 import BeautifulSoup
from lib.kafka_consumer import KafkaConsumer
from random import choice

class Worker():

  def __init__(self,**kwargs):
    self.logger                             = kwargs['logger']
    self.uas                                = kwargs['uas']
    self.producer_queue                     = kwargs['producer_queue']
    self.kafka_instagram_search_topic       = kwargs['kafka_instagram_search_topic']
    self.kafka_google_search_topic          = kwargs['kafka_google_search_topic']
    self.kafka_instagram_search_reply_topic = kwargs['kafka_instagram_search_reply_topic']
    self.kafka_google_search_reply_topic    = kwargs['kafka_google_search_reply_topic']
    self.kafka_outgoing_msgs_topic          = kwargs['kafka_outgoing_msgs_topic']
    self.consumer = KafkaConsumer(
      logger=self.logger,
      kafka_topics=[self.kafka_google_search_topic,self.kafka_instagram_search_topic],
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
      self.headers = self.get_headers()
      self.google_worker(msg) if msg['topic'] == self.kafka_google_search_topic else self.instagram_worker(msg)

  def google_worker(self,msg):
    self.logger.info('google worker')
    #topic = '{}_reply'.format(msg['topic'])
    topic = self.kafka_google_search_reply_topic

    query = msg['msg'].split()
    query = '+'.join(query)

    url       = "https://www.google.co.za/search?q="+urllib.parse.quote(query)+"&source=lnms&tbm=isch"

    try:
      soup      = BeautifulSoup(urllib.request.urlopen(urllib.request.Request(url,headers=self.headers)),'html.parser')
      ActualImages = []
      for a in soup.find_all("div",{"class":"rg_meta"}):                    
        link , Type =json.loads(a.text)["ou"]  ,json.loads(a.text)["ity"] 
        ActualImages.append((link,Type))

    except Exception as e:
      self.logger.error(f'google some error stuff {e}')
      self.queue_put(topic,msg['key'],'error')
      return 
        
    total_images = len(ActualImages)
    self.logger.info(f'there are total {total_images} images')  
    if total_images == 0:
      self.queue_put(topic,msg['key'],None)
      return

    for i in ActualImages:
      self.logger.info(f'sending image {i[0]}')
      self.queue_put(topic,msg['key'],i[0])

    self.logger.info(f"done with {msg['key']}")
    self.queue_put(topic,msg['key'],'done')

  def instagram_worker(self,msg):
    self.logger.info(f'instagram worker')
    #topic = '{}_reply'.format(msg['topic'])
    topic = self.kafka_instagram_search_reply_topic
    try:
      k = InstagramScraper(self.headers)
      results = k.profile_page_recent_posts('https://www.instagram.com/{}/?hl=en'.format(msg['msg']))
      if results is None or len(results) == 0:
        self.queue_put(topic,msg['key'],None)
        return

      for i in results:
        self.logger.info(f"instgram url {i['display_url']}")
        self.queue_put(topic,msg['key'],i['display_url'])
      self.queue_put(topic,msg['key'],'done')

    except Exception as e:
      self.logger.error(f'instagram some error stuff {e}')
      self.queue_put(topic,msg['key'],'error')
      return 

  def queue_put(self,topic,key,msg):
    self.logger.info(f'sending msg {msg} to {topic}')
    self.producer_queue.put({
      'topic': topic,
      'key':   key,
      'msg':   msg
    })

  def get_headers(self):
    return {'User-Agent': choice(self.uas)}
