import threading
import json
import requests
import urllib3
from os import unlink
from lib.kafka_consumer import KafkaConsumer
from random import randint, choice

class Downloader():

  def __init__(self,**kwargs):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    self.bot = kwargs['bot']
    self.logger = kwargs['logger']
    self.uas = kwargs['uas']

    self.consumer = KafkaConsumer(
      logger=self.logger,
      kafka_topics=kwargs['kafka_bot_urls_topics'],
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
      self.msg = json.loads(msg['msg'])
      self.logger.info(f'loaded json {self.msg}')
      self.get_image(self.msg['url'])

  def get_image(self,url):
    headers = {'User-Agent': choice(self.uas)}
    try:
      photo = requests.get(url,headers=headers,verify=False)
    except Exception as e:
      error = f'''error ocurred while fetching {url} for "{self.msg['query']}" {self.msg['search']} search, error - {e}'''
      self.logger.error(error)
      self.bot.send_message(
        chat_id=self.msg['chat_id'],
        text=error
      )
      return
    if photo.status_code == requests.codes.ok:
      img_file = '/tmp/' + str(randint(100000,1000000))
      with open(img_file,'wb') as img:
        img.write(photo.content)
      try:
        self.bot.send_photo(
          chat_id=self.msg['chat_id'],
          photo=open(img_file,'rb'),
          caption=f'''img result for "{self.msg['query']}" {self.msg['search']} search'''
        )
      except Exception as e:
        error = f'sorry, some error ocurred while submitting image to telegram (URL {url}, error {e})'
        self.logger.error(error)
        self.bot.send_message(
          chat_id=self.msg['chat_id'],
          text=error
        )
      unlink(img_file)
