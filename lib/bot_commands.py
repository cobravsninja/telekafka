import json

class BotCommands():

  def __init__(self,**kwargs):
    self.bot                            = kwargs['bot']
    self.producer_queue                 = kwargs['producer_queue']
    self.logger                         = kwargs['logger']
    self.chat_ids                       = kwargs['chat_ids']
    self.kafka_google_requests_topic    = kwargs['kafka_google_requests_topic']
    self.kafka_instagram_requests_topic = kwargs['kafka_instagram_requests_topic']

  def command_handler(self,update,context):
    self.logger.info(f'checking command:\n{update}')
    if not self.check_id(update.message.chat):
      self.logger.info(f'received unknown msg from:\n{update}')
      return
    command = update.message.text.split(' ')[0]
    query = ' '.join(update.message.text.split(' ')[1:])
    topic = self.kafka_google_requests_topic if command.upper() == '/GOOGLE' else self.kafka_instagram_requests_topic
    self.logger.info('ya tut')

    try:
      self.put_to_queue(topic,update.message.message_id,
        json.dumps({
          'chat_id': update.message.chat_id,
          'query':   query
        })
      )
    except Exception as e:
      self.logger.error(f'exception {e}')

    self.logger.info('ya tut2')

  def message_handler(self,update,context):
    self.logger.info(f'checking msg:\n{update}')
    if not self.check_id(update.message.chat):
      self.logger.info(f'received unknown msg from:\n{update}')
      return

  def put_to_queue(self,topic,key,msg):
    self.logger.info('sending data to queue {}'.format({'topic': topic,'key': key,'msg': msg}))
    self.producer_queue.put({'topic': topic,'key': key,'msg': msg})

  def unknown_command_handler(self,update,context):
    self.bot.send_message(chat_id=update.message.chat.id,text='unknown command, sorry')

  def check_id(self,msg):
    if msg.id in self.chat_ids:
      if self.chat_ids[msg.id] == msg.type:
        return True

  def send_msg(self,chat_id,msg):
    self.bot.send_message(chat_id=chat_id,text=msg)
