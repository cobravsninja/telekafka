# bot modules
import telegram
from telegram.ext import Updater
from telegram.ext import CommandHandler, MessageHandler, Filters
from lib.bot_commands import BotCommands

# etc
import json

class Bot():
  def __init__(self,**kwargs):
    self.logger = kwargs['logger']
    self.config = kwargs['config']
    self.logger.info('creating bot')
    self.bot = telegram.Bot(token=self.config['bot_token'])
    self.updater = Updater(bot=self.bot,use_context=True)
    dispatcher = self.updater.dispatcher

    # bot commands init
    self.bot_commands = BotCommands(
      bot=self.bot,
      producer_queue=kwargs['producer_queue'],
      logger=self.logger,
      chat_ids=self.config['chat_ids'],
      kafka_google_requests_topic=self.config['kafka_google_requests_topic'],
      kafka_instagram_requests_topic=self.config['kafka_instagram_requests_topic']
    )

    # bot command handler
    for i in ['google','instagram']:
      handler = CommandHandler(i,self.bot_commands.command_handler)
      dispatcher.add_handler(handler)

    # bot msg handler
    echo_handler = MessageHandler(Filters.text,self.bot_commands.message_handler)
    dispatcher.add_handler(echo_handler)

    # unknown commands
    unknown_handler = MessageHandler(Filters.command,self.bot_commands.unknown_command_handler)
    dispatcher.add_handler(unknown_handler)
    
    # start polling
    self.logger.info('starting bot polling...')
    #print('starting bot polling...')
    self.updater.start_polling()

  def get_bot(self):
    return self.bot

  def get_updater(self):
    return self.updater

  def stop(self):
    self.logger.info('stopping bot, please wait...')
    #print('stopping bot, please wait...')
    self.updater.stop()
