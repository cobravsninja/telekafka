import logging

class Logger():

  # logging levels
  LEVELS = { 'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
  }

  def __new__(self,appname,filename=None,console=True,level='info'):
    level = self.LEVELS[level]
    self.logger = logging.getLogger(appname)
    self.logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(name)s %(threadName)s [%(levelname)s]: %(message)s')

    if console is True:
      ch = logging.StreamHandler()
      ch.setLevel(level)
      ch.setFormatter(formatter)
      self.logger.addHandler(ch)

    if filename:
      fh = logging.FileHandler(filename)
      fh.setLevel(level)
      fh.setFormatter(formatter)
      self.logger.addHandler(fh)

    return self.logger
