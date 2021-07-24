import logging
from logging import getLogger
from logging.handlers import RotatingFileHandler

from flask import Flask
from flask_cors import CORS

LOGGER = getLogger(__name__)
handler = RotatingFileHandler('./log/api.log', maxBytes=1000000, backupCount=3)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)

app = Flask(__name__)
CORS(app)
app.config['JSON_AS_ASCII'] = False
app.config['CORS_HEADERS'] = 'Content-Type'

import api.wordcloud
import api.bill
import api.member
