import assets
import os
import config
from dotenv import load_dotenv
from flask import Flask
from flask_caching import Cache
from flask_cors import CORS
import logging
import time
import sys


load_dotenv()
PROJECT_ID = os.getenv("PROJECTID")


logfile = "serve.log"
logging.basicConfig(filename=logfile, level=logging.DEBUG, format = '%(levelname)s: %(filename)s: %(message)s')
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format = '%(levelname)s: %(filename)s: %(message)s')
logging.info('API server started at %s', time.asctime(time.localtime()))

app = Flask(__name__)
app.config.from_object(config)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
app.config['CORS_HEADERS'] = "Content-Type"
cache = Cache(app)
cors = CORS(app)
assets.init(app)

from aqandu import basic_routes
