import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY')

class DevConfig(Config):
    ENV = 'dev'
    DEBUG = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    TWC_BUCKET_NAME = 'twc-models'
    CACHE_TYPE = 'null'
    ASSETS_DEBUG = True

class ProdConfig(Config):
    ENV = 'prod'
    CACHE_TYPE = 'simple'
    TWC_BUCKET_NAME = 'twc-models'

class TestingConfig(Config):
    ENV = 'test'
    TWC_BUCKET_NAME = 'twc-models-test12958'