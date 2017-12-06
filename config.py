import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    CASSANDRA_LOGLEVEL = 'INFO'
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = 'this-really-needs-to-be-changed'


class ProductionConfig(Config):
    KEYSPACE = "hydroview"    # Production keyspace
    HOSTS = ['85.24.137.186', '85.24.137.188']    # Production cluster nodes
    PORT = 9042    # Production cluster port


class StagingConfig(Config):
    CASSANDRA_LOGLEVEL = 'DEBUG'
    DEVELOPMENT = True
    DEBUG = True
    KEYSPACE = "hydroview_staging"    # Staging keyspace (usually same as development)
    HOSTS = ['127.0.0.1']    # Staging cluster nodes (usually same as development)
    PORT = 9042    # Stating cluster port (usually same as development)


class DevelopmentConfig(Config):
    CASSANDRA_LOGLEVEL = 'DEBUG'
    DEVELOPMENT = True
    DEBUG = True
    KEYSPACE = "hydroview_development"    # Development keyspace
    HOSTS = ['127.0.0.1']    # Development cluster nodes
    PORT = 9042    # Development cluster port


class TestingConfig(Config):
    CASSANDRA_LOGLEVEL = 'DEBUG'
    TESTING = True
    KEYSPACE = "hydroview_testing"
    HOSTS = ['127.0.0.1']    # Testing cluster nodes (usually same as development)
    PORT = 9042    # Testing cluster port (usually same as development)
