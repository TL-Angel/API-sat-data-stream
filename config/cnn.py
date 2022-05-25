
# -*- coding: utf-8 -*-
__author__ = "David Pastrana"
__copyright__ = "@ROBINA, Oct 2022"
__credits__ = ["David Pastrana"]
__license__ = "GPL"
__version__ = "2.0.0"
__email__ = "losphiereth@outlook.com"
__status__ = "Development"

from os import environ
import sys
sys.path.append('../')  # noqa E402
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

class CnnMongo():
    def __init__(self, name_cnn='cnn_sat',
                 db_name='sat',
                 verbose=True):

        self.verbose = verbose
        self.user = environ.get("MONGODB_USER")
        self.pwd = environ.get("MONGODB_PASS")
        self.host = environ.get("MONGODB_HOST")
        self.port = environ.get("MONGODB_PORT")
        self.auth = environ.get("MONGODB_AUTH")
        self.new_client = None
        self.timeout = 2000
        self.__connect()
        self.db_sat = self.new_client[db_name]

    def __connect(self):
        try:
            self.new_client = MongoClient(
                'mongodb://{0}:{1}@{2}:{3}/{4}'.format(
                    self.user,
                    self.pwd,
                    self.host,
                    self.port,
                    self.auth
                ),
                serverSelectionTimeoutMS=self.timeout
            )
            info = self.new_client.server_info()
            if self.verbose:
                print('::: [+] User {0} connected! access to db:'.format(
                    self.user), self.new_client.list_database_names())
        except OperationFailure as e:
            if self.verbose:
                print('::: [+] Database authentication failed!')
        except ConnectionFailure as e:
            if self.verbose:
                print('::: [+] Database connection error!, no server found.')

MONGO_CLIENT = CnnMongo()
