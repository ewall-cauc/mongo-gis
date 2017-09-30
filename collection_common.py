#! /usr/bin/env python
# -*- coding=utf-8 -*-

import os
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from abc import abstractmethod, ABCMeta
from functools import wraps
import itertools


class CollectionCommon(object):
    __metaclass__ = ABCMeta

    def __init__(self, host, database, collection, replica, port=27017, **kwargs):
        self.collection = collection
        self.replica = replica
        self.conn = MongoClient(host, port, replicaset=replica)
        self.db = self.conn[database]
        self.col = self.db[collection]
        if kwargs:
            self.user = kwargs.pop("user")
            self.password = kwargs.pop("password")

    def add_user_info(self, user, password):
        if not hasattr(self, "user"):
            setattr(self, "user", user)
        if not hasattr(self, "password"):
            setattr(self, "password", password)

    def authenticate(func):
        """
        make authentication before do some operation
        :return:
        """

        @wraps(func)
        def __wrap__(*args, **kwds):
            print args
            if not isinstance(args[0], CollectionCommon):
                raise TypeError
            user = getattr(args[0], "user", "")
            password = getattr(args[0], "password", "")
            if not user or not password:
                raise ValueError("Mongodb User or Password empty!")
            args[0].db.authenticate(user, password)
            return func(*args, **kwds)

        return __wrap__

    def do_index(self, *args):
        index = list(itertools.chain.from_iterable([_["indexes"] for _ in args[0]]))
        if not all(map(lambda x: x[0] in self.label, index)):
            raise KeyError("make sure collection field exist!")
        index = list()
        for _ in args[0]:
            index.append(IndexModel(_["indexes"], name=_["name"]))
        self.col.create_indexes(index)

    @authenticate
    def initialize(self, file_path, *args):
        """
        drop the collection if exist, then insert collection
        :param file_path: location mif and mid file exist
        :return:
        """

        if self.collection in self.db.collection_names():
            self.db.drop_collection(self.collection)
        self.db.create_collection(self.collection)
        self.insert(file_path)
        # todo: create_index for collection in future
        if args:
            self.do_index(*args)

    @staticmethod
    def check_file_path(file_path):
        """
        check the file_path, make sure *.mif and *.mid exist
        :param file_path:
        :return:
        """
        if not file_path:
            raise Exception("FilePathError,file path param could not be ignore!")
        if not os.path.isfile(file_path) and os.path.splitext(file_path)[1] not in [".mif", ".mid"]:
            raise Exception("FileTypeError,AdminArea check_file_path only support *.mid or *.mif file!")
        for _ in [os.path.splitext(file_path)[0] + _ for _ in [".mif", ".mid"]]:
            if not os.path.exists(_):
                raise Exception("FileExistError,{} do not exist!".format(_.encode("utf-8")))
        return True

    @abstractmethod
    def insert(self, file_path):
        pass

    @abstractmethod
    def query(self):
        pass


if __name__ == "__main__":
    pass
