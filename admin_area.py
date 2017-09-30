#! /usr/bin/env python
# -*- coding=utf-8 -*-


import geopandas
from shapely.geometry import mapping
from collection_common import CollectionCommon
import pymongo


class AdminArea(CollectionCommon):
    def __init__(self, host, database, collection, replica, port=27017, **kwargs):
        super(AdminArea, self).__init__(host, database, collection, replica, port, **kwargs)

    def insert(self, file_path):
        # todo: modify insert to bulk boost mongodb write ability
        """
        extract the record in mif-file, save in mongodb
        append the document into collection
        :param file_path: location mif and mid file exist
        :return:
        """
        AdminArea.check_file_path(file_path)
        admin_mif = geopandas.GeoDataFrame.from_file(file_path)
        setattr(self, "label", admin_mif.axes[1])
        for _ in admin_mif.values:
            item = dict(zip(admin_mif.axes[1], _))
            item["geometry"] = mapping(item["geometry"])
            item["AdminName"] = item["AdminName"].decode("gbk")
            self.col.insert(item)

    def query(self):
        """
        :return:
        """
        result = admin.col.find({"AdminName": u"福建省"})
        print result[0]["geometry"]["coordinates"]
        pass


if __name__ == "__main__":
    user_info = {"user": "root", "password": "kotei$88"}
    admin = AdminArea(host="localhost", database="test", collection="admin_area", replica="kotei", port=27018,
                      **user_info)
    index_cfg = [
        {"name": "admincode", "indexes": [("AdminID", 0), ("MeshCode", 1)]},
        {"name": "ad", "indexes": [("AdminID", 0)]},
        {"name": "Location", "indexes": [("geometry", "2dsphere")]}
    ]
    admin.initialize(u"/home/road/admin.mif", index_cfg)
    pass
