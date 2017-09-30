#! /usr/bin/env python
# -*- coding=utf-8 -*-

import os
import geopandas
from shapely.geometry import mapping
from collection_common import CollectionCommon


class Road(CollectionCommon):
    def __init__(self, host, database, collection, replica, port=27017):
        super(Road, self).__init__(host, database, collection, replica, port)

    def query(self):
        pass

    def insert(self, file_path):
        Road.check_file_path(file_path)
        road_mif = geopandas.GeoDataFrame.from_file(file_path)
        # print (road_mif.to_dict()["geometry"][0])
        print dir(road_mif.values[0])
        print road_mif.values[0]
        for _ in road_mif.values:
            pass
        # print mapping(road_mif.to_dict()["geometry"][0])
        # road_mif.plot("geometry", cmap='OrRd')
        pass


if __name__ == "__main__":
    road = Road(host="localhost", database="test", collection="test", replica="kotei")
    road.insert(u"/home/road/road.mif")
    pass
