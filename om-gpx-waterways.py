import sqlite3
import json
import overpy
import gpxpy
import logging
import os
import random


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError("Unknown type: {} {}".format(obj, type(obj)))


class WaterwaysReader:
    def __init__(self, gpx_filename):
        self.gpx_filename = gpx_filename

        self.api = overpy.Overpass()
        self.connect = sqlite3.connect("geodata-cache.sqlite")

        gpx_file = open(gpx_filename, 'r', encoding='UTF-8')
        self.gpx = gpxpy.parse(gpx_file)
        logging.info("parsed file {}".format(self.gpx_filename))

        try:
            # self.connect.execute("""DROP TABLE  RIVERS_DATA""")
            self.connect.execute("""
                    CREATE TABLE IF NOT EXISTS RIVERS_DATA (
                        lat number(3, 8) ,
                        lon number(3, 8) ,
                        geodata_json TEXT
                    );
                """)
            logging.info("Success: CREATE TABLE IF NOT EXISTS RIVERS_DATA")
            self.connect.execute("CREATE UNIQUE INDEX IF NOT EXISTS RIVERS_DATA_IDX ON RIVERS_DATA (lat, lon)")
            logging.info("Success: CREATE UNIQUE INDEX IF NOT EXISTS RIVERS_DATA_IDX ON RIVERS_DATA")
            dataFromSql = self.connect.execute("SELECT * FROM RIVERS_DATA");
            for row in dataFromSql.fetchall():
                logging.debug(str(row))
                pass

        except Exception as ex:
            logging.warning("possibly table RIVERS_DATA already exists: {}".format(ex))

    def read_river_names(self, lat, lon, distance):
        query_string = """
        <bbox-query s="{:.5f}" n="{:.5f}" w="{:.5f}" e="{:.5f}"/>
        <recurse type="node-way"/>
        <query type="way">
          <item/>
          <has-kv k="waterway"/>
          <has-kv k="name"/>
        </query>
        <print/>
        """.format(lat - distance,
                   lat + distance,
                   lon - distance,
                   lon + distance)
        logging.debug(query_string)

        result = self.api.query(query_string)
        logging.debug(result)

        rivers = []
        for way in result.ways:
            river = way.tags.get("waterway", "n/a") + " " + way.tags.get("name", "n/a")
            if river not in rivers:
                rivers.add(river)
            logging.debug("parsed rivers={}, source={}".format(rivers, way.tags))
        return rivers

    def get_river_names(self, lat, lon):
        dataFromSql = self.connect.execute("SELECT geodata_json FROM RIVERS_DATA WHERE lat=? and lon=?", ([lat, lon]))
        fetchall = dataFromSql.fetchall()
        if len(fetchall) > 0:
            str_from_cache = fetchall[0][0]
            list_of_rivers = json.loads(str_from_cache)
            return set(list_of_rivers)

        if random.randint(0, 1000) < 995:
            return set()

        try:
            list_of_rivers = self.read_river_names(lat, lon, 0.05)
            dumps = json.dumps(list_of_rivers)
            self.connect.execute("INSERT INTO RIVERS_DATA (lat, lon, geodata_json) values (?,?,?)",
                                 ([lat, lon, dumps]))
            self.connect.commit()
            return list_of_rivers
        except Exception as ex:
            logging.warning("Exception : {}, returning empty set".format(ex))
            return []

    def read_all_river_names(self):
        result = []
        for track in self.gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    try:
                        pmd = self.get_river_names(point.latitude, point.longitude)
                        if len(pmd) > 0:
                            logging.debug("get_river_names: {} {} : {}".format(point.latitude, point.longitude, pmd))
                        for riverName in pmd:
                            if riverName not in result:
                                result.append(riverName)
                        logging.debug("result = {}".format(result))
                    except Exception as theException:
                        print("не будем разбираться что там за эксепшен...", theException, point)

        return result


if __name__ == "__main__":
    logging.basicConfig(filename='om-gpx-waterways.log', level=logging.DEBUG, filemode="w", encoding="UTF-8")

    for filename in os.listdir("."):
        logging.debug("file found: {}".format(filename))
        if filename.endswith(".gpx"):
            wr = WaterwaysReader(filename)
            rivers = wr.read_all_river_names()
            # rivers = wr.read_river_names(lat=55.69453, lon=37.55680, distance=0.01)

            print(filename)
            for i in rivers:
                print("\t{}".format(i))

        else:
            logging.debug("skip file as non GPX: {}".format(filename))

    logging.info("END")
    # print(json.dumps(finalResult, default=vars))
