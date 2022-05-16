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


class MemorialsReader:
    def __init__(self, gpx_filename):
        self.gpx_filename = gpx_filename

        self.api = overpy.Overpass()
        self.connect = sqlite3.connect("geodata-cache.sqlite")

        gpx_file = open(gpx_filename, 'r', encoding='UTF-8')
        self.gpx = gpxpy.parse(gpx_file)
        logging.info("parsed file {}".format(self.gpx_filename))

        try:
            # self.connect.execute("""DROP TABLE  MEMORIALS_DATA""")
            self.connect.execute("""
                    CREATE TABLE IF NOT EXISTS MEMORIALS_DATA (
                        lat number(3, 8) ,
                        lon number(3, 8) ,
                        geodata_json TEXT
                    );
                """)
            logging.info("Success: CREATE TABLE IF NOT EXISTS MEMORIALS_DATA")
            self.connect.execute("CREATE UNIQUE INDEX IF NOT EXISTS MEMORIALS_DATA_IDX ON MEMORIALS_DATA (lat, lon)")
            logging.info("Success: CREATE UNIQUE INDEX IF NOT EXISTS MEMORIALS_DATA_IDX ON MEMORIALS_DATA")
            data_from_sql = self.connect.execute("SELECT * FROM MEMORIALS_DATA");
            for row in data_from_sql.fetchall():
                logging.debug(str(row))
                pass

        except Exception as ex:
            logging.warning("possibly table MEMORIALS_DATA already exists: {}".format(ex))

    def read_memorial_names_from_overpass(self, lat, lon):
        query_string = """
        <query type="node">
          <around lat="{:.5f}" lon="{:.5f}" radius="1000m"/>
          <has-kv k="historic" regv="memorial|monument"/>
        </query>
        <print />
        """.format(lat, lon)
        logging.debug(query_string)

        result = self.api.query(query_string)
        logging.debug(result)

        memorials = [[float(n.lat), float(n.lon), n.tags["name"], str(n.tags)] if "name" in n.tags else None for n in result.nodes]
        logging.debug("parsed memorials={}".format(memorials))
        return memorials

    def get_memorial_names(self, lat, lon):
        dataFromSql = self.connect.execute("SELECT geodata_json FROM MEMORIALS_DATA WHERE lat=? and lon=?",
                                           ([lat, lon]))
        fetchall = dataFromSql.fetchall()
        if len(fetchall) > 0:
            str_from_cache = fetchall[0][0]
            list_of_memorials = json.loads(str_from_cache)
            return list_of_memorials

        if random.randint(0, 1000) < 990:
            return set()

        try:
            list_of_memorials = self.read_memorial_names_from_overpass(lat, lon)
            dumps = json.dumps(list_of_memorials)
            self.connect.execute("INSERT INTO MEMORIALS_DATA (lat, lon, geodata_json) values (?,?,?)",
                                 ([lat, lon, dumps]))
            self.connect.commit()
            return list_of_memorials
        except Exception as ex:
            logging.warning("Exception : {}, returning empty set".format(ex))
            return set()

    def read_all_memorial_names(self):
        result = {}
        for track in self.gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    try:
                        pmd = self.get_memorial_names(point.latitude, point.longitude)
                        if len(pmd) > 0:
                            logging.debug("get_river_names: {} {} : {}".format(point.latitude, point.longitude, pmd))
                        for item in pmd:
                            if item != None:
                                result[item[2]] = [item[0], item[1], item[3]]
                        # print(result)
                        pass # logging.debug("result = {}".format(result))
                    except Exception as theException:
                        print("не будем разбираться что там за эксепшен...", theException, point)

        return result


if __name__ == "__main__":
    logging.basicConfig(filename='om-gpx-memorials.log', level=logging.DEBUG, filemode="w", encoding="UTF-8")

    for filename in os.listdir("."):
        logging.debug("file found: {}".format(filename))
        if filename.endswith(".gpx"):
            wr = MemorialsReader(filename)
            memorials = wr.read_all_memorial_names()

            print(filename)
            for k in memorials.keys():
                print("\t{}\t-\t[{} {} {}]".format(k, memorials[k][0], memorials[k][1], memorials[k][2]))
            # for i in sorted(memorials):
            #     print("\t{}".format(i))

        else:
            logging.debug("skip file as non GPX: {}".format(filename))

    logging.info("END")
