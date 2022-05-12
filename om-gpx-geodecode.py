import gpxpy
import gpxpy.gpx
import urllib.request, json
from printree import ptree
import sqlite3
import random
import logging
import os


class PointMetadata:
    def __init__(self, display_name, address, time):
        self.display_name = display_name
        self.address = address
        self.time = time

    def key(self):
        k = []
        if 'country' in self.address:
            k.append(self.address['country'])
        if 'region' in self.address:
            k.append(self.address['region'])
        if 'state' in self.address:
            k.append(self.address['state'])
        if 'county' in self.address:
            k.append(self.address['county'])
        if 'district' in self.address:
            k.append(self.address['district'])
        if 'city' in self.address:
            k.append(self.address['city'])
        if 'town' in self.address:
            k.append(self.address['town'])
        # if 'suburb' in self.address:
        #     k.append(self.address['suburb'])
        if 'municipality' in self.address:
            k.append(self.address['municipality'])
        if 'village' in self.address:
            k.append(self.address['village'])
        if 'hamlet' in self.address:
            k.append(self.address['hamlet'])
        # if 'road' in self.address:
        #     k.append(self.address['road'])
        # if 'highway' in self.address:
        #     k.append(self.address['highway'])
        return k

    def __repr__(self):
        return "[{}, {}, {}, {}]".format(self.key(), self.display_name, self.address, self.time)


class GPXRendererStats:
    def __init__(self, file_name):
        self.file_name = file_name
        self.counterCache = 0
        self.counterGeocode = 0
        self.tracks = 0
        self.segments = 0
        self.points = 0

    def add_cache(self):
        self.counterCache += 1

    def add_geocoder(self):
        self.counterGeocode += 1

    def add_tracks(self):
        self.tracks += 1

    def add_segments(self):
        self.segments += 1

    def add_points(self):
        self.points += 1
        if (self.points % 1000 == 0):
            logging.debug("parsed {} points from file {}".format(self.points, self.file_name))

    def __str__(self):
        return "Stats: file={}, cache={}, geocoder={}, tracks={}, segments={}, points={}".format(
            self.file_name,
            self.counterCache,
            self.counterGeocode,
            self.tracks,
            self.segments,
            self.points)


class OMGPXTools:
    def __init__(self, file_name):
        self.connect = sqlite3.connect("geodata-cache.sqlite")
        self.stats = GPXRendererStats(file_name)
        self.file_name = file_name
        try:
            self.connect.execute("""
                    CREATE TABLE IF NOT EXISTS GEODATA (
                        lat number(3, 8) ,
                        lon number(3, 8) ,
                        geodata_json TEXT
                    );
                """)
            logging.info("Success: CREATE TABLE IF NOT EXISTS GEODATA")
            self.connect.execute("CREATE UNIQUE INDEX IF NOT EXISTS GEODATA_IDX ON GEODATA (lat, lon)")
            logging.info("Success: CREATE UNIQUE INDEX IF NOT EXISTS GEODATA_IDX ON GEODATA")
        except Exception as ex:
            logging.warning("possibly table GEODATA already exists: {}".format(ex))
            dataFromSql = self.connect.execute("SELECT count(*) FROM GEODATA");
            for row in dataFromSql.fetchall():
                logging.debug(str(row))
                pass

        gpx_file = open(file_name, 'r', encoding='UTF-8')
        self.gpx = gpxpy.parse(gpx_file)
        logging.info("parsed file {}".format(self.file_name))

    def listPoints(self):
        res = {}
        lPrevoius = 0
        pointsCount = 0
        for track in self.gpx.tracks:
            self.stats.add_tracks()
            for segment in track.segments:
                self.stats.add_segments()
                for point in segment.points:
                    self.stats.add_points()
                    try:
                        pmd = self.getPointMetadata(point.latitude, point.longitude, point.time)
                        # print(pmd.display_name)
                        res[str(pmd.key())] = pmd
                        if lPrevoius < len(res):
                            l = list(x.key() for x in res.values())
                            dict = self.generatePrintableTree(l)
                            # print(ptree(dict))
                        else:
                            # print(point.latitude, point.longitude, point.time, pmd.display_name)
                            pass
                        lPrevoius = len(res)
                    except Exception as theException:
                        print("не будем разбираться что там за эксепшен...", theException, point)

        logging.info(str(self.stats))
        return res

    def getPointMetadata(self, lat, lon, time):
        dataFromSql = self.connect.execute("SELECT geodata_json FROM GEODATA WHERE lat=? and lon=?", ([lat, lon]))
        fetchall = dataFromSql.fetchall()
        if len(fetchall) > 0:
            strFromCache = fetchall[0][0]
            self.stats.add_cache()
            return self.parsePointMetadataFromString(strFromCache, time)

        if random.randint(0, 1000) < 990:
            return PointMetadata('display_name', {}, time)

        nominatimUrl = "https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={}&lon={}".format(lat, lon)
        with urllib.request.urlopen(nominatimUrl) as url:
            strFromServer = url.read().decode()
            logging.debug("fetch from {}: {}".format(nominatimUrl, strFromServer))
            insertResult = self.connect.execute("INSERT INTO GEODATA (lat, lon, geodata_json) values (?,?,?)",
                                                ([lat, lon, strFromServer]))
            self.connect.commit()
            self.stats.add_geocoder()
            return self.parsePointMetadataFromString(strFromServer, time)

    def parsePointMetadataFromString(self, decodedSring, time):
        data = json.loads(decodedSring)
        return PointMetadata(data['display_name'], data['address'], time)

    def generatePrintableTree(self, list):
        tree = {}
        for item in list:
            currTree = tree

            for key in item:
                if key not in currTree:
                    currTree[key] = {}
                currTree = currTree[key]

        return tree


if __name__ == "__main__":
    logging.basicConfig(filename='om-gpx-geodecode.log', level=logging.DEBUG, filemode="w", encoding="UTF-8")

    for filename in os.listdir("."):
        logging.debug("file found: {}".format(filename))
        if filename.endswith(".gpx"):
            gpx = OMGPXTools(filename)
            finalResult = gpx.listPoints()
            l = list(x.key() for x in finalResult.values())
            dict = gpx.generatePrintableTree(l)
            print(filename)
            print(ptree(dict))
        else:
            logging.debug("skip file as non GPX: {}".format(filename))

    logging.info("END")
    # print(json.dumps(finalResult, default=vars))
