import gpxpy
import gpxpy.gpx
import urllib.request, json
from printree import ptree
import sqlite3
import random
import logging


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


class OMGPXTools:

    def __init__(self, fileName):
        self.connect = sqlite3.connect("geodata-cache.sqlite")
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

        self.fileName = fileName
        gpx_file = open(fileName, 'r', encoding='UTF-8')
        self.gpx = gpxpy.parse(gpx_file)
        logging.info("parsed file {}".format(self.fileName))

    def listPoints(self):
        res = {}
        lPrevoius = 0
        pointsCount = 0
        for track in self.gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    pointsCount += 1
                    if (pointsCount % 1000 == 0):
                        logging.debug("parsed {} points from file {}".format(pointsCount, self.fileName))
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

        return res

    def getPointMetadata(self, lat, lon, time):
        dataFromSql = self.connect.execute("SELECT geodata_json FROM GEODATA WHERE lat=? and lon=?", ([lat, lon]))
        fetchall = dataFromSql.fetchall()
        if len(fetchall) > 0:
            strFromCache = fetchall[0][0]
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

    for filename in ["_1_.gpx",
                     "_2_happy_grader_day.gpx",
                     "_3_.gpx",
                     "_3_ (1).gpx",
                     "_4_.gpx",
                     "_4_ (1).gpx",
                     "_5_.gpx",
                     "_5_ (1).gpx",
                     "_6_.gpx",
                     "_6_ (1).gpx"]:
        gpx = OMGPXTools(filename)
        finalResult = gpx.listPoints()
        l = list(x.key() for x in finalResult.values())
        dict = gpx.generatePrintableTree(l)
        print(filename)
        print(ptree(dict))
        logging.info("END")
    
