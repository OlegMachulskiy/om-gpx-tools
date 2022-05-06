import gpxpy
import gpxpy.gpx
import urllib.request, json
from printree import ptree
import sqlite3
import random

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
        gpx_file = open(fileName, 'r', encoding='UTF-8')
        self.gpx = gpxpy.parse(gpx_file)
        self.connect = sqlite3.connect("geodata-cache.sqlite")
        try:
            self.connect.execute("""
                    CREATE TABLE GEODATA (
                        lat number(3, 8) ,
                        lon number(3, 8) ,
                        geodata_json TEXT
                    );
                """)
        except Exception as ex:
            print("possibly table GEODATA  already exists ", ex)
            dataFromSql = self.connect.execute("SELECT * FROM GEODATA");
            for row in dataFromSql.fetchall():
                print(row)

    def listPoints(self):
        res = {}
        lPrevoius = 0
        for track in self.gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
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
                        print("да и похуй...", theException, point)

        return res

    def getPointMetadata(self, lat, lon, time):
        dataFromSql = self.connect.execute("SELECT geodata_json FROM GEODATA WHERE lat=? and lon=?", ([lat, lon]))
        fetchall = dataFromSql.fetchall()
        if len(fetchall)>0:
            strFromCache = fetchall[0][0]
            return self.parsePointMetadataFromString(strFromCache, time)

        if random.randint(0, 1000) < 995:
            return PointMetadata('display_name', {}, time)

        nominatimUrl = "https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={}&lon={}".format(lat, lon)
        with urllib.request.urlopen(nominatimUrl) as url:
            strFromServer = url.read().decode()
            print("fetch from {}: {}".format(nominatimUrl, strFromServer))
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
    gpx = OMGPXTools("yar-kineshma-1.gpx")
    finalResult = gpx.listPoints()
    l = list(x.key() for x in finalResult.values())
    dict = gpx.generatePrintableTree(l)
    print(ptree(dict))
    #print(json.dumps(finalResult, default=vars))
