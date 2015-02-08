import time
import csv

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from cassandra.cluster import Cluster

class kafkaProducer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

    def createUserData(self, date, time,playername, points):
        if playername:
            stringRow = "%s,%s,%s,%s" % (date,time,playername,points)
            self.producer.send_messages("playplay", stringRow)

    def readAndStream(self):

        with open('data/QBData.csv', "rU") as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in filereader:
                self.createUserData(row[0],row[1],row[2],row[3])
                time.sleep(1)
        filereader.close()

playProducer = kafkaProducer("localhost:9092")
playProducer.readAndStream()



