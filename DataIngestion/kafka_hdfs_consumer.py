import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os


class Consumer(object):

    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic, max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.topic = topic
        self.group = group
        self.block_cnt = 0


    def consume_topic(self, output_dir):

        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        #open file for writing
        self.temp_file_path = "/home/ubuntu/FantasyFootball/ingestion/kafka_%s_%s_%s.dat" % (self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path,"w")
        one_entry = False 

        while True:
            try:
                messages = self.consumer.get_messages(count=100, block=False)

                #OffsetAndMessage(offset=43, message=Message(magic=0,
                # attributes=0, key=None, value='some message'))
                for message in messages:
                    one_entry = True
                    self.tempfile.write(message.message.value + "\n")

                if self.tempfile.tell() > 2000:
                    self.save_to_hdfs(output_dir)

                self.consumer.commit()
            except:
                self.consumer.seek(0, 2)

        if one_entry:
            self.save_to_hdfs(output_dir, self.topic)

        self.consumer.commit()

    def save_to_hdfs(self, output_dir):
        self.tempfile.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')

        hadoop_path = "/user/solivero/playerpoints/history/%s_%s_%s.dat" % (self.group, self.topic, timestamp)
        cached_path = "/user/solivero/playerpoints/cached/%s_%s_%s.dat" % (self.group, self.topic, timestamp)
        print "Block " + str(self.block_cnt) + ": Saving file to HDFS " + hadoop_path
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,hadoop_path))
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,cached_path))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "/home/ubuntu/fantasyfootball/ingestion/kafka_%s_%s_%s.dat" % (self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':
    group = "hdfs"
    output = "/data"
    topic = "hdfs"
    
    print "\nConsuming topic: [%s] into HDFS" % topic
    cons = Consumer(addr="localhost:9092", group="hdfs", topic="playplay")
    cons.consume_topic("user/fantasyfootball")