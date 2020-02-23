from confluent_kafka import Producer
import socket
import argparse
import sys


def produce_messages(brokers,topic,key_value):
	conf = {'bootstrap.servers': brokers,'client.id': socket.gethostname()}
	producer = Producer(conf)
	producer.produce(topic, key='key', value="value")
	producer.flush()
	print('Producing...')


def main(args):
	produce_messages(args.brokers,args.topic,args.key)
	print('OK')


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-b", "--bokers_list", required=True, action='append', dest="brokers")
	parser.add_argument("-t", "--topic", dest="topic", required=True)
	parser.add_argument("-k", "--key", dest="key", default="test")
	parser.add_argument("-i", "--docker_replicas",dest="docker_replicas", default=1)
	arguments = parser.parse_args()
	sys.exit(main(arguments))

