# coding=utf-8
import pika
import Configurate as cnf

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue=cnf.log_queue_name)

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)

channel.basic_consume(callback,
                      queue=cnf.log_queue_name,
                      no_ack=True)

channel.start_consuming()