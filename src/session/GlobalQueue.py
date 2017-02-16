# coding=utf-8
# интерфейс очереди

from itertools import count
import logging
import Configurate as cnf
from Configurate import is_clients_cmd, is_pps_cmd
import Commands as CMD
import pika
from threading import Thread

from DataBase import Data_Base as DB

id_queue = count()

def __log__():

    #DB().mess_to_log(mess) # для тестирования
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=cnf.log_queue_name)
    exchange = ''
    while True:
        msg = yield
        if not msg:
            continue

        channel.basic_publish(
            exchange='',  # точка обмена
            routing_key=cnf.log_queue_name,  # имя очереди
            body=msg
        )
    return

def to_queue(point=None, flag=False):
    """
    добавляет в очередь сообщение в байтах
    :param packet:
    :return:
    """

    print("to queue...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=cnf.queue_name)
    exchange = ''

    while True:
        packet = yield
        if not flag:
            packet = cnf.to_bytes_from_data_message(packet)

        if point:
            exchange = point

        channel.basic_publish(
            exchange=exchange,  # точка обмена
            routing_key=cnf.queue_name,  # имя очереди
            body=packet
        )
        print("to queue is ok")
    return


def init_queue(name=cnf.queue_name, address=cnf.queue_addr):

    connect = pika.BlockingConnection(pika.ConnectionParameters(address))
    channel = connect.channel()

    queue_ = cnf.ntuple_queue(
        name,
        address,
        connect,
        channel,
        pika.BasicProperties(delivery_mode=2)
    )

    queue_.chanl.queue_declare(queue=name)
    print("Connect to queue...", (name, address))
    return queue_


def init_rabbitmq(address=cnf.queue_addr):
    conn = pika.BlockingConnection(pika.ConnectionParameters(address))
    chanl = conn.channel()

    chanl.exchange_declare(exchange='clients', type='direct')
    chanl.exchange_declare(exchange='pp', type='direct')

    chanl.queue_declare(queue="client_0")
    chanl.queue_declare(queue=cnf.queue_name)
    chanl.queue_declare(queue=cnf.log_queue_name)
    chanl.queue_bind("client_0", "clients")

    return conn, chanl


class Server_Thread(Thread):
    """
    подключится к глобальной очереди
    получать из нее сообщения
    анализировать их
        передать в точку доступа клиентов
        передать в точку достпуа ПП
    """
    my_key   = cnf.key_send
    global_queue = None
    log_queue = None

    def __init__(self):
        Thread.__init__(self)
        # подключится к очереди
        # создать две точки обмена

        # создать точки обмена
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=cnf.queue_name)
        self.channel.queue_declare(queue=cnf.log_queue_name)
        self.channel.queue_declare(queue='client_0')

        self.channel.exchange_declare(exchange='clients', type='direct')
        self.channel.exchange_declare(exchange='pp'     , type='direct')

        self.channel.queue_bind("client_0", "clients")

    def run(self):
        self.channel.basic_consume(
            self._callback,
            queue=cnf.queue_name,
            no_ack=True
        )

        self.channel.start_consuming()
        pass


    # отправка в очередь для ПП
    def _to_queue_pp(self, mess, rout_key):
        """
        возможно нужны какие-то преобразования сообщения
        :param mess: [<байты>]
        :return: true or false
        """
        print ("to queue pp")
        self.channel.basic_publish(
            exchange='PPost',  # точка обмена
            routing_key=rout_key,  # имя очереди
            body=mess
        )
        return

    # отправка в очередь для клиентов
    def _to_queue_clients(self, mess, rout_key):
        print ("to queue client")
        self.channel.basic_publish(
            exchange='clients',  # точка обмена
            routing_key=rout_key,  # имя очереди
            body=mess
        )
        pass

    # получение из соощений данных
    def _get_cmd(self, mess):
        message = cnf.from_bytes_get_data_message(mess)
        print ("return cmd = " + str(message.cmd))
        return message.cmd

    def _get_rout_key(self, mess):
        """
        узнать получателя сообщения и вернуть имя его очереди
        :param mess:
        :return: rout_key из сообщения mess
        """
        return 'client_0'

    # не используется
    def _is_private_message(self, mess):
        return False

        # if str(mess).find('cl'):
        #     # пришло имя очереди от клиента
        #     # TODO подключить к точке обмена очередь
        #     pass
        # if str(mess).find('pp'):
        #     # пришло имя очереди от клиента
        #     # TODO подключить к точке обмена очередь
        #     pass
        # to_queue(mess, self.global_queue.chanl, "clients", True)
        # return True


    def _callback(self, ch, method, properties, body):
        """
        анализ сообщения
        :param message: сообщение в кодировке
        :return:

        """
        print ("Global queue callback")
        if self._is_private_message(body):
            return

        cmd, rout_key = self._get_cmd(body), self._get_rout_key(body)

        if is_clients_cmd(cmd):
            print ("add to queue client")
            self._to_queue_clients(body, rout_key)
            self._basic_ask()
            return
        elif is_pps_cmd(cmd):
            print ("add to queue pp")
            self._to_queue_pp(body, rout_key)
            self._basic_ask()
            return

       # DB().mess_to_log("fail server thread....")
        return

    def _basic_ask(self):
        print ("message is receive....")
        pass

    def close_session(self):
        # TODO остановить свой поток
        self.channel.queue_delete(queue=cnf.queue_name)
        print ("global queue is remove..")

        pass

if __name__ == '__main__':
    init_rabbitmq()