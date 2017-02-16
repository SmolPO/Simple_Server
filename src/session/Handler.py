# coding=utf-8
from threading import Thread
import pika
from RecvHandler import Recv_Handler
from SendHandler import Send_Handler

import Configurate as cnf

class Handler(Thread):

    # классы
    recv_heandler = None
    send_heandler = None
    connect       = None
    app           = None
    type_         = None
    # соединение
    socket = None


    def __init__(self, class_connect, socket, type_):
        Thread.__init__(self)
        self.connect = class_connect
        self.socket = socket
        self.type_ = type_

        self.recv_heandler = Recv_Handler(self)
        self.send_heandler = Send_Handler(self)


    def run(self):
        self.send_heandler.start()
        self.recv_heandler.start()

        self.recv_heandler.join()
