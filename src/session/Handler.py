# coding=utf-8
from threading import Thread
import pika
from RecvHandler import Recv_Handler
from SendHandler import Send_Handler

import Configurate as cnf

class Handler(Thread):

    # классы
    recv_handler = None
    send_handler = None
    connect       = None
    app           = None
    type_         = None
    index_handler = None
    # соединение
    socket = None


    def __init__(self, class_connect, socket, type_, index_handler):
        Thread.__init__(self)
        self.connect = class_connect
        self.socket = socket
        self.type_ = type_
        self.index_handler = index_handler
        self.recv_handler = Recv_Handler(self)
        self.send_handler = Send_Handler(self)


    def run(self):
        self.send_handler.start()
        self.recv_handler.start()
        self.recv_handler.join()
        return