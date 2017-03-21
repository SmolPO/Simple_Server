# coding=utf-8
"""
подключение клиентов и устройств к серверу
"""
import socket

from threading import Thread
from itertools import count

import configurate.Configurate as cnf

from Handler import Handler

class Connect(Thread):
    app          = None
    sock         = None
    connect = None
    channel = None
    list_handlers = {} # список всех подключенных пользователей # ??? добавить maxlen, так как у нас не может быть больше MAX_CONNECT
    id_clients = None

    def __init__(self, app):
        Thread.__init__(self)
        self.app         = app

        # инициализация генераторов
        self.id_clients = count()
        self.id_PP = count()

    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.bind(('', cnf.PORT))
            self.sock.listen(cnf.MAX_CONNECT)
        except:
            print ("connect except...")
            return

        try:
             while 1:
                # Проверка подключения. Первый пакет должен содержать пароль и логин.
                # Только после этого создается класс Client и весь последующий интерфейс
                print ("Start...")
                print (str(self.list_handlers))
                conn, addr = self.sock.accept()
                print('Connection address:' + str(addr))
                self.create_client_handler(conn)
           #     if self.authentication_and_create_handler(conn):
           #          print ("Это наш клиент!!!")
           #      else:
           #          conn.close()
           #          print ("Плохой клиент!!!")
           #          continue
           #      print("check connect is good")
        finally:
            print ("sock.close....")
            self.sock.close()

    def authentication_and_create_handler(self, conn, next_id=None):
        # принимает один байт для подтверждения
        # отправляет следующий номер либо из генератора id_clients, либо из id_PP

        data = conn.recv(2)
        if not data:
            print("disconnect")
            return False

        if data == cnf.type_receivers['client']:  # заглушка, замена аутотификации на сервере. Не забыть изменить!!!
            print("Client!")
            next_id = self.id_clients.next()
            self.create_client_handler(conn)
            return True

        elif data == cnf.type_receivers['pp']:
            print('PP')
            next_id = self.id_PP.next()
            self.create_PP_handler(conn)
            return True
        elif data:
            print("пришло что-то странное.... ")
            print (data)
            return False

        conn.send(bytes(next_id))
        return True

    def send_list_handler(self, conn):
        c = count()
        next(c)
        cnt = len(self.list_handlers)
        for k in self.list_handlers:
            if next(c) < cnt:
                conn.send(1)
            conn.send(k)

    def create_client_handler(self, conn):
        index_handler = next(self.id_clients)
        key_ =  str(index_handler) + "_client"
        handler = Handler(self, conn, cnf.type_receivers['client'], index_handler)  # тип возвращается функцией аутотификации
        self.list_handlers[key_] = handler
        handler.start()

    def create_PP_handler(self, conn):
        index_handler = next(self.id_clients)
        key_ = str(index_handler) + "_pp"
        handler = Handler(self, conn, cnf.type_receivers['pp'], index_handler)  # тип возвращается функцией аутотификации
        self.list_handlers[index_handler] = handler
        handler.start()