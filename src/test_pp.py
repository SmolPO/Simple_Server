# coding=utf-8

import socket
from collections import namedtuple
from itertools import count
BUFFER_SIZE = 30

import configurate.Configurate as cnf
from configurate import Commands as CMD

def main():
    TCP_IP = cnf.server_address
    TCP_PORT = cnf.PORT
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((TCP_IP, TCP_PORT))
    cmd = CMD.NEW_PP
    sender = 0
    receiver = 100 # id сервера
    data = 0
    size_next = 0

    msg = cnf.ntuple_data_message(0, cmd, sender, receiver, size_next, data)
    mess = cnf.to_bytes_from_data_message(msg)

    if not sock.send(mess):
        return
    answer = sock.recv(cnf.SIZE_HEADER)

    if not answer:
        print ("нет ответа")
        return
    message = cnf.to_data_message_from_bytes_(answer)
    self_id = message.data

    while 1:
        receiver = input("Продолжить, получатель?\n")
        id_ = msg.id + 1
        msg = cnf.ntuple_data_message(id_, CMD.ON_LIGHT, self_id, receiver, msg.size_next, msg.data)
        mess = cnf.to_bytes_from_data_message(msg)

        sock.send(mess)
        tmp = sock.recv(cnf.SIZE_HEADER) or None
        if not tmp:
            sock.close()
            return

        answer = sock.recv(24)
        if not answer:
            sock.close()
            return
        answer_ = cnf.to_data_message_from_bytes_(answer)
        print ("received ->" + str(answer_))

if __name__ == '__main__':
    main()