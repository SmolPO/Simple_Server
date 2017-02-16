# coding=utf-8

import socket
from collections import namedtuple
from itertools import count

import Configurate as cnf
from configurate import Commands as CMD

def authentication(sock):
    if not sock.send(cnf.password):
         return False
    data = sock.recv(cnf.BUFFER_SIZE)
    if not data:

        return False

    return True


def main():
    TCP_IP = cnf.server_address
    TCP_PORT = cnf.PORT
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((TCP_IP, TCP_PORT))



    cmd = CMD.OFF_LIGHT
    sender = 1
    date = 1
    size_next  = 0

    msg = cnf.ntuple_date_message(0, cmd, sender, date, size_next)

    # аутотификация
    if not authentication(sock):
        return False

    mess = cnf.to_bytes_from_date_message(msg)

    if not sock.send(mess):
        return

    while 1:
        cmd_ = input("Продолжить?\n")
        if cmd_ == 0:
            return
        id_ = msg.id + 1
        msg = cnf.ntuple_date_message(id_, msg.cmd, msg.sender, msg.date, msg.size_next)
        mess = cnf.to_bytes_from_date_message(msg)

        sock.send(mess)
        tmp = sock.recv(cnf.SIZE_HEADER) or None
        if not tmp:
            sock.close()
            return

        answer = sock.recv(cnf.SIZE_HEADER)
        if not answer:
            sock.close()
            return

if __name__ == '__main__':
    main()