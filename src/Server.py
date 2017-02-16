# coding=utf-8
from Application import Application


def Server():
    app_ = Application()
    app_.start()
    app_.join()

if __name__ == '__main__':
    Server()