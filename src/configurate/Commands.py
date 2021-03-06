# coding=utf-8

ON_LIGHT   = 1
OFF_LIGHT  = 2
NEW_CLIENT = 4
NEW_PP     = 5
GET_SELF_ID = 6 # ответ сервера на подключение. Получение ID

DISCONNECT = 3

step_comands = 100

class Commands:
    """
    команды
    0   - 99  - клиент
        0  - 30  - база данных
        31 - 50  - управление светом
        51 - 101 - прочие
    101 - 199 - сервер
    201 - 299 - ПП
    301 - 399 - СВ
    400 - лог, текстовое сообщение на сервер

    """

    class SrvCmd:
        index_commands = 1
        DISCONN_SV  = 101
        NEW_PP      = 102
        CMD_FROM_PP = 103
        CMD_FROM_SV = 104
        NEW_MAP     = 105
        TAKE_DB     = 106
        NOT_ANSWER  = 107

    class ClnCmd:
        index_commands = 0
     # data
        GET_DB      = 1
        GET_MAP     = 2
    # controlLight
        ON_LIGHT    = 11
        OF_LIGTH    = 12
        WINK        = 13
    # with analis
        SET_NEW_MAP = 21
        CONNECT_NEW_CLIENT = 22

    class PPCmd:
        index_commands = 2
        CONNECT_NEW_PP  = 201
        DIS_CONN_SV = 202
        PROBLEM_SV  = 203

    class SVCmd:
        index_commands = 3
        ON_LIGHT    = 301
        OF_LIGTH    = 302
        PROBLEM     = 303

    class LOG:
        log = 400

def get_text_message_from_cmd(cmd):
    """
    получить текствое сообщение ипо номеру команды
    :param cmd:
    :return:
    """
    if cmd == ON_LIGHT:
        return u"включени"
    elif cmd == OFF_LIGHT:
        return u"выключени"
    elif cmd == DISCONNECT:
        return u"разрыв соединения"

