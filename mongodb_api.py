# -*- coding: utf-8 -*-
"""Classes responsavel conexao, e efetuar processos basicos.

Processos basicos de select, insert, delete e update.
"""

from pymongo import MongoClient
import copy
import subprocess

MSG_ERRO = "%s invalido(a)!"
HOST_INVALID = MSG_ERRO % ('Host')
PORT_INVALID = MSG_ERRO % ('Porta')
USER_INVALID = MSG_ERRO % ('Usuario')
PASS_INVALID = MSG_ERRO % ('Senha')
MSG_CONNECTION_BD = 'Nao foi possivel conectar no Banco. \n Msg Orig: %s '
ERROR_DATABASE_EXIST = "Esse database '%s' ja existe."
ERROR_DATABASE_NOT_FIND = "Esse banco '%s' nao foi encontrado!"
ERROR_COLLECTION_NOT_FIND = "Collection '%s' nao foi encontrada!"
ERROR_COLLECTION_EXIST = "Essa collection '%s' ja existe."
ERROR_DATABASE_EXIST = "Esse database '%s' ja existe."
ERRO_NOT_CREATE_COLLECTION = 'Nao foi possivel criar essa essa colection. Msg Orig: %s'
ERRO_NOT_DROP_COLLECTION = 'Nao foi possivel apagar essa collection %s'


MONGO_CMD = """['mongo', '--host', '%s', '--quiet', '--eval' ]"""

QUERY_CMD_DEFAULT = 'db.%s'
# Essa constante recebe a query log em seguida o Banco de dados
QUERY_CMD_FIND = QUERY_CMD_DEFAULT + '.find(%s, %s).toArray()'

REPLACE_MONGO = {'ISODate("': "'",
                 'ObjectId("': "'",
                 '")': "'",
                 'false': 'False',
                 'true': 'True'}
CMD_LIST_DB_MONGO = QUERY_CMD_DEFAULT % ('adminCommand({listDatabases: 1})')
CMD_LIST_COLLECTION = QUERY_CMD_DEFAULT % 'getCollectionNames()'
CMD_CREATE_COLLECTION = QUERY_CMD_DEFAULT % ("createCollection('%s', %s)")
CMD_GET_NAME_DB = QUERY_CMD_DEFAULT % ('getName()')
CMD_DROP_DATABASE = QUERY_CMD_DEFAULT % ('dropDatabase()')
CMD_RENAME_COLLECTION = QUERY_CMD_DEFAULT + '.renameCollection("%s")'
CMD_COPY_DATABASE = QUERY_CMD_DEFAULT % ('copyDatabase("%s", "%s")')
COUNT_COLLECTION = QUERY_CMD_DEFAULT + '.count()'
DROP_COLLECTION = QUERY_CMD_DEFAULT + '.drop()'
CONNECTION_DB = "mongodb://%s:%s"


def validate_field_null(p_field, p_msg_erro):
    """"Funcao para validar os campos que nao pode ser nulo ou vazio."""
    if not p_field:
        raise Exception(p_msg_erro)


def execute_cmd_subprocess(p_cmd):
    """Metodo para executar um comando."""
    result = subprocess.Popen(p_cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE).communicate()
    if result[1]:
        raise Exception(result[1])
    return result[0]


class MongoAPI(object):
    """Classe responsavel por conectar e executar query."""

    def __init__(self, p_host, p_port, p_user, p_pass):
        """Metodo construtor."""
        self.__client_mongo = None
        self.__connect(p_host, p_port, p_user, p_pass)

    def __connect(self, p_host, p_port, p_user, p_pass):
        """Metodo para conectar no banco de dados do mongo."""
        try:
            validate_field_null(p_host, HOST_INVALID)
            validate_field_null(p_port, PORT_INVALID)
            validate_field_null(p_user, USER_INVALID)
            validate_field_null(p_pass, PASS_INVALID)
            # Ainda nao foi definido a senha
            # Quando o devel for altera a senha do mongo
            # devera mudar essa linha de conexao
            coonnection = CONNECTION_DB % (p_host, p_port)
            self.__client_mongo = MongoClient(coonnection)
            # self.__client_mongo.server_info()
        except Exception, e:
            raise Exception(MSG_CONNECTION_BD % str(e))

    def __validate_query(self, p_query):
        """Metodo para validar a query.

        p_query pode ser um dict, lista, ou tupla
        repeitando o primeiro valor query do mongo e a
        segundo parametro os campos para selecionar.
        """
        show_fields = None
        query = p_query

        if isinstance(p_query, (list, tuple)):
            if len(p_query) > 1:
                query = p_query[0]
                show_fields = p_query[1]
            else:
                query = p_query[0]
                show_fields = None

        return query, show_fields

    def __check_exist_field_list(self, p_value, p_list):
        """Metodo para verificar se o valor existe na funcao.

        Para o funcionamento correto desse metodo  devera passar
        uma lista simples
        """
        response = False
        for element in p_list:
            if element == p_value:
                response = True
                break

        return response

    def __check_collections(self, p_database, p_collection):
        """Metodo para verificar se as collections existe."""
        collections = self.show_collections(p_database)

        return self.__check_exist_field_list(p_collection, collections)

    def __validade_database(self, p_database):
        """Metodo para validar o database."""
        if not self.check_database(p_database):
            raise Exception(ERROR_DATABASE_NOT_FIND % p_database)

    def __validade_collections(self, p_database, p_collection):
        """"Metodo para valida as collections."""
        if not self.__check_collections(p_database, p_collection):
            raise Exception(ERROR_COLLECTION_NOT_FIND % p_collection)

    def check_database(self, p_database):
        """Metodo para verificar se existe o banco de dados."""
        databases = self.show_databases()
        return self.__check_exist_field_list(p_database, databases)

    def show_databases(self):
        """Metodo para mostrar todos os bancos criados."""
        return self.__client_mongo.database_names()

    def show_collections(self, p_database):
        """Metodo para mostrar todas collections criadas."""
        self.__validade_database(p_database)
        database = self.__client_mongo[p_database]
        return database.collection_names()

    def create_collection(self, p_database, p_name_collection):
        """Metodo para criar uma nova colecao."""
        if self.__check_collections(p_database, p_name_collection):
            raise Exception(ERROR_COLLECTION_EXIST % p_name_collection)

        database = self.__client_mongo[p_database]
        database.create_collection(p_name_collection)

    def delete_database(self, p_database):
        """"Metodo para apagar um database."""
        self.__validade_database(p_database)
        self.__client_mongo.drop_database(p_database)

    def delete_collection(self, p_database, p_collection):
        """Metodo para apagar uma collection."""
        self.__validade_collections(p_database, p_collection)

        database = self.__client_mongo[p_database]
        database.drop_collection(p_collection)

    def create_database(self, p_name_database, p_name_collection, p_value):
        """Metodo para criar um novo database.

        Devera passar o nome do banco, a collection e o valor
        em JSON
        """
        if self.check_database(p_name_database):
            raise Exception(ERROR_DATABASE_EXIST % p_name_database)

        database = self.__client_mongo[p_name_database]

        coll = database[p_name_collection]
        coll.insert(p_value)

    def query_mongo(self, p_database, p_collection, p_query):
        """Metodo para fazer consultas no banco de dados do mongo."""
        self.__validade_collections(p_database, p_collection)
        query, show_fields = self.__validate_query(p_query)

        collection = self.__client_mongo[p_database]
        database = collection[p_collection]

        list_response_query = []
        for result in database.find(query, show_fields):
            list_response_query.append(result)

        return list_response_query

    def rename_collection(self, p_name_database, p_name_collection_old,
                          p_new_collection_name):
        """Metodo para atualizar o nome da collection."""
        self.__validade_collections(p_name_database, p_name_collection_old)
        database = self.__client_mongo[p_name_database]
        collection = database[p_name_collection_old]
        collection.rename(p_new_collection_name)

    def rename_database(self, p_old_name_database, p_new_name_database):
        """Metodo para atualizar o name do database."""
        self.__validade_database(p_old_name_database)

        if self.check_database(p_new_name_database):
            raise Exception(ERROR_DATABASE_EXIST % p_new_name_database)

        self.__client_mongo.admin.command("copydb",
                                          fromdb=p_old_name_database,
                                          todb=p_new_name_database)

        self.delete_database(p_old_name_database)

    def close_connection(self):
        """Metodo para fechar a conexao com o mongo."""
        self.__client_mongo.close()


class MongoCMDAPI(object):
    """Classe responsavel para acessar o mongo via CMD."""

    def __init__(self, p_host, p_port, p_user, p_password):
        """Metodo construtor."""
        self.__set_host(p_host)
        self.__set_port(p_port)
        self.__set_user(p_user)
        self.__set_password(p_password)
        self.__cmd_default = eval(MONGO_CMD % (p_host))

    def __set_host(self, p_value):
        """Metodo validar o ip do Host."""
        validate_field_null(p_value, HOST_INVALID)
        self.__host = p_value

    def __set_port(self, p_value):
        """Metodo validar a porta."""
        validate_field_null(p_value, PORT_INVALID)
        self.__port = p_value

    def __set_user(self, p_value):
        """Metodo validar a Usuario."""
        validate_field_null(p_value, USER_INVALID)
        self.__user = p_value

    def __set_password(self, p_value):
        """Metodo validar a senha do usuario."""
        validate_field_null(p_value, PASS_INVALID)
        self.__password = p_value

    def __get_host(self, p_value):
        """Metodo validar o ip do Host."""
        return self.__host

    def __get_port(self, p_value):
        """Metodo validar a porta."""
        return self.__port

    def __get_user(self, p_value):
        """Metodo validar a Usuario."""
        return self.__user

    def __get_password(self, p_value):
        """Metodo validar a senha do usuario."""
        return self.__password

    def __parser_json(self, p_result):
        """Metodo parsear o Json."""
        for key in REPLACE_MONGO:
            p_result = p_result.replace(key, REPLACE_MONGO[key])
        return eval(p_result)

    def __validate_result_query(self, p_result):
        """Metodo para validar o resultado da query."""
        if p_result['ok'] != 1:
            msg = p_result['errmsg']
            raise Exception(ERRO_NOT_CREATE_COLLECTION % msg)

    def __validate_query(self, p_query):
        """Metodo para validar a query.

        p_query pode ser um dict, lista, ou tupla
        repeitando o primeiro valor query do mongo e a
        segundo parametro os campos para selecionar.
        """
        show_fields = None
        query = p_query

        if isinstance(p_query, (list, tuple)):
            if len(p_query) > 1:
                query = p_query[0]
                show_fields = p_query[1]
            else:
                query = p_query[0]
                show_fields = None

        return query, show_fields

    def __return_conn_mongo(self):
        """Metodo retornar a copia dos comandos default executarn no mongo."""
        return copy.copy(self.__cmd_default)

    def __execute_query(self, p_cmd, p_database=None, p_parser=True):
        """Metodo para executar as query e retorno."""
        cmd_default = self.__return_conn_mongo()
        cmd_default.append(p_cmd)
        if p_database:
            cmd_default.append(p_database)
        if p_parser:
            return self.__parser_json(execute_cmd_subprocess(cmd_default))

        return execute_cmd_subprocess(cmd_default)

    def show_databases(self):
        """Metodo mostrar todos os Databases."""
        return self.__execute_query(CMD_LIST_DB_MONGO)

    def show_collections(self, p_database):
        """Metodo para retornar todas as collections do banco."""
        return self.__execute_query(CMD_LIST_COLLECTION, p_database)

    def create_collection(self, p_database, p_name_collection):
        """"Metodo para criar uma nova collection."""
        cmd_create = CMD_CREATE_COLLECTION % (p_name_collection, {})
        result = self.__execute_query(cmd_create, p_database)
        self.__validate_result_query(result)

    def create_database(self, p_database, p_collection):
        """Metodo para criar um novo database.

        Devera passar o nome do banco e uma collection:
        p_database -> Banco de dados
        p_colection -> collection
        """
        self.create_collection(p_database, p_collection)

    def delete_collection(self, p_database, p_collection):
        """Metodo para apagar uma collection."""
        drop_colle = DROP_COLLECTION % (p_collection)
        if not self.__execute_query(drop_colle, p_database):
            raise Exception(ERRO_NOT_DROP_COLLECTION % p_collection)

    def delete_database(self, p_database):
        """"Metodo para apagar um database."""
        self.__execute_query(CMD_DROP_DATABASE, p_database)

    def count_collection(self, p_database, p_collection):
        """Metodo que traz a quantidade dos registros das collections."""
        query = COUNT_COLLECTION % p_collection
        return self.__execute_query(query, p_database)

    def query_mongo(self, p_database, p_collection, p_query):
        """Metodo realizar select no mongo."""
        query, show_fields = self.__validate_query(p_query)
        query = QUERY_CMD_FIND % (p_collection, query, show_fields)
        return self.__execute_query(query, p_database)

    def query_mongo_not_parser(self, p_database, p_collection, p_query):
        """Metodo para fazer consulta no mongo sem parser."""
        not_parser = False
        query = p_query % p_collection
        return self.__execute_query(query, p_database, not_parser)

    def rename_collection(self, p_name_database, p_name_collection_old,
                          p_new_collection_name):
        """Metodo para atualizar o nome da collection."""
        query = CMD_RENAME_COLLECTION % (p_name_collection_old,
                                         p_new_collection_name)
        result = self.__execute_query(query, p_name_database)
        self.__validate_result_query(result)

    def rename_database(self, p_old_name_database, p_new_name_database):
        """Metodo para atualizar o name do database."""
        query = CMD_COPY_DATABASE % (p_old_name_database, p_new_name_database)
        self.__execute_query(query)
        self.delete_database(p_old_name_database)

    host = property(__get_host, __set_host, None, None)
    port = property(__get_port, __set_port, None, None)
    user = property(__get_user, __set_user, None, None)
    password = property(__get_password, __set_password, None, None)
