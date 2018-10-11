from py2neo import Graph

username = "neo4j"
password = "941114"
host = "localhost"
port = 7474
auth_URL = "http://{}:{}@{}:{}".format(username, password, host, port)
dbclient = Graph(auth_URL)