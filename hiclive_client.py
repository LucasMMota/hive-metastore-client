__version__ = '0.2.0'

from src.hive_metastore import ThriftHiveMetastore
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

DEFAULT_HOST = 'HOST'
DEFAULT_PORT = 9083


def connect(host=DEFAULT_HOST, port=DEFAULT_PORT):
    transport = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = ThriftHiveMetastore.Client(protocol)
    transport.open()

    return client


client = connect()
r = client.get_schema('cool_database_lucas', 'cool_test_lucas_2')
print(r)
