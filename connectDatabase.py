from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

client_id = 'LlZWfSJtcLDoUsgJtLyKAzHM'
client_secret = 'SppfwfDiLsA.A8YPAuFsR,8WjZUldLiy8X3+,0pXN4hXexqtNq0+SNPY0REnDq+QrpMXhvXqxiLSmONdOR4fE6U1WbZahsiN-GEnq5H8x_rNK307t4SyA_woCxKGps41'
token = 'AstraCS:LlZWfSJtcLDoUsgJtLyKAzHM:fd5171b9c2fbd86b51f969d7b91929545f687c77e69ed005adb89592f5474455'

cloud_config= {
        'secure_connect_bundle': './secure-connect-Zomato.zip'
}

auth_provider = PlainTextAuthProvider(client_id, client_secret)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select * from zomato.ratings").one()
# row = session.execute("select release_version from system.local").one()
if row:
    print(row)
else:
    print("An error occurred.")
    # print(row)

