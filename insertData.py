import openpyxl
from pathlib import Path
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

client_id = 'LlZWfSJtcLDoUsgJtLyKAzHM'
client_secret = 'SppfwfDiLsA.A8YPAuFsR,8WjZUldLiy8X3+,0pXN4hXexqtNq0+SNPY0REnDq+QrpMXhvXqxiLSmONdOR4fE6U1WbZahsiN-GEnq5H8x_rNK307t4SyA_woCxKGps41'
token = 'AstraCS:LlZWfSJtcLDoUsgJtLyKAzHM:fd5171b9c2fbd86b51f969d7b91929545f687c77e69ed005adb89592f5474455'

cloud_config= {
        'secure_connect_bundle': './secure-connect-Zomato.zip'
}

KEYSPACE = "zomato"

auth_provider = PlainTextAuthProvider(client_id, client_secret)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

print("setting keyspace . . .")
session.set_keyspace(KEYSPACE)

print("creating table . . .")
session.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        id int PRIMARY KEY,
        url text,
        address text,
        name text,
        online_order boolean,
        book_table boolean,
        rate text,
        votes int,
        phone text,
        location text,
        rest_type text,
        dish_liked text,
        cuisines text,
        approx_cost_for_two_people int,
        reviews_list text,
        menu_item text,
        listed_in_type text,
        listed_in_city text
    )
""")
print("Table is created")


print("creating Prepared Statement for table . . .")
prepared = session.prepare("""
        INSERT INTO ratings (
            id, url, address, name, online_order, book_table, rate, votes, phone, location, rest_type, dish_liked,
            cuisines, approx_cost_for_two_people, reviews_list, menu_item, listed_in_type, listed_in_city)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
print("created Prepared Statement for table")

# Read Excel sheet
xlsx_file = Path('zomato.xlsx')
workbook = openpyxl.load_workbook(xlsx_file)

# Read the active sheet:
sheet = workbook.active

print("Inserting data into table . . .")
for index, rates in enumerate(sheet.iter_rows(values_only=True)):
    if index != 0:
        id = index
        url = rates[0]
        address = rates[1]
        name = rates[2]
        online_order = True if rates[3] > 'Yes' else False
        book_table = True if rates[4] > 'Yes' else False
        rate = rates[5]
        votes = rates[6]
        phone = rates[7] if rates[7] is None else rates[7].replace('_x000D_\n', ', ')
        
        location = rates[8]
        rest_type = rates[9]
        dish_liked = rates[10]
        cuisines = rates[11]
        approx_cost_for_two_people = rates[12] if rates[12] is None else int(rates[12].replace(',', ''))
        reviews_list = rates[13]
        menu_item = rates[14]
        listed_in_type = rates[15]
        listed_in_city = rates[16]

        session.execute(prepared, [id, url, address, name, online_order, book_table, rate, votes, phone, location, rest_type, dish_liked, cuisines, approx_cost_for_two_people, reviews_list, menu_item, listed_in_type, listed_in_city])

        if index % 100 == 0:
            print(index)

print("Inserted data into table")

#closing the file
workbook.close()

#closing Cassandra connection
session.shutdown()