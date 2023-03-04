import json
import re
import dicttoxml as dicttoxml
from lxml import etree
import pandas as pd
import sqlite3


def clean_cells(df):
    global c
    for column in df:
        for i in range(0, df.shape[0]):
            if re.search('[a-zA-Z]', str(df[column].values[i])):
                c = c + 1
            df[column].values[i] = ''.join(re.findall(r'\d+', str(df[column].values[i])))
    df.to_csv(csv_checked_name, index=False, header=True)


def clean_csv():
    data_frame = pd.DataFrame(pd.read_csv(csv_name))
    clean_cells(data_frame)
    if c == 1:
        print(f"1 cell was corrected in {csv_checked_name}")
    else:
        print(f"{c} cells were corrected in {csv_checked_name}")


def xlsx_to_csv():
    xlsx_file = pd.read_excel(file_name, sheet_name='Vehicles', dtype=str)
    xlsx_file.to_csv(csv_name, index=False, header=True)
    if xlsx_file.shape[0] == 1:
        print(f"1 line was added to {csv_name}")
    else:
        print(f"{xlsx_file.shape[0]} lines were added to {csv_name}")


def csv_to_s3db():
    df = pd.DataFrame(pd.read_csv(csv_checked_name))
    conn = sqlite3.connect(s3db_name)
    create_table_query = "CREATE TABLE IF NOT EXISTS convoy(vehicle_id INTEGER PRIMARY KEY UNIQUE, engine_capacity INTEGER NOT NULL , fuel_consumption INTEGER NOT NULL, maximum_load INTEGER NOT NULL);"
    insert_values_query = "INSERT OR IGNORE INTO convoy(vehicle_id, engine_capacity, fuel_consumption, maximum_load) VALUES (?, ?, ?, ?);"
    cursor_name = conn.cursor()
    cursor_name.execute(create_table_query)
    for row in df.itertuples():
        cursor_name.execute(insert_values_query,
                            (row.vehicle_id, row.engine_capacity, row.fuel_consumption, row.maximum_load))
    conn.commit()

    if df.shape[0] == 1:
        print(f"1 record was inserted into {s3db_name}")
    else:
        print(f"{df.shape[0]} records were inserted into {s3db_name}")


def s3db_to_json():
    conn = sqlite3.connect(s3db_name)
    df = pd.DataFrame(pd.read_sql_query("SELECT * FROM convoy", conn))
    convoy_dict = df.to_dict(orient='records')
    with open(json_name, 'w') as out_file:
        json.dump({'convoy': convoy_dict}, out_file)
    if df.shape[0] == 1:
        print(f"1 vehicle was saved into {json_name}.json")
    else:
        print(f"{df.shape[0]} vehicles were saved into {json_name}")


def s3db_to_xml():
    with open(json_name, 'r') as json_file:
        obj = json.load(json_file)
    xml = dicttoxml.dicttoxml(obj, attr_type=False, root='convoy', item_func=lambda x: 'vehicle')
    root = etree.fromstring(xml)
    tree = etree.ElementTree(root)
    tree.write(xml_name)
    if len(root) == 1:
        print(f'1 vehicle was saved into {xml_name}')
    else:
        print(f'{len(root)} vehicles were saved into {xml_name}')


c = 0
file_name = input("Input file name\n")
csv_name = file_name.split(".")[0].removesuffix("[CHECKED]") + ".csv"
csv_checked_name = file_name.split(".")[0].removesuffix("[CHECKED]") + "[CHECKED].csv"
s3db_name = file_name.split(".")[0].removesuffix("[CHECKED]") + ".s3db"
json_name = file_name.split(".")[0].removesuffix("[CHECKED]") + ".json"
xml_name = file_name.split(".")[0].removesuffix("[CHECKED]") + ".xml"

if file_name.endswith(".xlsx"):
    xlsx_to_csv()
    clean_csv()
    csv_to_s3db()
    s3db_to_json()
    s3db_to_xml()
elif file_name == csv_name:
    clean_csv()
    csv_to_s3db()
    s3db_to_json()
    s3db_to_xml()
elif file_name == csv_checked_name:
    csv_to_s3db()
    s3db_to_json()
    s3db_to_xml()
elif file_name == s3db_name:
    s3db_to_json()
    s3db_to_xml()
else:
    s3db_to_xml()
