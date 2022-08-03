import json
import logging

import pandas as pd
import requests

logger = logging.getLogger()
formatter = logging.Formatter(
    "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
)
general_fh = logging.FileHandler("./logs.txt")
general_fh.setFormatter(formatter)
general_fh.setLevel("INFO")
logger.addHandler(general_fh)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
    'cookie': 'http_x_authentication=eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo4MTc3MjEyfQ.qVS7cmXl8UZNUH8VxkoA6z3mAIVhyzb0px5CDDY6BVc'}


def scrap_data(program_id):
    row = {}
    try:
        content = requests.get("https://netology.ru/backend/api/program_families/" + str(program_id),
                               headers=headers).content.decode()
        data = json.loads(content)
        row['program_family_price_type'] = data.get('price_type', None)
        row['program_family_main_direction_id'] = data.get('main_direction_id', None)
        current_program_urlcode = data.get('current_program_urlcode', None)
        if current_program_urlcode:
            content = requests.get("https://netology.ru/backend/api/user/programs/" + current_program_urlcode,
                                   headers=headers).content.decode()
            data = json.loads(content)
            row['current_program_starts_on'] = data.get('current_program_starts_on', None)
            row['available_program_starts_on'] = data.get('available_program_starts_on', None)
            row['program_type'] = data.get('program_type', None)
            program_id = data.get('program_id', None)

            if program_id and programs.get(program_id, None):
                program = programs.get(program_id, None)
                row['program_duration'] = program.get('duration', None)
                row['program_price_type'] = program.get('price_type', None)
                row['program_type'] = program.get('program_type', None)
                row['program_starting_soon'] = program.get('starting_soon', None)




    except Exception as e:
        logger.log(msg=e, level=logging.getLevelName("ERROR"))
        logger.log(msg=id, level=logging.getLevelName("WARN"))

    return row


def add_program_data(row):
    program_id = row["program_id"]
    if program_id in program_id_to_data.keys():
        data = program_id_to_data[program_id]
        for key, value in data.items():
            row[key] = value
    return row


# content = requests.get("https://netology.ru/backend/api/programs", headers=headers).content.decode()
# data = json.loads(content)
# programs = dict([(d['id'], d) for d in data])

df = pd.read_csv("../content/train_enriched.csv")

program_ids = df.program_id.unique()
program_id_to_data = {}

for program_id in program_ids:
    data = scrap_data(program_id)
    program_id_to_data[program_id] = data

df = df.apply(lambda row: add_program_data(row), axis=1)
df.to_csv("../content/train_enriched.csv", index=False)
