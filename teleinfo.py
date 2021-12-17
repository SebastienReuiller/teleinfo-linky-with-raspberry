#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Sébastien Reuiller"
# __licence__ = "Apache License 2.0"

# Python 3, prérequis : pip install -r requirements.txt
#
# Exemple de trame:
# {
#  'BASE': '123456789'       # Index heure de base en Wh
#  'OPTARIF': 'HC..',        # Option tarifaire HC/BASE
#  'IMAX': '007',            # Intensité max
#  'HCHC': '040177099',      # Index heure creuse en Wh
#  'IINST': '005',           # Intensité instantanée en A
#  'PAPP': '01289',          # Puissance Apparente, en VA
#  'MOTDETAT': '000000',     # Mot d'état du compteur
#  'HHPHC': 'A',             # Horaire Heures Pleines Heures Creuses
#  'ISOUSC': '45',           # Intensité souscrite en A
#  'ADCO': '000000000000',   # Adresse du compteur
#  'HCHP': '035972694',      # index heure pleine en Wh
#  'PTEC': 'HP..'            # Période tarifaire en cours
# }


import logging
import time
from datetime import datetime
from dataclasses import dataclass

import requests
import serial
from influxdb import InfluxDBClient

# création du logguer
logging.basicConfig(filename='/var/log/teleinfo/releve.log', level=logging.INFO, format='%(asctime)s %(message)s')
logging.info("Teleinfo starting..")

# clés téléinfo
INT_MESURE_KEYS = ['BASE', 'IMAX', 'HCHC', 'IINST', 'PAPP', 'ISOUSC', 'ADCO', 'HCHP']

# Nom de la base de données où seront stockées les mesures de télémétrie
MEASUREMENTS_DB_NAME = "teleinfo"


@dataclass
class SerialPortConfig:
    port: str
    baudrate: int
    parity: str
    stopbits: int
    bytesize: int
    timeout: int

linky_to_raspberry_serial_port_config = SerialPortConfig(
    port='/dev/ttyS0',
    baudrate=1200,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.SEVENBITS,
    timeout=1
)


class MeasurementDBClient:
    def __init__(self, influx_db_client: InfluxDBClient) -> None:
        self.influx_db_client = influx_db_client
        self.is_connected = False

    def connect(self) -> None:
        self.is_connected = False
        while not self.is_connected:
            try:
                logging.info("Database %s exists?" % MEASUREMENTS_DB_NAME)
                if not {'name': MEASUREMENTS_DB_NAME} in self.influx_db_client.get_list_database():
                    logging.info("Database %s creation.." % MEASUREMENTS_DB_NAME)
                    self.influx_db_client.create_database(MEASUREMENTS_DB_NAME)
                    logging.info("Database %s created!" % MEASUREMENTS_DB_NAME)
                self.influx_db_client.switch_database(MEASUREMENTS_DB_NAME)
                logging.info("Connected to %s!" % MEASUREMENTS_DB_NAME)
            except requests.exceptions.ConnectionError:
                logging.info('InfluxDB is not reachable. Waiting 5 seconds to retry.')
                time.sleep(5)
            else:
                self.is_connected = True

    def add_measures(self, measures: dict, execution_datetime: datetime) -> None:
        if not self.is_connected:
            self.connect()

        points = []
        for measure, value in measures.items():
            point = {
                "measurement": measure,
                "tags": {
                    # identification de la sonde et du compteur
                    "host": "raspberry",
                    "region": "linky"
                },
                "time": execution_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "fields": {
                    "value": value
                }
            }
            points.append(point)

        self.influx_db_client.write_points(points)


def verif_checksum(data: str, checksum: str) -> bool:
    data_unicode = 0
    for caractere in data:
        data_unicode += ord(caractere)
    sum_unicode = (data_unicode & 63) + 32
    return (checksum == chr(sum_unicode))


def main(measurement_db_client: MeasurementDBClient) -> None:
    with serial.Serial(linky_to_raspberry_serial_port_config.as_dict()) as ser:

        logging.info("Teleinfo is reading on /dev/ttyS0..")

        trame = dict()

        # boucle pour partir sur un début de trame
        line = ser.readline()
        while b'\x02' not in line:  # recherche du caractère de début de trame
            line = ser.readline()

        # lecture de la première ligne de la première trame
        line = ser.readline()

        while True:
            line_str = line.decode("utf-8")
            logging.debug(line)

            try:
                # separation sur espace /!\ attention le caractere de controle 0x32 est un espace aussi
                [key, val, *_] = line_str.split(" ")

                # supprimer les retours charriot et saut de ligne puis selectionne le caractere
                # de controle en partant de la fin
                checksum = (line_str.replace('\x03\x02', ''))[-3:-2]

                if verif_checksum(f"{key} {val}", checksum):
                    # creation du champ pour la trame en cours avec cast des valeurs de mesure en "integer"
                    trame[key] = int(val) if key in INT_MESURE_KEYS else val

                if b'\x03' in line:  # si caractère de fin dans la ligne, on insère la trame dans influx
                    del trame['ADCO']  # adresse du compteur : confidentiel!
                    execution_datetime = datetime.utcnow()

                    # insertion dans influxdb
                    measurement_db_client.add_measures(trame, execution_datetime)

                    # ajout timestamp pour debugger
                    trame["timestamp"] = int(execution_datetime.timestamp())
                    logging.debug(trame)

                    trame = dict()  # on repart sur une nouvelle trame
            except Exception as e:
                logging.error("Exception : %s" % e, exc_info=True)
                logging.error("%s %s" % (key, val))
            line = ser.readline()


if __name__ == '__main__':
    influx_db_client = InfluxDBClient(host='localhost', port=8086)
    measurement_db_client = MeasurementDBClient(influx_db_client)
    main(measurement_db_client)
