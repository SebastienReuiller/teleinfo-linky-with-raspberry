#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# __author__ = "Sébastien Reuiller"
# __licence__ = "Apache License 2.0"
"""Send teleinfo to influxdb."""

# Python 3, prerequis : pip install pySerial influxdb
#
# Exemple de trame:
# {
#  'OPTARIF': 'HC..',        # option tarifaire
#  'IMAX': '007',            # intensité max
#  'HCHC': '040177099',      # index heure creuse en Wh
#  'IINST': '005',           # Intensité instantanée en A
#  'PAPP': '01289',          # puissance Apparente, en VA
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
import requests
import serial
from influxdb import InfluxDBClient

# clés téléinfo
INT_MEASURE_KEYS = ['IMAX', 'HCHC', 'IINST', 'PAPP', 'ISOUSC', 'ADCO', 'HCHP']

# création du logguer
logging.basicConfig(filename='/var/log/teleinfo/releve.log',
                    level=logging.INFO, format='%(asctime)s %(message)s')
logging.info("Teleinfo starting..")

# connexion a la base de données InfluxDB
CLIENT = InfluxDBClient('localhost', 8086)
DB = "teleinfo"
CONNECTED = False
while not CONNECTED:
    try:
        logging.info("Database %s exists?", DB)
        if {'name': DB} not in CLIENT.get_list_database():
            logging.info("Database %s creation..", DB)
            CLIENT.create_database(DB)
            logging.info("Database %s created!", DB)
        CLIENT.switch_database(DB)
        logging.info("Connected to %s!", DB)
    except requests.exceptions.ConnectionError:
        logging.info('InfluxDB is not reachable. Waiting 5 seconds to retry.')
        time.sleep(5)
    else:
        CONNECTED = True


def add_measures(measures):
    """Add measures to array."""
    points = []
    for measure, value in measures.items():
        point = {
            "measurement": measure,
            "tags": {
                "host": "raspberry",
                "region": "linky"
            },
            "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fields": {
                "value": value
                }
            }
        points.append(point)

    CLIENT.write_points(points)


def main():
    """Main function to read teleinfo."""
    with serial.Serial(port='/dev/ttyS0', baudrate=1200, parity=serial.PARITY_NONE,
                       stopbits=serial.STOPBITS_ONE,
                       bytesize=serial.SEVENBITS, timeout=1) as ser:

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
            ar_split = line_str.split(" ")
            try:
                key = ar_split[0]
                if key in INT_MEASURE_KEYS:
                    value = int(ar_split[1])
                else:
                    value = ar_split[1]

                trame[key] = value
                if b'\x03' in line:  # si caractère de fin dans la ligne,
                                     # on insère la trame dans influx
                    del trame['ADCO']  # adresse du compteur : confidentiel!
                    time_measure = time.time()

                    # insertion dans influxdb
                    add_measures(trame)

                    # ajout timestamp pour debugger
                    trame["timestamp"] = int(time_measure)
                    logging.debug(trame)

                    trame = dict()  # on repart sur une nouvelle trame
            except Exception as error:
                logging.error("Exception : %s", error)
            line = ser.readline()


if __name__ == '__main__':
    if CONNECTED:
        main()
