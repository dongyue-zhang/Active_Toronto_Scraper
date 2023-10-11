import requests
import json
from datetime import datetime
import os
import io
import pandas as pd
import ast
from decouple import config
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
import mysql.connector as MySQL
import urllib.parse
import logging
import argparse

GOOGLE_API_KEY = config('GOOGLEAPIKEY')
HOST = config('HOST')
DBUSER = config('DBUSER')
PASSWORD = config('PASSWORD')
DATABASE = config('DATABASE')
GOOGLE_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json?address='
PROVINCE = 'Ontario'
RESOURCE_API = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=da46e4ac-d4ab-4b1c-b139-6362a0a43b3c'
FACILITY_LIST_URL = 'https://www.toronto.ca/data/parks/prd/facilities/recreationcentres/index.html'
CITY_OF_TORONTO_URL = 'https://www.toronto.ca'
FACILITY_URL_PREFIX = 'https://www.toronto.ca/data/parks/prd/facilities/complex/'
LOCATIONS = 'Locations'
DROPIN = 'Drop-in.json'
FACILITIES = 'Facilities.json'
REGISTERED_PROGRAMS = 'Registered Programs.json'


def getResources():
    global dropins, facilities, locations, registeredPrograms
    params = {'key': 'value'}

    logger.info('Requesting resources from City of Toronto OpenAPI: ' + RESOURCE_API)
    try:
        r = requests.get(url=RESOURCE_API, params=params)
        response = r.json()
    except (ConnextionError, Exception) as e:
        logger.warning(('Could not get resources from {}:'.format(RESOURCE_API)))
        logger.warning(e)

    try:
        resources = response['result']['resources']
        resources_dict = {}
        for resource in resources:
            name = resource['name']
            url = resource['url']

            if resource['name'] in [DROPIN, FACILITIES, REGISTERED_PROGRAMS]:
                logger.info('Getting source file: ' + resource['name'])
                content = requests.get(url=url, params=params).json()
                resources_dict[name] = content
            elif resource['name'] == LOCATIONS:
                logger.info('Getting source file: ' + resource['name'])
                csv = requests.get(url=url, params=params).content
                locations = pd.read_csv(io.StringIO(csv.decode('utf-8')), sep=',', header=0)
                # fill NaN values with ''
                locations = locations.fillna('')

        dropins = resources_dict[DROPIN]
        facilities = resources_dict[FACILITIES]
        registeredPrograms = resources_dict[REGISTERED_PROGRAMS]
    except (Exception) as e:
        logger.warning(e)


def getAvalibilities():
    logger.info('Extracting avalibilities from file: ' + DROPIN)
    try:
        avalibilities = []
        for dropin in dropins:
            avalibility = {}
            avalibility['location_id'] = dropin['Location ID']
            avalibility['course_title'] = dropin['Course Title']
            if ':' in avalibility['course_title']:
                type = avalibility['course_title'].split(':')[0].strip()
            elif '(' in avalibility['course_title']:
                type = avalibility['course_title'].split('(')[0].strip()
            elif '-' in avalibility['course_title']:
                type = avalibility['course_title'].split('-')[0].strip()
            else:
                type = avalibility['course_title']
            avalibility['type'] = type
            avalibility['age_min'] = dropin['Age Min']
            avalibility['age_max'] = dropin['Age Max']
            avalibility['start_time'] = dropin['Start Date Time']
            startDatetime = datetime.strptime(
                dropin['Start Date Time'], '%Y-%m-%dT%H:%M:%S')
            endHour = dropin['End Hour']
            endMin = dropin['End Min']
            endDatetime = startDatetime.replace(hour=endHour, minute=endMin)
            endDatetimeStr = endDatetime.strftime('%Y-%m-%dT%H:%M:%S')
            avalibility['end_time'] = endDatetimeStr
            avalibility['category'] = dropin['Category']
            avalibilities.append(avalibility)
        avalibilities = sorted(avalibilities, key=lambda x: (x['category'], x['type'], x['course_title'], x['location_id']))
        return avalibilities
    except (Exception) as e:
        logger.warning(e)


def getOriginalFacilities(availablities):
    logger.info('Extracting facilities original data from file: ' + LOCATIONS)
    try:
        locationList = locations.filter(
            items=['Location ID', 'Location Name', 'District', 'Street No', 'Street No Suffix', 'Street Name', 'Street Type', 'Postal Code']).values.tolist()

        locationIDs = set()
        facilitiesNoGeo = []

        for availablity in availablities:
            locationID = availablity['location_id']
            locationIDs.add(locationID)

        for locationID in locationIDs:
            for locat in locationList:
                if locationID == locat[0]:
                    street = str(locat[3]) + str(locat[4]) + ' ' + str(locat[5]) + ' ' + str(locat[6])
                    facilitiesNoGeo.append(
                        {'location_id': locat[0], 'facility_name': locat[1], 'city': locat[2], 'street': street, 'province': PROVINCE, 'postal_code': locat[7], 'phone': None, 'url': None})

        return facilitiesNoGeo
    except (Exception) as e:
        logger.warning(e)


def getGeoToFacilities(facilities):
    logger.info('Start getting coordinations for facilities...')
    try:
        for facility in facilities:
            logger.info('Getting latitude and longitude for facility: ' + facility['facility_name'])
            addressStr = facility['street'] + ' ' + facility['city'] + ' ' + facility['province']
            addressStr = addressStr.replace(' ', '%20')
            url = GOOGLE_API_URL + addressStr + '&key=' + GOOGLE_API_KEY
            params = {'key': 'value'}
            r = requests.get(url=url, params=params)
            response = r.json()
            geometry = response['results'][0]['geometry']['location']
            facility['lat'] = geometry['lat']
            facility['lng'] = geometry['lng']
            if facility['postal_code'] == '':
                facility['postal_code'] = response['results'][0]['address_components'][-1]['short_name']
        return facilities
    except (Exception) as e:
        logger.warning(e)


def getPhoneUrlToFacilities(facilities):
    logger.info('Start getting phone numbers and urls for facilities...')
    try:
        # get recreation list
        logger.info('Getting recreation list from {}'.format(FACILITY_LIST_URL))
        option = webdriver.ChromeOptions()
        service = Service()
        option.add_argument('headless')
        driver = webdriver.Chrome(service=service, options=option)
        driver.get(FACILITY_LIST_URL)
        time.sleep(1)
        html = driver.page_source.encode('utf-8')
        driver.quit()

        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('div', attrs={'class': 'pfrListing'})
        trs = table.table.tbody.find_all('tr')
        phoneList = []
        for tr in trs:
            a = tr.find('th', attrs={'data-info': 'Name'}).a
            name = a.text.strip()
            url = a.get('href')
            phone = tr.find('td', attrs={'data-info': 'Phone'}).text.strip()
            phoneList.append({'Name': name, 'phone': phone, 'url': CITY_OF_TORONTO_URL + url})

        # if a facility is not on the list, get phone number from its website
        for facility in facilities:
            logger.info('Getting phone number and urls for facility: ' + facility['facility_name'])
            for phone in phoneList:
                if facility['facility_name'] == phone['Name']:
                    facility['phone'] = phone['phone']
                    facility['url'] = phone['url']
                    logger.info('Got phone number for' + facility['facility_name'])
                    break

            if facility['phone'] is None:
                url = FACILITY_URL_PREFIX + str(facility['location_id']) + '/index.html'
                facility['url'] = url
                r = requests.get(url=url)
                html = r.text
                soup = BeautifulSoup(html, 'lxml')
                li = soup.find('div', attrs={'id': 'pfr_complex_loc'}).find('ul').find('li')
                if 'Phone' in li.text.strip():
                    facility['phone'] = li.text.strip().split(':')[1].strip()
                    logger.info('Got phone number for' + facility['facility_name'])

        sorted(facilities, key=lambda x: x['location_id'])
        return facilities
    except (Exception) as e:
        logger.warning(e)


def insert_data_to_empty_db(availablities, facilities):
    # primary keys for tables
    language_id = 'En'
    city_id = 2
    translation_id = ''
    category_id = ''
    type_id = ''
    activity_id = ''
    address_id = ''
    facility_id = ''

    # control varialbles for iterations
    category = ''
    type = ''
    activity = ''
    facility = ''
    country = 'Canada'

    # row affected counting for insertions
    row_affected_traslation = 0
    row_affected_language_traslation = 0
    row_affected_facility = 0
    row_affected_categoty = 0
    row_affected_type = 0
    row_affected_activity = 0
    row_affected_availability = 0
    row_affected_address = 0
    row_affected_activity_facility = 0
    row_affected_reference_facility_locationorigin = 0

    # inserting sql staments
    TRANSLATION_SQL = 'INSERT INTO `translation` () VALUES();'
    LANGUAGE_TRANSLATION_SQL = 'INSERT INTO `language_translation` (`TRANSLATION_ID`,`LANGUAGE_ID`, `DESCRIPTION`) VALUES (%s, %s, %s);'
    CATEGORY_SQL = 'INSERT INTO `category` (`CITY_ID`, `TITLE_TRANSLATION_ID`) VALUES (%s, %s);'
    TYPE_SQL = 'INSERT INTO `type` (`CATEGORY_ID`, `TITLE_TRANSLATION_ID`) VALUES (%s, %s);'
    ACTIVITY_SQL = 'INSERT INTO `activity` (`TYPE_ID`, `TITLE_TRANSLATION_ID`) VALUES (%s, %s);'
    ACTIVITY_FACILITY_SQL = 'INSERT INTO `facility_activity` (`FACILITY_ID`, `ACTIVITY_ID`) VALUES (%s, %s);'
    AVAILABILITY_SQL = 'INSERT INTO `availability` (`FACILITY_ID`, `ACTIVITY_ID`, `START_TIME`, `END_TIME`, `MIN_AGE`, `MAX_AGE`) VALUES (%s, %s, %s, %s, %s, %s);'
    ADDRESS_SQL = 'INSERT INTO `address` ( `STREET_TRANSLATION_ID`, `CITY`, `PROVINCE`, `POSTAL_CODE`, `COUNTRY`, `LATITUDE`, `LONGITUDE`) VALUES (%s, %s, %s, %s, %s, %s, %s);'
    FACILITY_SQL = 'INSERT INTO `facility` (`PHONE`, `ADDRESS_ID`, `TITLE_TRANSLATION_ID`, `URL`, `CITY_ID`) VALUES (%s, %s, %s, %s, %s);'
    REFERENCE_FACILITY_LOCATIONORIGIN_SQL = 'INSERT INTO `reference_facility_locationorigin` (`FACILITY_ID`, `LOCATION_ID`) VALUES (%s, %s);'
    FIND_FACILITY_SQL = 'SELECT facility.id FROM `facility` INNER JOIN `translation` INNER JOIN `language_translation` WHERE decription =  %s'

    logger.info('Connecting to MySQL...')
    try:
        mydb = MySQL.connect(
            host=HOST,
            user=DBUSER,
            password=PASSWORD,
            database=DATABASE
        )

        for facility in facilities:
            facility_name = facility['facility_name']
            street = facility['street']
            city = facility['city']
            province = facility['province']
            postal_code = facility['postal_code'].replace(' ', '')
            lat = facility['lat']
            lng = facility['lng']
            phone = facility['phone']
            url = facility['url']
            location_id = facility['location_id']

            # insert a new row into Table Translation
            translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
            row_affected_traslation += 1
            logger.info('Inserted a new Translation: ' + str(translation_id))

            # insert a new row into Table Language_Translation
            language_translation_val = (translation_id, language_id, street)
            executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
            row_affected_language_traslation += 1
            logger.info('Inserted a new Language_Translation: ' + street)

            # insert a new row into Table Address
            address_val = (translation_id, city, province, postal_code, country, lat, lng)
            address_id = executeInsertSQL(ADDRESS_SQL, address_val, mydb)
            row_affected_address += 1
            logger.info('Inserted a new Address: ' + str(address_id))

            # insert a new row into Table Translation
            translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
            row_affected_traslation += 1
            logger.info('Inserted a new Translation: ' + str(translation_id))

            # insert a new row into Table Language_Translation
            language_translation_val = (translation_id, language_id, facility_name)
            executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
            row_affected_language_traslation += 1
            logger.info('Inserted a new Language_Translation: ' + facility_name)

            # insert a new row into Table Facility
            facility_val = (phone, address_id, translation_id, url, city_id)
            facility_id = executeInsertSQL(FACILITY_SQL, facility_val, mydb)
            row_affected_facility += 1
            logger.info('Inserted a new Facility: ' + str(facility_id))

            # insert a new row into Table Reference_Facility_Locationorigin
            reference_facility_locationorigin_val = (facility_id, location_id)
            executeInsertSQL(REFERENCE_FACILITY_LOCATIONORIGIN_SQL, reference_facility_locationorigin_val, mydb)
            row_affected_reference_facility_locationorigin += 1
            logger.info('Insert a new Reference_Facility_Locationorigin: ' + str(facility_id))

            facility['facility_id'] = facility_id

            # facility = facility_current

        for availablity in availablities:
            # get current values
            category_current = availablity['category']
            type_current = availablity['type']
            activity_current = availablity['course_title']
            facility_current = availablity['location_id']

            for facility in facilities:
                if facility['location_id'] == facility_current:
                    facility_id = facility['facility_id']

            # insertion of new facilities
            # if facility_current != facility:
            #     # retrieve facility info for the avaibility from facilities
            #     for facility in facilities:
            #         if facility['location_id'] == facility_current:
            #             facility_name = facility['facility_name']
            #             street = facility['street']
            #             city = facility['city']
            #             province = facility['province']
            #             postal_code = facility['postal_code'].replace(' ', '')
            #             lat = facility['lat']
            #             lng = facility['lng']
            #             phone = facility['phone']
            #             url = facility['url']

            #     # insert a new row into Table Translation
            #     translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
            #     row_affected_traslation += 1
            #     logger.info('Inserted a new Translation: ' + str(translation_id))

            #     # insert a new row into Table Language_Translation
            #     language_translation_val = (translation_id, language_id, street)
            #     executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
            #     row_affected_language_traslation += 1
            #     logger.info('Inserted a new Language_Translation: ' + street)

            #     # insert a new row into Table Address
            #     address_val = (translation_id, city, province, postal_code, country, lat, lng)
            #     address_id = executeInsertSQL(ADDRESS_SQL, address_val, mydb)
            #     row_affected_address += 1
            #     logger.info('Inserted a new Address: ' + str(address_id))

            #     # insert a new row into Table Translation
            #     translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
            #     row_affected_traslation += 1
            #     logger.info('Inserted a new Translation: ' + str(translation_id))

            #     # insert a new row into Table Language_Translation
            #     language_translation_val = (translation_id, language_id, facility_name)
            #     executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
            #     row_affected_language_traslation += 1
            #     logger.info('Inserted a new Language_Translation: ' + facility_name)

            #     # insert a new row into Table Facility
            #     facility_val = (phone, address_id, translation_id, url, city_id)
            #     facility_id = executeInsertSQL(FACILITY_SQL, facility_val, mydb)
            #     row_affected_facility += 1
            #     logger.info('Inserted a new Facility: ' + str(facility_id))

            #     # insert a new row into Table Reference_Facility_Locationorigin
            #     reference_facility_locationorigin_val = (facility_id, facility_current)
            #     executeInsertSQL(REFERENCE_FACILITY_LOCATIONORIGIN_SQL, reference_facility_locationorigin_val, mydb)
            #     row_affected_reference_facility_locationorigin += 1
            #     logger.info('Insert a new Reference_Facility_Locationorigin: ' + str(facility_id))

            #     facility = facility_current

            # insertion of categories
            if category_current != category:
                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logger.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Languge_Translation
                language_translation_val = (translation_id, language_id, category_current)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logger.info('Inserted a new Language_Translation: ' + category_current)

                # insert a new row into Table Category
                category_val = (city_id, translation_id)
                category_id = executeInsertSQL(CATEGORY_SQL, category_val, mydb)
                row_affected_categoty += 1
                logger.info('Inserted a new Category: ' + str(category_id) + '(' + category_current + ')')

                category = category_current

            # insertion of types
            if type_current != type:
                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logger.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Languge_Translation
                language_translation_val = (translation_id, language_id, type_current)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logger.info('Inserted a new Language_Translation: ' + type_current)

                # insert a new row into Table Type
                type_val = (category_id, translation_id)
                type_id = executeInsertSQL(TYPE_SQL, type_val, mydb)
                row_affected_type += 1
                logger.info('Inserted a new Type: ' + str(type_id) + '(' + type_current + ')')

                type = type_current

            # insertion of activities
            if activity_current != activity:
                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logger.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Languge_Translation
                language_translation_val = (translation_id, language_id, activity_current)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logger.info('Inserted a new Language_Translation: ' + activity_current)

                # insert a new row into Table Activity
                activity_val = (type_id, translation_id)
                activity_id = executeInsertSQL(ACTIVITY_SQL, activity_val, mydb)
                row_affected_activity += 1
                logger.info('Inserted a new Activity: ' + str(activity_id) + '(' + activity_current + ')')

                # insert a new row into Table Activity_Facility
                activity_facility_val = (facility_id, activity_id)
                executeInsertSQL(ACTIVITY_FACILITY_SQL, activity_facility_val, mydb)
                row_affected_activity_facility += 1
                logger.info('Inserted a new Activity_Facility: ' + str(facility_id) + '-' + str(activity_id))

                activity = activity_current

            start_time = availablity['start_time']
            end_time = availablity['end_time']
            age_min = availablity['age_min']
            age_max = availablity['age_max']

            # insert a new row into Table Availability
            availability_val = (facility_id, activity_id, start_time, end_time, age_min, age_max)
            availability_id = executeInsertSQL(AVAILABILITY_SQL, availability_val, mydb)
            row_affected_availability += 1
            logger.info('Inserted a new Availability: ' + str(availability_id) + '(' + start_time + '-' + end_time + ')')

        mydb.commit()
        logger.info('Inserted into Translation ' + str(row_affected_traslation) + ' rows')
        logger.info('Inserted into Language_Translation ' + str(row_affected_language_traslation) + ' rows')
        logger.info('Inserted into Address ' + str(row_affected_address) + ' rows')
        logger.info('Inserted into Facility ' + str(row_affected_facility) + ' rows')
        logger.info('Inserted into Category ' + str(row_affected_categoty) + ' rows')
        logger.info('Inserted into Type ' + str(row_affected_type) + ' rows')
        logger.info('Inserted into Activity ' + str(row_affected_activity) + ' rows')
        logger.info('Inserted into Activity_Facility ' + str(row_affected_activity_facility) + ' rows')
        logger.info('Inserted into Availability ' + str(row_affected_availability) + ' rows')
        mydb.close()
        logger.info('Database disconnected')
    except Exception as e:
        logger.warning(e)


def executeInsertSQL(sql: str, val: dict, db: MySQL.MySQLConnection):
    cursor = db.cursor()
    if val is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, val)
    return cursor.lastrowid


def writeListToTxt(filename, mode, list):
    with open(os.getcwd() + '/' + filename + '.txt', mode) as fp:
        for item in list:
            fp.write('%s\n' % item)


def setuplogger():
    global logger
    parser = argparse.ArgumentParser()
    parser.add_argument('-log',
                        '--loglevel',
                        default='debug',
                        help='Provide logging level. Example --loglevel debug, default=debug')
    args = parser.parse_args()
    logger = logging.getLogger()
    logger.setLevel(args.loglevel.upper())

    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', '%Y-%m-%d %H:%M:%S')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler('logs.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info('Loggers setup')


def run():
    setuplogger()
    logger.info('Start running Active-Toronto Scraper...')
    try:
        getResources()
        availabilities = getAvalibilities()
        facilities = getOriginalFacilities(availabilities)
        facilities = getGeoToFacilities(facilities)
        facilities = getPhoneUrlToFacilities(facilities)
        # writeListToTxt("facilities", "w", facilities)
        insert_data_to_empty_db(availabilities, facilities)
        logger.info('------------------------------------------------End------------------------------------------------')
    except Exception as e:
        logger.warning(e)


if __name__ == '__main__':
    run()
