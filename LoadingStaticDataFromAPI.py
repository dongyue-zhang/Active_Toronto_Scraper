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
RESOURSE_API = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=da46e4ac-d4ab-4b1c-b139-6362a0a43b3c'
FACILITY_LIST_URL = 'https://www.toronto.ca/data/parks/prd/facilities/recreationcentres/index.html'
CITY_OF_TORONTO_URL = 'https://www.toronto.ca'
FACILITY_URL_PREFIX = 'https://www.toronto.ca/data/parks/prd/facilities/complex/'
LOCATIONS = 'Locations'
DROPIN = 'Drop-in.json'
FACILITIES = 'Facilities.json'
REGISTERED_PROGRAMS = 'Registered Programs.json'


def getResourses():
    global dropins, facilities, locations, registeredPrograms
    params = {'key': 'value'}

    logging.info('Requesting resourses from City of Toronto OpenAPI: ' + RESOURSE_API)
    try:
        r = requests.get(url=RESOURSE_API, params=params)
        response = r.json()
    except (ConnextionError, Exception) as e:
        logging.warning(('Could not get resourses from {}:'.format(RESOURSE_API)))
        logging.warning(e)

    try:
        resoursesJSON = response['result']['resources']
        resourses = {}
        for resourseJSON in resoursesJSON:
            name = resourseJSON['name']
            url = resourseJSON['url']

            if resourseJSON['name'] in [DROPIN, FACILITIES, REGISTERED_PROGRAMS]:
                logging.info('Getting sourse file: ' + resourseJSON['name'])
                content = requests.get(url=url, params=params).json()
                resourses[name] = content
            elif resourseJSON['name'] == LOCATIONS:
                logging.info('Getting sourse file: ' + resourseJSON['name'])
                csv = requests.get(url=url, params=params).content
                locations = pd.read_csv(io.StringIO(
                    csv.decode('utf-8')), sep=',', header=0)
                locations = locations.fillna('')

        dropins = resourses[DROPIN]
        facilities = resourses[FACILITIES]
        registeredPrograms = resourses[REGISTERED_PROGRAMS]
    except (Exception) as e:
        logging.warning(e)


def getAvalibilities():
    logging.info('Extracting avalibilities from file: ' + DROPIN)
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
        sorted(avalibilities, key=lambda x: x['course_title'])
        sorted(avalibilities, key=lambda x: x['type'])
        sorted(avalibilities, key=lambda x: x['category'])
        return avalibilities
    except (Exception) as e:
        logging.warning(e)


def getOriginalFacilities():
    logging.info('Extrating facilities original data from file: ' + LOCATIONS)
    try:
        availablities = getAvalibilities()
        locationList = locations.filter(
            items=['Location ID', 'Location Name', 'District', 'Street No', 'Street No Suffix', 'Street Name', 'Street Type', 'Postal Code']).values.tolist()

        locationIDs = set()
        locationsNoGeo = []

        for availablity in availablities:
            locationID = availablity['location_id']
            locationIDs.add(locationID)

        for locationID in locationIDs:
            for locat in locationList:
                if locationID == locat[0]:
                    street = str(locat[3]) + str(locat[4]) + ' ' + str(locat[5]) + ' ' + str(locat[6])
                    locationsNoGeo.append(
                        {'location_id': locat[0], 'facility_name': locat[1], 'city': locat[2], 'street': street, 'province': PROVINCE, 'postal_code': locat[7], 'phone': None, 'url': None})

        return locationsNoGeo
    except (Exception) as e:
        logging.warning(e)


def getGeoToFacilities(facilities):
    logging.info('Start getting coordinations for facilities...')
    try:
        for facility in facilities:
            logging.info('Getting latitude and longitude for facility: ' + facility['facility_name'])
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
        logging.error(e)


def getPhoneUrlToFacilities(facilities):
    logging.info('Start getting phone numbers and urls for facilities...')
    try:
        # get recreation list
        logging.info('Getting recreation list from {}'.format(FACILITY_LIST_URL))
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        driver = webdriver.Chrome(options=option)
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
            logging.info('Getting phone number and urls for facility: ' + facility['facility_name'])
            for phone in phoneList:
                if facility['facility_name'] == phone['Name']:
                    facility['phone'] = phone['phone']
                    facility['url'] = phone['url']
                    logging.info('Got phone number for' + facility['facility_name'])
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
                    logging.info('Got phone number for' + facility['facility_name'])

        sorted(facilities, key=lambda x: x['location_id'])
        return facilities
    except (Exception) as e:
        logging.warning(e)


def saveStaticDataToDB(availablities, facilities):
    # primary keys for tables
    language_id = 'En'
    city_id = 2
    translation_id = ''
    category_id = ''
    type_id = ''
    activity_id = ''
    address_id = ''
    facility_id = ''

    # control varialbles for iteration
    category = ''
    type = ''
    activity = ''
    facility = ''
    country = 'Canada'

    # counting row affected in tables
    row_affected_traslation = 0
    row_affected_language_traslation = 0
    row_affected_facility = 0
    row_affected_categoty = 0
    row_affected_type = 0
    row_affected_activity = 0
    row_affected_availability = 0
    row_affected_address = 0
    row_affected_activity_facility = 0

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

    logging.info('Connecting to MySQL...')
    try:
        mydb = MySQL.connect(
            host=HOST,
            user=DBUSER,
            password=PASSWORD,
            database=DATABASE
        )

        for availablity in availablities:
            # get current values
            category_current = availablity['category']
            type_current = availablity['type']
            activity_current = availablity['course_title']
            facility_current = availablity['location_id']

            # insertion of new facilities
            if facility_current != facility:
                # retrieve facility info for the avaibility from facilities
                for facility in facilities:
                    if facility['location_id'] == facility_current:
                        facility_name = facility['facility_name']
                        street = facility['street']
                        city = facility['city']
                        province = facility['province']
                        postal_code = facility['postal_code'].replace(' ', '')
                        lat = facility['lat']
                        lng = facility['lng']
                        phone = facility['phone']
                        url = facility['url']

                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logging.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Language_Translation
                language_translation_val = (translation_id, language_id, street)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logging.info('Inserted a new Language_Translation: ' + street)

                # insert a new row into Table Address
                address_val = (translation_id, city, province, postal_code, country, lat, lng)
                address_id = executeInsertSQL(ADDRESS_SQL, address_val, mydb)
                row_affected_address += 1
                logging.info('Inserted a new Address: ' + str(address_id))

                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logging.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Language_Translation
                language_translation_val = (translation_id, language_id, facility_name)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logging.info('Inserted a new Language_Translation: ' + facility_name)

                # insert a new row into Table Facility
                facility_val = (phone, address_id, translation_id, url, city_id)
                facility_id = executeInsertSQL(FACILITY_SQL, facility_val, mydb)
                row_affected_facility += 1
                logging.info('Inserted a new Facility: ' + str(facility_id))

                facility = facility_current

            # insertion of categories
            if category_current != category:
                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logging.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Languge_Translation
                language_translation_val = (translation_id, language_id, category_current)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logging.info('Inserted a new Language_Translation: ' + category_current)

                # insert a new row into Table Category
                category_val = (city_id, translation_id)
                category_id = executeInsertSQL(CATEGORY_SQL, category_val, mydb)
                row_affected_categoty += 1
                logging.info('Inserted a new Category: ' + str(category_id) + '(' + category_current + ')')

                category = category_current

            # insertion of types
            if type_current != type:
                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logging.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Languge_Translation
                language_translation_val = (translation_id, language_id, type_current)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logging.info('Inserted a new Language_Translation: ' + type_current)

                # insert a new row into Table Type
                type_val = (category_id, translation_id)
                type_id = executeInsertSQL(TYPE_SQL, type_val, mydb)
                row_affected_type += 1
                logging.info('Inserted a new Type: ' + str(type_id) + '(' + type_current + ')')

                type = type_current

            # insertion of activities
            if activity_current != activity:
                # insert a new row into Table Translation
                translation_id = executeInsertSQL(TRANSLATION_SQL, None, mydb)
                row_affected_traslation += 1
                logging.info('Inserted a new Translation: ' + str(translation_id))

                # insert a new row into Table Languge_Translation
                language_translation_val = (translation_id, language_id, activity_current)
                executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val, mydb)
                row_affected_language_traslation += 1
                logging.info('Inserted a new Language_Translation: ' + activity_current)

                # insert a new row into Table Activity
                activity_val = (type_id, translation_id)
                activity_id = executeInsertSQL(ACTIVITY_SQL, activity_val, mydb)
                row_affected_activity += 1
                logging.info('Inserted a new Activity: ' + str(activity_id) + '(' + activity_current + ')')

                # insert a new row into Table Activity_Facility
                activity_facility_val = (facility_id, activity_id)
                executeInsertSQL(ACTIVITY_FACILITY_SQL, activity_facility_val, mydb)
                row_affected_activity_facility += 1
                logging.info('Inserted a new Activity_Facility: ' + str(facility_id) + '-' + str(activity_id))

                activity = activity_current

            start_time = availablity['start_time']
            end_time = availablity['end_time']
            age_min = availablity['age_min']
            age_max = availablity['age_max']

            # insert a new row into Table Availability
            availability_val = (facility_id, activity_id, start_time, end_time, age_min, age_max)
            availability_id = executeInsertSQL(AVAILABILITY_SQL, availability_val, mydb)
            row_affected_availability += 1
            logging.info('Inserted a new Availability: ' + str(availability_id) + '(' + start_time + '-' + end_time + ')')

        mydb.commit()
        logging.info('Inserted into Translation ' + str(row_affected_traslation) + ' rows')
        logging.info('Inserted into Language_Translation ' + str(row_affected_language_traslation) + ' rows')
        logging.info('Inserted into Address ' + str(row_affected_address) + ' rows')
        logging.info('Inserted into Facility ' + str(row_affected_facility) + ' rows')
        logging.info('Inserted into Category ' + str(row_affected_categoty) + ' rows')
        logging.info('Inserted into Type ' + str(row_affected_type) + ' rows')
        logging.info('Inserted into Activity ' + str(row_affected_activity) + ' rows')
        logging.info('Inserted into Activity_Facility ' + str(row_affected_activity_facility) + ' rows')
        logging.info('Inserted into Availability ' + str(row_affected_availability) + ' rows')
        mydb.close()
        logging.info('Database disconnected')
    except Exception as e:
        logging.warning(e)


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


def setupLogging():
    parser = argparse.ArgumentParser()
    parser.add_argument('-log',
                        '--loglevel',
                        default='warning',
                        help='Provide logging level. Example --loglevel debug, default=warning')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=args.loglevel.upper(),
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('Logging now setup.')


def run():
    setupLogging()
    logging.info('Start running Active-Toronto Scraper...')
    try:
        getResourses()
        availabilities = getAvalibilities()
        facilities = getOriginalFacilities()
        facilities = getGeoToFacilities(facilities)
        facilities = getPhoneUrlToFacilities(facilities)
        saveStaticDataToDB(availabilities, facilities)
    except Exception as e:
        logging.warning(e)


run()
