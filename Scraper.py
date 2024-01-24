import argparse
import io
import json
import logging
import os
import time
from datetime import datetime

import mysql.connector as MySQL
import pandas as pd
import requests
# import schedule
from bs4 import BeautifulSoup
from decouple import config
# from selenium import webdriver
# from selenium.webdriver.firefox.service import Service
import argparse

GOOGLE_API_KEY = config('GOOGLE_API_KEY')
HOST = 'mysqldb'
# HOST = '143.198.57.221'
DBUSER = config('MYSQL_USER')
PASSWORD = config('MYSQL_PASSWORD')
DATABASE = config('MYSQL_DATABASE')
GOOGLE_API_URL = "https://maps.googleapis.com/maps/api/geocode/json?address="
PROVINCE = "Ontario"
RESOURCE_API = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=da46e4ac-d4ab-4b1c-b139-6362a0a43b3c"
FACILITY_LIST_URL = "https://www.toronto.ca/data/parks/prd/facilities/recreationcentres/index.html"
CITY_OF_TORONTO_URL = "https://www.toronto.ca"
FACILITY_URL_PREFIX = "https://www.toronto.ca/data/parks/prd/facilities/complex/"
LOCATIONS = "Locations"
DROPIN = "Drop-in.json"
FACILITIES = "Facilities.json"
REGISTERED_PROGRAMS = "Registered Programs.json"

# inserting sql staments
TRANSLATION_SQL = "INSERT INTO `translation` () VALUES();"
LANGUAGE_TRANSLATION_SQL = "INSERT INTO `language_translation` (`TRANSLATION_ID`,`LANGUAGE_ID`, `DESCRIPTION`) VALUES (%s, %s, %s);"
CATEGORY_SQL = (
    "INSERT INTO `category` (`CITY_ID`, `TITLE_TRANSLATION_ID`) VALUES (%s, %s);"
)
TYPE_SQL = "INSERT INTO `type` (`CATEGORY_ID`, `TITLE_TRANSLATION_ID`) VALUES (%s, %s);"
ACTIVITY_SQL = "INSERT INTO `activity` (`ID`, `TYPE_ID`, `TITLE_TRANSLATION_ID`) VALUES (%s, %s, %s);"
ACTIVITY_FACILITY_SQL = (
    "INSERT INTO `facility_activity` (`FACILITY_ID`, `ACTIVITY_ID`) VALUES (%s, %s);"
)
AVAILABILITY_SQL = "INSERT INTO `availability` (`FACILITY_ID`, `ACTIVITY_ID`, `START_TIME`, `END_TIME`, `MIN_AGE`, `MAX_AGE`) VALUES (%s, %s, %s, %s, %s, %s);"
ADDRESS_SQL = "INSERT INTO `address` ( `STREET_TRANSLATION_ID`, `CITY`, `PROVINCE`, `POSTAL_CODE`, `COUNTRY`, `LATITUDE`, `LONGITUDE`) VALUES (%s, %s, %s, %s, %s, %s, %s);"
FACILITY_SQL = "INSERT INTO `facility` (`PHONE`, `ADDRESS_ID`, `TITLE_TRANSLATION_ID`, `URL`, `CITY_ID`) VALUES (%s, %s, %s, %s, %s);"
REFERENCE_FACILITY_LOCATIONORIGIN_SQL = "INSERT INTO `reference_facility_locationorigin` (`FACILITY_ID`, `LOCATION_ID`) VALUES (%s, %s);"

# FIND_FACILITY_SQL = 'SELECT facility.id FROM `facility` INNER JOIN `translation` INNER JOIN `language_translation` WHERE decription =  %s'
FIND_FACILITY_BY_LOCATION_ID = "SELECT `facility_id` FROM `reference_facility_locationorigin` WHERE `location_id` = %s;"
FIND_ACTIVITY_BY_ID = "SELECT * FROM `activity` WHERE `id` = %s;"
FIND_TYPE_BY_DESC = "SELECT * FROM `type` INNER JOIN `translation` ON `type`.title_translation_id = `translation`.id INNER JOIN `language_translation` ON `translation`.id = `language_translation`.translation_id WHERE description = %s"
FIND_CATEGORY_BY_DESC = "SELECT * FROM `category` INNER JOIN `translation` ON `category`.title_translation_id = `translation`.id INNER JOIN `language_translation` ON `translation`.id = `language_translation`.translation_id WHERE description = %s"

# global mydb


# primary keys for tables
language_id = "En"
city_id = 2
translation_id = 0
category_id = 0
type_id = 0
activity_id = 0
address_id = 0
facility_id = 0
country = "Canada"

# # control varialbles for iterations
# category = ''
# type = ''
# activity = ''
# facility = ''


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


def getResources():
    params = {"key": "value"}
    global dropins, facilities, locations, registeredPrograms
    logger.info("Requesting resources from City of Toronto OpenAPI: " + RESOURCE_API)
    try:
        r = requests.get(url=RESOURCE_API, params=params)
        response = r.json()
    except (ConnectionError, Exception) as e:
        logger.warning(("Could not get resources from {}:".format(RESOURCE_API)))
        logger.warning(e)

    try:
        resources = response["result"]["resources"]
        resources_dict = {}
        for resource in resources:
            name = resource["name"]
            url = resource["url"]

            if resource["name"] in [DROPIN, FACILITIES, REGISTERED_PROGRAMS]:
                logger.info("Getting source file: " + resource["name"])
                content = requests.get(url=url, params=params).json()
                resources_dict[name] = content
            elif resource["name"] == LOCATIONS:
                logger.info("Getting source file: " + resource["name"])
                csv = requests.get(url=url, params=params).content
                locations = pd.read_csv(
                    io.StringIO(csv.decode("utf-8")), sep=",", header=0
                )
                # fill NaN values with ''
                locations = locations.fillna("")

        dropins = resources_dict[DROPIN]
        facilities = resources_dict[FACILITIES]
        registeredPrograms = resources_dict[REGISTERED_PROGRAMS]
    except Exception as e:
        logger.warning(e)


def getAvalibilities():
    logger.info("Extracting avalibilities from file: " + DROPIN)
    try:
        availabilities = []
        for dropin in dropins:
            availability = {}
            availability["start_time"] = dropin["Start Date Time"]
            startDatetime = datetime.strptime(
                dropin["Start Date Time"], "%Y-%m-%dT%H:%M:%S"
            )
            endHour = dropin["End Hour"]
            endMin = dropin["End Min"]
            endDatetime = startDatetime.replace(hour=endHour, minute=endMin)
            endDatetimeStr = endDatetime.strftime("%Y-%m-%dT%H:%M:%S")
            
            if datetime.strptime(endDatetimeStr, "%Y-%m-%dT%H:%M:%S") > datetime.now():
                availability["end_time"] = endDatetimeStr

                availability["category"] = dropin["Category"]
                availability["location_id"] = dropin["Location ID"]
                availability["course_id"] = dropin["Course_ID"]
                availability["course_title"] = dropin["Course Title"]
                if ":" in availability["course_title"]:
                    type = availability["course_title"].split(":")[0].strip()
                elif "(" in availability["course_title"]:
                    type = availability["course_title"].split("(")[0].strip()
                elif "-" in availability["course_title"]:
                    type = availability["course_title"].split("-")[0].strip()
                else:
                    type = availability["course_title"]
                availability["type"] = type
                if dropin["Age Min"] != "None":
                    availability["age_min"] = dropin["Age Min"]
                else:
                    availability["age_min"] = None

                if dropin["Age Max"] != "None":
                    availability["age_max"] = dropin["Age Max"]
                else:
                    availability["age_max"] = None

                availabilities.append(availability)
        availabilities = sorted(
            availabilities,
            key=lambda x: (
                x["category"],
                x["type"],
                x["course_title"],
                x["location_id"],
            ),
        )
        return availabilities
    except Exception as e:
        logger.warning(e)


def getOriginalFacilities(availablities):
    logger.info("Extracting facilities original data from file: " + LOCATIONS)
    try:
        locationList = locations.filter(
            items=[
                "Location ID",
                "Location Name",
                "District",
                "Street No",
                "Street No Suffix",
                "Street Name",
                "Street Type",
                "Postal Code",
            ]
        ).values.tolist()

        locationIDs = set()
        facilitiesNoGeo = []

        for availablity in availablities:
            locationID = availablity["location_id"]
            locationIDs.add(locationID)

        for locationID in locationIDs:
            for locat in locationList:
                if locationID == locat[0]:
                    street = (
                        str(locat[3])
                        + str(locat[4])
                        + " "
                        + str(locat[5])
                        + " "
                        + str(locat[6])
                    )
                    facilitiesNoGeo.append(
                        {
                            "location_id": locat[0],
                            "facility_name": locat[1],
                            "city": locat[2],
                            "street": street,
                            "province": PROVINCE,
                            "postal_code": locat[7],
                            "phone": None,
                            "url": None,
                        }
                    )

        return facilitiesNoGeo
    except Exception as e:
        logger.warning(e)


def getGeoToFacilities(facilities):
    logger.info("Start getting coordinations for facilities...")
    try:
        facilities_geo = []
        for facility in facilities:
            facilities_geo.append(getGeo(facility))
        return facilities_geo
    except Exception as e:
        logger.warning(e)


def getGeo(facility):
    logger.info(
        "Getting latitude and longitude for facility: " + facility["facility_name"]
    )
    addressStr = (
        facility["street"] + " " + facility["city"] + " " + facility["province"]
    )
    addressStr = addressStr.replace(" ", "%20")
    url = GOOGLE_API_URL + addressStr + "&key=" + GOOGLE_API_KEY
    params = {"key": "value"}
    r = requests.get(url=url, params=params)
    response = r.json()
    geometry = response["results"][0]["geometry"]["location"]
    facility["lat"] = geometry["lat"]
    facility["lng"] = geometry["lng"]
    if facility["postal_code"] == "":
        facility["postal_code"] = response["results"][0]["address_components"][-1][
            "short_name"
        ]
    return facility


def getPhoneUrlToFacilities(facilities):
    
    logger.info("Start getting phone numbers and urls for facilities...")
    try:
        # get recreation list
        logger.info("Getting recreation list from {}".format(FACILITY_LIST_URL))
        # option = webdriver.FirefoxOptions()
        # service = Service()
        # option.add_argument("--no-sandbox")
        # # option.add_argument("--headless")
        # option.add_argument("--disable-infobars")
        # option.add_argument("--disable-dev-shm-usage")
        # driver = webdriver.Firefox(service=service, options=option)
        # driver.get(FACILITY_LIST_URL)
        # time.sleep(1)
        # html = driver.page_source.encode("utf-8")
        # driver.quit()

        # soup = BeautifulSoup(html, "lxml")
        # print(soup)
        # table = soup.find("div", attrs={"class": "pfrListing"})
        # trs = table.table.tbody.find_all("tr")
        # phoneList = []
        # for tr in trs:
        #     a = tr.find("th", attrs={"data-info": "Name"}).a
        #     name = a.text.strip()
        #     url = a.get("href")
        #     phone = tr.find("td", attrs={"data-info": "Phone"}).text.strip()
        #     phoneList.append(
        #         {"Name": name, "phone": phone, "url": CITY_OF_TORONTO_URL + url}
        #     )

        phoneList = []
        with open("FacilitiyList.txt") as file:
            for line in file:
                line = line.strip()
                dicts = line.split(', ')
                phoneList.append({dicts[0].split(': ')[0].strip("{").strip("'") : dicts[0].split(': ')[1].strip("'").strip('"'), dicts[1].split(': ')[0].strip("'") : dicts[1].split(': ')[1].strip("'"), dicts[2].split(': ')[0].strip("'") : dicts[2].split(': ')[1].strip("}").strip("'")})

        # if a facility is not on the list, get phone number from its website
        for facility in facilities:
            logger.info(
                "Getting phone number and urls for facility: "
                + facility["facility_name"]
            )
            facility['phone'] = None
            facility['url'] = None
            for phone in phoneList:
                if facility["facility_name"] == phone["Name"]:
                    facility["phone"] = phone["phone"]
                    facility["url"] = phone["url"]
                    logger.info("Got phone number for" + facility["facility_name"])
                    break

            if facility["phone"] is None:
                url = FACILITY_URL_PREFIX + str(facility["location_id"]) + "/index.html"
                facility["url"] = url
                r = requests.get(url=url)
                html = r.text
                soup = BeautifulSoup(html, "lxml")
                li = (
                    soup.find("div", attrs={"id": "pfr_complex_loc"})
                    .find("ul")
                    .find("li")
                )
                if "Phone" in li.text.strip():
                    facility["phone"] = li.text.strip().split(":")[1].strip()
                    logger.info("Got phone number for" + facility["facility_name"])

        sorted(facilities, key=lambda x: x["location_id"])
        return facilities
    except Exception as e:
        logger.warning(e)


def facility_exists(location_id: int):
    facility_id = 0
    cursor = mydb.cursor()
    cursor.execute(FIND_FACILITY_BY_LOCATION_ID, (location_id,))
    rows = cursor.fetchall()
    if len(rows) != 0:
        facility_id = rows[0][0]
    cursor.close()
    
    return facility_id


def activity_exists(activity: int):
    activity_id = 0
    cursor = mydb.cursor()
    cursor.execute(FIND_ACTIVITY_BY_ID, (activity,))
    rows = cursor.fetchall()
    if len(rows) != 0:
        activity_id = rows[0][0]
    cursor.close()
    return activity_id


def type_exists(type_des):
    type_id = 0
    cursor = mydb.cursor()
    cursor.execute(FIND_TYPE_BY_DESC, (type_des,))
    rows = cursor.fetchall()
    if len(rows) != 0:
        type_id = rows[0][0]
    cursor.close()
    return type_id


def category_exists(category):
    category_id = 0
    cursor = mydb.cursor()
    cursor.execute(FIND_CATEGORY_BY_DESC, (category,))
    rows = cursor.fetchall()
    if len(rows) != 0:
        category_id = rows[0][0]
    cursor.close()
    return category_id


def get_new_facilities(facilities):
    logger.info("Getting new facilities...")
    new_facilities = []
    for facility in facilities:
        if facility_exists(int(facility["location_id"])) == 0:
            new_facilities.append(facility)
    return new_facilities


def update_db(availabilities, facilities):
    try:
        if mydb == None:
            connect_db()

        if len(facilities) != 0:
            for facility in facilities:
                insert_new_facility(facility)
        mydb.commit()

        store_new_availabilities(availabilities)

        mydb.commit()
        log_rows_affected()

        mydb.close()
        logger.info("Database disconnected")
    except Exception as e:
        logger.warning(e)


def insert_data_to_empty_db(availablities, facilities):
    # # primary keys for tables
    # language_id = 'En'
    # city_id = 2
    # translation_id = 0
    # category_id = 0
    # type_id = 0
    # activity_id = 0
    # address_id = 0
    # facility_id = 0

    # control varialbles for iterations
    category = ""
    type = ""
    activity = ""
    facility = ""
    country = "Canada"

    # # row affected counting for insertions
    # row_affected_traslation = 0
    # row_affected_language_traslation = 0
    # row_affected_facility = 0
    # row_affected_categoty = 0
    # row_affected_type = 0
    # row_affected_activity = 0
    # row_affected_availability = 0
    # row_affected_address = 0
    # row_affected_activity_facility = 0
    # row_affected_reference_facility_locationorigin = 0

    logger.info("Connecting to MySQL...")
    try:
        mydb = connect_db()

        for facility in facilities:
            facility_id = insert_new_facility(facility)
            facility["facility_id"] = facility_id

        for availablity in availablities:
            # get current values
            category_current = availablity["category"]
            type_current = availablity["type"]
            activity_current = availablity["course_title"]
            facility_current = availablity["location_id"]
            activity_id = availablity["course_id"]

            
            # for facility in facilities:
            #     if facility['location_id'] == facility_current:
            #         facility_id = facility['facility_id']
            facility_id = facility_exists(facility_current)

            # insertion of categories
            if category_current != category:
                category_id = insert_new_category(category_current)
                category = category_current

            # insertion of types
            if type_current != type:
                type_id = insert_new_type(type_current, category_id)
                type = type_current

            # insertion of activities
            if activity_current != activity:
                activity_id = insert_new_activity(
                    activity_current, activity_id, type_id, facility_id
                )
                activity = activity_current

            inser_new_availability(availablity, facility_id, activity_id)
        mydb.commit()
        log_rows_affected()

        mydb.close()
        logger.info("Database disconnected")
    except Exception as e:
        logger.warning(e)


def store_new_availabilities(availabilities):
    for availability in availabilities:
        category_id = category_exists(availability["category"])
        type_id = type_exists(availability["type"])
        activity_id = activity_exists(availability["course_id"])
        facility_id = facility_exists(availability["location_id"])
        if category_id == 0:
            category_id = insert_new_category(availability["category"])

        if type_id == 0:
            type_id = insert_new_type(availability["type"], category_id)

        if activity_id == 0:
            activity_id = insert_new_activity(
                availability["course_title"],
                availability["course_id"],
                type_id,
                facility_id
            )

        insert_new_availability(availability, facility_id, activity_id)


def executeInsertSQL(sql: str, val):
    cursor = mydb.cursor()
    if val is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, val)
    return cursor.lastrowid


def insert_new_facility(facility):
    facility_name = facility["facility_name"]
    street = facility["street"]
    city = facility["city"]
    province = facility["province"]
    postal_code = facility["postal_code"].replace(" ", "")
    lat = facility["lat"]
    lng = facility["lng"]
    phone = facility["phone"]
    url = facility["url"]
    location_id = facility["location_id"]
    global city_id, country, language_id
    global row_affected_traslation, row_affected_language_traslation, row_affected_address, row_affected_traslation, row_affected_facility, row_affected_language_traslation, row_affected_reference_facility_locationorigin

    # insert a new row into Table Translation
    translation_id = executeInsertSQL(TRANSLATION_SQL, None)
    row_affected_traslation += 1
    logger.info("Inserted a new Translation: " + str(translation_id))

    # insert a new row into Table Language_Translation
    language_translation_val = (translation_id, language_id, street)
    executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val)
    row_affected_language_traslation += 1
    logger.info("Inserted a new Language_Translation: " + street)

    # insert a new row into Table Address
    address_val = (translation_id, city, province, postal_code, country, lat, lng)
    address_id = executeInsertSQL(ADDRESS_SQL, address_val)
    row_affected_address += 1
    logger.info("Inserted a new Address: " + str(address_id))

    # insert a new row into Table Translation
    translation_id = executeInsertSQL(TRANSLATION_SQL, None)
    row_affected_traslation += 1
    logger.info("Inserted a new Translation: " + str(translation_id))

    # insert a new row into Table Language_Translation
    language_translation_val = (translation_id, language_id, facility_name)
    executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val)
    row_affected_language_traslation += 1
    logger.info("Inserted a new Language_Translation: " + facility_name)

    # insert a new row into Table Facility
    facility_val = (phone, address_id, translation_id, url, city_id)
    facility_id = executeInsertSQL(FACILITY_SQL, facility_val)
    row_affected_facility += 1
    logger.info("Inserted a new Facility: " + str(facility_id))

    # insert a new row into Table Reference_Facility_Locationorigin
    reference_facility_locationorigin_val = (facility_id, location_id)
    executeInsertSQL(
        REFERENCE_FACILITY_LOCATIONORIGIN_SQL, reference_facility_locationorigin_val
    )
    row_affected_reference_facility_locationorigin += 1
    logger.info("Insert a new Reference_Facility_Locationorigin: " + str(facility_id))

    return facility_id


def insert_new_category(new_category):
    global city_id, row_affected_traslation, row_affected_language_traslation, row_affected_categoty
    # insert a new row into Table Translation
    translation_id = executeInsertSQL(TRANSLATION_SQL, None)
    row_affected_traslation += 1
    logger.info("Inserted a new Translation: " + str(translation_id))

    # insert a new row into Table Languge_Translation
    language_translation_val = (translation_id, language_id, new_category)
    executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val)
    row_affected_language_traslation += 1
    logger.info("Inserted a new Language_Translation: " + new_category)

    # insert a new row into Table Category
    category_val = (city_id, translation_id)
    category_id = executeInsertSQL(CATEGORY_SQL, category_val)
    row_affected_categoty += 1
    logger.info(
        "Inserted a new Category: " + str(category_id) + "(" + new_category + ")"
    )

    return category_id


def insert_new_type(new_type, category_id):
    global row_affected_traslation, row_affected_language_traslation, row_affected_type
    # insert a new row into Table Translation
    translation_id = executeInsertSQL(TRANSLATION_SQL, None)
    row_affected_traslation += 1
    logger.info("Inserted a new Translation: " + str(translation_id))

    # insert a new row into Table Languge_Translation
    language_translation_val = (translation_id, language_id, new_type)
    executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val)
    row_affected_language_traslation += 1
    logger.info("Inserted a new Language_Translation: " + new_type)

    # insert a new row into Table Type
    type_val = (category_id, translation_id)
    type_id = executeInsertSQL(TYPE_SQL, type_val)
    row_affected_type += 1
    logger.info("Inserted a new Type: " + str(type_id) + "(" + new_type + ")")

    return type_id


def insert_new_activity(new_activity, activity_id, type_id, facility_id):
    global row_affected_traslation, row_affected_language_traslation, row_affected_activity, row_affected_activity_facility
    # insert a new row into Table Translation
    translation_id = executeInsertSQL(TRANSLATION_SQL, None)
    row_affected_traslation += 1
    logger.info("Inserted a new Translation: " + str(translation_id))

    # insert a new row into Table Languge_Translation
    language_translation_val = (translation_id, language_id, new_activity)
    executeInsertSQL(LANGUAGE_TRANSLATION_SQL, language_translation_val)
    row_affected_language_traslation += 1
    logger.info("Inserted a new Language_Translation: " + new_activity)

    # insert a new row into Table Activity
    activity_val = (activity_id, type_id, translation_id)
    executeInsertSQL(ACTIVITY_SQL, activity_val)
    row_affected_activity += 1
    logger.info(
        "Inserted a new Activity: " + str(activity_id) + "(" + new_activity + ")"
    )

    # insert a new row into Table Activity_Facility
    activity_facility_val = (facility_id, activity_id)
    executeInsertSQL(ACTIVITY_FACILITY_SQL, activity_facility_val)
    row_affected_activity_facility += 1
    logger.info(
        "Inserted a new Activity_Facility: " + str(facility_id) + "-" + str(activity_id)
    )

    return activity_id


def insert_new_availability(availablity, facility_id, activity_id):
    start_time = availablity["start_time"]
    end_time = availablity["end_time"]
    age_min = availablity["age_min"]
    age_max = availablity["age_max"]
    global row_affected_availability

    # insert a new row into Table Availability
    availability_val = (
        facility_id,
        activity_id,
        start_time,
        end_time,
        age_min,
        age_max,
    )
    availability_id = executeInsertSQL(AVAILABILITY_SQL, availability_val)
    row_affected_availability += 1
    logger.info(
        "Inserted a new Availability: "
        + str(availability_id)
        + "("
        + start_time
        + "-"
        + end_time
        + ")"
    )

    return availability_id


def writeListToTxt(filename, mode, list):
    with open(os.getcwd() + "/" + filename + ".txt", mode) as fp:
        for item in list:
            fp.write("%s\n" % item)


def connect_db():
    global mydb
    try:
        mydb = MySQL.connect(
            host=HOST, user=DBUSER, password=PASSWORD, database=DATABASE
        )
        logger.info("Connected to MySQL")
    except Exception as e:
        logger.warning(e)


def setuplogger():
    global logger
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-log",
        "--loglevel",
        default="debug",
        help="Provide logging level. Example --loglevel debug, default=debug",
    )
    args = parser.parse_args()
    logger = logging.getLogger()
    logger.setLevel(args.loglevel.upper())

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler("logs.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info("Loggers setup")


def log_rows_affected():
    global row_affected_traslation, row_affected_language_traslation, row_affected_address, row_affected_facility, row_affected_categoty, row_affected_type, row_affected_activity, row_affected_activity_facility, row_affected_availability
    logger.info("Inserted into Translation " + str(row_affected_traslation) + " rows")
    logger.info(
        "Inserted into Language_Translation "
        + str(row_affected_language_traslation)
        + " rows"
    )
    logger.info("Inserted into Address " + str(row_affected_address) + " rows")
    logger.info("Inserted into Facility " + str(row_affected_facility) + " rows")
    logger.info("Inserted into Category " + str(row_affected_categoty) + " rows")
    logger.info("Inserted into Type " + str(row_affected_type) + " rows")
    logger.info("Inserted into Activity " + str(row_affected_activity) + " rows")
    logger.info(
        "Inserted into Activity_Facility "
        + str(row_affected_activity_facility)
        + " rows"
    )
    logger.info(
        "Inserted into Availability " + str(row_affected_availability) + " rows"
    )

    row_affected_traslation = 0
    row_affected_language_traslation = 0
    row_affected_address = 0
    row_affected_facility = 0
    row_affected_categoty = 0
    row_affected_type = 0
    row_affected_activity = 0
    row_affected_activity_facility = 0
    row_affected_availability = 0


# def run():
#     setuplogger()
#     logger.info('Start running Active-Toronto Scraper...')
#     try:
#         getResources()
#         availabilities = getAvalibilities()
#         facilities = getOriginalFacilities(availabilities)
#         connect_db()
#         facilities = get_new_facilities(facilities)
#         facilities = getGeoToFacilities(facilities)
#         facilities = getPhoneUrlToFacilities(facilities)
#         # writeListToTxt("facilities", "w", facilities)
#         insert_data_to_empty_db(availabilities, facilities)
#         logger.info('------------------------------------------------End------------------------------------------------')
#     except Exception as e:
#         logger.warning(e)


def update():
    setuplogger()
    logger.info("Start weekly updating...")
    try:
        getResources()
        availabilities = getAvalibilities()
        facilities = getOriginalFacilities(availabilities)
        connect_db()
        facilities = get_new_facilities(facilities)
        if (len(facilities) != 0 ):
            facilities = getGeoToFacilities(facilities)
            facilities = getPhoneUrlToFacilities(facilities)
            update_db(availabilities, facilities)
        
        logger.info(
            "------------------------------------------------End------------------------------------------------"
        )
    except Exception as e:
        logger.warning(e)


if __name__ == "__main__":
    time.sleep(60)
    update()
    schedule.every().saturday.at("02:00").do(update)
    while 1:
        schedule.run_pending()
        time.sleep(1)
