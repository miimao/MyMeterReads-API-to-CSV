import logging
from time import sleep
import pandas
import requests
import json
from datetime import date, timedelta
import base64
import smtplib
import secrets
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filename="MeterReaderDataGen.log",
)

def generate_cw_token(Company_ID, Public_Key, Private_Key):
    token = "{}+{}:{}".format(Company_ID, Public_Key, Private_Key)
    token = base64.b64encode(bytes(token, "utf-8"))
    token = token.decode("utf-8")
    return token


def last_20_Lines_of_log_file(fname):
    log = ""
    with open(fname) as file:
        for line in file.readlines()[-20:]:
            log += f"\n{line}"
    return log

def remove_excluded_propertys(meter_read_json,excluded_propertys_list):
    removed = 0
    meter_read_removed_exclusions_json = []
    for read in meter_read_json:
        if read["property_name"] in excluded_propertys_list:
            removed += 1
        else:
            meter_read_removed_exclusions_json.append(read)
    logging.info(f"Un-Modified List: {len(meter_read_json)}, Modified List: {len(meter_read_removed_exclusions_json)}, Removed: {removed}")
    return meter_read_removed_exclusions_json

email = secrets.email
email_password = secrets.email_password
company_id = secrets.company_id
cw_manage_public = secrets.cw_manage_public
cw_manage_private = secrets.cw_manage_private
client_id = secrets.client_id
cw_token = generate_cw_token(company_id, cw_manage_public, cw_manage_private)


#Get Yesterdays date
date = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
logging.info("Starting MyMeterReader Data Collection")
try:
    url = f"{secrets.mymeterreadurl}{date}{secrets.mymeterreadkey}"

    payload = {}
    headers = {}

    response_meter_read = requests.request(
        "GET", url, headers=headers, data=payload
    )
    if response_meter_read.status_code != 200:
        logging.error(f"MyMeterReads did not respond with an Success Code of 200. Status code recieved: {response_meter_read.status_code}")
        logging.error(f"Response Data: {response_meter_read.content}")
        raise ValueError("MyMeterReads did not respond with an Success Code of 200")

    else:
        if secrets.excluded_propertys != []:
            logging.info(f"Removing {secrets.excluded_propertys} from the meter read data set")
            meter_read_dict = remove_excluded_propertys(response_meter_read.json(),secrets.excluded_propertys)
        else:
            meter_read_dict = response_meter_read.json()
        meter_read = pandas.DataFrame.from_dict(meter_read_dict)
        meter_read.to_csv(f"{secrets.ftpdir}/meter_read_{date}.csv", index=False)
        with smtplib.SMTP("smtp.office365.com", 587) as smtp:
            smtp.starttls()
            smtp.ehlo()
            smtp.login(email, email_password)
            subject = f"MyMeterReads Data Import File Generated {date}"
            body = "The MyMeterRead data import script was successful."
            msg = f"Subject: {subject}\n\n{body}"
            smtp.sendmail(email, "admin@ctcatlanta.net", msg)
        logging.info(f"Meter Read for {date} Successful, FILE: meter_read_{date}.csv")

except Exception as Argument:
    logging.exception("Error occurred while Trying to Generate MeterReadData")
    log = last_20_Lines_of_log_file("MeterReaderDataGen.log")
    try:
        with smtplib.SMTP("smtp.office365.com", 587) as smtp:
            smtp.starttls()
            smtp.ehlo()
            smtp.login(email, email_password)
            subject = f"MyMeterReads Data Import File FAIL! {date}"
            body = f"Please see the linked KB article for information on this.\nhttps://kb.ctcatlanta.net/books/non-site-specific-information/page/mymeterreads-data-import-script\n\nLAST 20 LINES OF LOG FILE\n{log}"
            msg = f"Subject: {subject}\n\n{body}"
            smtp.sendmail(email, "admin@ctcatlanta.net", msg)
    except:
        logging.exception("Error occurred while Trying to Send Email Alert of MeterReadData Failure")
    headers_cw = {
        "Authorization": "Basic " + cw_token,
        "clientId": client_id,
        "Accept": "*/*",
        "Content-Type": "application/json",
    }
    ticket_posted = False
    while ticket_posted != True:
        logging.info("Attempting to post alert ticket to ConnectWise Manage")
        try:
            # Get the current codebase form cw and use it to make the url used to post a ticket
            url = f"https://na.myconnectwise.net/login/companyinfo/connectwise"
            payload = {}
            response_cw_info_json = requests.request(
                "GET", url, headers=headers_cw, data=payload
            ).json()
            cw_url = (
                f"https://na.myconnectwise.net/"
                + response_cw_info_json["Codebase"]
                + "apis/3.0"
            )
            payload = {
                "summary": "MyMeterReads Data Import Script FAIL",
                "board": {"id": 21, "name": "Alerts"},
                "company": {"id": 19300, "identifier": "HOADV"},
                "initialDescription": f"Please see the linked KB article for information on this.\nhttps://kb.ctcatlanta.net/books/non-site-specific-information/page/mymeterreads-data-import-script\n\nLAST 20 LINES OF LOG FILE\n{log}",
                "type": {"id": 367, "name": "Property Specific System"},
                "subType": {"id": 748, "name": "Water Meter Reader"},
                "item": {"id": 24, "name": "Failure"},
            }

            response_cw_ticket_post_json = requests.request(
                "POST", f"{cw_url}/service/tickets", headers=headers_cw, json=payload
            )
            logging.info("Posted Alert Ticket to ConnectWise")
            if response_cw_ticket_post_json.status_code != 201:
                logging.error(
                    "ConnectWise Ticket Alert Post did not respond with Success Code 201"
                )
                raise ValueError(
                    "ConnectWise Ticket Alert Post did not respond with Success Code 201"
                )
            else:
                ticket_posted = True
        except:
            logging.error(
                "Unable to Post ticket to ConnectWise. Trying again in 5 minutes."
            )
            sleep(300)
