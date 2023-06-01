import requests
import pandas as pd
from datetime import datetime, timedelta

# CSV writer for demo purposes
import csv

TIME_PERIOD = 1

START_DATE = (datetime.utcnow() - timedelta(days=TIME_PERIOD)).strftime("%Y-%m-%d")
END_DATE = (datetime.utcnow() - timedelta(days=TIME_PERIOD)).strftime("%Y-%m-%d")

CREDENTIALS = {
    'client_id': "<CLIENT_ID>",
    'client_secret': "<CLIENT_SECRET>",
    'grant_type': "refresh_token",
    'refresh_token': "<REFRESH_TOKEN>",
    'manager_customer_id': "<MANAGER_CUSTOMER_ID>",
}

REFRESH_TOKEN_API = "https://oauth2.googleapis.com/token"
LOCAL_SERVICE_ADS_API = "https://localservices.googleapis.com/v1"

def get_access_token(url, credentials):
    access_token = ""
    params = {
        'client_id': credentials['client_id'],
        'client_secret': credentials['client_secret'],
        'grant_type': credentials['grant_type'],
        'refresh_token': credentials['refresh_token']
    }

    response = requests.post(url=url, params=params)
    print(response)
    result = response.json()
    if 'access_token' in result.keys():
        access_token = result['access_token']

    return access_token

def get_detailed_lead_reports(url, access_token):
    results = []
    api_url = url + "/detailedLeadReports:search"

    headers = {
        "Authorization": "Bearer %s" % (access_token),
        "Content-Type": "application/json"
    }

    params = {
        "query": "manager_customer_id:" + CREDENTIALS['manager_customer_id'],
        "startDate.day": datetime.strptime(START_DATE, "%Y-%m-%d").day,
        "startDate.month": datetime.strptime(START_DATE, "%Y-%m-%d").month,
        "startDate.year": datetime.strptime(START_DATE, "%Y-%m-%d").year,
        "endDate.day": datetime.strptime(END_DATE, "%Y-%m-%d").day,
        "endDate.month": datetime.strptime(END_DATE, "%Y-%m-%d").month,
        "endDate.year": datetime.strptime(END_DATE, "%Y-%m-%d").year,
        "pageSize": 100
    }

    print(params)

    while True:
        response = requests.get(url=api_url, headers=headers, params=params)
        print(response.status_code)
        
        if response.status_code == 200:
            for row in response.json()['detailedLeadReports']:
                data = [
                    START_DATE,
                    row['leadId'] if 'leadId' in row.keys() else "",
                    row['accountId'] if 'accountId' in row.keys() else "",
                    row['businessName'] if 'businessName' in row.keys() else "",
                    row['leadCreationTimestamp'] if 'leadCreationTimestamp' in row.keys() else "",
                    row['leadType'] if 'leadType' in row.keys() else "",
                    row['leadCategory'] if 'leadCategory' in row.keys() else "",
                    row['geo'] if 'geo' in row.keys() else "",
                    row['phoneLead']['chargedCallTimestamp'] if 'chargedCallTimestamp' in row.get('phoneLead', {}) else "",
                    row['phoneLead']['chargedConnectedCallDurationSeconds'] if 'chargedConnectedCallDurationSeconds' in row.get('phoneLead', {}) else "",
                    row['phoneLead']['consumerPhoneNumber'] if 'consumerPhoneNumber' in row.get('phoneLead', {}) else "",
                    row['chargeStatus'] if 'chargeStatus' in row.keys() else "",
                    row['currencyCode'] if 'currencyCode' in row.keys() else "",
                    row['disputeStatus'] if 'disputeStatus' in row.keys() else "",
                    row['timezone']['id'] if 'id' in row.get('timezone', {}) else ""
                ]

                print(row['leadId'])

                results.append(data)

            if 'nextPageToken' not in response.json().keys():
                break
            else:
                params['pageToken'] = response.json()['nextPageToken']
        else:
            break

    return results


access_token = get_access_token(REFRESH_TOKEN_API, CREDENTIALS)
print('Access Token: '+access_token)

if access_token:
    csv_headers = [
        'date',
        'leadId',
        'accountId',
        'businessName',
        'leadCreationTimestamp',
        'leadType',
        'leadCategory',
        'geo',
        'phoneLead_chargedCallTimestamp',
        'phoneLead_chargedConnectedCallDurationSeconds',
        'phoneLead_consumerPhoneNumber',
        'chargeStatus',
        'currencyCode',
        'disputeStatus',
        'timezone_id'
    ]

    detailed_lead_reports = get_detailed_lead_reports(LOCAL_SERVICE_ADS_API, access_token)

    filename = "detailed_lead_reports_%s_%s.csv" % (START_DATE, END_DATE)
    # writing to csv file
    with open(filename, 'w', newline='', encoding="utf-8") as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(csv_headers)
        # writing the data rows
        csvwriter.writerows(detailed_lead_reports)

        print("Finished writing to CSV file: %s" % filename)
        print(filename)