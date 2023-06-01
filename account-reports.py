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

def get_account_reports(url, access_token):
    results = []
    api_url = url + "/accountReports:search"

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
            for row in response.json()['accountReports']:
                data = [
                    START_DATE,
                    row['accountId'] if 'accountId' in row.keys() else "",
                    row['businessName'] if 'businessName' in row.keys() else "",
                    row['averageWeeklyBudget'] if 'averageWeeklyBudget' in row.keys() else "",
                    row['averageFiveStarRating'] if 'averageFiveStarRating' in row.keys() else "",
                    row['totalReview'] if 'totalReview' in row.keys() else "",
                    row['phoneLeadResponsiveness'] if 'phoneLeadResponsiveness' in row.keys() else "",
                    row['currentPeriodChargedLeads'] if 'currentPeriodChargedLeads' in row.keys() else "",
                    row['previousPeriodChargedLeads'] if 'previousPeriodChargedLeads' in row.keys() else "",
                    row['currentPeriodTotalCost'] if 'currentPeriodTotalCost' in row.keys() else "",
                    row['previousPeriodTotalCost'] if 'previousPeriodTotalCost' in row.keys() else "",
                    row['currencyCode'] if 'currencyCode' in row.keys() else "",
                    row['currentPeriodPhoneCalls'] if 'currentPeriodPhoneCalls' in row.keys() else "",
                    row['previousPeriodPhoneCalls'] if 'previousPeriodPhoneCalls' in row.keys() else "",
                    row['currentPeriodConnectedPhoneCalls'] if 'currentPeriodConnectedPhoneCalls' in row.keys() else "",
                    row['previousPeriodConnectedPhoneCalls'] if 'previousPeriodConnectedPhoneCalls' in row.keys() else "",
                    row['impressionsLastTwoDays'] if 'impressionsLastTwoDays' in row.keys() else ""
                ]

                print(row['accountId'])

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
        'accountId',
        'businessName',
        'averageWeeklyBudget',
        'averageFiveStarRating',
        'totalReview',
        'phoneLeadResponsiveness',
        'currentPeriodChargedLeads',
        'previousPeriodChargedLeads',
        'currentPeriodTotalCost',
        'previousPeriodTotalCost',
        'currencyCode',
        'currentPeriodPhoneCalls',
        'previousPeriodPhoneCalls',
        'currentPeriodConnectedPhoneCalls',
        'previousPeriodConnectedPhoneCalls',
        'impressionsLastTwoDays'
    ]

    account_reports = get_account_reports(LOCAL_SERVICE_ADS_API, access_token)

    filename = "account_reports_%s_%s.csv" % (START_DATE, END_DATE)
    # writing to csv file
    with open(filename, 'w', newline='', encoding="utf-8") as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(csv_headers)
        # writing the data rows
        csvwriter.writerows(account_reports)

        print("Finished writing to CSV file: %s" % filename)
        print(filename)