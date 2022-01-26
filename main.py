import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
import os
import pickle
import datetime
import schedule
import time
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# here enter the id of your google sheet
SAMPLE_SPREADSHEET_ID_input = '19xxZq03NZ33MZhPlSDik7zNnBu7XZixA-dQ8IHn-FKI'
SAMPLE_RANGE_NAME = "'Availability (EMEA)'!A4:D2000"
TABLE_NAME = 'EUR_table_save.csv'
email_to = 'tbacala@cisco.com'
email_from = 'tavi.bacala@gmail.com'
gmail_password = 'sswivnrebhpfzcdu'

def grab_data():
    global values_input, service
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'my_json_file.json', SCOPES)  # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
                                      range=SAMPLE_RANGE_NAME).execute()
    values_input = result_input.get('values', [])
    new_data = pd.DataFrame(values_input[1:], columns=values_input[0]).set_index('SKU')[
        ['Estimated Lead Time (calendar days) if booked this week']]
    return new_data

def send_table(from_email, to_email, subject, table):
    if table is not None:
        html = """\
                <html>
                  <head></head>
                  <body>
                    {0}
                  </body>
                </html>
                """.format(table.to_html())
    else:
        html = """\
                <html>
                  <head></head>
                  <body>
                    {0}
                  </body>
                </html>
                """.format(f'I checked, nothing changed from {time.ctime(os.path.getmtime(TABLE_NAME))}')
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    part2 = MIMEText(html, 'html')
    msg.attach(part2)

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    server.login(email_from, gmail_password)
    server.sendmail(from_email, [to_email], msg.as_string())
    server.close()

def job():
    new_data = grab_data().fillna(0).replace('', 0).replace('None', 0)
    old_data = pd.read_csv(TABLE_NAME).set_index('SKU')

    combined = old_data.join(new_data, how='outer', lsuffix=' old', rsuffix=' new')
    try:
        combined['Difference'] = (combined['Estimated Lead Time (calendar days) if booked this week new'].astype(float) -
                                  combined['Estimated Lead Time (calendar days) if booked this week old'].astype(float))
        combined = combined[combined['Difference'] != 0.0]
        new_data.to_csv(TABLE_NAME)
        if len(combined) > 0:
            combined = combined.drop('Difference', axis=1)
            send_table(email_from, email_to, f'There was some change {datetime.datetime.today():%Y-%m-%d}', combined)
        else:
            send_table(email_from, email_to, f'There was no change {datetime.datetime.today():%Y-%m-%d}', None)
    except Exception as e:
        print(str(e))
        send_table(email_from, email_to, f'There were some errors on {datetime.datetime.today():%Y-%m-%d}', combined)

if __name__ == "__main__":
    job()
    schedule.every().day.at("09:00").do(job)
    while True:
        schedule.run_pending()
        time.sleep(60)  # wait one minute
