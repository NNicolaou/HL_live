from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import os
from pandas.errors import EmptyDataError

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
SPREADSHEET_ID = '1Efpnu_YVffR5Dtu6qO4RBgcjopsMWc3cTo7rk8j8VAY'
def get_sheet_service_from_google_credentials(credentials_file_name: str='horatio_investments', generated_token_name: str='token'):
    dirname = os.path.dirname(__file__)
    token_file = os.path.join(dirname, generated_token_name + '.json')
    credentials_file = os.path.join(dirname, credentials_file_name + '.json')

    google_store = file.Storage(token_file)
    google_creds = google_store.get()
    if not google_creds or google_creds.invalid:
        flow = client.flow_from_clientsecrets(credentials_file, SCOPES)
        google_creds = tools.run_flow(flow, google_store)
    sheets_service = build('sheets', 'v4', http=google_creds.authorize(Http()))
    return sheets_service

parameter_sheet_service = get_sheet_service_from_google_credentials()


def get_range_format(tab_name, start_column_letter='', start_row_number='', end_column_letter='', end_row_number=''):
    if end_column_letter == '' and end_row_number == '':
        return tab_name + '!' + start_column_letter + str(start_row_number)
    else:
        return tab_name + '!' + start_column_letter + str(start_row_number) + ':' + \
               end_column_letter + str(end_row_number)

def get_formatted_update_value(tab_name, column_letter, row_number, new_value):
    data = {
        'values': [[new_value]],
        'range': get_range_format(tab_name, column_letter, row_number)
    }
    return data

def get_cell_value(tab_name, column_letter, row_number, sheet_id, sheets_service):
    cell = get_range_format(tab_name, start_column_letter=column_letter, start_row_number=row_number)
    response = sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=cell).execute()
    if 'values' in response:
        return response['values'][0][0]
    else:
        raise EmptyDataError

def get_range_values(tab_name, sheet_id, sheets_service, start_column_letter='',
                     start_row_number='', end_column_letter='', end_row_number=''):
    ranges = get_range_format(tab_name, start_column_letter, start_row_number, end_column_letter, end_row_number)
    response = sheets_service.spreadsheets().values().batchGet(spreadsheetId=sheet_id, ranges=ranges).execute()
    return response['valueRanges'][0]['values']


def get_sheet_column_letters(column_number: int):
    if column_number > 0:
        alphabet_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K',
                        12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U',
                        22: 'V', 23: 'W', 24: 'X', 25: 'Y', 26: 'Z'}
        column_letter = ''
        while column_number > 0:
            column_number_remainder = column_number % 26
            if column_number_remainder == 0:
                column_letter = column_letter + 'Z'
                column_number = column_number / 26 - 1
            else:
                column_letter = column_letter + alphabet_map[column_number_remainder]
                if column_number / 26 < 1:
                    break
                else:
                    # the use of "//" - https://www.python.org/dev/peps/pep-0238/
                    column_number = column_number // 26
        return column_letter[::-1]
    else:
        raise AttributeError

def send_batch_update(data, sheet_id, sheets_service):
    body = {
        'valueInputOption': 'USER_ENTERED',
        'data': data
    }
    sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

def append_values(sheet_id, values, range, major_dimension, sheets_service, **kwargs):
    """Appends values to a spreadsheet.
    Reference: `request <https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append>`__
    :param spreadsheet_id:      The ID of the spreadsheet to update.
    :param values:              The values to be appended in the body. list or list of list
    :param major_dimension:     The major dimension of the values provided (e.g. row or column first?)
    :param range:               The A1 notation of a range to search for a logical table of data.
                                Values will be appended after the last row of the table.
    :param kwargs:              Query & standard parameters (see reference for details).
    """
    body = {
        'values': values,
        'majorDimension': major_dimension
    }
    request = sheets_service.spreadsheets().values().append(spreadsheetId=sheet_id,
                                                            range=range,
                                                            body=body,
                                                            valueInputOption=kwargs.get('valueInputOption', None),
                                                            insertDataOption=kwargs.get('insertDataOption', None))
    return request.execute()

def append_rows(sheet_id, values, range, sheets_service):
    return append_values(sheet_id=sheet_id, values=values, range=range, major_dimension='ROWS',
                         sheets_service=sheets_service, insertDataOption='INSERT_ROWS', valueInputOption='USER_ENTERED')

def append_columns(sheet_id, values, range, sheets_service):
    return append_values(sheet_id=sheet_id, values=values, range=range, major_dimension='COLUMNS',
                         sheets_service=sheets_service, insertDataOption='INSERT_COLUMNS', valueInputOption='USER_ENTERED')

if __name__ == '__main__':
    print(get_range_values('nnb distribution', SPREADSHEET_ID, parameter_sheet_service, start_column_letter='A',
                     end_column_letter='J'))