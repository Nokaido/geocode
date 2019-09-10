import requests
from urllib.parse import quote
import datetime
import json
import pandas as pd
import locale
import pyodbc
import time
import requests
from IPython.display import clear_output
import numpy as np
import datetime
from requests.exceptions import ProxyError
from IPython.display import clear_output





class Code():
    def __init__(self):
        self.country_dict = {"Deutschland": "DE",
                        "Belgien": "BE",
                        "China (Volksrepublik)": "CN",
                        "Finnland": "FI",
                        "Frankreich": "FR",
                        "United Kingdom": "UK",
                        "Grossbritannien": "UK",
                        "Italien": "IT",
                        'Luxemburg': "LU",
                        "Schweiz": "CH",
                        "Österreich": "AT",
                        "Mexiko": "MX",
                        "Niederlande": "NL",
                        "Schweden": "SE",
                        "Tschechische Republik": "CZ",
                        "Spanien": "ES",
                        "Ungarn": "HU",
                        "Singapur": "SG",
                        "Portugal": "PT",
                        "Vereinigte Staaten von Amerika": "US"}
        self.url_adress = 'http://dev.virtualearth.net/REST/v1/Locations'
        self.example_headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

    def geocode_to_json(self, def_id, df, plz = 'TXT_PLZ', city = 'TXT_ORT', country = 'TXT_LAND', street ='TXT_STRASSE', id='ID', save_evry = 0, output='output.csv', encoding='utf-8', debug = False, status = False):
        '''

        :param def_id:
        :param df:
        :param plz:
        :param city:
        :param country:
        :param street:
        :param id:
        :param save_evry:
        :param output:
        :param encoding:
        :param debug:
        :return:
        '''

        if(len(set(['JSON','STATUS','EDIT_DATE']) - set(df.columns.values)) > 0):
            df['JSON'] = '{}'
            df['STATUS'] = 0
            df['EDIT_DATE'] = datetime.datetime.now()

        counter = 0
        start_time = datetime.datetime.now()

        df_out = pd.DataFrame()
        df_out['ID'] = df[id]
        df_out['JSON'] = df['JSON']
        df_out['STATUS'] = df['STATUS']
        df_out['EDIT_DATE'] = df['EDIT_DATE']

        for adr, row in df.query('STATUS == 0').iterrows():
            plz = str(df.loc[adr].TXT_PLZ).replace('/', '').replace('.0', '')
            city = str(df.loc[adr].TXT_ORT).replace('/', ' ')
            country = self.country_dict[str(df.loc[adr].TXT_KURZ)]
            street = str(df.loc[adr].TXT_STRASSE).replace('/', ' ')
            street_number = str(df.loc[adr].TXT_HAUSNR).replace('/', ' ')

            search_string =  street + ' ' + street_number + ', ' + plz + ' ' + city + ' ' + country

            while(True):
                time.sleep(1)
                self.debug('',False,True) #clear output

                self.debug('search_string: ' + search_string, debug)

                adress_coded = quote(street + ' ' + street_number, safe='')
                city_coded = quote(city,safe='')
                search_query = '' + adress_coded + ', ' + plz + ' ' + city_coded + ' ' + country

                self.debug('search_query: ' + search_query, debug)

                server_status = ''
                json_data = '{}'

                request_url = '{}?CountryRegion={}&postalCode={}&locality={}&addressLine={}&key={}'.format(self.url_adress, country, plz, city_coded, adress_coded, def_id)

                self.debug('request_url: ' + request_url, debug)

                web_dict = self.query_REST(request_url, debug)

                if(web_dict['STATUS'] != 0):
                    df.at[adr, 'JSON'] = web_dict['JSON']
                    df.at[adr, 'STATUS'] = web_dict['STATUS']
                    df.at[adr, 'EDIT_DATE'] = datetime.datetime.now()
                    counter +=1
                    self.safe(df,save_evry,counter,output,encoding)
                    break


            self.status(df,counter,start_time,status)


        df_out = pd.DataFrame()
        df_out['ID'] = df[id]
        df_out['JSON'] = df['JSON']
        df_out['STATUS'] = df['STATUS']
        df_out['EDIT_DATE'] = df['EDIT_DATE']
        self.safe(df, 1, counter, output, encoding)
        return df_out

    def status(self, df, counter, start_time, status=True):
        '''


        :param df:
        :param counter:
        :param start_time:
        :param status:
        :return:
        '''

        print('Done: ' + str(df.query('STATUS > 0').shape[0]) + ' ' + str(int(df.query('STATUS > 0').shape[0] / df.shape[0] * 100 * 1000) / 1000) + '%')
        print('Workload: ' + str(df.query('STATUS == 0').shape[0]) + ' to do')
        time_gone = datetime.datetime.now()
        td = time_gone - start_time
        time_togo = (td.total_seconds() / counter) * (df.query('STATUS == 0').shape[0])

        days = int(time_togo / (24 * 60 * 60))
        hours = ((time_togo / (24 * 60 * 60)) - days) * 24
        fin_date = datetime.datetime.now() + datetime.timedelta(days=days, hours=hours)

        print('ETA: ' + str(days) + ' Days and ' + str(hours) + ' Hours ')
        print('Due date: ' + fin_date.strftime('%d.%m.%Y %H:%M'))


    def safe(self, df, save_evry, counter, output, encoding):
        '''

        :param df:
        :param save_evry:
        :param counter:
        :param output:
        :param encoding:
        :return:
        '''
        if (save_evry > 0 and counter % save_evry == 0):
            df.to_csv(output, sep=';', encoding = encoding, index=False)

    def query_REST(self, request_url, debug=False):
        '''


        :param request_url:
        :param debug:
        :return:
        '''
        web_data = '{}'
        web_dict = {'JSON':'{}', 'STATUS':0}

        try:
            with requests.get(request_url, headers=self.example_headers) as url:

                if (url.content.decode().find("coordinates") > 0):
                    web_dict['JSON'] = json.loads(url.content.decode())
                    web_dict['STATUS'] = 1


                    if (web_dict['JSON']['statusDescription'] == 'OK'):
                        self.debug('LATITUDE: ' + str(web_dict['JSON']['resourceSets'][0]['resources'][0]['geocodePoints'][0]['coordinates'][0]) + ' LONGITUDE: ' + str(
                            web_dict['JSON']['resourceSets'][0]['resources'][0]['geocodePoints'][0]['coordinates'][1]), debug)

                    else:
                        self.debug(url.content.decode(), debug)
                else:
                    if (url.content.decode().find("The resource cannot be found.") > 0):
                        self.debug('Adress not found ' + url.content.decode(), debug)
                        web_dict['STATUS'] = 3
                        web_dict['JSON'] = url.content.decode()


                    self.debug('retry because: \n' + url.content.decode(), debug)

                    web_dict['STATUS'] = 3
                    web_dict['JSON'] = url.content.decode()


        except ProxyError as e:
            self.debug('Waiting 1 Min for Proxy', debug)
            time.sleep(60 * 5)
        return web_dict



    def debug(self, message, debug=True, cleanup=False):
        '''


        :param message:
        :param debug:
        :param cleanup:
        :return:
        '''
        if debug:
            print(message)
        if cleanup:
            clear_output()
