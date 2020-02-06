from GeoCode import Code
import pandas as pd

df = pd.read_csv('output.csv', sep=';', encoding='utf-8')
code = Code()
df = code.geocode_to_json(def_id='AlhBq-JK5uTCezWtwHV9ZrTho_vS_2uy4TIlVae2Q1amIzDa9amSrOUtiJnxWSLO',
                          df=df,country='TXT_KURZ',
                          id='ID_PARTNER',
                          save_evry=100,
                          debug = True,
                          status= True)