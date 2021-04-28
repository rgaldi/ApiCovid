import pandas as pd
import numpy as np
import os
import psycopg2 

#Tratando DataFrame 
curr_dir = os.path.dirname(os.path.realpath(__file__))

csv_casos = pd.read_csv(f'{curr_dir}/utils/caso.csv')
csv_casos = csv_casos.dropna(subset=['city'])
# csv_casos = csv_casos[3000:4500]

conn = psycopg2.connect(host='localhost', dbname='covid19', user='postgres', password='Papag@1o')

#Criando tabela
with conn:
    with conn.cursor() as cur:
        try:
            print("Criando tabela...")
            cur.execute(""" CREATE TABLE public.covid19
            (
                date date,
                states text,
                city text,
                place_type text,
                confirmed text,
                deaths text,
                order_for_place text,
                is_last text,
                estimated_population_2019 text,
                estimated_population text,
                city_ibge_code text,
                confirmed_per_100k text,
                death_rate text
            )
            WITH (
                OIDS = FALSE
            );

            ALTER TABLE public.covid19
                OWNER to postgres;""")
            conn.commit()
            print("Tabela Criada!")
        except:
            pass
            print("Tabela já foi criada...")

lista_teste = []
print("Lendo Arquivo...")
for k,v in csv_casos.iterrows():
    date = v['date']    
    state = v['state']
    city = v['city']
    place_type = v['place_type']
    confirmed = v['confirmed']
    deaths = v['deaths']
    order_for_place = v['order_for_place']
    is_last = v['is_last']
    estimated_population_2019 = v['estimated_population_2019']
    estimated_population = v['estimated_population']
    city_ibge_code = v['city_ibge_code']
    confirmed_per_100k_inhabitants = v['confirmed_per_100k_inhabitants']
    death_rate = v['death_rate']
    
    lista_teste.append([date,
    state,
    city,
    place_type,
    confirmed,
    deaths,
    order_for_place,
    is_last,
    estimated_population_2019,
    estimated_population,
    city_ibge_code,
    confirmed_per_100k_inhabitants,
    death_rate])

with conn:
    with conn.cursor() as cur:
        print("Enviando para o banco...")
        cur.executemany(f"""INSERT INTO covid19(
        date, states, city, place_type, confirmed, deaths, order_for_place, is_last, estimated_population_2019,
        estimated_population, city_ibge_code, confirmed_per_100k, death_rate)
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) """, lista_teste)
        conn.commit()


print("Enviado!")
cur.close()
conn.close()
