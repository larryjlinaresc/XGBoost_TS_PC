###########################################################################################################
######################## WE CREATE A DATAS SET FROM THE DATA OF REE( RED ELECTRICA ESPAÑA) ################
###########################################################################################################
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time

def fetch_data(start_date, end_date):
    base_url = 'https://apidatos.ree.es/es/datos/demanda/demanda-tiempo-real'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'time_trunc': 'hour'
    }

    # Enviar la solicitud GET
    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        print("{} and {}".format(start_date,end_date))
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.json())
        print("{} and {}".format(start_date,end_date))
        return None

# Definir el período de tiempo total
start_date = datetime(2014, 12, 1)
end_date = datetime(2023, 12, 31)

# Lista para almacenar todos los datos
all_data = []

current_start_date = start_date

while current_start_date < end_date:
    current_end_date = current_start_date + timedelta(days=20)
    if current_end_date > end_date:
        current_end_date = end_date

    # Formatear las fechas para la solicitud
    formatted_start_date = current_start_date.strftime('%Y-%m-%dT%H:%M')
    formatted_end_date = current_end_date.strftime('%Y-%m-%dT%H:%M')
    #print(all_data)
    # Obtener datos para el intervalo actual
    
    data = fetch_data(formatted_start_date, formatted_end_date)
    if data:
        for entry in data['included']:
            for record in entry['attributes']['values']:
                record_date = record['datetime']
                record_value = record['value']
                all_data.append({'datetime': record_date, 'value': record_value})

    # Avanzar al siguiente intervalo
    current_start_date = current_end_date + timedelta(days=1)
    time.sleep(10)

# Convertir los datos a un DataFrame de pandas
df = pd.DataFrame(all_data)

# Guardar los datos en un archivo CSV
df.to_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\consumo_mw_por_hora.csv', index=False)

print("Datos guardados en 'consumo_mw_por_hora.csv'")


###########################################################################################################
######################## WE MATCH THE CONSUME DATA WITH THE TEMPATURE IN THTA RANGE OF TIME ###############
###########################################################################################################


from datetime import datetime, timedelta
import time
# Reemplaza 'YOUR_API_KEY' con tu clave de API de AEMET
API_KEY = 'XXXX'

BASE_URL = 'https://opendata.aemet.es/opendata/api' 

# Lista de estaciones meteorológicas importantes en la península ibérica 
stations = [
    '3195',  # Madrid
    '0076',   # Barcelona
    '8414A',   # Valencia
    '5783',   # Sevilla
    '9390',  # Zaragoza
    '6156X',  # Málaga
    '1690A',   # Bilbao
    '2867',   # Salamanca
    '2539',   # Valladolid    
    '3129',
    # Añade más estaciones si es necesario
]

# Función para obtener el endpoint de datos históricos de una estación específica
def get_station_data(station_id, start_date, end_date):
    url = f'{BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_date}T00:00:00UTC/fechafin/{end_date}T23:59:59UTC/estacion/{station_id}/'
    params = {
        'api_key': API_KEY
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        response_data = response.json()
        print(response.json())
        if 'datos' in response_data:
            data_url = response_data['datos']
            data_response = requests.get(data_url)
            if data_response.status_code == 200:
                return data_response.json()
            else:
                print(f"Error fetching data from {data_url}: {data_response.status_code}")
                return []
        else:
            print("Error: 'datos' key not found in the response.")
            return []
    else:
        print(f"Error: {response.status_code}")
        print(response.json())
        return []

def calculate_daily_average_temperature(weather_data):
    df = pd.DataFrame(weather_data)
    print(df)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['tmed'] = df['tmed'].str.replace(',', '.').astype(float)
    df['temp'] = pd.to_numeric(df['tmed'], errors='coerce')
    print(df)
    # Filtrar filas con datos válidos
    df = df.dropna(subset=['temp'])
    print(df)
    # Calcular la temperatura promedio diaria para todas las estaciones
    daily_avg_temp = df.groupby('fecha')['temp'].mean().reset_index()
    print(df)
    # Expandir la temperatura diaria promedio a cada hora del día
    daily_avg_temp['hour'] = daily_avg_temp['fecha'].apply(lambda x: [x + timedelta(hours=h) for h in range(24)])
    hourly_avg_temp = daily_avg_temp.explode('hour').rename(columns={'hour': 'datetime', 'temp': 'avg_temp'})
    
    return hourly_avg_temp[['datetime', 'avg_temp']]

# Obtener datos históricos de AEMET en intervalos de 20 días
start_date = datetime(2014, 12, 1)
end_date = datetime(2023, 12, 31)
delta = timedelta(days=20)

all_weather_data = []

while start_date < end_date:
    interval_end_date = start_date + delta
    if interval_end_date > end_date:
        interval_end_date = end_date
    
    for station_id in stations:
        data = get_station_data(station_id, start_date.strftime('%Y-%m-%d'), interval_end_date.strftime('%Y-%m-%d'))
        all_weather_data.extend(data)
        time.sleep(10)
    
    start_date = interval_end_date + timedelta(days=1)
    

# Calcular la temperatura promedio por hora
average_hourly_temperature = calculate_daily_average_temperature(all_weather_data)

# Guardar los resultados en un archivo CSV
average_hourly_temperature.to_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\average_hourly_temperature.csv', index=False)

# Mostrar los resultados
print(f"Temperatura promedio por hora en la península ibérica desde 2014-12-01 hasta 2023-12-31:")
print(average_hourly_temperature)



# Leer el archivo CSV existente con datos de temperatura promedio por hora
df = pd.read_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\average_hourly_temperature.csv')

# Convertir la columna 'datetime' a objetos datetime
df['datetime'] = pd.to_datetime(df['datetime'])

# Crear un nuevo DataFrame para almacenar los datos transformados
transformed_data = []

# Iterar sobre cada fila en el DataFrame original
for index, row in df.iterrows():
    # Obtener la fecha y la temperatura promedio
    current_datetime = row['datetime']
    avg_temp = row['avg_temp']
    
    # Generar entradas de 10 minutos dentro de la misma hora
    for i in range(6):
        new_datetime = current_datetime + timedelta(minutes=i*10)
        transformed_data.append([new_datetime, avg_temp])

# Crear un DataFrame a partir de los datos transformados
transformed_df = pd.DataFrame(transformed_data, columns=['datetime', 'avg_temp'])

# Formatear la columna 'datetime' en el formato deseado
transformed_df['datetime'] = transformed_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000+01:00')

# Guardar los datos transformados en un nuevo archivo CSV
transformed_df.to_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\transformed_average_temperature.csv', index=False)

# Mostrar una muestra de los resultados transformados
print(transformed_df.head(20))





# Leer los dos archivos CSV
temperature_df = pd.read_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\transformed_average_temperature.csv')
energy_df = pd.read_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\consumo_mw_por_hora.csv')  # Cambia 'energy_consumption.csv' por el nombre de tu archivo


# Asegurarse de que las columnas 'datetime' están en formato datetime
temperature_df['datetime'] = pd.to_datetime(temperature_df['datetime'], utc=True)
energy_df['datetime'] = pd.to_datetime(energy_df['datetime'], utc=True)

# Eliminar la información de zona horaria
temperature_df['datetime'] = temperature_df['datetime'].dt.tz_convert(None)
energy_df['datetime'] = energy_df['datetime'].dt.tz_convert(None)

# Realizar la unión de los dos DataFrames en la columna 'datetime'
merged_df = pd.merge(energy_df, temperature_df, on='datetime', how='inner')

# Renombrar las columnas según se requiera
merged_df = merged_df.rename(columns={'datetime': 'Date', 'value': 'consume_energy', 'avg_temp': 'Temperature'})

# Guardar el DataFrame fusionado en un nuevo archivo CSV
merged_df.to_csv('C:\\Users\\larry\\Documents\\Power_demand\\XGBoost_TS_PC\\Data\\merged_energy_temperature.csv', index=False)

# Mostrar una muestra del DataFrame resultante
print(merged_df.head(20))
