# Implementación de Bodega de Datos

Desarrollado por: **Caleb Damian Naranjo Albuja**

## Requisitos

* Instalar MySQL Workbench 8.0 y MySQL Server usando MySQL Installer 8.0.31
* Replique el modelo de datos presente en la carpeta evidencesSem3/Diagrama_ModeloDatos_CDNA_Staging
* Descargue e instale python 3.10.8 (https://www.python.org/downloads/), recuerde marcar la casilla "Add Python to PATH". 
* Descargue e instale GIT (https://git-scm.com/downloads)
* Descargue el código zip del repo de github o clone el repositorio de git con el terminal de Visual Studio Code
---
### Clonar el repositorio
Para clonar el repositorio, ejecuta:
```bash
git clone https://github.com/calebdamian/DWImplementation.git
```
---

* Descomprima y abra la carpeta con Visual Studio Code (si ha decidido descargar el zip en lugar de clonar el repositorio) 
* En Visual Studio Code instale las extensiones necesarias de python como: Python y Pylance, también recomiendo instalar las extensiones Rainbow CSV y Code Runner para una mejor experiencia de desarrollo
* Ejecute el siguiente comando con VsCode Terminal: 
```bash
pip install PyMySQL
```
### Configuración de la base de datos

1. Ir al archivo .properties 
2. Cambie los valores de [DatabaseSection] con su propia información

Su archivo .properties debería tener este aspecto (tenga en cuenta que debe modificar los valores con la información de su propia base de datos):
```
[DatabaseSection]
DB_TYPE = mysql
DB_HOST = localhost
DB_PORT = SuPuerto
DB_USER = SuDbUser
DB_PWD = SuDbPWd
STG_NAME = NombreBaseDeDatosStaging
SOR_NAME = NombreBaseDeDatosSOR
[CSVSection]
CHANNELS_PATH= csvs/channels.csv
COUNTRIES_PATH= csvs/countries.csv
CUSTOMERS_PATH = csvs/customers.csv
PRODUCTS_PATH = csvs/products.csv
PROMOTIONS_PATH = csvs/promotions.csv
SALES_PATH = csvs/sales.csv
TIMES_PATH = csvs/times.csv
```
### Verificar la conexión a la base de datos
* Diríjase al archivo py_startup.py
 y reemplace su contenido con lo siguiente: 
```
# Testing file

import configparser
import traceback
from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times
from util.db_connection import Db_Connection


try:
    #ext_channels()
    #ext_countries()
    #ext_customers()
    #ext_products()
    #ext_promotions()
    #ext_sales()
    #ext_times()
    config = configparser.ConfigParser()
    config.read(".properties")
    config.get("DatabaseSection", "DB_TYPE")
    sectionName = "DatabaseSection"
    stg_conn = Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
    )    

    conn = stg_conn.start()
    print(conn)
    conn.dispose()
except:
    traceback.print_exc()
finally:
    pass
```
* Ejecute el código
* Deberá observar un resultado similar a este si la configuración de la base de datos está realizada correctamente:

```
Engine(mysql+pymysql://root:***@localhost:3306/cdnastg)
```
Ahora puede cambiar el contenido del archivo py_startup.py eliminando las líneas de código agregadas y dejando las líneas que tienen un "#", el cual debe ser retirado.

# Ejecutar el proyecto

Diríjase al archivo py_startup.py y ejecútelo con el terminal de Visual Studio Code hecho para Python
## Verificar Funcionamiento

* Ingrese a su MySQL Workbench
* Acceda al servidor con su usuario y contraseña correspondientes
* Ejecute las siguientes instrucciones en una nueva pestaña de consultas (recuerde cambiar el nombre de la base de datos por el correspondiente):
```
USE NombreBaseDeDatosStaging;
SELECT COUNT(CHANNEL_ID) AS Channels_Count FROM CHANNELS;
SELECT COUNT(COUNTRY_ID) AS Countries_Count FROM COUNTRIES;
SELECT COUNT(CUST_ID) AS Customers_Count FROM CUSTOMERS;
SELECT COUNT(PROD_ID) AS Products_Count FROM PRODUCTS;
SELECT COUNT(PROMO_ID) AS Promotions_Count FROM PROMOTIONS;
SELECT COUNT(PROD_ID) AS Sales_Count FROM SALES;
SELECT COUNT(TIME_ID) AS Times_Count FROM TIMES;
```
### Resultado de la ejecución
Deberá obtener los siguientes valores luego de una ejecución correcta de la sentencia SQL y del proyecto de Python:
* Channels_Count: 5
* Countries_Count: 23
* Customers_Count: 55500
* Products_Count: 72
* Promotions_Count: 503
* Sales_Count: 918843
* Times_Count: 1826

---
---
---
### English Translation

# Data Warehouse Implementation

Developed by: **Caleb Damian Naranjo Albuja**

## Requirements

* Install MySQL Workbench 8.0 and MySQL Server using MySQL Installer 8.0.31
* Download and install python 3.10.8 (https://www.python.org/downloads/), remember to check the "Add Python to PATH" box 
* Download and install GIT (https://git-scm.com/downloads)
* Download the zip code from github repo | Clone the git repo with VsCode Terminal 
For cloning the repo, run:
```bash
git clone https://github.com/calebdamian/DWImplementation.git
```


* Unzip and open the folder with VSCode (if you decided to download the zip instead of cloning the repo) 
* At VSCode install the python required extensions such as: Python and Pylance, I also recommend to install Rainbow CSV and Code Runner extensions for better dev experience
* Run the following command with VsCode Terminal: 
```bash
pip install PyMySQL
```
### Database Setup

1. Go to .properties file 
2. Change the [DatabaseSection] values with your own information

Your .properties file should look like this (please note you should modify the values to your own database information):
```
[DatabaseSection]
DB_TYPE = mysql
DB_HOST = localhost
DB_PORT = YourPort
DB_USER = YourDbUser
DB_PWD = YourDbPWd
STG_NAME = YourStagingDatabaseName
SOR_NAME = YourSORDatabaseName
[CSVSection]
CHANNELS_PATH= csvs/channels.csv
COUNTRIES_PATH= csvs/countries.csv
CUSTOMERS_PATH = csvs/customers.csv
PRODUCTS_PATH = csvs/products.csv
PROMOTIONS_PATH = csvs/promotions.csv
SALES_PATH = csvs/sales.csv
TIMES_PATH = csvs/times.csv
```

## Running the project

* Go to py_startup.py and run it with VsCode Python Terminal
