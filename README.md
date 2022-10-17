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
