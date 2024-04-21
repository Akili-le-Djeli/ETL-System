import sqlite3
import pandas as pd
import numpy as np
import pickle
from sklearn.preprocessing import MinMaxScaler

#Daten extraction 
#dateipfad = r"C:\\Users\\almueller\\Downloads\\continuous_factory_process.csv"
dateipfad = r"C:\Users\Asus\Documents\MA_ETL-System\continuous_factory_process.csv"

def create_database_schema(db_name='newcode.db'):
    # Verbindung zur SQLite-Datenbank herstellen (falls nicht vorhanden, wird eine neue erstellt)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Faktentabelle für Zielgroesse erstellen
    cursor.execute("DROP TABLE IF EXISTS Zielgroesse")
    cursor.execute('''CREATE TABLE Zielgroesse (
        ZielgroesseID INT PRIMARY KEY,
        DatensatzID INT NOT NULL,
        ParameterID INT NOT NULL,
        Timestamp TIMESTAMP NOT NULL,
        gemessene_Werte REAL NOT NULL,
        Data_Pre_processingID INT NOT NULL,
        LocalisationID INT NOT NULL,
        AnlageID INT NOT NULL,
        FOREIGN KEY(DatensatzID) REFERENCES Datensatz(DatensatzID),
        FOREIGN KEY(ParameterID) REFERENCES parameter(ParameterID),
        FOREIGN KEY(Data_Pre_processingID) REFERENCES Data_pre_processing(Data_Pre_processingID),
        FOREIGN KEY(LocalisationID) REFERENCES Localisation(LocalisationID),
        FOREIGN KEY(AnlageID) REFERENCES Anlage(AnlageID)
        
                )''')

    # Dimensionstabelle für Timestamp erstellen
    cursor.execute("DROP TABLE IF EXISTS Timestamp")
    cursor.execute('''CREATE TABLE Timestamp (
        TimestampID INT NOT NULL  PRIMARY KEY,
        ist_echte_Timestamp BLOB NOT NULL,
        Aufgedeckter_Zeitraum DATETIME NOT NULL,
        Time TIME NOT NULL,
        Date DATE NOT NULL
                    )''')
    # Dimenstionstabelle für Anlage erstellen
    cursor.execute("DROP TABLE IF EXISTS Anlage")
    cursor.execute('''CREATE TABLE Anlage (
        AnlageID INT NOT NULL PRIMARY KEY,
        Maschine VARCHAR(255) NOT NULL,
        Prozesse VARCHAR(255) NOT NULL,
        Hersteller VARCHAR(255) NOT NULL,
        Anzahl_der_Prozesse VARCHAR(255) NOT NULL
                    )''')

    # Dimenstionstabelle für Data pre_processing erstellen
    cursor.execute("DROP TABLE IF EXISTS Data_pre_processing")
    cursor.execute('''CREATE TABLE Data_pre_processing (
        Data_Pre_processingID INT NOT NULL PRIMARY KEY,
        Aufgabe VARCHAR(255) NOT NULL,
        Datenmanipulation VARCHAR(255) NOT NULL,
        Setspliting VARCHAR(255) NOT NULL                  
                    )''')
    #Dimenstionstabelle für Parameter ersetllen
    cursor.execute("DROP TABLE IF EXISTS parameter")
    cursor.execute('''CREATE TABLE parameter (
            ParameterID INT PRIMARY KEY,
            Sensoren VARCHAR(255) NOT NULL,
            Variablen VARCHAR(255) NOT NULL                                 
                    )''')
    #Dimenstionstabelle für  Datensatz erstellen
    cursor.execute("DROP TABLE IF EXISTS Datensatz")
    cursor.execute('''CREATE TABLE Datensatz (
        DatensatzID INT NOT NULL PRIMARY KEY,
        Name der Daten VARCHAR(255) NOT NULL,
        Zweck der Daten VARCHAR(255) NOT NULL,
        Plattform Rohdaten VARCHAR(255) NOT NULL,
        Gruppierung der Themenbereiche VARCHAR(255) NOT NULL                   
                    )''')
    #Dimenstionstabelle für Localisation erstellen
    cursor.execute("DROP TABLE IF EXISTS Localisation")
    cursor.execute('''CREATE TABLE Localisation (
        LocalisationID INT NOT NULL PRIMARY KEY,
        Land VARCHAR(255) NOT NULL,
        Bundesland VARCHAR(255) NOT NULL                  
                    )''')
    #Faktentabelle für Rohdaten erstellen
    cursor.execute("DROP TABLE IF EXISTS Rohdaten")
    cursor.execute('''CREATE TABLE Rohdaten (
        RohdatenID INT NOT NULL PRIMARY KEY,
        DatensatzID INT NOT NULL,
        ParameterID INT NOT NULL,
        Timestamp TIMESTAMP NOT NULL,
        gemessene_Werte REAL NOT NULL,
        Data_Pre_processingID INT NOT NULL,
        LocalisationID INT NOT NULL,
        AnlageID INT NOT NULL,
        FOREIGN KEY(DatensatzID) REFERENCES Datensatz(DatensatzID),
        FOREIGN KEY(ParameterID) REFERENCES parameter(ParameterID),
        FOREIGN KEY(Data_Pre_processingID) REFERENCES Data_pre_processing(Data_Pre_processingID),
        FOREIGN KEY(LocalisationID) REFERENCES Localisation(LocalisationID),
        FOREIGN KEY(AnlageID) REFERENCES Anlage(AnlageID)
                    
                    )''')
    #Faktentabelle für Eingangsgroesse erstellen
    cursor.execute("DROP TABLE IF EXISTS Eingangsgroesse")
    cursor.execute('''CREATE TABLE Eingangsgroesse (
        EingangsgroesseID INT PRIMARY KEY,
        DatensatzID INT NOT NULL,
        ParameterID INT NOT NULL,
        Timestamp TIMESTAMP NOT NULL,
        gemessene_Werte REAL NOT NULL,
        Data_Pre_processingID INT NOT NULL,
        LocalisationID INT NOT NULL,
        AnlageID INT NOT NULL,
        FOREIGN KEY(DatensatzID) REFERENCES Datensatz(DatensatzID),
        FOREIGN KEY(ParameterID) REFERENCES parameter(ParameterID),
        FOREIGN KEY(Data_Pre_processingID) REFERENCES Data_pre_processing(Data_Pre_processingID),
        FOREIGN KEY(LocalisationID) REFERENCES Localisation(LocalisationID),
        FOREIGN KEY(AnlageID) REFERENCES Anlage(AnlageID)

                )''')

def transform_csv(filename, dataset_id, timestamp_column, data_indices, units_list):
    df = pd.read_csv(filename)
    input_data_columns_list = []
    for i, spalte in enumerate(data_indices):
        spalten_name = df.columns[spalte]
        print(f'Input_Data_Column_ID {i}, ist Spalte {spalte} im Orignal-CSV und hat den Name: {spalten_name}')
        input_data_columns_list.append((i, dataset_id, spalten_name, units_list[i]))
    input_data_columns_df = pd.DataFrame(data=input_data_columns_list, columns=['Input_Data_Column_ID', 'Dataset_ID', 'Name', 'Unit'])

if __name__ == "__main__":
    print('Main function executed')
    db_name = 'newcode.db'
    create_database_schema(db_name=db_name)
    print('Database schema created')   
    df = pd.read_csv(dateipfad)
    conn = sqlite3.connect(db_name)
    df.to_sql('continuous_factory_process', conn, if_exists='replace', index=False)
    # print(df.to_string())

    # read dateipfad and dump using pickle
    with open(dateipfad, 'rb') as f:
        data = f.read()

    # Create Rohdata
    raw_data = pd.DataFrame(data=[data], columns=['raw_data'])
    raw_data.to_sql('raw_data', conn, if_exists='replace', index=False)
    
    cursor = conn.cursor()
    # Insert data into Input_Data
    cursor.execute('''CREATE TABLE Input_Data_Column (
        Input_Data_Column_ID PRIMARY KEY,
        Dataset_ID INT NOT NULL,
        Name TEXT NOT NULL,
        Unit Text
                        )''')     
    
    
    cursor.execute('''CREATE TABLE Input_Data (
        ID INT NOT NULL,
        Dataset_ID INT NOT NULL,
        Input_Data_Column_ID INT NOT NULL,
        Timestamp TIMESTAMP NOT NULL,
        Value REAL,
        PRIMARY KEY (ID, Dataset_ID)   
        FOREIGN KEY(Input_Data_Column_ID) REFERENCES Input_Data_Column(Input_Data_Column_ID)               
                        )''')
    
    # das hier ist nicht wichtig
    transformed_df = df[['time_stamp', 'AmbientConditions.AmbientHumidity.U.Actual']]
    transformed_df.insert(0, 'ID', range(len(transformed_df)))
    transformed_df.insert(1, 'Dataset_ID', 0)
    transformed_df.insert(2, 'Input_Data_Column_ID', 0)
    transformed_df.rename(columns={'time_stamp': 'Timestamp', 'AmbientConditions.AmbientHumidity.U.Actual': 'Value'}, inplace=True)
   
    
    input_data_columns_df = pd.DataFrame(data=[[0, 0, 'AmbientConditions.AmbientHumidity.U.Actual', 'Percent']], columns=['Input_Data_Column_ID', 'Dataset_ID', 'Name', 'Unit'])
    input_data_columns_df.to_sql('Input_Data_Column', conn, if_exists='append', index=False)

    transformed_df.to_sql('Input_Data', conn, if_exists='append', index=False)
    print('Input data inserted')

    spalten_indices = range(1, 100)

    transform_csv(filename=dateipfad, dataset_id=0, timestamp_column='time_stamp', data_indices=spalten_indices, units_list=['Percent']*len(spalten_indices))
    