import sqlite3
import pandas as pd
import numpy as np


# Verbindung zur SQLite-Datenbank herstellen (falls nicht vorhanden, wird eine neue erstellt)
conn = sqlite3.connect('newcode.db')
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
    value REAL,
    FOREIGN KEY(DatensatzID) REFERENCES Datensatz(DatensatzID),
    FOREIGN KEY(ParameterID) REFERENCES parameter(ParameterID),
    FOREIGN KEY(Data_Pre_processingID) REFERENCES Data_pre_processing(Data_Pre_processingID),
    FOREIGN KEY(LocalisationID) REFERENCES Localisation(LocalisationID),
    FOREIGN KEY(AnlageID) REFERENCES Anlage(AnlageID),
    FOREIGN KEY(DatensatzID) REFERENCES Anlage(DatensatzID) 
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
#Dimenstionstabelle für Data Datensatz erstellen
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
    value REAL,
    FOREIGN KEY(DatensatzID) REFERENCES Datensatz(DatensatzID),
    FOREIGN KEY(ParameterID) REFERENCES parameter(ParameterID),
    FOREIGN KEY(Data_Pre_processingID) REFERENCES Data_pre_processing(Data_Pre_processingID),
    FOREIGN KEY(LocalisationID) REFERENCES Localisation(LocalisationID),
    FOREIGN KEY(AnlageID) REFERENCES Anlage(AnlageID),
    FOREIGN KEY(DatensatzID) REFERENCES Anlage(DatensatzID)            
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
    value REAL,
    FOREIGN KEY(DatensatzID) REFERENCES Datensatz(DatensatzID),
    FOREIGN KEY(ParameterID) REFERENCES parameter(ParameterID),
    FOREIGN KEY(Data_Pre_processingID) REFERENCES Data_pre_processing(Data_Pre_processingID),
    FOREIGN KEY(LocalisationID) REFERENCES Localisation(LocalisationID),
    FOREIGN KEY(AnlageID) REFERENCES Anlage(AnlageID),
    FOREIGN KEY(DatensatzID) REFERENCES Anlage(DatensatzID) 
               )''')

conn.commit()
conn.close()

#Daten extraction
dateipfad = "C:\Users\Asus\Documents\MA_ETL-System\continuous_factory_process.csv"
datei_struktur = {
    'Maschine 1,2,3':
    {
        'Zeitstempel': 0,
        'Input_start': 1,
        'Input_ende': 42,
        'Output_start': 42,
        'Output_ende': 72 
    },
    'Maschine 4,5':
    {
        'Zeitstempel': 0,
        'Input_start': 72,
        'Input_ende': 86,
        'Output_start': 86,
        'Output_ende': 116 
    },
}
def lese_geordneten_tabellarischen_datensatz(csv_dateipfad, datei_struktur):
    df = pd.read_csv(csv_dateipfad)
    input_df = None
    output_df = None
    timestamp_df = None
    results = []

    for maschine, struktur in datei_struktur.items():
        timestamp_df = df[df.columns[struktur['Zeitstempel']]]
        input_df = df[df.columns[struktur['input_start']:struktur['input_ende']]]
        output_df = df[df.columns[struktur['Output_start']:struktur['Output_ende']]]
        results.append((maschine, timestamp_df, input_df, output_df))
    
    return results
print('Test')
df = lese_geordneten_tabellarischen_datensatz(dateipfad, datei_struktur)
print(df.to_string())

# Daten Transformation

# Load in der Datenbank


# Beispiel: Daten in die Dimensionstabellen einfügen
cursor.execute("INSERT INTO Datensatz (DatensatzID, Name der Daten, Zweck der Daten, Plattform, Gruppierung der Themenbereiche) VALUES (?, ?, ?, ?, ?)", (1, 'multi-stage continous-flow manufacturing process', 'Vorhersage der Output' 'Kaggle','Produktqualität'))
cursor.execute("INSERT INTO Zielgroesse (ZielgroesseID, DatensatzID, parameterID, Timestamp, gemessene_Werte, Data_Pre_processingID, LocalisationID, AnlageID) VALUES (?, ?, ?, ?, ?, ?)", (1, 1, 1,df.columns[0], df.columns[42-72]&[86-116], 1, 1, 1))
cursor.execute("INSERT INTO Timestamp (TimestampID, ist_echte_Timestamp, Aufgedeckter_Zeitraum, Time, Date) VALUES (?, ?, ?, ?, ?)", (1, 'ja', '2024-02-12 12:00:00', '12:00:00', '2024-02-12'))
cursor.execute("INSERT INTO Anlage (AnlageID, Maschine, Prozesse, Hersteller, Anzahl_der_Prozesse) VALUES (?, ?, ?, ?, ?)", (1, 'Maschinenname', 'Prozessname', 'Herstellername'))
cursor.execute("INSERT INTO Data_pre_processing (Data_Pre_processingID, Aufgabe, Datenmanipulation, Setspliting) VALUES (?, ?, ?, ?)", (1, 'Regression', 'Ja', 'Ja'))
cursor.execute("INSERT INTO parameter (parameterID, Sensoren, Variablen) VALUES (?, ?, ?)", (1, 'Sensorname',df.iloc[0]))
cursor.execute("INSERT INTO Eingangsgroesse (EingangsgroesseID, DatensatzID, ParameterID, Timestamp, gemessene_Werte, Data_Pre_processingID, LocalisationID, AnlageID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (1, 1, 1,df.columns[0],df.columns[1-42]&[72-86], 1, 1, 1))
cursor.execute("INSERT INTO Localisation (LocalisationID, Land, Bundesland) VALUES (?, ?, ?)", (1, 'USA', 'Detroit, Michigan'))
cursor.execute("INSERT INTO Rohdaten (RohdatenID, DatensatzID, ParameterID, Timestamp, gemessene_Werte, Data_Pre_processingID, LocalisationID, AnlageID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (1, 1, 1, '2024-02-12 12:00:00', 123.45, 1, 1, 1))

