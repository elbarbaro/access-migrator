import pyodbc

# Ruta del archivo .accdb que ya debe existir
db_path = r"db_2015.accdb"
conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    fr"DBQ={db_path};"
)

# Conectar
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Crear tabla Subjects_2015
cursor.execute("""
CREATE TABLE Subjects_2015 (
    ID AUTOINCREMENT PRIMARY KEY,
    Name TEXT(100),
    Code TEXT(20),
    Credits INTEGER
)
""")

# Insertar datos
subjects = [
    ("Mathematics I", "MATH1", 5),
    ("Physics I", "PHYS1", 4),
    ("Introduction CS", "CS101", 6),
]
cursor.executemany("INSERT INTO Subjects_2015 (Name, Code, Credits) VALUES (?, ?, ?)", subjects)

# Crear tabla Professors_2015
cursor.execute("""
CREATE TABLE Professors_2015 (
    ID AUTOINCREMENT PRIMARY KEY,
    FullName TEXT(100),
    Department TEXT(50),
    Email TEXT(100)
)
""")

# Insertar datos
professors = [
    ("Dr. Alice Johnson", "Mathematics", "alice.johnson@uni.edu"),
    ("Dr. Bob Smith", "Physics", "bob.smith@uni.edu"),
    ("Dr. Carla Martinez", "Computer Sci.", "carla.martinez@uni.edu"),
]
cursor.executemany("INSERT INTO Professors_2015 (FullName, Department, Email) VALUES (?, ?, ?)", professors)

# Finalizar
conn.commit()
cursor.close()
conn.close()

print("Tables created and data inserted successfully using pyodbc.")
