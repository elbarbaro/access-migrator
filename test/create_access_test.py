import pypyodbc
import os

# Nombre del archivo a crear
db_filename = "test_access.accdb"

# Elimina el archivo si ya existe
if os.path.exists(db_filename):
    os.remove(db_filename)

# Crear archivo .accdb usando pypyodbc
pypyodbc.win_create_mdb(db_filename)

# Cadena de conexión al nuevo archivo Access
conn_str = (
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
    rf"DBQ={db_filename};"
)

conn = pypyodbc.connect(conn_str)
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

# Insertar datos en Subjects_2015
subjects = [
    ("Mathematics I", "MATH1", 5),
    ("Physics I", "PHYS1", 4),
    ("Introduction to CS", "CS101", 6),
]
cursor.executemany(
    "INSERT INTO Subjects_2015 (Name, Code, Credits) VALUES (?, ?, ?)",
    subjects
)

# Crear tabla Professors_2015
cursor.execute("""
CREATE TABLE Professors_2015 (
    ID AUTOINCREMENT PRIMARY KEY,
    FullName TEXT(100),
    Department TEXT(50),
    Email TEXT(100)
)
""")

# Insertar datos en Professors_2015
professors = [
    ("Dr. Alice Johnson", "Mathematics", "alice.johnson@uni.edu"),
    ("Dr. Bob Smith", "Physics", "bob.smith@uni.edu"),
    ("Dr. Carla Martinez", "Computer Sci.", "carla.martinez@uni.edu"),
]
cursor.executemany(
    "INSERT INTO Professors_2015 (FullName, Department, Email) VALUES (?, ?, ?)",
    professors
)

# Guardar y cerrar
conn.commit()
cursor.close()
conn.close()

print(f"Access file '{db_filename}' created successfully with test data.")
