import os
import win32com.client
import pyodbc

# Get the absolute path for the database file
current_dir = os.path.dirname(os.path.abspath(__file__))
db_filename = os.path.join(current_dir, "test_access.accdb")

# Elimina el archivo si ya existe
if os.path.exists(db_filename):
    os.remove(db_filename)

# Crear el archivo .accdb usando Access
try:
    # Crear una instancia de Access
    access = win32com.client.Dispatch("Access.Application")
    
    # Crear una nueva base de datos
    access.DBEngine.CreateDatabase(db_filename, 1033)
    
    # Abrir la base de datos
    access.OpenCurrentDatabase(db_filename)
    
    # Crear las tablas usando SQL
    access.DoCmd.RunSQL("""
    CREATE TABLE Subjects_2015 (
        ID AUTOINCREMENT PRIMARY KEY,
        Name TEXT(100),
        Code TEXT(20),
        Credits INTEGER
    )
    """)
    
    access.DoCmd.RunSQL("""
    CREATE TABLE Professors_2015 (
        ID AUTOINCREMENT PRIMARY KEY,
        FullName TEXT(100),
        Department TEXT(50),
        Email TEXT(100)
    )
    """)
    
    # Insertar datos en Subjects_2015
    for subject in [
        ("Mathematics I", "MATH1", 5),
        ("Physics I", "PHYS1", 4),
        ("Introduction to CS", "CS101", 6),
    ]:
        access.DoCmd.RunSQL(f"""
        INSERT INTO Subjects_2015 (Name, Code, Credits) 
        VALUES ('{subject[0]}', '{subject[1]}', {subject[2]})
        """)
    
    # Insertar datos en Professors_2015
    for professor in [
        ("Dr. Alice Johnson", "Mathematics", "alice.johnson@uni.edu"),
        ("Dr. Bob Smith", "Physics", "bob.smith@uni.edu"),
        ("Dr. Carla Martinez", "Computer Sci.", "carla.martinez@uni.edu"),
    ]:
        access.DoCmd.RunSQL(f"""
        INSERT INTO Professors_2015 (FullName, Department, Email) 
        VALUES ('{professor[0]}', '{professor[1]}', '{professor[2]}')
        """)
    
    # Cerrar la base de datos y la aplicación
    access.CloseCurrentDatabase()
    access.Quit()
    
    print(f"Base de datos {db_filename} creada exitosamente")
except Exception as e:
    print(f"Error creando la base de datos: {str(e)}")
    if 'access' in locals():
        try:
            access.Quit()
        except:
            pass
    raise

print(f"Access file '{db_filename}' created successfully with test data.")
