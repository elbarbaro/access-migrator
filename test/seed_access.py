import pypyodbc
import os
from datetime import datetime, timedelta
import random
from faker import Faker

# Configurar Faker para generar datos aleatorios
fake = Faker('es_ES')  # Usar localización en español

def connect_to_access(db_path):
    """Conectar a la base de datos Access"""
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
    )
    return pypyodbc.connect(conn_str)

def generate_random_records(num_records=100):
    """Generar registros aleatorios"""
    records = []
    # Generar una fecha base (por ejemplo, 1 año atrás)
    base_date = datetime.now() - timedelta(days=365)
    
    for _ in range(num_records):
        # Generar fecha aleatoria dentro del último año
        random_days = random.randint(0, 365)
        created_at = base_date + timedelta(days=random_days)
        
        # Asegurarnos de que los nombres no contengan caracteres especiales
        first_name = fake.first_name().encode('ascii', 'ignore').decode('ascii')
        last_name = fake.last_name().encode('ascii', 'ignore').decode('ascii')
        
        record = (
            first_name,  # FirstName
            last_name,   # LastName
            created_at.strftime('%Y-%m-%d %H:%M:%S')  # CreatedAt como string
        )
        records.append(record)
    
    return records

def insert_records(conn, table_name, records):
    """Insertar registros en la tabla"""
    cursor = conn.cursor()
    
    # Preparar la consulta SQL
    sql = f"""
    INSERT INTO {table_name} (FirstName, LastName, CreatedAt)
    VALUES (?, ?, ?)
    """
    
    try:
        # Insertar los registros uno por uno para mejor control de errores
        for record in records:
            try:
                cursor.execute(sql, record)
                print(f"Registro insertado: {record}")
            except Exception as e:
                print(f"Error al insertar registro {record}: {str(e)}")
                continue
        
        conn.commit()
        print(f"Se insertaron registros exitosamente")
    except Exception as e:
        print(f"Error al insertar registros: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    # Ruta al archivo Access
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, "school_2015.accdb")
    
    if not os.path.exists(db_path):
        print(f"Error: No se encontró el archivo {db_path}")
        return
    
    try:
        # Conectar a la base de datos
        conn = connect_to_access(db_path)
        print("Conexión exitosa a la base de datos")
        
        # Generar registros aleatorios
        records = generate_random_records(100)
        
        # Insertar los registros
        insert_records(conn, "Students", records)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Conexión cerrada")

if __name__ == "__main__":
    main()
