import pandas as pd
import numpy as np
from typing import Dict, Any

def apply_transformations(df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
    # Crear una copia del DataFrame para no modificar el original
    df = df.copy()
    print("Entro al procesar las transformaciones", rules)
    for col, actions in rules.items():
        if col not in df.columns:
            continue
            
        try:
            # Obtener el tipo de dato de la columna
            col_type = df[col].dtype
            
            # Si la columna es numérica y necesitamos aplicar transformaciones de string,
            # la convertimos a string primero
            needs_string_ops = any([
                actions.get('trim'),
                actions.get('to_upper'),
                actions.get('to_lower')
            ])
            
            if needs_string_ops and not pd.api.types.is_string_dtype(col_type):
                df[col] = df[col].astype(str)
                col_type = df[col].dtype
            
            # Aplicar transformaciones según el tipo de dato
            if actions.get('trim'):
                df[col] = df[col].str.strip()
            
            if 'replace_null' in actions:
                df[col] = df[col].fillna(actions['replace_null'])
            
            if 'to_upper' in actions and actions['to_upper']:
                df[col] = df[col].str.upper()
            
            if 'to_lower' in actions and actions['to_lower']:
                df[col] = df[col].str.lower()
                    
        except Exception as e:
            print(f"Error procesando columna {col}: {str(e)}")
            print(f"Tipo de columna: {col_type}")
            print(f"Valores de ejemplo: {df[col].head().tolist()}")
            # Continuar con la siguiente columna en lugar de fallar
            continue
            
    return df 