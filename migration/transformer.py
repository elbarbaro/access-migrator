import pandas as pd
from typing import Dict, Any

def apply_transformations(df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
    for col, actions in rules.items():
        if col not in df.columns:
            continue
        if actions.get('trim') and df[col].dtype == object:
            df[col] = df[col].str.strip()
        if 'replace_null' in actions:
            df[col] = df[col].fillna(actions['replace_null'])
        if 'to_upper' in actions and actions['to_upper']:
            df[col] = df[col].str.upper()
        if 'to_lower' in actions and actions['to_lower']:
            df[col] = df[col].str.lower()
        # Add more transformation rules as needed
    return df 