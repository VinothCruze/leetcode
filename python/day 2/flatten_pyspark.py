from pyspark.sql import functions as F
from pyspark.sql import types as T

def flatten_df(df, prefix=""):
    flat_cols = []
    explode_cols = []

    for field in df.schema.fields:
        col_name = prefix + field.name

        # Case 1: StructType → flatten recursively
        if isinstance(field.dataType, T.StructType):
            flat_cols.extend(flatten_df(df.select(F.col(field.name + ".*")), prefix=col_name + "_"))
        
        # Case 2: Array of Structs → explode + flatten
        elif isinstance(field.dataType, T.ArrayType) and isinstance(field.dataType.elementType, T.StructType):
            explode_cols.append((col_name, field.name))
        
        # Case 3: Primitive field → just rename
        else:
            flat_cols.append(F.col(field.name).alias(col_name))

    # If no explode needed → return simple flattening
    if not explode_cols:
        return flat_cols
    
    # If arrays of structs exist → explode one array and recurse
    col_name, field_name = explode_cols[0]
    df_exploded = df.withColumn(field_name, F.explode_outer(F.col(field_name)))

    return flatten_df(df_exploded, prefix)

