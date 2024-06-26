import pandas as pd
import mysql.connector
import numpy as np

def info(perfil_id):
    # Conexion a BD
    conexion_w = mysql.connector.connect(
        host = "10.97.144.113",
        user = "root",
        password = "propiedades",
        database="propiedades",
    )

    # Perfilamiento de usuarios
    query = f"""
        SELECT id, sepomex_id, purpose, type_children
        FROM profiled_user_recomendation
        WHERE id = {perfil_id}
    """
    mycursor_w = conexion_w.cursor()
    mycursor_w.execute(query)
    myresult = mycursor_w.fetchall()
    profiled_user= []
    for x in myresult:
        profiled_user.append(x)
    result_profiled = pd.DataFrame(profiled_user, columns=['id','sepomex_id', 'purpose', 'type_children'])
    
    print(result_profiled)
    print(result_profiled['sepomex_id'])

    # Propiedades similares
    json_result_properties = {}
    if  len(result_profiled) != 0 :
        sepomex_id = result_profiled['sepomex_id'][0]
        purpose = result_profiled['purpose'][0]
        type_children = result_profiled['type_children'][0]

        query = f"""
            SELECT id, scoreTotal, description, sepomex_id, purpose, type_children
            FROM properties
            WHERE sepomex_id = {sepomex_id}
            AND purpose = {purpose}
            AND type_children = {type_children}
            AND status = 1
            ORDER BY scoreTotal DESC
            limit 100
        """
        print(query)
        mycursor_w = conexion_w.cursor()
        mycursor_w.execute(query)
        myresult = mycursor_w.fetchall()
        properties = []
        for x in myresult:
            properties.append(x)
        result_properties = pd.DataFrame(properties,columns=['id', 'scoreTotal', 'description', 'sepomex_id', 'purpose', 'type_children'])

        print(result_properties)

        json_result_properties = result_properties.to_json(orient ='index') 
        print("json_index = ", json_result_properties, "\n") 


h = info(1)
