import pandas as pd
import mysql.connector
import numpy as np
import os
import google.generativeai as genai
import json

def post_gemini(json_result_properties: dict):
    genai.configure(api_key="AIzaSyBsNf6f-iGUKqr8gJyJQ5BtmyOZgn7adsE")

    # Create the model
    # See https://ai.google.dev/api/python/google/generativeai/GenerativeModel
    generation_config = {
    "temperature": 0,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 8192,
    "response_mime_type": "application/json",
    }

    model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config=generation_config
    )

    output = """[{id: str,
            reason: str,
            price: int,
            scoreTotal: float}]"""

    perfil = "estudiante"
    transaction = "venta"
    price = 150000
    habitaciones = 4
    parking_num = 1

    prompt = f"""
        Eres una persona con un perfil de {perfil}, un inmueble en {transaction}, con un presupuesto de {price} pesos MXN y con una elasticidad de -5%. Tienes que encontrar los inmuebles ideales para este perfil.
        El inmueble tiene que contar con al menos {habitaciones} cuartos y con al menos {parking_num} estacionamientos. 
        Ten en cuenta lo siguientes para los campos del JSON para la selección de las mejores propiedades:
        - price: Presupuesto de la persona.
        - scoreTotal: Puntaje total de la propiedad del 1 al 100, teniendo en cuenta que hay unas propiedades que sobrepasan el 100 por bonificaciones adicionales.
        - description: Descripción general del inmueble.
        - purpose: Propósito
        - sepomex_id: Alcaldía de CDMX
        - type_children: Tipo de propiedad.
        - bedrooms: Habitaciones o recámaras
        - bathrooms: Baños
        Deberás de tener en cuenta que el presupuesto de la persona no deberá superar el precio de la propiedad.
        A continuación, te presentamos todas las propiedades acordes al perfil,
        ```
        {json_result_properties}
        ```
        Deberás responder con la razón del por qué escogiste esas propiedades, el id de la propiedad, y el puntaje de compatibilidad de acuerdo al perfil del 1 al 100.
        Devuele un máximo de 9 opciones de propiedades.
        Responde con el siguiente JSON schema:
        ```
        {
            output
        }
        ```
    """
    print(prompt)

    chat_session = model.start_chat(
        history=[
            {
                "role": "user",
                "parts": [
                    prompt,
                ],
            }
        ]
    )

    print("||||||||||||||||||||||||||||")

    response = chat_session.send_message("Dame la respuesta")

    print(response.text)
    json_response = json.loads(response.text)
    return json_response

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

        purpose_str = ""
        if purpose == 1:
            purpose_str = "sale_price"
        else:
            purpose_str = "rental_price"

        query = f"""
            SELECT property_id, score, description, sepomex_id, purpose, type_children, {purpose_str} as price, bathrooms, bedrooms
            FROM properties_search
            WHERE sepomex_id = {sepomex_id}
            AND purpose = {purpose}
            AND type_children = {type_children}
            AND status = 1
            ORDER BY score DESC
            limit 50
        """
        print(query)
        mycursor_w = conexion_w.cursor()
        mycursor_w.execute(query)
        myresult = mycursor_w.fetchall()
        properties = []
        for x in myresult:
            properties.append(x)
        result_properties = pd.DataFrame(properties,columns=['id', 'scoreTotal', 'description', 'sepomex_id', 'purpose', 'type_children','price', 'bathrooms', 'bedrooms'])

        print(result_properties)

        json_result_properties = result_properties.to_json(orient ='records') 
        # print("json_index = ", json_result_properties, "\n") 
        post_gemini(json_result_properties)


h = info(1)
