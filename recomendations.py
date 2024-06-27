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
    price = 850000
    habitaciones = 4
    parking_num = 1

    prompt = f"""
        Eres una persona con un perfil de {perfil}, un inmueble en {transaction}, con un presupuesto de {price} pesos MXN y con una elasticidad de 5%. 
        Tienes que encontrar los inmuebles ideales para este perfil, teniendo en cuenta lo siguiente:
        El inmueble tiene que contar con al menos {habitaciones} recámaras y con {parking_num} estacionamientos.
        La persona quiere un inmueble cerca a Universidad Insurgentes Plantel Viaducto, Universidad Autónoma de la Ciudad de México Plantel Centro Histórico, Panteón Francés de la Piedad, Museo del Juguete Antiguo México, Mercado Jamaica.

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

def post_recomendations():
   
    # user_lifetime, purpose, max_price, city_id, places_interest, bedrooms, parking_num
    user_lifetime = "estudiante" 
    purpose = 1 
    max_price = 1000000
    city_id = 6
    places_interest = "hola" 
    bedrooms = 1 
    parking_num = 2

    # Conexion a BD
    conexion_w = mysql.connector.connect(
        host = "10.97.144.113",
        user = "root",
        password = "propiedades",
        database="propiedades",
    )

    # Guarda datos de usuario perfilado
    query = f"""
        INSERT INTO profiled_recomendation (
            user_lifetime,
            purpose,
            max_price,
            city_id,
            places_interest,
            bedrooms,
            parking_num
        )
        VALUES (
            "{user_lifetime}",
            {purpose},
            {max_price},
            {city_id},
            "{places_interest}",
            {bedrooms},
            {parking_num}
        )
    """
    mycursor_w = conexion_w.cursor()
    mycursor_w.execute(query)
    conexion_w.commit()
    row_insert = mycursor_w.rowcount
    print("-------- Se guarda informacion de quiz")

    

    # profiled_user= []
    # for x in myresult:
    #     profiled_user.append(x)
    # result_profiled = pd.DataFrame(profiled_user, columns=['id','sepomex_id', 'purpose', 'type_children'])
    
    # print(result_profiled)
    # print(f"RESULTADO")
    # print(result_profiled['sepomex_id'])

    # Propiedades similares
    # print(f"Total: {len(result_profiled)}")
    json_result_properties = {}
    if  row_insert > 0:
        print("-------- Perfila propiedades de su interes")
        purpose_str = ""
        if purpose == 1:
            purpose_str = "sale_price"
        else:
            purpose_str = "rental_price"

        query = f"""
            SELECT property_id, score, description, sepomex_id, purpose, type_children, {purpose_str} as price, bathrooms, bedrooms, parking_num
            FROM properties_search
            WHERE city_id = {city_id}
            AND purpose = {purpose}
            AND type_children in (1,2)
            AND status = 1
            -- AND {purpose_str} >= 1000000
            -- AND {purpose_str} <= 1050000
            AND bedrooms >= {bedrooms}
            -- AND parking_num >= 1
            ORDER BY score DESC
            limit 50
        """
        # print(query)
        mycursor_w = conexion_w.cursor()
        mycursor_w.execute(query)
        myresult = mycursor_w.fetchall()
        properties = []
        for x in myresult:
            properties.append(x)
        result_properties = pd.DataFrame(properties,columns=['id', 'score', 'description', 'sepomex_id', 'purpose', 'type_children', 'price', 'bathrooms', 'bedrooms','parking_num'])

        # print(result_properties)

        json_result_properties = result_properties.to_json(orient ='records') 
        # print("json_index = ", json_result_properties, "\n") 

        # Envia datos a Gemini
        print("-------- Envia datos a Gemini")
        recomendetions = post_gemini(json_result_properties)
        get_recomendations(recomendetions, conexion_w)

def get_recomendations(recomendetions, conexion_w):
    print("-------- Obtiene la informacion de las recomendaciones")

    property_list = []
    for property in recomendetions:
        property_id = property['id']
        property_list.append(property_id)
    
    listToStrIds = ','.join([str(elem) for elem in property_list])
    
    query = f"""
        SELECT property_id, score, description, sepomex_id, purpose, type_children, score, IF(purpose = 1, sale_price, rental_price) as price, bathrooms, bedrooms, parking_num
        FROM properties_search
        WHERE property_id in ({listToStrIds})
    """
    # print(query)
    mycursor_w = conexion_w.cursor()
    mycursor_w.execute(query)
    myresult = mycursor_w.fetchall()
    properties = []
    for x in myresult:
        properties.append(x)
    result_properties = pd.DataFrame(properties,columns=['id', 'score', 'description', 'sepomex_id', 'purpose', 'type_children', 'price', 'bathrooms', 'bedrooms','parking_num'])
    
    print(result_properties)

h = post_recomendations()
