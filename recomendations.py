import pandas as pd
import mysql.connector
import numpy as np
import os
import google.generativeai as genai
import json

def post_gemini(json_result_properties: dict, perfil, purpose, price, bedrooms, parking_num, places_of_interest):
    print("-------- Envia datos a GeminI")
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
            score: float}]"""

    prompt = f"""
        Eres una persona con un perfil de {perfil}, un inmueble en {purpose}, con un presupuesto de {price} pesos MXN y con una elasticidad de 5%. 
        Tienes que encontrar los inmuebles ideales para este perfil, teniendo en cuenta lo siguiente:
        El inmueble tiene que contar con al menos {bedrooms} recámaras y con {parking_num} estacionamientos.
        La persona quiere un inmueble cerca de {places_of_interest}.

        Ten en cuenta lo siguientes para los campos del JSON para la selección de las mejores propiedades:
        - price: Presupuesto maximo de la persona.
        - score: Puntaje total de la propiedad del 1 al 100, teniendo en cuenta que hay unas propiedades que sobrepasan el 100 por bonificaciones adicionales.
        - description: Descripción general del inmueble.
        - purpose: Propósito
        - sepomex_id: Alcaldía de CDMX
        - type_children: Tipo de propiedad.
        - bedrooms: Habitaciones o recámaras
        - bathrooms: Baños
        Deberás de tener en cuenta que el presupuesto de la persona no deberá superar el precio de {price} la propiedad.
        A continuación, te presentamos todas las propiedades acordes al perfil,
        ```
        {json_result_properties}
        ```
        Deberás responder con la razón del por qué escogiste esas propiedades, el id de la propiedad, y el puntaje de compatibilidad de acuerdo al perfil del 1 al 100.
        Devuele un máximo de 5 opciones de propiedades.
        Responde con el siguiente JSON schema:
        ```
        {
            output
        }
        ```
    """
    # print(prompt)

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

    # print("||||||||||||||||||||||||||||")

    response = chat_session.send_message("Dame la respuesta")

    # print(response.text)
    json_response = json.loads(response.text)
    return json_response

def get_recomendations(recomendetions, conexion_w):
    print("-------- Obtiene la informacion de las recomendaciones")

    property_list = []
    for property in recomendetions:
        property_id = property['id']
        property_list.append(property_id)
    
    listToStrIds = ','.join([str(elem) for elem in property_list])
    
    query = f"""
        SELECT 
            property_id, 
            score, 
            description, 
            sepomex_id, 
            purpose, 
            type_children, 
            IF(purpose = 1, sale_price, rental_price) as price, 
            bathrooms, 
            bedrooms, 
            parking_num,
            users_id,
            thumbnail,
            colony,
            city,
            zipcode,
            state,
            street,
            external_num,
            state_id,
            size_house,
            size_ground
        FROM properties_search
        WHERE property_id in ({listToStrIds})
    """
    
    mycursor_w = conexion_w.cursor()
    mycursor_w.execute(query)
    myresult = mycursor_w.fetchall()
    properties = []
    for propiedad in myresult:
        tmp = {
           "id": propiedad[0],
           "score": propiedad[1],
           "is_main_duplicated": 0,
           "sepomex_id": propiedad[3],
           "created": "2024-03-14 02:24:49",
           "users_id": propiedad[10],
           "showroom_info": None,
           "thumbnail": propiedad[11],
           "picture": f"https://propiedadescom.s3.amazonaws.com/files/336x200/{propiedad[11]}",
           "alt_thumbnail": "Foto de departamento en renta en cozumel 11198, juárez, cuauhtémoc, df / cdmx, 0 No. 01",
           "on_amazon": 1,
           "highlighted": 0,
           "highlighted_package": None,
           "subtype": propiedad[5],
           "type": 1,
           "type_children": propiedad[5],
           "type_str": "Casa" if propiedad[5] == 2 else "Departamento",
           "colony": propiedad[12],
           "city": propiedad[13],
           "zipcode": propiedad[14],
           "state": propiedad[15],
           "street": propiedad[16],
           "currency": "MXN",
           "short_address": f"{propiedad[16]} #{propiedad[17]}, Col. {propiedad[12]}",
           "numero_exterior": None,
           "size_ground": propiedad[20],
           "size_m2": None,
           "bathrooms": propiedad[7],
           "min_bedrooms": None,
           "min_bathrooms": None,
           "min_ground": None,
           "precio": propiedad[6],
           "precio_format": propiedad[6],
           "price_min": "$ 0",
           "price_max": "$ 0",
           "bedrooms": propiedad[8],
           "unit": 0,
           "purpose":  propiedad[4],
           "purpose_str": "Renta" if  propiedad[8] == 2 else "Venta",
           "phone_contact": None,
           "infoga": None,
           "from_data": 0,
           "url": f"https://penelope.propiedades.com/inmuebles/-{propiedad[0]}",
           "from_xml": None,
           "is_showroom": 0,
           "update": "2024-04-29",
           "description": propiedad[2],
           "total_photos": 0,
           "latitude": "19.4210472",
           "longitude": "-99.1714477",
           "full_address": f"{propiedad[16]} #{propiedad[17]}, Col. {propiedad[12]} C.P. {propiedad[14]}, {propiedad[13]}",
           "ground_unit": 0,
           "construction_unit": 0,
           "size_house": propiedad[19],
           "rental_price": propiedad[6],
           "sale_price": propiedad[6],
           "bathrooms_half": 0,
           "external_num": propiedad[17],
           "internal_num": None,
           "state_id": propiedad[18],
           "zone_id": None,
           "has_street": 1,
           "hide_street": None,
           "hide_external_num": 0,
           "hide_internal_num": 0,
           "verifiedUserWhatsapp": "",
           "isAuction": False,
           "isExclusive": False,
           "clean_age": 7
        }
        properties.append(tmp)
        # result_properties = pd.DataFrame(properties,columns=['property_id', 'score', 'description', 'sepomex_id', 'purpose', 'type_children', 'price', 'bathrooms', 'bedrooms','parking_num'])
    # print(json_response)
    return properties

def save_user_lifetime(conexion_w, user_lifetime, purpose, max_price, city_id, places_interest, bedrooms, parking_num):
    # Guarda datos de usuario perfilado
    print("-------- Se guarda informacion de quiz")
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
            {places_interest},
            {bedrooms},
            {parking_num}
        )
    """
    mycursor_w = conexion_w.cursor()
    mycursor_w.execute(query)
    conexion_w.commit()
    row_insert = mycursor_w.rowcount
    return row_insert

def get_properties_to_places_of_interest(places_of_interest):
    json_object = json.loads(places_of_interest)

   
    places_list = []
    for interest in json_object:
        place = interest['name']
        places_list.append(place)
    
    list_places = ', '.join([str(elem) for elem in places_list])

    return list_places

def post_user_preferences():
    # Obtenemos datos del front
    user_lifetime = "estudiante" 
    purpose = 1 
    max_price = 1000000
    city_id = 6
    places_interest = """[
        {
            "latitude": 1000,
            "longitude": 2000,
            "name": "Hospital angeles"
        },
        {
            "latitude": 1000,
            "longitude": 2000,
            "name": "Escuela primaria"
        },
        {
            "latitude": 1000,
            "longitude": 2000,
            "name": "Centro comercial"
        }
    ]"""
    bedrooms = 1 
    parking_num = 2

    # Conexion a BD
    conexion_w = mysql.connector.connect(
        host = "10.97.144.113",
        user = "root",
        password = "propiedades",
        database="propiedades",
    )

    # Guardamos datos del quiz
    str_places_interest = json.dumps(places_interest)
    row_insert = save_user_lifetime(conexion_w, user_lifetime, purpose, max_price, city_id, str_places_interest, bedrooms, parking_num)
    
    if  row_insert > 0:
        print("-------- Perfila propiedades de su interes")
        list_places = get_properties_to_places_of_interest(places_interest)
        
        json_result_properties = {}
        purpose_str = ""
        purpose_value = ""

        if purpose == 1:
            purpose_str = "sale_price"
            purpose_value = "venta"
        else:
            purpose_str = "rental_price"
            purpose_value = "renta"

        query = f"""
            SELECT property_id, score, description, sepomex_id, purpose, type_children, {purpose_str} as price, bathrooms, bedrooms, parking_num
            FROM properties_search
            WHERE city_id = {city_id}
            AND purpose = {purpose}
            AND type_children in (1,2)
            AND status = 1
            AND highlighted != 1
            AND bedrooms >= {bedrooms}
            ORDER BY score DESC
            limit 50
        """
        
        mycursor_w = conexion_w.cursor()
        mycursor_w.execute(query)
        myresult = mycursor_w.fetchall()
        properties = []

        for x in myresult:
            properties.append(x)
        result_properties = pd.DataFrame(properties,columns=['id', 'score', 'description', 'sepomex_id', 'purpose', 'type_children', 'price', 'bathrooms', 'bedrooms','parking_num'])
        json_result_properties = result_properties.to_json(orient ='records') 

        # Envia datos a Gemini
        recomendetions_ia = post_gemini(json_result_properties, user_lifetime, purpose_value, max_price, bedrooms, parking_num, list_places)
        
        # Obtenemos informacion de propiedades recomendadas
        recomendetions = get_recomendations(recomendetions_ia, conexion_w)
        return recomendetions

h = post_user_preferences()