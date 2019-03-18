#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 09:28:09 2019

@author: benji
"""
import pymongo
import warnings
import os
import pandas as pd
import folium
import matplotlib.pyplot as plt
warnings.filterwarnings('ignore')
#from IPython.display import Image
from PIL import Image
import requests
from io import BytesIO
from folium.plugins import HeatMap
import numpy as np

#from IPython.core.display import HTML

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient["twitter"]

collection = db["tweets"]

print(collection.count(), 'documents')


#limit -> cuantos mostrar
def masRetweets(limit):
    data = "No se han encontrado datos"
    try:
      max_retweets = collection.find().sort('retweet_count', -1).limit(int(limit))
      data = pd.DataFrame(list(max_retweets), columns=['id_str', 'text', 'retweet_count'])
    except:
      print("An exception occurred")
    
    return data

#limit -> cuantos mostrar
def masFavoritos(limit):
    data = "No se han encontrado datos"
    try:
        max_favorites = collection.find().sort('favorite_count', -1).limit(int(limit))
        data = pd.DataFrame(list(max_favorites), columns=['id_str', 'text', 'favorite_count'])
    except:
      print("An exception occurred")
    
    return data

#usuario -> usuario a mostrar
def twitsUsuario(usuario):
    user_tweets = collection.find({'user.screen_name': usuario})
    data = pd.DataFrame(list(user_tweets), columns=['id_str', 'text', 'retweet_count', 'favorite_count', 'created_at'])
    if(data.size==0):
        data="Usuario no encontrado"
    return data

#hastag -> debe llebar # delante
def twitsHastag(hastag):
    if(len(hastag)>0):
        if(hastag[0]!='#'):
           hastag = '#'+hastag
        hashtags = collection.find({'text': {'$regex': hastag}})                                     
        data = pd.DataFrame(list(hashtags), columns=['id_str', 'text', 'retweet_count', 'favorite_count', 'created_at'])
        
        if(data.size==0):
            data="Hastag no encontrado"
    else:
        data = "El hastag no es correcto"
    
    return data


def usuarioMasSeguidores():
    max_followers = collection.find().sort('user.followers_count', -1).limit(1)
    user = max_followers[0]['user']
    print(user['name'])
    print(int(user['statuses_count']), 'tweets')
    print(int(user['followers_count']), 'followers')
    print(user['description'])
    
    response = requests.get('https://avatars.io/twitter/'+user['screen_name'])
    img = Image.open(BytesIO(response.content))
    img.show()
    

def agrupadosIdiomas():
    
    tweets_by_lang = [
        {'$group': {'_id': '$lang', 'count': {'$sum': 1}}},
        {'$sort': {'count': 1}}
    ]
    result = collection.aggregate(tweets_by_lang)
    data = pd.DataFrame(list(result))
    
    plt.figure(figsize=(10, 5))
    plt.bar(data['_id'], data['count'])
    plt.show()

def agrupadosFuente():
    
    tweets_by_source = [
        {'$group': {'_id': '$source', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    result = collection.aggregate(tweets_by_source)
    data = pd.DataFrame(list(result))
    
    plt.figure(figsize=(10, 5))
    plt.pie(data['count'], labels=data['_id'], autopct='%1.1f%%')
    plt.show()



#limit -> cuantos mostrar
def masRecientes(limit):
    data = "No se han encontrado datos"
    
    try:
        tweets_by_date = [
            {'$project': {'_id': '$id_str', 'text': '$text', 'user': '$user.screen_name',
                          'created_at': {'$dateFromString': {'dateString': '$created_at'}}
                         }
            }, 
            {'$sort': {'created_at': -1}},
            {'$limit': int(limit)}
        ]
        result = collection.aggregate(tweets_by_date)
        data = pd.DataFrame(list(result), columns=['_id', 'user', 'text', 'created_at'])
    except:
        print("An exception occurred")
    
    return data


def ubicacionTwits():
    user_geo = collection.find_one({'geo': {'$ne': None}})
    coordinates = user_geo['geo']['coordinates']
    place = user_geo['place']['full_name']
    print(coordinates)

    mapa = folium.Map(location=coordinates, zoom_start=12, tiles='OpenStreetMap')
    folium.Marker(coordinates, popup=place).add_to(mapa)
    
    # create map
    #data = (np.random.normal(size=(100, 3)) * np.array([[1, 1, 1]]) + np.array([[48, 5, 1]])).tolist()
    #HeatMap(data).add_to(mapa)
    #mapa.render()
    
    print(type(mapa.get_root()))
    #Image.open(mapa.get_root())

    
    



def menu():
	"""
	Función que limpia la pantalla y muestra nuevamente el menu
	"""
	os.system('clear') # NOTA para windows tienes que cambiar clear por cls
	print ("Selecciona una opción")
	print ("\t1 - Ver los twits con más retwits")
	print ("\t2 - Ver los twits con más favoritos")
	print ("\t3 - Ver twits de un usuario")
	print ("\t4 - Ver hastag")
	print ("\t5 - Ver usuario con más seguidores")
	print ("\t6 - Twits agrupados por idioma")
	print ("\t7 - Twits agrupados por fuente")
	print ("\t8 - Twits más recientes")
	print ("\t9 - Ubicación twits")
	print ("\t0 - Salir")
    
if __name__ == '__main__':

    terminar = False    

 
 
    while not terminar:
    	# Mostramos el menu
    	#menu()
        #os.system('clear') # NOTA para windows tienes que cambiar clear por cls
        print ("\nSelecciona una opción")        
        print ("\t1 - Ver los twits con más retwits")
        print ("\t2 - Ver los twits con más favoritos")
        print ("\t3 - Ver twits de un usuario")
        print ("\t4 - Ver hastag")
        print ("\t5 - Ver usuario con más seguidores")
        print ("\t6 - Twits agrupados por idioma")
        print ("\t7 - Twits agrupados por fuente")
        print ("\t8 - Twits más recientes")
        print ("\t9 - Ubicación twits")
        print ("\t0 - Salir")
     
    	# solicituamos una opción al usuario
        opcionMenu = input("Inserta una opción: ")
        
        if opcionMenu=="1":
            print ("")
            data = masRetweets(input("Has pulsado la opción 1...\nInserta cantidad de twits que quieres ver: "))
            print(data)
            #input("\npulsa una tecla para continuar")

        elif opcionMenu=="2":
            print("")
            data = masFavoritos(input("Has pulsado la opción 2...\nInserta cantidad de twits que quieres ver: "))
            print(data)
            #input("\npulsa una tecla para continuar")

        elif opcionMenu=="3":
            print ("")
            print(twitsUsuario(input("Has pulsado la opción 3...\nInserta el usuario: ")))
            #input("\npulsa una tecla para continuar")

        elif opcionMenu=="4":
            print ("")

            print(twitsHastag(input("Has pulsado la opción 4...\nInserta el hastag: ")))
            #input("\npulsa una tecla para continuar")


        elif opcionMenu=="5":
            print ("")
            print("Has pulsado la opción 5...\n")
            print(usuarioMasSeguidores())
            #input("\npulsa una tecla para continuar")

        elif opcionMenu=="6":
            print ("")
            print("Has pulsado la opción 6...\n")
            print(agrupadosIdiomas())
            #input("\npulsa una tecla para continuar")

        elif opcionMenu=="7":
            print ("")
            print("Has pulsado la opción 7...\n")
            print(agrupadosFuente())
            #input("\npulsa una tecla para continuar")

        if opcionMenu=="8":
            print ("")
            data = masRecientes(input("Has pulsado la opción 8...\nInserta cantidad de twits que quieres ver: "))
            print(data)
            #input("\npulsa una tecla para continuar")

        if opcionMenu=="9":
            print ("")
            print("Has pulsado la opción 9...\n")
            data = ubicacionTwits()
            print(data)
            #input("\npulsa una tecla para continuar")

        elif opcionMenu=="0":
            terminar = True
            
        else:
            print ("")
            #print("No has pulsado ninguna opción correcta...\npulsa una tecla para continuar")
     


    #data = maxRetweets(10);
    #print(str(data["id_str"]) +" "+ str(data["text"]) +"  "+ str(data["retweet_count"]))
    
    


    
