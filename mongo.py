import pymongo
from pymongo import MongoClient, IndexModel
import emociones
import spotipy.util as util
import spotipy
from os import listdir
import datetime
import time
import numpy as np
import pandas as pd

client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario

def getAuthToken(username,client_id,client_secret):
    
    scope = 'user-modify-playback-state user-read-playback-state user-read-recently-played user-read-currently-playing user-modify-playback-state'
    redirect_uri = 'http://localhost:4200/callback'

    token = util.prompt_for_user_token(username, scope, client_id, client_secret,redirect_uri)
        
    return token
    
def procesarCanciones():
    token = getAuthToken('11125830071',"8ef0ee1341c140f2bfebf028664b280d","98eb3c80931b4aa1a15200905ec3709d")
    
    sp = spotipy.Spotify(auth=token)
    
    arrayCanciones = []
    
    for fichero in listdir("Canciones"):
        respuesta = sp.track(fichero.split('.')[0])
        emocion = emociones.clasificadorEmociones(fichero.split('.')[0])
        cancion = {"cancion_id": respuesta['id'],
                       "titulo": respuesta['name'],
                       "artistas": [artista['name'] for artista in respuesta['artists']],
                       "preview_url": respuesta['preview_url'],
                       "imagen": respuesta['album']['images'][0]['url'],
                       "emocion": emocion}
        arrayCanciones.append(cancion)
        
    canciones.insert_many(arrayCanciones)
    
def cambiarFechas():
    cursor = cancion_usuario.find({}, {'_id': False})
    
    
    for aux in cursor:

        fecha = aux['fecha']
        hora = aux['fecha'].split('T')[1]
        convertidaFecha = time.mktime(datetime.datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%S").timetuple())
        convertidaHora = time.mktime(datetime.datetime.strptime(hora, "%H:%M:%S").timetuple())
        
        cancion_usuario.update_one({'usuario_id': aux['usuario_id'], 'cancion_id': aux['cancion_id'], "fecha": fecha},{'$set': {'fecha': convertidaFecha }})
        #print(datetime.datetime.fromtimestamp(int(convertidaFecha)).strftime('%Y-%m-%dT%H:%M:%S'))
        #print(fecha)
        #print(datetime.datetime.fromtimestamp(int(convertidaHora)).strftime('%H:%M:%S'))
        #print(hora)
        
def convertidoresHoras():

    print(datetime.datetime.fromtimestamp(int(1523003197.0)).strftime("%Y-%m-%dT%H:%M:%S"))
    print(datetime.datetime.fromtimestamp(int(-2208950319.0)).strftime("%H:%M:%S"))
    
def getBD():
    
    cursor = usuarios.find({}, {'_id': False})

    for aux in cursor:
        print(aux)


    print(cursor.count())
    
    cursor = canciones.find({}, {'_id': False})
    
    #for aux in cursor:
        #print(aux)
    
    print(cursor.count())
          
    #cursor = cancion_usuario.find({"valoracion": {"$gt": 1}})
    cursor = cancion_usuario.find({}, {'_id': False})
        
    for aux in cursor:
        print(aux)
        
    print(cursor.count())
  
    cursor = usuario_usuario.find({}, {'_id': False})
    
    for aux in cursor:
        print(aux)
    
    print(cursor.count())
    
def getEmocionesUsuarios():
    
    cursor = cancion_usuario.find({}, {'_id': False}).sort([("fecha", pymongo.DESCENDING)]).limit(100)
    
    
    df = pd.DataFrame(columns=['Emocion'])
    
    for aux in cursor:
        
        emocion = canciones.find({'cancion_id': aux['cancion_id']}, {'_id': False})[0]['emocion']
        df = df.append({'Emocion': emocion}, ignore_index=True)
        print(canciones.find({'cancion_id': aux['cancion_id']}, {'_id': False})[0])
    
    emocionesStats = df.groupby('Emocion').groups
    print(emocionesStats)
    print(cursor.count())



#usuario_usuario.create_index([('usuario_id', pymongo.TEXT),('seguido_id', pymongo.TEXT)],unique=True)    
#cancion_usuario.create_index([('cancion_id', pymongo.TEXT),('fecha', pymongo.TEXT)],unique=True)
#index1 = IndexModel([("cancion_id", pymongo.TEXT),
#                     ("fecha", pymongo.TEXT)], unique=True)
#index2 = IndexModel([("usuario_id", pymongo.TEXT)])
#cancion_usuario.create_indexes([index1, index2])
#usuarios.remove()
#usuarios.insert_one(data)

#procesarCanciones()

#cancion_usuario.insert_one({'cancion_id':'ide','usuario_id':'idi','fecha':'ayer'})
#cancion_usuario.remove()

#cancion_usuario.drop()
#usuario_usuario.drop()

#usuarios.update({'identificador': 'mariopirey'}, {'$rename': { 'identificador': 'usuario_id'}})
 #cancion_usuario.update_many({"usuario_id": "11125830071","cancion_id": "6leiozlpP23gdhy9j8eHbx"},{'$set': {"valoracion": "2"}})


getBD()    
#cambiarFechas()




