import pymongo
from pymongo import MongoClient
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


    print('Usuarios: '+str(cursor.count()))
    
    cursor = canciones.find({}, {'_id': False})
    
    #for aux in cursor:
    #    print(aux['acustico'])
    
    print('Canciones: '+str(cursor.count()))
          
    #cursor = cancion_usuario.find({"valoracion": {"$gt": 1}})
    cursor = cancion_usuario.find({'usuario_id': '11125830071'}, {'_id': False}).sort([("fecha", pymongo.ASCENDING)])
        
    #for aux in cursor:
    #    print(aux)
        
    print('Cancion-Usuario: '+str(cursor.count()))
      
    cursor = usuario_usuario.find({}, {'_id': False})
    
    for aux in cursor:
        print(aux)
    
    print('Usuario-Usuario: '+str(cursor.count()))
    
def getEmocionesUsuarios():
    
    cursor = cancion_usuario.find({}, {'_id': False}).sort([("fecha", pymongo.DESCENDING)])
    #cursor = cancion_usuario.find({'hora' : {'$gte': -2208987916.0, '$lt': -2208937516.0}})
    
    df = pd.DataFrame(columns=['Emocion'])
    
    for aux in cursor:
        
        emocion = canciones.find({'cancion_id': aux['cancion_id']}, {'_id': False})[0]['emocion']
        df = df.append({'Emocion': emocion}, ignore_index=True)
    
    print(df['Emocion'].value_counts())
    print(cursor.count())
    
def getNoRepetidos():
    
                
    cursor = cancion_usuario.aggregate( 
            [
                {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id"}, 'valoracion': {"$first": "$valoracion"}, 'count': { '$sum': 1 }} },
                
            ]
        );
       
    copia = list(cursor)  
    result = pd.DataFrame(list(aux['_id'] for aux in copia)).join(pd.DataFrame(list(aux['valoracion'] for aux in copia),columns=['valoracion']))
   
    print(result)
    result.to_csv('sin_join')
    
def anadirValores():
    
    cursor = canciones.find({})
    
    for aux in cursor:
        valores = emociones.clasificadorEmociones(aux['cancion_id'])
        #canciones.update_one({'cancion_id': aux['cancion_id'] },{'$set': {"fiesta": valores['fiesta'],'triste': valores['triste'], 'relajado': valores['relajado']}})
        canciones.update_one({'cancion_id': aux['cancion_id'] },{'$set': {"acustico": valores['acustico']}})
        print(aux)
        
def anadirValores2():
    
    cursor = usuarios.find({})
    
    p = {}
    
    for aux in cursor:
        cursor2 = cancion_usuario.find({'usuario_id': aux['usuario_id']})
        for aux2 in cursor2:            
            valores = emociones.clasificadorEmociones(aux2['cancion_id'])
            usuarios.update({'usuario_id': aux['usuario_id']}, {'$inc': {valores['emocion']: 1}})

 
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
#usuarios.update_many({},{'$set': {"Exaltado": 0, 'Sereno': 0, 'Calmado': 0, 'Relajado': 0, 'Aburrido': 0, 'Triste': 0, 'Alegre:': 0, "Activo": 0, 'Deprimido': 0, 'Excitado': 0, 'Enfado': 0, 'Frustrado': 0 }})
#cancion_usuario.remove({'valoracion': 0})

#cancion_usuario.remove({'cancion_id': '5bhDL4GgUvr5LBrpjqyrte'})
#usuarios.update_many( {}, { '$rename': { 'Alegre:': 'Alegre'} } )
getBD()
#anadirValores2()
#cursor = canciones.find({'cancion_id': '1cm7v7dBQDjewVTYlqjUX6'})
#print(cursor[0])
#anadirValores()    
#cambiarFechas()
#getEmocionesUsuarios()

#print(time.mktime(datetime.datetime.strptime('2018-04-15T01:32:51.370Z', "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()))
#getNoRepetidos()



