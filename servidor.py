import json
from flask import Flask, request,jsonify
import spotipy
from flask_cors import CORS
import requests
from pymongo import MongoClient
import os
import emociones
from bson.json_util import dumps

app = Flask(__name__)
CORS(app)
client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario

@app.errorhandler(401)
def not_authorized(error=None):
    message = {
            'status': 401,
            'message': 'Not Authorized: ',
    }
    resp = jsonify(message)
    resp.status_code = 401

    return resp

def download_file(url,local_filename):
    r = requests.get(url, stream=True)
    with open('Canciones/'+local_filename+'.mp3', 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk:
                f.write(chunk)
    return local_filename
    
def almacenar_cancion(cancion):
    
    identificador = cancion['id']
    download_file(cancion['preview_url'],identificador)
    os.system('essentia_streaming_extractor_music Canciones/'+identificador+'.mp3 Canciones/'+identificador+'.json configuracion.yaml')
    os.remove('Canciones/'+identificador+'.mp3')   

    emocion = emociones.clasificadorEmociones(identificador)
    
    cancionJSON = {"cancion_id": identificador,
               "titulo": cancion['name'],
               "artistas": [artista['name'] for artista in cancion['artists']],
               "preview_url": cancion['preview_url'],
                "imagen": cancion['album']['images'][0]['url'],
                "emocion": emocion}   
                
    canciones.insert_one(cancionJSON)
    
def crear_relacion(cancion,usuario,fecha):
    
    relacion = {"cancion_id": cancion,
                "usuario_id": usuario,
                "fecha":fecha,
                "valoracion": 0}   
                                
    cancion_usuario.insert_one(relacion)

@app.route("/Perfil")
def getPerfil():
    
    auth = request.headers.get('Authorization')
    
    try:
        sp = spotipy.Spotify(auth=auth)
        respuesta = sp.current_user()  
    except: 
        return not_authorized()
                      
    data = {}
    data['id'] = respuesta['id']

    try:
        usuarios.insert_one({'identificador': data['id']})
    except:
        print('Duplicado')        
        
    if len(respuesta['images']) > 0:
        data['image'] = respuesta['images'][0]['url']
    else:
        data['image'] = None
        
    if respuesta['display_name']:
        data['display_name'] = respuesta['display_name']
    else:
        data['display_name'] = data['id']
                        
    return json.dumps(data)
    
@app.route("/Recientes")
def getRecientes():
    
    auth = request.headers.get('Authorization')   
    cantidad = request.args.get('cantidad')
    
    try:
        sp = spotipy.Spotify(auth=auth)
        respuesta = sp.current_user_recently_played(limit=int(cantidad))
        usuario = sp.current_user()
    except: 
        return not_authorized()
        
    cancionesArray = []
    
    for aux in respuesta['items']: 
        
        cancion = canciones.find({'cancion_id': aux['track']['id']}, {'_id': False})
        if not cancion.count() > 0: 
            if aux['track']['preview_url']:
                almacenar_cancion(aux['track'])
                cancion = canciones.find({'cancion_id': aux['track']['id']}, {'_id': False})
          

        relacion = cancion_usuario.find({'usuario_id': usuario['id'],'cancion_id': aux['track']['id'],'fecha': aux['played_at'].split('.')[0] }, {'_id': False})
        
        cancion = dumps(cancion[0])
        cancion = json.loads(cancion)
        
        if relacion.count() > 0:
            cancion['valoracion'] = relacion[0]['valoracion'] 
        else:
            crear_relacion(aux['track']['id'],usuario['id'],aux['played_at'].split('.')[0])
            cancion['valoracion'] = "0"
        
        cancionesArray.append(cancion)
                
    return json.dumps(cancionesArray)
    
@app.route("/Actual")
def getActual():
    
    auth = request.headers.get('Authorization')
    
    try:        
        sp = spotipy.Spotify(auth=auth)
        respuesta = sp.currently_playing('ES')
        usuario = sp.current_user()
    except: 
        return not_authorized()
                 
    if respuesta:  
                 
        cancion = canciones.find({'cancion_id': respuesta['item']['id']}, {'_id': False})
        if not cancion.count() > 0:
            if respuesta['item']['preview_url']:
                almacenar_cancion(respuesta['item'])
                cancion = canciones.find({'cancion_id': respuesta['item']['id']}, {'_id': False}) 
               
#        relacion = cancion_usuario.find({'usuario_id': usuario['id'],'cancion_id': respuesta['item']['id'] }, {'_id': False})
        
#        cancion = dumps(cancion[0])
#        cancion = json.loads(cancion)
                        
#        if relacion.count() > 0:
#            cancion['valoracion'] = relacion[0]['valoracion'] 
#        else:
#            cancion['valoracion'] = "0"
    else:
        return ('', 204)
        
    return json.dumps(cancion[0])
    
@app.route('/Cancion/<cancion_id>', methods=['PUT'])
def actualizarValoracion(cancion_id):
    
    auth = request.headers.get('Authorization')
    cancion = request.json
    valoracion = cancion['valoracion']
   
    try:        
        sp = spotipy.Spotify(auth=auth)
        usuario = sp.current_user()
    except: 
        return not_authorized()
        
    relacion = cancion_usuario.find({'usuario_id': usuario['id'],'cancion_id': cancion_id})
    
    if relacion.count() > 0:
        cancion_usuario.update_many({"usuario_id": usuario['id'],"cancion_id": cancion_id},{'$set': {"valoracion": int(valoracion)}})
        
    return ('', 200)
    
    
if __name__ == "__main__":
    app.run(threaded=True,host='0.0.0.0',port=8888)