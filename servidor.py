import json
from flask import Flask, request,jsonify
import spotipy
from flask_cors import CORS
import requests
from pymongo import MongoClient
import os
import emociones
from bson.json_util import dumps
import time
import datetime

app = Flask(__name__)
CORS(app)
client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario

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
    
    fechaConvertida = time.mktime(datetime.datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%S").timetuple())
    hora = fecha.split('T')[1]
    horaConvertida = time.mktime(datetime.datetime.strptime(hora, "%H:%M:%S").timetuple())
    
    relacion = {"cancion_id": cancion,
                "usuario_id": usuario,
                "fecha":fechaConvertida,
                "hora": horaConvertida,
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
                      
    usuario = usuarios.find({'usuario_id': respuesta['id']}, {'_id': False})
    
    if usuario.count() == 0: 
        if len(respuesta['images']) > 0:
            image = respuesta['images'][0]['url']
        else:
            image = None
        
        if respuesta['display_name']:
            display_name = respuesta['display_name']
        else:
            display_name = respuesta['id']
        usuarios.insert_one({'usuario_id': respuesta['id'], 'image': image, 'display_name': display_name})
        usuario = usuarios.find({'usuario_id': respuesta['id']}, {'_id': False})
                    
    return dumps(usuario[0])
    
@app.route("/Usuarios")
def getUsuarios():
    
    auth = request.headers.get('Authorization')
    
    try:
        sp = spotipy.Spotify(auth=auth)
        usuario = sp.current_user()
    except: 
        return not_authorized()
        
    respuesta = usuarios.find({'usuario_id':{"$ne": usuario['id']}}, {'_id': False})
    
    respuesta = dumps(respuesta)
    respuesta = json.loads(respuesta)
    
    for usua in respuesta:
        relacion = usuario_usuario.find({'usuario_id': usuario['id'], 'seguido_id': usua['usuario_id']}, {'_id': False})
    
        if relacion.count() > 0:
            usua['seguido'] = True
        else:
            usua['seguido'] = False
                        
    return json.dumps(respuesta)
    
@app.route('/Usuario', methods=['PUT'])
def actualizarDatos():
    
    auth = request.headers.get('Authorization')
    datos = request.form
   
    try:        
        sp = spotipy.Spotify(auth=auth)
        usuario = sp.current_user()
    except: 
        return not_authorized()
        
    consulta = usuarios.find({'usuario_id': usuario['id']})
    
    if consulta.count() > 0:
        usuarios.update_one({"usuario_id": usuario['id']},{'$set': {"fecha_nacimiento": datos['fecha_nacimiento'], 'sexo': datos['sexo'] }})
        
    return ('', 200)
    
@app.route('/Usuario/<usuario_id>', methods=['PUT'])
def seguirUsuario(usuario_id):
    
    auth = request.headers.get('Authorization')
    
    try:        
        sp = spotipy.Spotify(auth=auth)
        usuario = sp.current_user()
    except: 
        return not_authorized()
    
    relacion = usuario_usuario.find({'usuario_id': usuario['id'], 'seguido_id': usuario_id}, {'_id': False})
    
    if relacion.count() > 0:
        usuario_usuario.remove({'usuario_id': usuario['id'], 'seguido_id': usuario_id})
    else:
        usuario_usuario.insert_one({'usuario_id': usuario['id'], 'seguido_id': usuario_id})
        

    return ('', 200)
    
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
        
        if cancion.count() > 0:
            fecha = time.mktime(datetime.datetime.strptime(aux['played_at'].split('.')[0], "%Y-%m-%dT%H:%M:%S").timetuple())
            relacion = cancion_usuario.find({'usuario_id': usuario['id'],'cancion_id': aux['track']['id'],'fecha': fecha }, {'_id': False})
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
               
        relacion = cancion_usuario.find({'usuario_id': usuario['id'],'cancion_id': respuesta['item']['id'] }, {'_id': False})

        cancion = dumps(cancion[0])
        cancion = json.loads(cancion)
                      
        if relacion.count() > 0:
            cancion['valoracion'] = relacion[0]['valoracion'] 
        else:
            cancion['valoracion'] = "0"
    else:
        return ('', 204)
        
    return json.dumps(cancion)
    
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