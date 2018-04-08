from pymongo import MongoClient
import pandas as pd
import numpy as np

client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario
cancionesDF
relaciones
cancionesRatings
corrMatrix
ratings


def leerDatos():
    
    cursor = canciones.find({}, {'_id': False})
    cancionesDF = pd.DataFrame(list(cursor))
    
    #cursor = cancion_usuario.find({ 'valoracion': { '$ne': 0 }}, {'_id': False})
    cursor = cancion_usuario.find({}, {'_id': False})
        
    relaciones = pd.DataFrame(list(cursor))
    
    contador = 0
    for aux in relaciones['cancion_id']:
         emocion = cancionesDF[cancionesDF['cancion_id'] == aux].iloc[0]['emocion']
         
         if emocion == 'Exaltado':            
             relaciones['valoracion'][contador] = 1
         elif emocion == 'Alegre':
             relaciones['valoracion'][contador] = 2
         elif emocion == 'Exaltado':
             relaciones['valoracion'][contador] = 3
         elif emocion == 'Excitado':
             relaciones['valoracion'][contador] = 4    
         elif emocion == 'Activo':
             relaciones['valoracion'][contador] = 5
         elif emocion == 'Triste':
             relaciones['valoracion'][contador] = 6
         elif emocion == 'Calmado':
             relaciones['valoracion'][contador] = 7
         contador = contador + 1 

    ratings = pd.merge(cancionesDF,relaciones)
    
    
    cancionesRatings = ratings.pivot_table(index=['usuario_id'],columns=['cancion_id'],values='valoracion')
    corrMatrix = cancionesRatings.corr(method='pearson')
    
    corrMatrix.to_csv("matriz")
    cancionesRatings.to_csv("canciones_usuarios")
    
def valoracionesUsuario(usuario):
    
    myRatings = cancionesRatings.loc[usuario].dropna()
    myRatings.to_csv("valoraciones", header=1)
    
    return myRatings
    
def cancionesSimilares(cancion):
    
    cancionRating = cancionesRatings[cancion]
    similarSongs = cancionesRatings.corrwith(cancionRating)
    similarSongs = similarSongs.dropna()

    similarSongs.to_csv("similares")
    
    return similarSongs
     
def estadisticasValoraciones():
    
    songStats = ratings.groupby('cancion_id').agg({'valoracion': [np.size,np.mean]})
    popularSongs = songStats['valoracion']['size'].gt(1)
    
    songStats[popularSongs].sort_values([('valoracion', 'mean')], ascending=False)[:15]
    
    #df = songStats[popularSongs].join(pd.DataFrame(similarSong, columns=['similarity']))
    
    #print(df)
    
    songStats.to_csv("estadisticas")

def motorRecomendacion(usuario):
    
    myRatings = valoracionesUsuario(usuario)
    
    posiblesSimilares = pd.Series()

    for i in range(0, len(myRatings.index)):
     #print ("Similares a " + myRatings.index[i] + "...")
     
         sims = corrMatrix[myRatings.index[i]].dropna()
    
         sims = sims.map(lambda x: x * myRatings[i])
     
         posiblesSimilares = posiblesSimilares.append(sims)
      
         
    posiblesSimilares = posiblesSimilares.groupby(posiblesSimilares.index).sum()    
    filtered = posiblesSimilares.drop(myRatings.index,errors='ignore')
    filtered.to_csv("recomendaciones")
    
    
#cursor3 = cancion_usuario.aggregate( 
#            [
#                {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id" } } },
#            ]
#        );
            
#df3 = pd.DataFrame(list(cursor3))

leerDatos()
#cancionesSimilares('0pyT9W877RcYtVdl1ZmlvQ')
motorRecomendacion('mariopirey')







