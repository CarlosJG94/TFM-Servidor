from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances

client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario

def get_datos():
    cursor = cancion_usuario.find({ 'valoracion': { '$ne': 0 }}, {'_id': False})
    cursor = cancion_usuario.aggregate( 
                [
                    {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id"}, 'valoracion': {"$first": "$valoracion"}, 'valoracion_emocion': {"$first": "$valoracion_emocion"},  'count': { '$sum': 1 }} },
                    
                ]
            );
           
    copia = list(cursor)    
    
    relaciones = pd.DataFrame(list(aux['_id'] for aux in copia)).join(pd.DataFrame(list(aux['valoracion'] for aux in copia),columns=['valoracion'])).join(pd.DataFrame(list(aux['valoracion_emocion'] for aux in copia),columns=['valoracion_emocion']))
    n_users = relaciones.usuario_id.unique().shape[0]
    n_items = relaciones.cancion_id.shape[0]
    print('Usuarios = ' + str(n_users) + ' | Canciones = ' + str(n_items))
    
    cancionesRatings = relaciones.pivot_table(index=['usuario_id'],columns=['cancion_id'],values='valoracion')

    return cancionesRatings
    
def predict_items(ratings,usuario):

    myRatings = ratings.loc[usuario].dropna()

    ratings = ratings.fillna(0)
    
    item_similarity = 1 - pairwise_distances(ratings.T, metric='cosine')
    np.savetxt('item_similarity', item_similarity, delimiter=",",fmt='%f')
    
    pred = ratings.dot(item_similarity).as_matrix() / np.array([np.abs(item_similarity).sum(axis=1)])

    recom_items = pd.DataFrame(pred, index=ratings.index.values, columns= list(ratings.columns.values) )

    userRatings = recom_items.loc[usuario]
  
    filtered = userRatings.drop(myRatings.index,errors='ignore')
    filtered = pd.DataFrame(filtered).sort_values(usuario,ascending=False)
    filtered.to_csv('Recomendaciones')
    
def predict_users(ratings,usuario):   
    
    ratings = ratings.fillna(0)
    user_similarity = 1 - pairwise_distances(ratings, metric='cosine')
    np.savetxt('user_similarity', user_similarity, delimiter=",")
    
    recom_users = pd.DataFrame(user_similarity, index=ratings.index.values, columns= ratings.index.values)
    recom_users.to_csv('Similitud_Usuarios')


cancionesRatings = get_datos()
predict_items(cancionesRatings,'ines.suso')
predict_users(cancionesRatings,'mariopirey')













