from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from math import sqrt
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors

client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario


#Obtener datos de las relaciones basados en un campo de rating
def get_datos(rating_id):
        
    if(rating_id != 'count'):
        cursor = cancion_usuario.aggregate( 
                    [
                        {"$match": { rating_id: { "$gt": 0 } } },
                        {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id"}, 'valoracion': {"$first": "$valoracion"}, 'valoracion_emocion': {"$first": "$valoracion_emocion"},  'count': { '$sum': 1 }} },
                        
                    ]
                );
    else:
        cursor = cancion_usuario.aggregate( 
                    [
                        {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id"}, 'valoracion': {"$first": "$valoracion"}, 'valoracion_emocion': {"$first": "$valoracion_emocion"},  'count': { '$sum': 1 }} },
                        
                    ]
                );
           
    copia = list(cursor)    
    
    relaciones = pd.DataFrame(list(aux['_id'] for aux in copia)).join(pd.DataFrame(list(aux['valoracion'] for aux in copia),columns=['valoracion']))
    relaciones = relaciones.join(pd.DataFrame(list(aux['valoracion_emocion'] for aux in copia),columns=['valoracion_emocion']))
    relaciones = relaciones.join(pd.DataFrame(list(aux['count'] for aux in copia),columns=['count']))  
    
    relaciones.to_csv('Datos')
           
    n_users = relaciones.usuario_id.unique().shape[0]
    n_items = relaciones.cancion_id.unique().shape[0]
    
    print('Usuarios = ' + str(n_users) + ' | Canciones = ' + str(n_items))
    
    return relaciones

#Obtener la matriz Usuarios-Canciones basada en un campo de rating
def getMatrix(datos,rating_id):
    
    cancionesRatings = datos.pivot_table(index=['usuario_id'],columns=['cancion_id'],values=rating_id)
    cancionesRatings.to_csv('Matriz_Inicial')
    
    return cancionesRatings
    
    
#Obtener la matriz de similitud Cancion-Cancion utilizando la función coseno 
def predict_items(ratings):
    
    ratings.fillna(0)
    
    item_similarity = 1 - pairwise_distances(ratings.T, metric='cosine')
    #np.savetxt('item_similarity', item_similarity, delimiter=",",fmt='%f')
        
    return item_similarity


#Obtener la matriz de similitud Usuario-Usuario utilizando la función coseno
def predict_users(ratings):   

    ratings.fillna(0)    
    user_similarity = 1 - pairwise_distances(ratings, metric='cosine')
    
    recom_users = pd.DataFrame(user_similarity, index=ratings.index.values, columns= ratings.index.values)
    recom_users.to_csv('Similitud_Usuarios')
    
    return user_similarity
    
def cancionesSimilares(item_similarity,cancion):
    
    similarSongs = item_similarity[cancion].sort_values(ascending=False)[:50]
    similarSongs.to_csv("similares")
    
    return similarSongs
    
    #return similarSongs
    
#funcion de evaluacion usando Root Mean Squared Error
def rmse(prediction, ground_truth):
    prediction = prediction[ground_truth.nonzero()].flatten()
    ground_truth = ground_truth[ground_truth.nonzero()].flatten()
    
    return sqrt(mean_squared_error(prediction, ground_truth))
    
    
#Estadisticas relativas a las canciones con mejores valoraciones
def estadisticasValoraciones(ratings,rating_id):
    
    songStats = ratings.groupby('cancion_id').agg({rating_id: [np.size,np.mean]})
    popularSongs = songStats[rating_id]['size'].gt(1)
    
    songStats = songStats[popularSongs].sort_values([(rating_id, 'mean')], ascending=False)
    songStats.to_csv('SongStats')
    
    
#Obtener recomendacion de canciones
def getRecommendation(usuario,rating_id):

    relaciones = get_datos(rating_id)
    cancionesRatings = getMatrix(relaciones,rating_id)  
    estadisticasValoraciones(relaciones,rating_id)
    
    #wide_artist_data_sparse = csr_matrix(cancionesRatings.T.values)
        
    #canciones con mayor rating del usuario
    myRatings = cancionesRatings.loc[usuario].dropna().sort_values(ascending=False)
    myRatings.to_csv('Ratings_usuario',header=1)
    
    cancionesRatings = cancionesRatings.fillna(0)
    model_knn = NearestNeighbors(metric = 'cosine', algorithm = 'brute')
    model_knn.fit(cancionesRatings)

    query_index = np.random.choice(cancionesRatings.shape[0])
    print(query_index)
    distances, indices = model_knn.kneighbors(cancionesRatings.iloc[query_index, :].reshape(1, -1), n_neighbors = 5)

    for i in range(0, len(distances.flatten())):
        if i == 0:
            print('Recommendations for {0}:\n'.format(cancionesRatings.index[query_index]))
        else:
            print('{0}: {1}, with distance of {2}:'.format(i, cancionesRatings.index[indices.flatten()[i]], 1 - distances.flatten()[i]))
    
    print(cancionesRatings.T.index[2])
    predict_users(cancionesRatings)
    item_similarity = predict_items(cancionesRatings)
    item_similarityDF = pd.DataFrame(item_similarity, index=list(cancionesRatings.columns.values), columns= list(cancionesRatings.columns.values) )

    posiblesSimilares = pd.Series()
    
    for i in range(0, len(myRatings.index)):
        sims = cancionesSimilares(item_similarityDF,myRatings.index[i])
        
        posiblesSimilares = posiblesSimilares.append(sims)
        
    posiblesSimilares = posiblesSimilares.groupby(posiblesSimilares.index).sum()
    filtered = posiblesSimilares.drop(myRatings.index,errors='ignore').sort_values(ascending=False)
    filtered.to_csv('Recomendaciones')
    
    #recom_items = cancionesRatings.fillna(0).dot(item_similarity).as_matrix() / np.array([np.abs(item_similarity).sum(axis=1)])
    
    #recom_items = pd.DataFrame(recom_items, index=cancionesRatings.index.values, columns= list(cancionesRatings.columns.values) )
    #recom_items.to_csv('recom_items')
    
    #userRatings = recom_items.loc[usuario]
  
    #filtered = userRatings.drop(myRatings.index,errors='ignore')
    #filtered = pd.DataFrame(filtered).sort_values(usuario,ascending=False)
    #filtered.to_csv('Recomendaciones')


#Obtener evaluacion de los resultados predichos
def getEvaluation(rating_id):
    
    cancionesRatings = getMatrix(get_datos(rating_id),rating_id)
    train_data, test_data = train_test_split(cancionesRatings, test_size=0.33)
    item_similarity = predict_items(train_data)
    
    recom_items = cancionesRatings.fillna(0).dot(item_similarity).as_matrix() / np.array([np.abs(item_similarity).sum(axis=1)])
    
    evaluation = rmse(recom_items,test_data.fillna(0).as_matrix())
    print(evaluation)
       
getRecommendation('11125830071','valoracion')
#getEvaluation('valoracion_emocion')
    