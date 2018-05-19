from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from math import sqrt
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import pairwise_distances
import math

client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario

####################################### Datos #############################################


#Obtener datos de las relaciones basados en un campo de rating (valracion, valoracion_emocion)
def getDatos():
        
    cursor = cancion_usuario.aggregate( 
        [
            {"$match": { 'valoracion': { "$gt": 0 } } },
            {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id"}, 'valoracion': {"$first": "$valoracion"}, 'valoracion_emocion': {"$first": "$valoracion_emocion"},  'contador': { '$sum': 1 }} },
                        
        ]
    );
           
    copia = list(cursor)   
    print(len(copia))
    
    relaciones = pd.DataFrame(list(aux['_id'] for aux in copia)).join(pd.DataFrame(list(aux['valoracion'] for aux in copia),columns=['valoracion']))
    relaciones = relaciones.join(pd.DataFrame(list(aux['valoracion_emocion'] for aux in copia),columns=['valoracion_emocion']))
    relaciones = relaciones.join(pd.DataFrame(list(aux['contador'] for aux in copia),columns=['contador']))  
    
    relaciones.to_csv('Recomendador/Datos')
           
    n_users = relaciones.usuario_id.unique().shape[0]
    n_items = relaciones.cancion_id.unique().shape[0]
    
    print('Usuarios = ' + str(n_users) + ' | Canciones = ' + str(n_items))
    
    return relaciones


#Obtener la matriz Usuarios-Canciones basada en un campo de rating
def getMatrix(datos,rating_id):
    
    cancionesRatings = datos.pivot_table(index=['usuario_id'],columns=['cancion_id'],values=rating_id)
    cancionesRatings.to_csv('Recomendador/Matriz_Inicial')
    
    return cancionesRatings
            
        
######################################## User Reccomendation ###################################

#Función para devolver similitudes entre usuarios al Front-End    
def getSimilarUsers(usuario):

    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion')

    distances, indices = getDistancesUsers(usuario,cancionesRatings.fillna(0),8)
    
    similarUsers = cancionesRatings.index[indices.flatten()]
    usuariosArray = {}
    
    for i in range(1,len(similarUsers)):
        usuariosArray[similarUsers[i]] = distances[i]  
        
    return usuariosArray 
    

#Generar matriz con datos de ratings y atributos propios de las usuarios
def getMatrix_Users(relaciones,ratings):
    
    cursor = usuarios.find({},{'_id': False,'usuario_id': 1, "Exaltado": 1, 'Sereno': 1, 'Calmado': 1, 'Relajado': 1, 'Aburrido': 1, 'Triste': 1, 'Alegre:': 1, "Activo": 1, 'Deprimido': 1, 'Excitado': 1, 'Enfado': 1, 'Frustrado': 1 })
    
    copia = list(cursor)    
    valores_usuarios = pd.DataFrame(copia, index=[aux['usuario_id'] for aux in copia])
    del valores_usuarios['usuario_id']
    
    print(valores_usuarios)
    
    aux = valores_usuarios.sum(axis=1)
    
    print(valores_usuarios/638)
        
#Obtener distancia-similitudes de usuarios para un usuario concreto 
def getDistancesUsers(usuario, cancionesRatings, k):
    
    model_knn = NearestNeighbors(metric = 'cosine')
    model_knn.fit(cancionesRatings)    
    
    distances, indices = model_knn.kneighbors(cancionesRatings.loc[usuario].reshape(1, -1), n_neighbors = k+1)
    distances = 1 - distances[0]
    
    for i in range(0, len(distances.flatten())):
        if i == 0:
            print('Recommendations for {0}:\n'.format(usuario))
        else:
            print('{0}: {1}, with distance of {2}:'.format(i, cancionesRatings.index[indices.flatten()[i]], distances.flatten()[i]))
    
    return distances, indices


#Obtener recomendacion de canciones
def getUserRecommendation(usuario):

    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion') 
               
    #canciones con mayor rating del usuario
    myRatings = cancionesRatings.loc[usuario].dropna().sort_values(ascending=False)
    myRatings.to_csv('Recomendador/Ratings_usuario',header=1)
    
    cancionesRatings = cancionesRatings.fillna(0)
    
#    songStats = relaciones.groupby('cancion_id').agg({'valoracion': [np.size]})
#    popularMovies = songStats['valoracion']['size'] > 1
#    
#    aux = songStats[popularMovies]
#    cancionesRatings = cancionesRatings[aux.index] 
    
    distances, indices = getDistancesUsers(usuario,cancionesRatings,5)
    similarUsers = cancionesRatings.index[indices.flatten()]
    mean_user_rating = cancionesRatings.mean(axis=1)
        
    sum_wt = np.sum(distances) - 1 
    
    if sum_wt == 0:
        sum_wt = 1
        
    product = 1
    posiblesSimilares = {}
    
    for i in range(0,len(cancionesRatings.columns)):
        wtd_sum = 0 
        for j in range(1, len(indices.flatten())):
            rating = cancionesRatings.loc[similarUsers[j],cancionesRatings.columns[i]]
            ratings_diff = rating - (mean_user_rating[similarUsers[j]])             
            product = ratings_diff * (distances[j])
            wtd_sum = wtd_sum + product
        
        prediction = mean_user_rating[usuario] + wtd_sum/sum_wt
        posiblesSimilares[cancionesRatings.columns[i]] = prediction
    
    predictions = pd.Series(posiblesSimilares, name='Ranking').drop(myRatings.index,errors='ignore')
    predictions = predictions.reset_index().sort_values(by='Ranking',ascending=False)
    predictions.to_csv("Recomendador/User_Recomendaciones")

    return (list(predictions['index'])[:10])
    
    
        
##################################### Item Reccomendation ########################################################    
    
#Generar matriz con datos de ratings y atributos propios de las canciones
def getMatrix_Items(ratings,relaciones):
    
    cursor = canciones.find({},{'_id': False, 'cancion_id': 1, 'feliz': 1, 'fiesta': 1, 'triste': 1, 'relajado': 1, 'agresivo': 1, 'acustico': 1})   
    
    copia = list(cursor)    
    valores_canciones = pd.DataFrame(copia, index=[aux['cancion_id'] for aux in copia])
    
    del valores_canciones['cancion_id']  
    valores_canciones = valores_canciones.loc[ratings.columns]
    
    matrix = ratings.T.join(valores_canciones).fillna(0)
    
    songStats = relaciones.groupby('cancion_id').agg({'valoracion_emocion': [np.mean]})   
    matrix = matrix.join(songStats)
        
    return matrix
    
    
#Encontrar canciones similares en función de ratings y atributos
def findksimilaritems(item_id,user_id,matrix,emocionesRatings,model_knn,k):
 
    find_values = matrix.loc[item_id].values.copy()
    
    find_values[-1] = emocionesRatings.loc[user_id,item_id] 
    
    distances, indices = model_knn.kneighbors(find_values.reshape(1,-1), n_neighbors = k+1)
    similarities = 1 - distances.flatten()
    
#    print('\n{0} most similar items for item {1}:'.format(k,item_id))
#    
#    similarItems = emocionesRatings.T.index[indices.flatten()]
#    
#    for i in range(1, len(indices.flatten())):
#        print('{0}: Item {1} : with similarity of {2}'.format(i,similarItems[i], similarities.flatten()[i]))
    
    return similarities,indices
    


def predict_itembased(user_id, item_id, ratings,emocionesRatings, matrix,model_knn,k):
    
    prediction = wtd_sum = 0
    similarities, indices = findksimilaritems(item_id,user_id,matrix,emocionesRatings,model_knn,k)
    sum_wt = np.sum(similarities) - 1
    product = 0
    #ratings = ratings.fillna(0)
    sum_aux = 1
    
    similarItems = ratings.T.index[indices.flatten()]
    #similarUsers = ratings.index[indices_users.flatten()]
    
    for j in range(0, len(indices.flatten())):
        if similarItems[j] == item_id:
            continue;
        else:     
            rating = ratings.loc[user_id,similarItems[j]]
            if not math.isnan(rating):
                product = rating * (similarities[j])
                wtd_sum = wtd_sum + product
                sum_aux = sum_aux + similarities[j]
                                
    prediction = wtd_sum/sum_wt
#    print('\nPredicted rating for user {0} -> item {1}: {2}'.format(user_id,item_id,prediction))      

    return prediction
    
def getItemRecommendation(usuario,k):
        
    datos = getDatos()
    cancionesRatings = getMatrix(datos,'valoracion')
    emocionesRatings = getMatrix(datos,'valoracion_emocion').fillna(0)
    matrix = getMatrix_Items(cancionesRatings,datos)
    
    model_knn = NearestNeighbors(metric ='cosine', algorithm = 'brute')    
    model_knn.fit(matrix)  
    
    myRatings = cancionesRatings.loc[usuario].dropna().sort_values(ascending=False)
    
    posiblesSimilares = {}
    
    for i in range(0,len(cancionesRatings.columns)):
        prediction = predict_itembased(usuario,cancionesRatings.columns[i],cancionesRatings,emocionesRatings,matrix,model_knn,k)
        posiblesSimilares[cancionesRatings.columns[i]] = prediction
        
    predictions = pd.Series(posiblesSimilares, name='Prediction').drop(myRatings.index,errors='ignore')
    predictions = predictions.reset_index().sort_values(by='Prediction',ascending=False)

    predictions.to_csv("Recomendador/Item_Recomendaciones")
    
    
    
############################### Performance ####################################
    
        
#funcion de evaluacion usando Root Mean Squared Error
def rmse(prediction, ground_truth):
    
    prediction = prediction[ground_truth.nonzero()].flatten()
    ground_truth = ground_truth[ground_truth.nonzero()].flatten()
    
    return sqrt(mean_squared_error(prediction, ground_truth))
    
    
def getUserPerformance(kn):
    
    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion') 
    cancionesRatings = cancionesRatings.fillna(0)
    
    mean_user_rating = cancionesRatings.mean(axis=1)            
    matrix_aux = []
    posiblesSimilares = []

        
    for i in range(0,len(cancionesRatings.index)):
        
        distances, indices = getDistancesUsers(cancionesRatings.index[i],cancionesRatings,kn)
        similarUsers = cancionesRatings.index[indices.flatten()]
        posiblesSimilares = []
        sum_wt = np.sum(distances) - 1
        if sum_wt == 0:
            sum_wt = 1
        product = 1
            
        for j in range(0,len(cancionesRatings.columns)):
            wtd_sum = 0
            rating = cancionesRatings.loc[cancionesRatings.index[i],cancionesRatings.columns[j]]
            
            if not rating == 0:            
                for k in range(1, len(indices.flatten())):
                    rating = cancionesRatings.loc[similarUsers[k],cancionesRatings.columns[j]]
                    ratings_diff = rating - (mean_user_rating[similarUsers[k]])             
                    product = ratings_diff * (distances[k])
                    wtd_sum = wtd_sum + product
               
                prediction = mean_user_rating[cancionesRatings.index[i]] + wtd_sum/sum_wt
            else:
                prediction = 0            
                
            posiblesSimilares.append(abs(prediction))

        matrix_aux.append(posiblesSimilares)
        
    matrix_aux = np.array(matrix_aux)

    score = rmse(cancionesRatings.fillna(0).as_matrix(), matrix_aux)
    print('\nRMSE (User Prediction): {0}'.format(score))
        
        
def getItemPerformance(k):
        
    datos = getDatos()
    cancionesRatings = getMatrix(datos,'valoracion')
    emocionesRatings = getMatrix(datos,'valoracion_emocion').fillna(0)
    matrix = getMatrix_Items(cancionesRatings,datos)
    
    model_knn = NearestNeighbors(metric ='cosine', algorithm = 'brute')    
    model_knn.fit(matrix)  
    
    matrix_aux = []
    posiblesSimilares = []
    
    for j in range(0,len(cancionesRatings.index)):
        posiblesSimilares = []
        for i in range(0,len(cancionesRatings.columns)):
            rating = cancionesRatings.loc[cancionesRatings.index[j],cancionesRatings.columns[i]]
            if not math.isnan(rating):
                prediction = predict_itembased(cancionesRatings.index[j],cancionesRatings.columns[i],cancionesRatings,emocionesRatings,matrix,model_knn,k)
            else: 
                prediction = 0
            posiblesSimilares.append(prediction)
        matrix_aux.append(posiblesSimilares)
        
    
    matrix_aux = np.array(matrix_aux)
    score = rmse(cancionesRatings.fillna(0).as_matrix(), matrix_aux)
    print('\nRMSE (Item Prediction): {0}'.format(score))
    print(matrix_aux)


                  
if __name__ == '__main__':   
        datos = getDatos()
        ratings = getMatrix(datos,'valoracion') 
        getMatrix_Users(datos,ratings)
        #getUserRecommendation('11125830071')
        #getItemPerformance(10)
        #getUserPerformance(10)
        #getItemRecommendation('11125830071',50)
