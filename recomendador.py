from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from math import sqrt
from sklearn.neighbors import NearestNeighbors
from scipy.sparse import csr_matrix
import math
from sklearn.model_selection import KFold

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
    cancionesRatings = getMatrix_Users(cancionesRatings).fillna(0)

    similarities, indices = getSimilaritiesUsers(usuario,cancionesRatings,12)
    
    similarUsers = cancionesRatings.index[indices.flatten()]
    usuariosArray = {}
    
    for i in range(1,len(similarUsers)):
        usuariosArray[similarUsers[i]] = similarities[i]  
        
    return usuariosArray 
    

#Generar matriz con datos de ratings y atributos propios de las usuarios
def getMatrix_Users(ratings):
    
    result = usuarios.find({},{'_id': False,'usuario_id': 1, "Exaltado": 1, 'Sereno': 1, 'Calmado': 1, 'Relajado': 1, 'Aburrido': 1, 'Triste': 1, 'Alegre': 1, "Activo": 1, 'Deprimido': 1, 'Excitado': 1, 'Enfado': 1, 'Frustrado': 1 })
    
    copia = list(result)    
    valores_usuarios = pd.DataFrame(copia, index=[aux['usuario_id'] for aux in copia])
    del valores_usuarios['usuario_id']
    
    sum_values = valores_usuarios.sum(axis=1)   
    div_result =  valores_usuarios.as_matrix() / sum_values.as_matrix()[:,None]
    
    values = pd.DataFrame(div_result,index=valores_usuarios.index, columns= valores_usuarios.columns)
    values = ratings.join(values)
    
    return values
   
   
#Obtener similitudes de usuarios para un usuario concreto 
def getSimilaritiesUsers(usuario, cancionesRatings, k):
    
    model_knn = NearestNeighbors(metric = 'cosine')
    fit = csr_matrix(cancionesRatings)
    model_knn.fit(fit)    
  
    distances, indices = model_knn.kneighbors(cancionesRatings.loc[usuario].reshape(1, -1), n_neighbors = k+1)
    similarities = 1 - distances[0]
    
    return similarities, indices


#Obtener recomendacion de canciones utilizando usuarios similares
def getUserRecommendation(usuario,kn):

    #Obtener datos para el entrenamiento del modelo
    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion')
    
    try:
        cancionesRatings.loc[usuario]
    except:
        aux = pd.DataFrame(0, index=np.arange(1), columns=cancionesRatings.columns).reindex([usuario])        
        cancionesRatings = cancionesRatings.append(aux)
        
    myRatings = cancionesRatings.loc[usuario].dropna() 
    matrix = getMatrix_Users(cancionesRatings).fillna(0)
    
    mean_user_rating = cancionesRatings.mean(axis=1)
    
    similarities, indices = getSimilaritiesUsers(usuario,matrix,kn)
    similarUsers = cancionesRatings.index[indices.flatten()]
    
    cancionesRatings = cancionesRatings.loc[similarUsers].dropna(axis='columns', how='all').fillna(0)    
    cancionesRatings = cancionesRatings.T.drop(myRatings.index,errors='ignore').T
        
    #sum_wt = np.sum(similarities) - 1     
                
    product = 0
    sum_aux = 0
    posiblesSimilares = {}
    
    for i in range(0,len(cancionesRatings.columns)):
        wtd_sum = 0 
        for j in range(1, len(similarUsers)):
            rating = cancionesRatings.loc[similarUsers[j],cancionesRatings.columns[i]]
            if rating > 0:
                ratings_diff = rating - (mean_user_rating[similarUsers[j]])             
                product = ratings_diff * (similarities[j])
                wtd_sum = wtd_sum + product
                sum_aux = sum_aux + (similarities[j])
        
        if sum_aux == 0:
            sum_aux = 1            
        prediction = mean_user_rating[usuario] + wtd_sum/sum_aux
        posiblesSimilares[cancionesRatings.columns[i]] = prediction
    
    
    predictions = pd.Series(posiblesSimilares, name='Ranking').reset_index().sort_values(by='Ranking',ascending=False)
    predictions.to_csv("Recomendador/User_Recomendaciones")

    return (list(predictions['index'])[:10])
    
def getCommunityRecommendation(usuario,kn):
    
    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion')
    
    try:
        cancionesRatings.loc[usuario]
    except:
        aux = pd.DataFrame(0, index=np.arange(1), columns=cancionesRatings.columns).reindex([usuario])        
        cancionesRatings = cancionesRatings.append(aux)
        
    myRatings = cancionesRatings.loc[usuario].dropna() 
    matrix = getMatrix_Users(cancionesRatings).fillna(0)
    
    mean_user_rating = cancionesRatings.mean(axis=1)
    
    similarities, indices = getSimilaritiesUsers(usuario,matrix,kn)
    similarUsers = cancionesRatings.index[indices.flatten()]
    
    search = cancionesRatings.loc[similarUsers].dropna(axis='columns', how='all').fillna(0)    
    search = cancionesRatings.T.drop(myRatings.index,errors='ignore').T
                       
    posiblesSimilares = {}
        
    cursor = usuario_usuario.find({'usuario_id': usuario}, {'_id': False, 'seguido_id':1})
    amigos = []
    amigos.append(usuario)
    for aux in cursor:
        amigos.append(aux['seguido_id'])
    
    for i in range(0,len(search.columns)):
        prediction = 0 
        for amigo in amigos:
            similarities, indices = getSimilaritiesUsers(amigo,matrix,kn)
            similarUsers = cancionesRatings.index[indices.flatten()]
            sum_aux = 0         
            product = 1
            wtd_sum = 0                
            for j in range(1, len(similarUsers)):
                rating = cancionesRatings.loc[similarUsers[j],search.columns[i]]
                if rating > 0:
                    ratings_diff = rating - (mean_user_rating[similarUsers[j]])             
                    product = ratings_diff * (similarities[j])
                    wtd_sum = wtd_sum + product
                    sum_aux = sum_aux + (similarities[j])
           
                if sum_aux == 0:
                    sum_aux = 1
            prediction = prediction + mean_user_rating[amigo] + wtd_sum/sum_aux
        
        prediction = prediction/len(amigos)
        posiblesSimilares[search.columns[i]] = prediction

    predictions = pd.Series(posiblesSimilares, name='Ranking').reset_index().sort_values(by='Ranking',ascending=False)
    predictions.to_csv("Recomendador/Community_Recomendaciones")

    return (list(predictions['index'])[:10])

     
##################################### Item Reccomendation ########################################################    
    
#Generar matriz con datos de ratings y atributos propios de las canciones
def getMatrix_Items(ratings,relaciones):
    
    #cursor = canciones.find({},{'_id': False, 'cancion_id': 1, 'feliz': 1, 'fiesta': 1, 'triste': 1, 'relajado': 1})   
    cursor = canciones.find({},{'_id': False, 'cancion_id': 1, 'volumen': 1, 'disonancia': 1, 'bpm': 1, 'timbre': 1, 'modal': 1}) 
    copia = list(cursor)    
    valores_canciones = pd.DataFrame(copia, index=[aux['cancion_id'] for aux in copia])
    
    del valores_canciones['cancion_id']  
    valores_canciones = valores_canciones.loc[ratings.columns]
    
    matrix = ratings.T.join(valores_canciones).fillna(0)
    
    #Juntar la media de las valoraciones media de las emociones por parte de los usuarios
    songStats = relaciones.groupby('cancion_id').agg({'valoracion_emocion': [np.mean]})   
    matrix = matrix.join(songStats)
        
    return matrix    
    
#Encontrar canciones similares en función de ratings y atributos
def findksimilaritems(item_id,user_id,matrix,emocionesRatings,model_knn,k):
 
    find_values = matrix.loc[item_id].values.copy()  
    #Valor que le ha dado el usuario a la emocion de la cancion
    find_values[-1] = emocionesRatings.loc[user_id,item_id] 
    
    distances, indices = model_knn.kneighbors(find_values.reshape(1,-1), n_neighbors = k+1)
    similarities = 1 - distances.flatten()
    
    return similarities,indices
    


def predict_itembased(user_id, item_id, cancionesRatings,emocionesRatings, matrix,model_knn,k):
    
    prediction = wtd_sum = product = sum_aux =  0
    similarities, indices = findksimilaritems(item_id,user_id,matrix,emocionesRatings,model_knn,k)
    
    similarItems = cancionesRatings.T.index[indices.flatten()]
    
    for j in range(0, len(indices.flatten())):
        if similarItems[j] == item_id:
            continue;
        else:     
            rating = cancionesRatings.loc[user_id,similarItems[j]]
            if rating > 0:             
                product = rating * (similarities[j])
                wtd_sum = wtd_sum + product
                sum_aux = sum_aux + similarities[j]
            
    if sum_aux == 0:
        sum_aux = 1                    
    prediction = wtd_sum/sum_aux

    return prediction
    
def getItemRecommendation(usuario,k):
        
    datos = getDatos()
    cancionesRatings = getMatrix(datos,'valoracion')
    emocionesRatings = getMatrix(datos,'valoracion_emocion').fillna(0)
    matrix = getMatrix_Items(cancionesRatings,datos)
    
    #Entrenamiento del modelo
    model_knn = NearestNeighbors(metric ='cosine', algorithm = 'brute') 
    fit = csr_matrix(matrix)
    model_knn.fit(fit)  

    #Canciones no escuchadas por el usuario
    myRatings = cancionesRatings.loc[usuario].dropna() 
    search = cancionesRatings.T.drop(myRatings.index,errors='ignore').T
    
    search = cancionesRatings
    posiblesSimilares = {}
    
    for i in range(0,len(search.columns)):
        prediction = predict_itembased(usuario,search.columns[i],cancionesRatings.fillna(0),emocionesRatings,matrix,model_knn,k)
        posiblesSimilares[search.columns[i]] = prediction
        
    predictions = pd.Series(posiblesSimilares, name='Prediction')
    predictions = predictions.reset_index().sort_values(by='Prediction',ascending=False)

    predictions.to_csv("Recomendador/Item_Recomendaciones")
    
    return (list(predictions['index'])[:10]), (list(predictions['Prediction'])[:10])
    
    
############################### Performance ####################################
    
        
#funcion de evaluacion usando Root Mean Squared Error
def rmse(prediction, ground_truth):
    
    prediction = prediction[ground_truth.nonzero()].flatten()
    ground_truth = ground_truth[ground_truth.nonzero()].flatten()
    
    return sqrt(mean_squared_error(prediction, ground_truth))
    

def getUserPerformance(kn):
    
    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion') 
 
    mean_user_rating = cancionesRatings.mean(axis=1)
    kf = KFold(n_splits=10)
    average = 0
    
    for train_index, test_index in kf.split(cancionesRatings):
        
        train, test = cancionesRatings.iloc[train_index], cancionesRatings.iloc[test_index]
        matrixTrain = getMatrix_Users(train).fillna(0)  
        matrix = getMatrix_Users(test).fillna(0) 
        
        model_knn = NearestNeighbors(metric = 'cosine')
        fit = csr_matrix(matrixTrain)
        model_knn.fit(fit)    
               
        matrix_aux = []
        posiblesSimilares = []
            
        for i in range(0,len(test.index)):
            
            distances, indices = model_knn.kneighbors(matrix.loc[test.index[i]].reshape(1, -1), n_neighbors = kn+1)
            similarities = 1 - distances[0]
    
            similarUsers = cancionesRatings.index[indices.flatten()]
            posiblesSimilares = []
            sum_wt = np.sum(similarities) - 1
            sum_aux = 0
            
            if sum_wt == 0:
                sum_wt = 1
            product = 1
                
            for j in range(0,len(test.columns)):
                wtd_sum = 0
                rating = cancionesRatings.loc[test.index[i],test.columns[j]]
                
                if rating > 0:            
                    for k in range(1, len(similarUsers)):
                        rating = cancionesRatings.loc[similarUsers[k],test.columns[j]]
                        if rating > 0:
                            ratings_diff = rating - (mean_user_rating[similarUsers[k]])             
                            product = ratings_diff * (similarities[k])
                            wtd_sum = wtd_sum + product
                            sum_aux = sum_aux + (similarities[k])
                   
                    if sum_aux == 0:
                        sum_aux = 1
                    prediction = mean_user_rating[test.index[i]] + wtd_sum/sum_aux
                else:
                    prediction = 0            
                    
                posiblesSimilares.append(abs(prediction))
    
            matrix_aux.append(posiblesSimilares)
            
        matrix_aux = np.array(matrix_aux)
        score = rmse(test.fillna(0).as_matrix(), matrix_aux)
        print('\nRMSE (Iteracion): {0}'.format(score))
        average = average + score
    
    average = average/10
    print('\nRMSE (User Prediction): {0}'.format(average))
    return average
            

    
def getCommunityPerformance(kn):
    
    relaciones = getDatos()
    cancionesRatings = getMatrix(relaciones,'valoracion') 
    mean_user_rating = cancionesRatings.mean(axis=1)   
     
    kf = KFold(n_splits=10)
    average = 0    
    
    for train_index, test_index in kf.split(cancionesRatings):
        train, test = cancionesRatings.iloc[train_index], cancionesRatings.iloc[test_index]
         
        matrix_aux = []
        
        matrixTrain = getMatrix_Users(train).fillna(0)  
        matrix = getMatrix_Users(cancionesRatings).fillna(0) 
        model_knn = NearestNeighbors(metric = 'cosine')
        fit = csr_matrix(matrixTrain)
        model_knn.fit(fit)
            
        for i in range(0,len(test.index)):
            
            posiblesSimilares = []
          
            cursor = usuario_usuario.find({'usuario_id': test.index[i]}, {'_id': False, 'seguido_id':1})
            amigos = []
            amigos.append(test.index[i])
            for aux in cursor:
                amigos.append(aux['seguido_id'])
                
            for j in range(0,len(test.columns)):
                prediction = 0 
                rating = cancionesRatings.loc[test.index[i],test.columns[j]]
    
                if rating > 0:
                    for amigo in amigos:
                        distances, indices = model_knn.kneighbors(matrix.loc[amigo].reshape(1, -1), n_neighbors = kn+1)
                        similarities = 1 - distances[0]
                        
                        #similarities, indices = getSimilaritiesUsers(amigo,train,kn)
                        similarUsers = cancionesRatings.index[indices.flatten()]
                        sum_aux = 0         
                        product = 1
                        wtd_sum = 0                
                        for k in range(1, len(similarUsers)):
                            rating = cancionesRatings.loc[similarUsers[k],test.columns[j]]
                            if rating > 0:
                                ratings_diff = rating - (mean_user_rating[similarUsers[k]])             
                                product = ratings_diff * (similarities[k])
                                wtd_sum = wtd_sum + product
                                sum_aux = sum_aux + (similarities[k])
                   
                        if sum_aux == 0:
                            sum_aux = 1
                        
                        prediction = prediction + mean_user_rating[amigo] + wtd_sum/sum_aux
                     
                    prediction = prediction/len(amigos)
                else:
                    prediction = 0
                    
                posiblesSimilares.append(prediction)
        
            matrix_aux.append(posiblesSimilares)
             
        matrix_aux = np.array(matrix_aux)
        score = rmse(test.fillna(0).as_matrix(), matrix_aux)
        print('\nRMSE (Iteracion): {0}'.format(score))
        average = average + score
        
    average = average/10
    print('\nRMSE (Community Prediction): {0}'.format(average))
    
    return average


def getItemPerformance(k):
        
    datos = getDatos()
    
    cancionesRatings = getMatrix(datos,'valoracion')
    emocionesRatings = getMatrix(datos,'valoracion_emocion').fillna(0)
    
    matrix = getMatrix_Items(cancionesRatings,datos).fillna(0) 
    
    kf = KFold(n_splits=10)
    average = 0
    
    for train_index, test_index in kf.split(matrix):
        train, test = matrix.iloc[train_index], matrix.iloc[test_index]
    
        model_knn = NearestNeighbors(metric ='cosine', algorithm = 'brute')    
        fit = csr_matrix(train)
        model_knn.fit(fit)  
        
        matrix_aux = []
        posiblesSimilares = []
        
        for i in range(0,len(cancionesRatings.index)):
            posiblesSimilares = []
            for j in range(0,len(test.index)):
                rating = cancionesRatings.loc[cancionesRatings.index[i],test.index[j]]
                if not math.isnan(rating):
                    prediction = predict_itembased(cancionesRatings.index[i],test.index[j],cancionesRatings.fillna(0),emocionesRatings,test,model_knn,k)
                else: 
                    prediction = 0
                posiblesSimilares.append(prediction)
            matrix_aux.append(posiblesSimilares)
            
        matrix_aux = np.array(matrix_aux)
        score = rmse(cancionesRatings.T.loc[test.index].T.fillna(0).as_matrix(), matrix_aux)
        print('\nRMSE (Iteracion): {0}'.format(score))
        average = average + score
    
    average = average/10
    print('\nRMSE (Item Prediction): {0}'.format(average))
    return average
    
        
    
if __name__ == '__main__': 
    
        #getUserRecommendation('dleon692',10)   
        #getUserPerformance(10)     
        #getItemRecommendation('1194174660',100)
        #getItemPerformance(10)
        #getCommunityRecommendation('11160226728',10)
        #getCommunityPerformance(15)
        userMAP()

        