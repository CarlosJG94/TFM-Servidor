# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 17:32:34 2018

@author: carlosgomes
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np


client = MongoClient('localhost', 27017)
db = client.Recomendador
usuarios = db.usuarios
canciones = db.canciones
cancion_usuario = db.cancion_usuario
usuario_usuario = db.usuario_usuario


cursor = canciones.find({}, {'cancion_id': 1,'_id': False})

df = pd.DataFrame(list(cursor))
cursor3 = cancion_usuario.aggregate( 
            [
                {"$group": { "_id": { 'usuario_id': "$usuario_id", 'cancion_id': "$cancion_id" } } },
            ]
        );
            
df3 = pd.DataFrame(list(cursor3))
#df3.to_csv("canciones")

cursor2 = cancion_usuario.find({}, {'_id': False})

df2 = pd.DataFrame(list(cursor2))
#df2.to_csv("canciones")

ratings = pd.merge(df,df2)
movieRatings = ratings.pivot_table(index=['usuario_id'],columns=['cancion_id'],values='valoracion')

corrMatrix = movieRatings.corr(method='pearson')

myRatings = movieRatings.loc['11125830071'].dropna()


for i in range(0, len(myRatings.index)):
    if myRatings[i] == 0:
        myRatings[i] = 1
        
myRatings.to_csv("canciones", header=1)

posiblesSimilares = pd.Series()

for i in range(0, len(myRatings.index)):
 #print ("Similares a " + myRatings.index[i] + "...")
 
 sims = corrMatrix[myRatings.index[i]].dropna()

 sims = sims.map(lambda x: x * myRatings[i])
 
 posiblesSimilares = posiblesSimilares.append(sims)
  
 posiblesSimilares = posiblesSimilares.groupby(posiblesSimilares.index).sum()
 
 filtered = posiblesSimilares.drop(myRatings.index,errors='ignore')

posiblesSimilares.to_csv("canciones")




#r = df.transpose()
#ratings = pd.concat([r,df2], axis=0)

#print(movieRatings)

cancionRating = movieRatings['0pyT9W877RcYtVdl1ZmlvQ']
similarSong = movieRatings.corrwith(cancionRating)
similarSong = similarSong.dropna()

df = pd.DataFrame(similarSong)
similarSong.sort_values(ascending=False)
print(similarSong)

songStats = ratings.groupby('cancion_id').agg({'valoracion': [np.size,np.mean]})
popularSongs = songStats['valoracion']['size'].gt(1)

songStats[popularSongs].sort_values([('valoracion', 'mean')], ascending=False)[:15]

df = songStats[popularSongs].join(pd.DataFrame(similarSong, columns=['similarity']))

#print(df)

#movieRatings.to_csv("canciones")