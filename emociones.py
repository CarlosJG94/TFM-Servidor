import json
import math

def clasificadorEmociones(fichero):
    
    fichero = 'Canciones/'+fichero+'.json'
    with open(fichero, 'r') as f:
        datos = json.load(f)    
    
    valores = {}

    valores['estilo'] = datos['highlevel']['genre_rosamerica']['value']
    valores['clave'] =  datos['tonal']['key_key']
    valores['escala_clave'] = datos['tonal']['key_scale']
    valores['bailable'] = datos['rhythm']['danceability']
    
    
    valores['volumen'] = datos['lowlevel']['average_loudness']
    valores['disonancia'] = datos['lowlevel']['dissonance']['mean']   
    valores['bpm'] = datos['rhythm']['bpm']
    valores['timbre'] =  datos['highlevel']['timbre']['probability']
    valores['tonal'] = datos['highlevel']['tonal_atonal']['probability']
    
    #Porcentaje emociones
    valores['acustico'] = datos['highlevel']['mood_acoustic']['all']['acoustic']
    valores['agresivo'] = datos['highlevel']['mood_aggressive']['all']['aggressive']
    valores['feliz'] = datos['highlevel']['mood_happy']['all']['happy']
    valores['fiesta'] = datos['highlevel']['mood_party']['all']['party']
    valores['triste'] = datos['highlevel']['mood_sad']['all']['sad']
    valores['relajado'] = datos['highlevel']['mood_relaxed']['all']['relaxed']
        
    #Calculo de valencia y activacion 
    valores['valencia'] = valores['feliz'] - valores['triste']
    valores['activacion'] = valores['fiesta'] - valores['relajado']
    
    angulo = math.degrees(math.atan2(valores['activacion'], valores['valencia']))

    if valores['activacion'] < 0 and angulo > 0:
        valores['angulo'] = angulo - 180
    elif valores['activacion'] > 0 and valores['valencia'] < 0:
        valores['angulo'] = 180 - (angulo * -1)
    else:
        valores['angulo'] = angulo
        
    if valores['angulo'] < 0:
        valores['angulo'] = (2*180) + valores['angulo']
        
    anguloAux = math.degrees(math.atan2(math.sqrt(math.pow(valores['activacion'],2)), math.sqrt(math.pow(valores['activacion'],2))))
    radioMax = 1/math.cos(math.radians(anguloAux))
    radio = math.sqrt(math.pow(valores['valencia'],2) + math.pow(valores['activacion'],2))

    porcentaje_emocion = (radio * 100)/radioMax
    
    if valores['angulo'] > 0 and valores['angulo'] <= 30:
        cercania_emocion = (valores['angulo'] * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Exaltado"
        else:
            emocion = "Alegre"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 30 and valores['angulo'] <= 60:
        cercania_emocion = ((valores['angulo'] - 30) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Excitado"
        else:
            emocion = "Exaltado"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 60 and valores['angulo'] <= 90:
        cercania_emocion = ((valores['angulo'] - 60) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Activo"
        else:
            emocion = "Excitado"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 90 and valores['angulo'] <= 120:
        cercania_emocion = ((valores['angulo'] - 90) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Enfado"
        else:
            emocion = "Activo"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 120 and valores['angulo'] <= 150:
        cercania_emocion = ((valores['angulo'] - 120) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Frustrado"
        else:
            emocion = "Enfado"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 150 and valores['angulo'] <= 180:
        cercania_emocion = ((valores['angulo'] - 180) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Triste"
        else:
            emocion = "Frustrado"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 180 and valores['angulo'] <= 210:
        cercania_emocion = ((valores['angulo'] - 180) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Deprimido"
        else:
            emocion = "Triste"
            cercania_emocion = 100 - cercania_emocion 
    elif valores['angulo'] > 210 and valores['angulo'] <= 240:
        cercania_emocion = ((valores['angulo'] - 210) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Aburrido"
        else:
            emocion = "Deprimido"
            cercania_emocion = 100 - cercania_emocion
    elif valores['angulo'] > 240 and valores['angulo'] <= 270:
        cercania_emocion = ((valores['angulo'] - 240) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Calmado"
        else:
            emocion = "Aburrido"
            cercania_emocion = 100 - cercania_emocion  
    elif valores['angulo'] > 270 and valores['angulo'] <= 300:
        cercania_emocion = ((valores['angulo'] - 270) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Relajado"
        else:
            emocion = "Calmado"
            cercania_emocion = 100 - cercania_emocion    
    elif valores['angulo'] > 300 and valores['angulo'] <= 330:
        cercania_emocion = ((valores['angulo'] - 300) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Sereno"
        else:
            emocion = "Relajado"
            cercania_emocion = 100 - cercania_emocion  
    elif valores['angulo'] > 330 and valores['angulo'] <= 360:
        cercania_emocion = ((valores['angulo'] - 330) * 100) / 30
        if cercania_emocion >= 50:
            emocion = "Alegre"
        else:
            emocion = "Sereno"
            cercania_emocion = 100 - cercania_emocion                
    else:
        emocion = "Neutral"
        cercania_emocion = 100
        
    valores['emocion'] = emocion
    
        
    return valores
    
if __name__ == '__main__':
    
    clasificadorEmociones('0A0RBBTrgfq9eClnw6ZXT7')
    

    