import streamlit as st
import pandas as pd
import pymongo
import spacy

from pymongo import ASCENDING, DESCENDING
from bson.son import SON
from unidecode import unidecode

client = pymongo.MongoClient("mongodb+srv://benalieur:C4qxymSvHupyQZG@imdbfilms.3qx7mj3.mongodb.net/test")

db = client["imdb"]
collection = db["films_series"]



list_films_series = []

films = collection.find()

for i in films:
    list_films_series.append(i)

df_scrapImdb = pd.DataFrame(list_films_series)

df_scrapImdb['score'] = df_scrapImdb['score'].apply(lambda x : round(x,1))

df_scrapImdb.drop(["_id", "date"], axis= 1, inplace=True)





st.set_page_config(layout="wide")



st.title('Affichage des données scrapées sur IMDB')
st.sidebar.header('Votre sélection :')


##################### Sélection du type ######################
choix_type = ["Films", "Séries"]


selection_type1 = st.sidebar.selectbox("Type", choix_type)

if selection_type1 == "Films":
    selection_type = "film"
    df_scrapImdb.drop(["saison"], axis= 1, inplace=True)

else:
    selection_type = "serie"
    df_scrapImdb.drop(["acteur"], axis= 1, inplace=True)

df = df_scrapImdb[df_scrapImdb['scrap_type'] == selection_type]

##################### Sélection des genres ######################


selection_genre = st.sidebar.multiselect("Genre", options=df['genre'].unique(), default=df['genre'].unique())

masques = [df['genre'] == i for i in selection_genre]

combo_mask = masques[0]

for masque in masques:
    combo_mask |= masque


df = df[combo_mask]


##################### Sélection des Pays ######################


selection_pays = st.sidebar.multiselect("Pays", options=df['pays'].unique(), default="United States")

masques_pays = [df['pays'] == i for i in selection_pays]

combo_mask_pays = masques_pays[0]

for masque in masques_pays:
    combo_mask_pays |= masque


df = df[combo_mask_pays]



##################### Sélection de la durée ######################

selection_duree = st.sidebar.select_slider(f"Durée (en minutes)", options=range(df["time"].min(), df["time"].max()))
df = df[df["time"] <= selection_duree]


##################### Sélection du nombre à afficher ######################

selection_nombre = st.sidebar.select_slider(f"Nombre de {selection_type1}", options=range(0,len(df)))

##################### Affichage ######################


df.drop(["scrap_type"], axis= 1, inplace=True)

df = df.set_index('title')

# df["time"] = df['time'].apply(lambda x : str(x) + "min")

for colums in df:
    df[colums] = df[colums].apply(lambda x : "Information non disponible" if x == "" else x)

df.rename(columns={"original_title":"Titre Original", "genre":"Genre", "score":"Score", "annee":"Année de sortie", "public":"Type de public", "time":"Durée (minutes)", "langue":"Langue", "pays":"Pays", "description":"Description", "acteur":"Acteurs", "saison":"Nombre de saisons",}, inplace=True)

st.dataframe(df[:selection_nombre])


#color #1A5613


