import streamlit as st

# Текстовое поле для ввода
user_input = st.text_input("Введите текст:")

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

df = pd.read_csv(r'C:\Users\PC\Desktop\test_df')

def rec_sys(text, df):
    texts = [text] + df['text'].tolist()

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(texts)

    cosine_similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()

    # Получаем индексы 5 наиболее похожих отелей
    top_indices = cosine_similarities.argsort()[-5:][::-1]

    # Получаем названия и оценки для 5 наиболее похожих отелей
    most_similar_hotels = df['name_ru'].iloc[top_indices].tolist()
    most_similar_scores = cosine_similarities[top_indices].tolist()

    return list(zip(most_similar_hotels, most_similar_scores))

# Отображение введенного текста
if user_input:
    st.write(rec_sys(user_input, df))
