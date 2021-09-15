#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 10 17:41:03 2021

@author: skems
"""

import streamlit as st
import numpy as np
import time

import session as ss
from compute_download_button import compute_download_button
import htmlhacks as hh
import df_formatter as ff
import count_DOU_articles as ca
import run_python_process as rp
import create_section_1_post as c1
import format_todays_section_2 as f2


def progress_bar(bar, duration):
    """
    Fills the progress `bar` for `duration` (float) seconds.
    """
    
    for percent_complete in range(101):
        time.sleep(duration / 100)
        bar.progress(percent_complete) 


def external_link(text, url):
    """
    Write a HTML link on the app:
    Its content is `text` (str) and
    the URL is `url` (str).
    """
    
    code = '<a href="{}" target="_blank">{}</a>'.format(url, text)
    hh.html(code)
    
    
@st.cache
def counts_dataframe(call):
    
    # Return empty DataFrame:
    if call == 0:
        df = ca.gen_empty_counts_df()
        error_msgs = []
    # Run mapping code:
    else:
        #df = ca.gen_empty_counts_df(no_value=np.random.randint(0,600))
        #error_msgs = []
        df, error_msgs = ca.count_through_pipeline()
        
    return df, error_msgs
     

def generate_formatters(df):
    
    cols   = df.columns
    n_cols = len(cols) 
    prefix_fmt_pairs = zip(cols, ['{}'] * n_cols)
    fmt_funcs = ff.build_fmt_funcs(cols, prefix_fmt_pairs)
    
    return fmt_funcs


def app_main():   
    
    # Design settings:
    four_columns  = np.array([0.40, 0.28, 0.22, 0.10])
    three_columns = np.array([0.40, 0.36, 0.24])
    two_columns   = np.array([three_columns[0] + three_columns[1], three_columns[2]])
    
    # Persistent attributes:
    session = ss.get(map_counter=0, ai_counter=0, prep2_counter=0, post2=None)
    
    # Count articles:
    
    # Place title and buttion:
    col1, col2 = st.columns(two_columns + np.array([0.09,-0.09]))
    with col1:
        st.markdown('### Matérias por estágio de captura')
    with col2:
        run_mapper = st.button('Mapear')
        if run_mapper:
            session.map_counter += 1
    
    # Count articles along the capture pipeline:
    df, error_msgs = counts_dataframe(session.map_counter)
    # Display counts DataFrame:
    fmt_funcs = generate_formatters(df)
    st.dataframe(df.style.format(fmt_funcs))
    for msg in error_msgs:
        st.error(msg)
        
    hh.html('<hr />')

    # Run IA:
    
    # Place title and button:
    col1, col2 = st.columns(two_columns + np.array([0.05,-0.05]))
    with col1:
        st.markdown('### Pré-ordenamento')
    with col2:        
        run_ai = st.button('Executar IA')
    
    # Display progress bar state:
    if session.ai_counter == 0:
        ai_bar = st.progress(0)
    else:
        ai_bar = st.progress(100)
    
    # Run AI:
    if run_ai:
        session.ai_counter += 1
        rp.run_python_process()
        progress_bar(ai_bar, 75)

    hh.html('<hr />')
    
    # Link to sheet & others:
    
    ranking_sheet_url = 'https://docs.google.com/spreadsheets/d/11dnbTxiighjkq8LzmKZAJ8PsrI2EOG6yNw4InWPhTGE'
    vetos_url = 'https://www.in.gov.br/leiturajornal?org=Presid%C3%AAncia%20da%20Rep%C3%BAblica&ato=Despacho'
    slack_url = 'https://app.slack.com/client/TGM1R75MM/CK41CADJA'

    col1, col2, col3, col4 = st.columns(four_columns)
    with col1:
        st.markdown('### Links importantes')    
    with col2:
        external_link('Planilha de matérias', ranking_sheet_url)
    with col3:
        external_link('Vetos de hoje', vetos_url)
    with col4:
        external_link('Slack', slack_url)

    hh.html('<hr />')

    # Generate posts:

    # Place title and buttons:
    col1, col2, col3 = st.columns(three_columns)
    with col1:
        st.markdown('### Preparação do boletim')
    with col2:
        preformatted_sec1 = c1.gen_preformatted_post('../templates/modelo_zap_materias_dou_1.txt')
        filename_sec1 = c1.gen_post_filename('dou_1_')
        st.download_button('Modelo da seção 1', preformatted_sec1, file_name=filename_sec1, mime='text/plain')
    with col3:
        filename_sec2 = f2.gen_post_filename('dou_2_')
        compute_download_button(session, 'post2', f2.etl_section2_post, 'Preparar seção 2', 'Baixar seção 2', filename_sec2)
