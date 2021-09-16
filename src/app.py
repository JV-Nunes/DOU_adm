import os
import streamlit as st
import htmlhacks as hh
import main as mm
#import auxiliar as aux


def wrong_credential(password):
    """
    Check if `password` (str) is equal to
    hard-coded value. Return False if it is 
    equal, and True if it is not.
    """

    correct = 'meteoro'

    if password == os.environ['DOU_ADM_PASSWORD']:
        return False
    else:
        return True
    

def authentication(block=True):
    """
    Only proceed loading the app if the correct 
    hard-coded password is provided.
    """

    # Pega credenciais:
    senha   = st.text_input('Senha:', value='', max_chars=None, type='password')
    
    if block and senha == '':
        st.stop()
    elif block and wrong_credential(senha):
        st.error('Senha errada.')
    else:
        # Muda o CSS para esconder a caixa de texto da senha:
        hh.html('<style>.stTextInput {display: none;}</style>')

        # Roda o app:
        mm.app_main()

            
            
# Set config:
st.set_page_config(page_title='Adm. do Boletim DOU Acredito', page_icon='🔍')

# Carrega CSS:
hh.localCSS("style.css")

# Banner de título do app:
hh.banner('Boletim DOU',  icon_url='https://storage.googleapis.com/brutos-publicos/cara_a_cara/acredito_fundobranco_pad.png',
          kind='title', icon_align='right')

# Solicita senha para continuar:
authentication()