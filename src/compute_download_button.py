#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 15:54:13 2021

@author: skems
"""

import streamlit as st

def compute_download_button(state, attr, setter, compute_label, download_label, filename=None, value0=None):
    """
    A button that the first click computes something and 
    the second click this something is downloaded.
    
    Parameters
    ----------
    
    state : SessionObject
        An object containing stateful attributes, that is, 
        attributes that retain their values during a given
        Streamlit session.   
    attr : str
        The name of the attribute in `state` used to store
        the value computed during the button's first click.  
    setter : callable
        The callable (e.g. function) used to compute the 
        value to be stored in `state.attr` and later 
        downloaded. Currently this callable does not accept
        any arguments.
    compute_label : str
        The button label when in compute mode (i.e. when the 
        click calls `setter`).
    download_label : str
        The button label when in download mode (i.e. the 
        second click).
    filename : str
        The name given to the file to be downloaded.
    value0 : almost anything
        The value of `state.attr` when the button is on 
        compute mode (first click).    
    """
    
    def build(s):
        setattr(s, attr,  setter())
        
    def reset(s):
        setattr(s, attr, value0)
    
    if getattr(state, attr) == value0:
        st.button(compute_label, on_click=build, args=(state,))
    else:
        st.download_button(download_label, getattr(state, attr), file_name=filename, mime='text/plain', on_click=reset, args=(state,))
