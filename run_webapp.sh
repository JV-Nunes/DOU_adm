#!/usr/bin/env bash

pwd=`cat data/app_password.txt`
export DOU_ADM_PASSWORD=$pwd

cd ./src
streamlit run app.py
