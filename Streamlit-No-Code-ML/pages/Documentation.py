import streamlit as st
from PIL import Image
import os

st.sidebar.success('The Research Lab™')

st.header('The Docs')
st.write('Welcome to this beta demo for our no-code-ml tool. This light demo allows you to customize, train, and predict your own machine learning model with zero coding. This is a work in process so if you find any bugs please feel free to reach out to our linkedin @ https://www.linkedin.com/company/the-research-lab')

st.header('A Few Notes')
image = Image.open(os.getcwd() + '\ActionBtns.png')
st.image(image,caption='The Control Center')
st.write('Use these drop downs to trigger actions. Only one action can be executed at a time. To avoid funny behavior from the application I suggest turning the buttons back to "No" after use.')