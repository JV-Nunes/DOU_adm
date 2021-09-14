import streamlit as st


def html(html_code):
    """
    Place `html_code` (str) in the Streamlit app.
    """
    st.write(html_code, unsafe_allow_html=True)


def localCSS(file_name):
    """
    Load a CSS style file `file_name` and use it to 
    style the webpage.
    """
    with open(file_name) as f:
        html(f'<style>{f.read()}</style>')


def banner(text, icon_url=None, kind='section', icon_align='left'):
    """
    Create an horizontal banner with text and icon and print it
    in the Streamlit app.

    Input
    -----

    text : str
        The text to appear in the banner.

    icon_url : str or None
        The URL of the image to be placed in the banner. If None,
        do not place and image in the banner.

    kind : str
        Banner kind, which will change its formatting. It is 
        a CSS class.

    icon_align : str
        Whether to align the image 'left' or 'right' in the 
        banner.
    """
    if icon_url == None:
        icon_html = ''
    else:
        icon_html = f'<div class="banner-icon {icon_align}"><img src="{icon_url}"></div>'

    html(
        f'<div class="banner {kind}"><div class="banner-text">{text}</div>{icon_html}</div>')
