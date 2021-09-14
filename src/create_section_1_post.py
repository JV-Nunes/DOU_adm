#!/usr/bin/env python

"""
This script takes no arguments. 

It loads a template for DOU section 1 whatsapp posts, set the 
current date in it and the (hard-coded) group link, and open
the file on a text editor.

Written by: Henrique S. Xavier, hsxavier@gmail.com, on 08/sep/2020.
"""

import sys
import subprocess
import random
from datetime import date


def random_zap_link():
    """
    Randomly return a whatsapp group link 
    from a pool of hard-coded links.
    """
    
    # Hard-coded:
    zap_link_5 = 'https://chat.whatsapp.com/IzlCqLTbLavFpI1V873e5G'
    zap_link_4 = 'https://chat.whatsapp.com/Jr6o6AVvbIF3aU9un6yT67'
    zap_link_3 = 'https://chat.whatsapp.com/JjS23bAbI1f8cVVEHL0RPK'
    zap_link_2 = 'https://chat.whatsapp.com/HgmP8M6xl95GZV36QXSVYq'
    zap_link_1 = 'https://chat.whatsapp.com/B4oeQM4Ji74Kr4Xm99BIZY'

    zap_links = [zap_link_1, zap_link_2, zap_link_3, zap_link_4, zap_link_5]
    #zap_links = [zap_link_3, zap_link_5]
    
    i = random.randint(0, len(zap_links) - 1)
    return zap_links[i]


def gen_empty_post(template_file, zap_link):
    """
    Take a TXT file with name `template_file` (str)
    as template for whatsapp posts for section 1 
    and fill the date and whatsapp group link 
    with the current date and `zap_link` (str), 
    respectively.

    Returns the filled template as a str.
    """
    # Load template file:
    with open(template_file, 'r') as f:
        template = f.read()
    
    # Set current date and whatsapp link:
    today_date  = date.today().strftime('%d/%m')
    todays_post = template % {'data': today_date, 'zap_link': zap_link}
    
    return todays_post


def gen_preformatted_post(template_file='templates/modelo_zap_materias_dou_1.txt'):
    """
    Generate a template for DOU section 1 whatsapp post
    with the current date and a sampled whatsapp group 
    link. Returns a str.
    """
    
    zap_link    = random_zap_link()
    todays_post = gen_empty_post(template_file, zap_link)

    return todays_post


def gen_post_filename(post_file_prefix='posts/dou_1_'):
    """
    Generate a filename (with path) starting with
    `post_file_prefix`, followed by the current date.
    Returns a str.
    """
    
    filename = post_file_prefix + date.today().strftime('%Y-%m-%d') + '.txt'
    
    return filename


def main(args=['create_section_1_post.py']):
    """
    Function that runs this file as a script.
    """
    
    # Hard-coded:
    n_args = 0
    
    # Docstring output:
    if len(args) != 1 + n_args: 
        print(__doc__)
        sys.exit(1)

    # START OF SCRIPT:
        
    # Generate preformatted post:
    todays_post = gen_preformatted_post()

    # Save post to file:
    filename = gen_post_filename()
    with open(filename, 'w') as f:
        f.write(todays_post)

    # Call editor:
    # Hard-coded stuff:
    text_editor      = 'gedit'
    subprocess.call([text_editor, filename])



# If running this code as a script:
if __name__ == '__main__':
    main(sys.argv)

