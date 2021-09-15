import random

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
