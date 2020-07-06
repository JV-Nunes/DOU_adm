#!/usr/bin/env python

"""
This script takes no arguments. 

It loads the DOU matérias from section 2 from Google Sheets 
'Artigos novos do DOU e classificação' (that contains matérias 
ranked by ML and then manually), selects those most relevant 
according to the manual classification in the spreadsheet (the 
criterium is hard coded below) and create a message almost ready 
(apart from minor corrections) to be posted on whatsapp. The 
message is opened in a text editor.

Written by: Henrique S. Xavier, hsxavier@gmail.com, on 03/jul/2020.
"""

import pandas as pd
import re
from datetime import date
import warnings
import subprocess
import google.auth
import os
import csv

### FUNCTIONS ###


def bigquery_to_pandas(query, project='gabinete-compartilhado', credentials_file='/home/skems/gabinete/projetos/keys-configs/gabinete-compartilhado.json'):
    """
    Run a query in Google BigQuery and return its results as a Pandas DataFrame. 

    Input
    -----

    query : str
        The query to run in BigQuery, in standard SQL language.
    project : str
        
    
    Given a string 'query' with a query for Google BigQuery, returns a Pandas 
    dataframe with the results; The path to Google credentials and the name 
    of the Google project are hard-coded.
    """

    # Set authorization to access GBQ and gDrive:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_file

    
    credentials, project = google.auth.default(scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery',
    ])
    
    return pd.read_gbq(query, project_id=project, dialect='standard', credentials=credentials)


def load_data_from_local_or_bigquery(query, filename, force_bigquery=False, save_data=True, 
                                     project='gabinete-compartilhado', 
                                     credentials_file='/home/skems/gabinete/projetos/keys-configs/gabinete-compartilhado.json'):
    """
    Loads data from local file if available or download it from BigQuery otherwise.
    
    
    Input
    -----
    
    query : str
        The query to run in BigQuery.
    
    filename : str
        The path to the file where to save the downloaded data and from where to load it.
        
    force_bigquery : bool (default False)
        Whether to download data from BigQuery even if the local file exists.
        
    save_data : bool (default True)
        Wheter to save downloaded data to local file or not.
        
    project : str (default 'gabinete-compartilhado')
        The GCP project where to run BigQuery.
        
    credentials_file : str (default path to 'gabinete-compartilhado.json')
        The path to the JSON file containing the credentials used to access GCP.
        
    
    Returns
    -------
    
    df : Pandas DataFrame
        The data either loaded from `filename` or retrieved through `query`.
    """
    
    # Download data from BigQuery and save it to local file:
    if os.path.isfile(filename) == False or force_bigquery == True:
        print('Loading data from BigQuery...')
        df = bigquery_to_pandas(query, project, credentials_file)
        if save_data:
            print('Saving data to local file...')
            df.to_csv(filename, quoting=csv.QUOTE_ALL, index=False)
    
    # Load data from local file:
    else:
        print('Loading data from local file...')
        df = pd.read_csv(filename)
        
    return df


def split_acts(text_series, splitter):
    """
    Split an article into acts over people (nomear, exonerar, etc).
    The preface is thrown away. Each act gets its own row, with 
    the same index as the original text.
    
    Input
    -----
    
    text_series : Pandas Series of str
        The text of the matérias to be split into acts, including 
        a token that identifies where the split should be made.
        
    splitter : str
        A token that identifies where the string should be split.
        
    Returns
    -------
    
    series : Pandas Series of str
        The nomear/exonerar/etc acts, one per row, with 
        same index as original text (so indices might repeat).
    """
    text_segments = text_series.str.split(splitter)
    acts = text_segments.apply(lambda text_list: text_list[1:])
    series = acts.explode()
    return series
    

def trim_act_tail(text_series):
    """
    Remove from the end of each nomear/exonerar/etc act
    in `text_series` (Pandas Series) the preface of the 
    following act, which is unnecessary information for the acts.
    """
   
    # Considerar . final do ato, a menos que acompanhe números ou * em seguida ou começe com "art" ou " n".
    sentence_end_regex = r'(?<!(?:[Rr][Tt]| [Nn]))\.(?:[^\d*]|$)'
    trimmed_act = text_series.str.split(sentence_end_regex).apply(lambda arr: arr[0]) + '.'
    
    return trimmed_act


def isolate_acts(text_series, act_regex):
    """
    Isolate each nomear/exonerar/etc act in each row of 
    `text_series` (Pandas Series) into a different row, 
    maintaining the same indices for the acts coming from 
    the same original `text_series` row. The acts are detected 
    by starting with the `act_regex`.
    
    Returns a Pandas Series.
    """
    
    # Mark splitting points:
    act_splitter = 'XXDIVXX'
    texts_with_splitter = text_series.str.replace(act_regex, act_splitter + r'\1', case=False)
    # Split acts:
    act_series = split_acts(texts_with_splitter, act_splitter)
    
    # Trim garbage from the end of the acts:
    cleaned_act_series = trim_act_tail(act_series)
    
    return cleaned_act_series


def remove_pattern(text_series, regex):
    """
    Remove a `regex` from all rows in a `text_series`.
    """
    cleaned_series = text_series.str.replace(regex, '', case=False)
    return cleaned_series


def remove_siape(text_series):
    """
    Remove SIAPE code (and related terms) from all rows in 
    `text_series`.
    """
    siape_regex = r',?\s*?(?:(?:matr[íi]cula)?\s*siape(?:cad)?|matr[íi]cula)\s*?n?.?\s*?(\d{5,7}),?'
    return remove_pattern(text_series, siape_regex)


def remove_cpf(text_series):
    """
    Remove CPF number (and related terms) from all rows in 
    `text_series`.
    """
    cpf_regex = r',?\s*?cpf\s*?n?\.?.?\s*?([\d.*-]{14,18}),?'
    return remove_pattern(text_series, cpf_regex)


def remove_no(text_series):
    no_regex = r',?\s*?c[oó]digo\s*?n.? ?[\.\d]{5,7},?'
    return remove_pattern(text_series, no_regex)


def remove_processo(text_series):
    processo_regex = r'(?:,?\s*?conforme\s*?|[\s\-.]*?)\(?Processo\s*?(?:SEI)?\s*?n?.?\s*?[\d.\-/]{15,20}\)?'
    return remove_pattern(text_series, processo_regex)


def fix_verbs(text_series):
    """
    Replace infinitive of main verbs of the acts (nomear, exonerar, etc.)
    by present tense.
    """
    clean_series = text_series.copy()
    clean_series = clean_series.str.replace(r'nomear ?(,?)\s*', r'Nomeia\1 ', case=False)
    clean_series = clean_series.str.replace(r'exonerar ?(,?)\s*', r'Exonera\1 ', case=False)
    clean_series = clean_series.str.replace(r'designar ?(,?)\s*', r'Designa\1 ', case=False)
    clean_series = clean_series.str.replace(r'dispensar ?(,?)\s*', r'Dispensa\1 ', case=False)
    return clean_series


def remove_preamble(text_series):
    """
    Remove preamble (whose end is identified by 'resolve:')
    from all rows in `text_series`.
    """
    preamble_regex = '^.*?resolve:\s*'
    return remove_pattern(text_series, preamble_regex)


def filter_low_cargos(text_series):
    """
    Remove rows from `text_series` that contains low cargos 
    and, also, do not contain high cargos (all hard-coded).
    """
    low_cargo_regex  = '(?:(?:das|fcp?e)[ -]*?[0123]{3}\.[1-3]|cge[ -]+?(iii|iv|v)(?:\W|$))'
    high_cargo_regex = '(?:(?:das|fcp?e)[ -]*?[0123]{3}\.[4-6]|cge[ -]+?(i|ii)(?:\W|$))'
    filtered = text_series.loc[~((text_series.str.contains(low_cargo_regex, case=False)) & 
                                ~(text_series.str.contains(high_cargo_regex, case=False)))]
    return filtered


def standardize_cargos(text_series):    
    """
    Standardize and simplify parts of text in `text_series` 
    (Pandas Series) describing cargos.
    """
    prefix_regex = [('DAS ',    r',?\s*?(?:c[óo]digo)?\s*?das[ -]*?[0123]{3}\.([1-6]),?'), 
                    ('CA ',     r',?\s*?(?:c[óo]digo)?\s*?ca[ -]+?(i{1,4})(?:\W|$),?'),
                    ('CA-APO ', r',?\s*?(?:c[óo]digo)?\s*?ca-apo[ -]*?([12]),?'),
                    ('',        r',?\s*?(?:c[óo]digo)?\s*?\W(CDT)\W,?'),
                    ('CCD ',    r',?\s*?(?:c[óo]digo)?\s*?ccd[ -]+?(i{1,3})(?:\W|$),?'),
                    ('CGE ',    r',?\s*?(?:c[óo]digo)?\s*?cge[ -]+?(i{1,3})(?:\W|$),?'),
                    ('',        r',?\s*?(?:c[óo]digo)?\s*?(CPAGLO),?'),
                    ('',        r',?\s*?(?:c[óo]digo)?\s*?\W(CSP)(?:\W|$),?'),
                    ('',        r',?\s*?(?:c[óo]digo)?\s*?\W(CSU)(?:\W|$),?'),
                    ('CD ',     r',?\s*?(?:c[óo]digo)?\s*?\Wcd(?:[ -]*?|\.)([123])(?:\W|$),?'),
                    ('',        r',?\s*?(?:c[óo]digo)?\s*?\W(NE)(?:\W|$),?'),
                    ('CETG ',   r',?\s*?(?:c[óo]digo)?\s*?cetg[ -]*?(iv|v|vi|vii)(?:\W|$),?'), 
                    ('FDS ',    r',?\s*?(?:c[óo]digo)?\s*?\Wfds[ -]*?(1)(?:\W|$),?'),
                    ('FCPE ',   r',?\s*?(?:c[óo]digo)?\s*?fc?pe[ -]*?[0-9]{3}\.([1-5]),?'),
                    ('',        '(natureza especial)'),
                    ('CNE ',    r',?\s*?(?:c[óo]digo)?\s*?cne[ -]*?([0-9]{2}),?')]
    
    new_text_series = text_series.copy()
    for prefix, regex in prefix_regex:
        new_text_series = new_text_series.str.replace(regex, ' (' + prefix + r'\1)', case=False)

    return new_text_series


def add_label_to_df(df, orgao_label_df, lookup_col='orgao'):
    """
    Modify `df` (Pandas DataFrame) in place by adding a 'label'
    column that translates regex patterns looked for in `df` column 
    `lookup_col` (default 'orgao') to labels. The regex patterns and the 
    respective labels are stored in columns 'regex' and 'label 'from 
    `orgao_label_df` (Pandas DataFrame).
    """
    df['label'] = None

    for i in range(len(orgao_label)):

        # Get one regex-label pair:
        regex = orgao_label_df.loc[i, 'regex']
        label = orgao_label_df.loc[i, 'label']

        df.loc[(df[lookup_col].str.contains(regex)) & (df['label'].isnull()), 'label'] = label
    
    # Default:
    df['label'].fillna('Outros')

    
def act_importance(text):
    """
    Return a number representing the importance of the cargo found 
    in `text` (str).
    
    Note that the cargos in `text` must be standardized to the 
    hard-coded tags.
    """
    
    tag_importance = [('(DAS 6)', 6), ('(DAS 5)', 5), ('(DAS 4)', 4), ('(FCPE 5)', 5), ('(FCPE 4)', 4),
                      ('(CGE I)', 5), ('(CGE II)', 4)]
    
    for tag, importance in tag_importance:
        if text.find(tag) != -1:
            return importance
    
    return 0   


def remove_nomeia_cargo_preamble(text_series):
    """
    Remove the preamble for a cargo/função from every 
    row in a `text_series`.
    
    (e.g. 'para exercer o cargo de')
    """
    # Regex of the beginning:
    enter_cargo_preamble_0 = ',?\s*?para\s*?(?:exercer|ocupar)\s*?'
    # Regex for cargo/função:
    enter_cargo_comissao   = 'o?\s*?cargo\s*?(?:em\s*?comiss[aã]o|comissionado)?'
    enter_cargo_funcao     = 'a?\s*?fun[cç][aã]o(?:\s*?comissionada)?(?:\s*?do\s*?poder\s*?executivo)?'
    # Regex for the final part:
    enter_cargo_preamble_1 = '\s*?de'
    # Full regex:
    preamble_regex = enter_cargo_preamble_0 + '(?:' + enter_cargo_funcao + '|' + enter_cargo_comissao + ')' \
                   + enter_cargo_preamble_1
    
    # Remove preamble:
    new_text_series = text_series.str.replace('(' + preamble_regex + ')', '', case=False)
    
    return new_text_series


def simplify_exonera_cargo_preamble(text_series):
    """
    Simplify preamble of a cargo/função in the case of 
    a exoneração/dispensa.
    
    (e.g.: 'do cargo comissioado de' -> 'do cargo de')
    """
    
    # Regexes:
    exit_cargo_preamble  = '(do\s*?cargo\s*?(?:em\s*?comissão|comissionado)?\s*?de)'
    exit_funcao_preamble = '(da\s*?função\s*?comissionada\s*?(?:do\s*?poder\s*?executivo)?\s*?de)'
    
    # Transform text series:
    new_text_series = text_series.copy()
    new_text_series = new_text_series.str.replace(exit_cargo_preamble, 'do cargo de', case=False)
    new_text_series = new_text_series.str.replace(exit_funcao_preamble, 'da função de', case=False)
    
    return new_text_series


def simplify_cargo_preamble(text_series):
    """
    Simplify preambles like 'para exercer o cargo de' and
    'da função comissionada do poder executivo' in `text_series`.
    """
    new_text_series = text_series.copy()
    
    # Location of exonera/dispensa and nomeia/designa acts:
    exonera_cases = new_text_series.str.contains('^(?:exonera|dispensa)', case=False)
    nomeia_cases  = new_text_series.str.contains('^(?:nomeia|designa)', case=False)
    
    # Replace large texts for shorter ones:
    new_text_series.loc[exonera_cases] = simplify_exonera_cargo_preamble(new_text_series.loc[exonera_cases])
    new_text_series.loc[nomeia_cases]  = remove_nomeia_cargo_preamble(new_text_series.loc[nomeia_cases])
    
    return new_text_series


def truncate_text(text, n_chars=400):
    """
    If `text` (str) is longer than `n_chars` (int), return 
    the first `n_chars` characters followed by '...'; 
    otherwise, return `text`.
    """
    text_len = len(text)
    if text_len <= n_chars:
        return text
    else:
        return text[:n_chars] + '...'
    

def prep_orgao_regex(name, acronym):
    """
    Given a orgão `name` and its `acronym`, return a regex
    that detects the name, possibly followed by the acronym 
    with possible variations in terms of spacing and other 
    formatting.
    """
    regex = name.replace(' ', r'\s*?') + '[\s-]*(?:\(?' + acronym + '\)?)?'
    return regex


def name_to_sigla(text_series):
    """
    Replace long reference of a orgão (name + possible acronym)
    by its acronym in a `text_series`. All orgãos are hard-coded. 
    """

    # Hard-coded acronyms and names of órgãos:
    sigla_list = ['FNDE', 'IBAMA', 'ICMBio', 'INCRA', 'FUNAI', 'CAPES', 'INEP', 'CNPq', 'ABIN']
    orgao_list = ['Fundo Nacional de Desenvolvimento da Educa[cç][aã]o',
                  'Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renov[aá]veis',
                  'Instituto Chico Mendes de Conserva[cç][aã]o da Biodiversidade',
                  'Instituto Nacional de Coloniza[cç][aã]o e Reforma Agr[aá]ria',
                  'Funda[cç][aã]o Nacional do [IÍ]ndio',
                  'Coordena[cç][aã]o de Aperfei[cç]oamento de Pessoal de N[ií]vel Superior',
                  'Instituto Nacional de Estudos e Pesquisas Educacionais An[ií]sio Teixeira',
                  'Conselho Nacional de Desenvolvimento Cient[ií]fico e Tecnol[oó]gico',
                  'Ag[eê]ncia Brasileira de Intelig[eê]ncia']
    # Create robust regexes out of name and acronym:
    regex_list = [prep_orgao_regex(name, acronym) for name, acronym in zip(orgao_list, sigla_list)]
    
    new_text_series = text_series.copy()
    for regex, sigla in zip(regex_list, sigla_list):
        new_text_series = new_text_series.str.replace(regex, sigla, case=False)
    
    return new_text_series


def remove_dates(text_series):
    """
    Remove references to dates that start with 'a partir de'
    or 'a contar de'.
    """
    # Preparing regex:
    mes_list = ['janeiro', 'fevereiro', 'mar[cç]o', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 
                'outubro', 'novembro', 'dezembro']
    mes_regex = '(?:' + '|'.join(mes_list) + ')'
    data_regex = r',? a (?:partir|contar) de (?:\d{1,2}.? de ' + mes_regex + ' de (?:20|19)\d{2}|\d{1,2}/\d{1,2}/\d{4}),?'
    data_regex = data_regex.replace(' ', '\s*?')
    
    new_text_series = text_series.str.replace(data_regex, '', case=False)
    
    return new_text_series


def assign_emoji(act_text):
    """
    Returns a certain hard-coded emoji given a hard-coded regex 
    found in `act_text` (str).
    """
    
    # List of regexes and emojis. The list is ordered by preference:
    regex_emoji = [('substitu', '🎒'), (r'pol[ií]cia\s*?(?:rodovi[aá]ria)?\s*?federal', '👮🏻'),
               (r'\(DAS 6\)', '👑'), (r'\((?:DAS|FCPE) 5\)', '🎩'), (r'\((?:DAS|FCPE) 4\)', '🧢'),  
               (r'\((?:CA|CGE) I{1,3}\)', '💼'), (r'\(CDT\)', '👓'), 
               (r'(?:grupo de trabalho|comitê|conselho)', '💬')]
    
    # Look for patterns:
    for regex, emoji in regex_emoji:
        if re.search(regex, act_text, flags=re.IGNORECASE) != None:
            return emoji
    
    # Default return:
    return '▪️'


def prepare_with_acts(materia_series, act_regex):
    """
    Process `materia_series` (Pandas Series) of matérias from DOU that 
    contains the pattern `act_regex`. Those are assumed to be 
    standard nomeações/exonerações/designações/dispensas.
    
    Returns
    -------
    
    cleaned_acts : Pandas Series
        The individual acts present in matérias, with low cargos
        removed and with its texts cleaned. Acts (each 
        nomeação/exoneração) in the same matéria share the same 
        index as the original matéria in `materia_series`.
    """
    # Isolate each act in a different row:
    raw_acts = isolate_acts(materia_series, act_regex)

    # Filter acts containing only low cargos:
    filtered_acts = filter_low_cargos(raw_acts)

    # Remove unwanted information:
    cleaned_acts  = filtered_acts
    cleaned_acts  = remove_siape(cleaned_acts)
    cleaned_acts  = remove_cpf(cleaned_acts)
    cleaned_acts  = remove_no(cleaned_acts)
    cleaned_acts  = remove_processo(cleaned_acts)
    
    # Clean text:
    cleaned_acts  = fix_verbs(cleaned_acts)
    cleaned_acts  = standardize_cargos(cleaned_acts)
    cleaned_acts  = simplify_cargo_preamble(cleaned_acts)
    cleaned_acts  = name_to_sigla(cleaned_acts)
    cleaned_acts  = remove_dates(cleaned_acts)
    
    return cleaned_acts


def prepare_no_acts(materia_series):
    """
    Clean `materia_series` (Pandas Series) of matérias from DOU 
    that do not contain typical verbs of nomeação/exoneração, etc.
    """
    cleaned_non_acts = materia_series
    cleaned_non_acts = remove_preamble(cleaned_non_acts)
    cleaned_non_acts = truncate_text(cleaned_non_acts)
    
    return cleaned_non_acts


def sort_orgaos_by_acts_importance(message_df, orgao_importance):
    """
    Define the order of the orgãos in the message, according to 
    the relevance of the acts.
    
    Input
    -----
    
    message_df : Pandas DataFrame
        A DataFrame containing the section in which the acts 
        will be published and their importance.
        
    orgao_importance : Pandas Series
        A series whose indices are the sections (labels for orgãos)
        and the values are the label's importances, for breaking 
        ties.
        
    Return
    ------
    
    ordered_sections : array
        The orgãos labels sorted as they should appear in the message.
    """
    
    # Compute stats for each orgao (message's sections):
    sections_groupby_importance = message_df.groupby('section')['importance']
    sections_importance_sum = sections_groupby_importance.sum()
    sections_importance_max = sections_groupby_importance.max()
    
    # Build DataFrame with the section's stats:
    sections_ranking_df = pd.DataFrame()
    sections_ranking_df['max'] = sections_importance_max
    sections_ranking_df['sum'] = sections_importance_sum
    sections_ranking_df = sections_ranking_df.join(orgao_importance, how='left')

    # Order the sections:
    ordered_sections = sections_ranking_df.sort_values(['max','sum', 'importance'], ascending=False).index.values
    
    return ordered_sections


### MAIN CODE ###


### Hard-coded stuff:

orgao_label_file = 'data/correspondencia_orgao_label_DOU_2.csv'
todays_dou_data  = 'temp/daily_ranked_dou_2_set.csv'
zap_group_link   = 'https://chat.whatsapp.com/GJGfNBzcZZT9xuueZUBbTU'
post_file_prefix = 'posts/dou_2_'
text_editor      = 'gedit'

prod_query = """
SELECT relevancia, identifica, secao, edicao, data_pub, orgao, ementa, resumo, fulltext, assina, cargo, url 
FROM `gabinete-compartilhado.executivo_federal_dou.sheets_classificacao_secao_2`
WHERE relevancia IS NOT NULL
AND   relevancia >= 3
"""

### Load data:

# Table that translates orgao to message topic:
print('Loading orgão-label table...')
orgao_label = pd.read_csv(orgao_label_file)

# Download today's ranked DOU (section 2) materias:
test_set = load_data_from_local_or_bigquery(prod_query, todays_dou_data, force_bigquery=True)
print('# matérias (all):', len(test_set))


### Prepare the data:

print('Processing the matérias...')

# Add label tag to all texts:
add_label_to_df(test_set, orgao_label)

# Use regex to detect typical act verbs:
enter_regex   = r'nomear|designar'
exit_regex    = r'exonerar|dispensar'
flexing_regex = r'(?!(?:am|á|ão|em))'
act_regex     = r'(' + enter_regex + '|' + exit_regex + ')' + flexing_regex

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    
    # Get articles with default act detectors:
    with_act_regex_df = test_set.loc[test_set.fulltext.str.contains(act_regex, case=False)]
    print('# matérias (containing act verbs):', len(with_act_regex_df))
    # Get articles without default act detectors:
    no_act_regex_df   = test_set.loc[~test_set.fulltext.str.contains(act_regex, case=False)]
    print('# matérias (without act verbs):', len(no_act_regex_df))

    # Clean acts for posting:
    cleaned_with_acts = prepare_with_acts(with_act_regex_df['fulltext'], act_regex)
    cleaned_no_acts   = prepare_no_acts(no_act_regex_df['fulltext'])
    # Join both kinds of matérias:
    cleaned_all = pd.concat([cleaned_with_acts, cleaned_no_acts], sort=False)


### Prepare the message:

print('Preparing the post...')

# Build zap message DataFrame:
message_df = pd.DataFrame()
message_df['text']       = cleaned_all
message_df['importance'] = cleaned_all.apply(act_importance)
message_df['section']    = test_set['label'][cleaned_all.index]
message_df['url']        = test_set['url'][cleaned_all.index]
message_df = message_df.reset_index(drop=True)

# Prepare the order in which the orgãos will appear in the message:
ordered_sections = sort_orgaos_by_acts_importance(message_df, orgao_label.set_index('label')['importance'])


### Print the message:

print('Writing post...')

filename = post_file_prefix + date.today().strftime('%Y-%m-%d') + '.txt'
with open(filename, 'w') as f:

    # Header:
    today = date.today().strftime(' (%d/%m)')
    f.write('♟️ *Alterações em cargos altos' + today + '* ♟️\n\n')

    # Loop over orgãos:
    for s in ordered_sections:
        f.write('*' + s + '*\n\n')

        # Select acts from this section:
        section_acts = message_df.loc[message_df['section'] == s].sort_values('importance', ascending=False)
        for t, u in zip(section_acts['text'].values, section_acts['url'].values):
            # Print message:
            e = assign_emoji(t)
            f.write(e + ' ' + t + '\n' + u + '\n\n')

            # Footnote:        
    f.write('*Gabinete Compartilhado Acredito*\n_Para se inscrever no boletim, acesse o link:_\n' + zap_group_link)


### Open text editor:

subprocess.call([text_editor, filename])
