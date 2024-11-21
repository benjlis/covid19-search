"""Streamlit app for FOIA Explorer COVID-19 Emails"""
import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import datetime
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder


st.set_page_config(page_title="COVID-19 Archive Prototype", layout="wide")
st.title("COVID-19 Archive Prototype")
"""
Our COVID-19 Archive aims to aggregate digitized documents related to the the
initial phases of the pandemic. To start, we have processed the emails of
Dr. Anthony Fauci, director of the National Institute of Allergy and Infectious
Diseases. They are now divided into individual emails, which can be searched
and sorted with the original metadata (from, to, subject, etc.) as well as new
metadata we generated using topic modeling and named entity recognition.

* Source: MuckRock/DocumentCloud | Contributor: Jason Leopold
* https://www.documentcloud.org/documents/20793561-leopold-nih-foia-anthony-fauci-emails
"""

conn = st.connection("postgresql", type="sql", ttl="1d" )

def get_entity_list(qual):
    q = f'SELECT entity from covid19.entities where entity_id <= 515 and \
    enttype {qual} order by entity'
    return conn.query(q)

def get_topic_list():
    tq = """select distinct top_topic
               from covid19.fauci_emails
               where top_topic is not null"""
    return conn.query(tq)

# build dropdown lists for entity search
person_list = get_entity_list("= 'PERSON' ")
org_list = get_entity_list("= 'ORG' ")
loc_list = get_entity_list("in ('GPE', 'LOC', 'NORP', 'FAC') ")
topic_list = get_topic_list()

"""## Daily Email Volume, January - May 2020"""

emcnts = """
select date(sent) date, count(*) emails
    from covid19.emails
    where file_id = 1000 and sent >= '2020-01-01'
    group by date
    order by date;
"""
cntsdf = conn.query(emcnts)
cntsdf['date'] = pd.to_datetime(cntsdf['date']).dt.date
st.vega_lite_chart(cntsdf, {"mark": {"type": "bar"},
                             "encoding": {
                                "x": {"field": 'date', "type": "temporal",
                                        "axis": {"format": '%m-%d'}},
                                "y": {"field": 'emails', "type": "quantitative"},
                                }
                            }, use_container_width=True)  


"""## Search Emails """
with st.form(key='query_params'):
    cols = st.columns(2)
    begin_date = cols[0].date_input('Start Date', datetime.date(2020, 1, 23))
    end_date = cols[1].date_input('End Date', datetime.date(2020, 5, 6))
    persons = st.multiselect('Person(s):', person_list)
    orgs = st.multiselect('Organization(s):', org_list)
    locations = st.multiselect('Location(s):', loc_list)
    topics = st.multiselect('Topic(s):', topic_list)
    ftq_text = st.text_input('Full Text Search:', '',
                             help='Perform full text search. Use double quotes \
                             for phrases, OR for logical or, and - for \
                             logical not.')
    query = st.form_submit_button(label='Execute Search')
    where_ent = where_ft = ''


""" ## Search Results """
entities = persons + orgs + locations
selfrom = """
select email_id, pg_number, sent, subject, from_email "from", to_emails "to",
       top_topic, entities
    from covid19.fauci_emails
"""
where = f"where sent between '{begin_date}' and '{end_date}'"
qry_explain = where[6:].replace("'", "")
where_ent = where_ft = where_top = ''
if entities:
    # build entity in list
    entincl = "'{"
    for e in entities:
        entincl += f'"{e}", '
    entincl = entincl[:-2] + "}'"
    where_ent = f" and entities && {entincl}::text[]"
    tq = ''
    if len(entities) > 1:
        tq = 'at least one of'
    qry_explain += f" and email references {tq} {entincl[2:-2]}"
if topics:
    topincl = "("
    for t in topics:
        topincl += f"'{t}', "
    topincl = topincl[:-2] + ')'
    where_top = f" and top_topic in {topincl}"
    qry_explain += f" and topic is {topincl[1:-1]}"
if ftq_text:
    if ftq_text[0] == "'":         # replace single quote with double
        ftq_text = '"' + ftq_text[1:-1:] + '"'
    where_ft = f" and to_tsvector('english', body) @@ websearch_to_tsquery\
('english', '{ftq_text}')"
    qry_explain += f' and text body contains "{ftq_text}"'
# execute query
emqry = selfrom + where + where_ent + where_top + where_ft + ' order by sent'
emdf = conn.query(emqry)
emcnt = len(emdf.index)
st.markdown(f"{emcnt} emails {qry_explain}")
# download results as CSV
csv = emdf.to_csv().encode('utf-8')
st.download_button(label="CSV download", data=csv,
                   file_name='foia-covid19.csv', mime='text/csv')
# generate AgGrid
gb = GridOptionsBuilder.from_dataframe(emdf)
gb.configure_default_column(value=True, editable=False)
gb.configure_grid_options(domLayout='normal')
gb.configure_selection(selection_mode='single', groupSelectsChildren=False)
gb.configure_column('email_id', hide=True)
gb.configure_column('pg_number', hide=True)
gb.configure_column('top_topic', hide=True)
gb.configure_column('entities', hide=True)
gb.configure_column('sent', maxWidth=150)
gb.configure_column('subject', maxWidth=600)
gb.configure_column('from', maxWidth=225)
gb.configure_column('to', maxWidth=425)

gridOptions = gb.build()
grid_response = AgGrid(emdf,
                       gridOptions=gridOptions,
                       return_mode_values='AS_INPUT',
                       update_mode='SELECTION_CHANGED',
                       allow_unsafe_jscode=False,
                       enable_enterprise_modules=False)
selected = grid_response['selected_rows']

# define DocumentCloud references
dc_base = 'https://www.documentcloud.org/documents/'
dc_aws = 'https://s3.documentcloud.org/documents/'
dc_id = '20793561'
dc_slug = 'leopold-nih-foia-anthony-fauci-emails'
dc_gif_sz = 'large'
dc_doc_url = dc_base + dc_id + '-' + dc_slug
dc_pg_gif = dc_base + dc_id + '/pages/' + dc_slug + '-p{pg}-' + dc_gif_sz + \
            '.gif'
dc_aws_pdf = dc_aws + dc_id + '/' + dc_slug + '.pdf'

if selected is not None:
    """## Email Details"""
    #el_disp = selected.iloc[0]["entities"][1:-1].replace("'", "")
    st.markdown(f'**Entities**: `{selected.iloc[0]["entities"]}`')
    st.markdown(f'**Topic Words:** `{selected.iloc[0]["top_topic"]}`')
    pg = int(selected.iloc[0]["pg_number"])
    st.markdown('**Email Preview:** ')
    st.markdown('<iframe src=' + dc_pg_gif.format(pg=pg) +
                ' width="100%" height="1300">', unsafe_allow_html=True)
    st.markdown(f'[**View Full Email on DocumentCloud**]({dc_aws_pdf}#page=\
{pg})')

else:
    st.write('Select row to view additional email details')
"""
## About
Columbia University's [History Lab](http://history-lab.org)
maintains the COVID-19 Archive and its associated tools.

### Sponsors
"""
logo, description, _ = st.columns([1,2,2])
with logo:
    st.image('static/nhprc-logo.png')
with description:
    """
Current funding for the COVID-19 Archive is provided by an
archival project grant from the [National Historical Publications & Records
Commission (NHPRC)](https://www.archives.gov/nhprc). 
    """
logo, description, _ = st.columns([1,2,2])
with logo:
    st.image('static/mellon-logo.png')
with description:  
    """
Initial funding for the tools associated with the COVID-19 Archive
was provided by the Mellon Foundation's [Email Archives:
Building Capacity and Community](https://emailarchivesgrant.library.illinois.edu)
program.
    """