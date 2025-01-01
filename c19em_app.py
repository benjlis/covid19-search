"""Streamlit app for FOIA Explorer COVID-19 Emails"""
import streamlit as st
from streamlit_pdf_viewer import pdf_viewer
import requests
import pandas as pd
import altair as alt
import psycopg2
import datetime
from st_aggrid import AgGrid, GridOptionsBuilder
import sqlgen as sg


st.set_page_config(page_title="COVID-19 Corpus", layout="wide")
st.title("COVID-19 Corpus")
"""
Our COVID-19 corpus aims aggregates digitized documents related to the the
initial phases of the pandemic.  They are now divided into individual emails, which can be searched
and sorted with the original metadata (from, to, subject, etc.) as well as new
metadata we generated using topic modeling and named entity recognition.
"""

conn = st.connection("postgresql", type="sql", ttl="1d", max_entries=2)

def get_entity_list(qual):
    q = f'SELECT entity from covid19.entities where enttype {qual} order by entity'
    return conn.query(q)

def get_topic_list():
    tq = "select topic from covid19.topics order by topic"
    return conn.query(tq)

# build dropdown lists for entity search
person_list = get_entity_list("= 'PERSON' ")
org_list = get_entity_list("= 'ORG' ")
loc_list = get_entity_list("in ('GPE', 'LOC', 'NORP', 'FAC') ")
topic_list = get_topic_list()

"""## Emails"""

chartqry = """
select date(sent) date, count(*) emails
    from covid19.emails
    where sent between '2019-11-01' and '2021-05-07'
    group by date
    order by date;
"""
chartdf = conn.query(chartqry)
chartdf['date'] = pd.to_datetime(chartdf['date'])

# Create the Vega-Lite chart with custom date format and tooltip
st.vega_lite_chart(chartdf, {
    "mark": {"type": "bar"},
    "encoding": {
        "x": {
            "field": 'date', 
            "type": "temporal",
            "axis": {
                "format": "%m-%Y"  # Format for x-axis labels
            }
        },
        "y": {"field": 'emails', "type": "quantitative"},
        "tooltip": [
            {"field": 'date', "type": "temporal", "format": "%m-%d-%Y"},  # Format for tooltip
            {"field": 'emails', "type": "quantitative"}
        ]
    }
}, use_container_width=True)


"""## Search"""
MIN_SENT = datetime.date(2019, 11, 1)
MAX_SENT = datetime.date(2021, 5, 8)

with st.form(key='query_params'):
    ftq_text = st.text_input('Full Text Search:', '',
                             help='Perform full text search. Use double quotes \
                             for phrases, OR for logical or, and - for \
                             logical not.')
    persons = st.multiselect('Person(s):', person_list)
    orgs = st.multiselect('Organization(s):', org_list)
    locations = st.multiselect('Location(s):', loc_list)
    topics = st.multiselect('Topic(s):', topic_list)
    dates = st.date_input("Date Range", value=[])
    null_date = st.checkbox("Include documents without a date", value=True) 
    query = st.form_submit_button(label='Execute Search')
# build query
selfrom = """select sent, subject, from_email "from", to_emails "to", 
                    foiarchive_file "file",  file_pg_start pg, email_id id, 
                    topic top_topic, entities, source_email_url,  preview_email_url
            from covid19.dc19_emails
        """
sql_predicates = []
display_predicates = []
# full text
if ftq_text:
    ftq_text = ftq_text.replace("'", '"')
    sg.add_predicate(sql_predicates, f"to_tsvector('english', body) @@ \
                        websearch_to_tsquery('english', '{ftq_text}')")
    sg.add_predicate(display_predicates, f"text body contains '{ftq_text}'")
# entities
entities = persons + orgs + locations
if entities:
    # build entity in list
    entincl = "'{"
    for e in entities:
        entincl += f'"{e}", '
    entincl = entincl[:-2] + "}'"
    entity_predicate = f"entities && {entincl}::text[]"
    sg.add_predicate(sql_predicates, entity_predicate)
    tq = ''
    if len(entities) > 1:
        tq = 'at least one of'
    entity_explain = f" and email references {tq} {entincl[2:-2]}"
    sg.add_predicate(display_predicates, entity_explain)
# topics
topics_predicate = sg.lov_predicate('topic', topics)
sg.add_predicate(sql_predicates, topics_predicate)
sg.add_predicate(display_predicates, topics_predicate)
# dates
start_date, end_date = sg.convert_daterange(dates, "%Y/%m/%d")
date_predicate = sg.daterange_predicate('sent',
                                        start_date, end_date, null_date, 
                                        MIN_SENT, MAX_SENT)
sg.add_predicate(sql_predicates, date_predicate)
sg.add_predicate(display_predicates, date_predicate)

where_clause = sg.where_clause(sql_predicates)
query_display = sg.where_clause(display_predicates)
# execute query
MAX_LIMIT = 2000
emqry = selfrom + where_clause + f' limit {MAX_LIMIT}'
emdf = conn.query(emqry)
emcnt = len(emdf.index)

st.write(f"""## Results   
{emcnt} {'(max limit)'if emcnt == MAX_LIMIT else ''} emails {query_display}  
select row to view additional details
""")
emdf['sent'] = pd.to_datetime(emdf['sent'])
emdf['sent'] = emdf['sent'].dt.strftime('%Y-%m-%d %H:%M')
# generate AgGrid
gb = GridOptionsBuilder.from_dataframe(emdf)
gb.configure_default_column(value=True, editable=False)
gb.configure_grid_options(domLayout='normal')
gb.configure_selection(selection_mode='single', groupSelectsChildren=False)
gb.configure_column('top_topic', hide=True)
gb.configure_column('entities', hide=True)
gb.configure_column('source_email_url', hide=True)
gb.configure_column('preview_email_url', hide=True)
gb.configure_column('sent', maxWidth=150)
gb.configure_column('subject', maxWidth=600)
gb.configure_column('from', maxWidth=225)
gb.configure_column('to', maxWidth=425)

# Define base height and additional height per row
base_height = 50  # Base height for the grid
row_height = 16    # Height per row
max_height = 260  # Maximum height for the grid
# Calculate total height
total_height = base_height + (row_height * emcnt)
# Ensure the total height does not exceed the maximum height
total_height = min(total_height, max_height)
gridOptions = gb.build()
grid_response = AgGrid(emdf,
                        gridOptions=gridOptions,
                        height=total_height,
                        return_mode_values='AS_INPUT',
                        update_mode='SELECTION_CHANGED',
                        allow_unsafe_jscode=False,
                        enable_enterprise_modules=False)
selected = grid_response['selected_rows']
if selected is not None:
    st.write(f"""details on `{selected.iloc[0]["subject"]}`  
                entities: `{selected.iloc[0]["entities"]}`  
                topic words: `{selected.iloc[0]["top_topic"]}`  
                first page preview:""")
    preview_pdf_url = selected.iloc[0]["preview_email_url"]
    response = requests.get(preview_pdf_url)
    if response.status_code == 200:
        with st.container(border=True):
            pdf_viewer(response.content)
        st.markdown(f'**[view full PDF]({selected.iloc[0]["source_email_url"]})**')
    else:
        st.write(f"Failed to download {preview_pdf_url}, \
                 status code: {response.status_code}.")
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