import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
import tiledb
import time
from pymongo import MongoClient
import pandas as pd
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os


st.set_page_config(
    page_title="Open Target Databases",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(""" <style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden; }
</style> """, unsafe_allow_html=True)

# ====================================== StreamLit App =======================================================

conn_string = 'postgresql://opentarget:opentarget@localhost/opentarget'
engine = create_engine(conn_string)
trait_category = pd.read_sql('SELECT trait_category FROM study', con=engine)
with st.sidebar:
    st.header('User Inputs:')
    with st.form(key='my_form'):
        # st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)
        db_choice = st.radio('Databases', ('postgres', 'TileDB', 'MongoDB', 'Mongo + pySpark'))
        category = st.selectbox("Trait Category", tuple(sorted(set(trait_category['trait_category']))))
        chromosome = st.selectbox(
            'Chromosome:',
            tuple(['all'] + [str(i) for i in range(1, 23)] + ['X', 'Y']),
        )
        # position = st.text_input('position:', '')
        # alt_allele = st.text_input('alt_allele:', '')
        # ref_allele = st.text_input('ref_allele:', '')
        postprob = st.slider('Credset Post Probability', 0.0, 1.0, 0.5)  # st.text_input("Credset Postpro:", '')
        st.text("")
        submit_button = st.form_submit_button(label='Submit')

# # ====================== Database Query based on User Inputs ==========================================
# Create a text element and let the reader know the data is loading
st.markdown(f"<h1 style='text-align: center;'>Open Target Genetics Database Modeling - Query Demo</h1>", unsafe_allow_html=True)
query_code = st.text('')
query_time = st.text('')
query_result = st.text(f'Making Queries from the ({db_choice}) database...')
results_cols_keep = ['study_id', 'trait_category', 'chr_id', 'position', 'ref_allele', 'alt_allele', 'rs_id',
                       'most_severe_consequence', 'postprob', 'type']


if db_choice == 'postgres':

    # join operation using native SQL
    code = '''
        SELECT s.study_id, s.trait_category, v.chr_id, v.position, v.ref_allele, v.alt_allele, v.rs_id,
               v.most_severe_consequence, c.postprob, c.type
        FROM study s 
        JOIN credset c ON c.study_id = s.study_id
        JOIN variant v ON v.variant_id = c.tag_variant_id;
    
    '''
    time_start = time.time()
    with engine.connect() as conn:
        cur = conn.execute(code)
    results = pd.DataFrame(cur.fetchall())
    results.columns = results_cols_keep

    time_end = time.time()
    finish_time = time_end - time_start

elif db_choice == 'TileDB':
    code = '''
            with tiledb.open(os.path.join('opentarget_arrays', 'study_tldb')) as study_array:
                study_df = study_array.df[:].reset_index()
            with tiledb.open(os.path.join('opentarget_arrays', 'credset_tldb')) as credset_array:
                credset_df = credset_array.df[:].reset_index()
            with tiledb.open(os.path.join('opentarget_arrays', 'variant_tldb')) as variant_array:
                variant_df = variant_array.df[:].reset_index()
            results = variant_df.merge(study_df.merge(credset_df,on='study_id'),
                                       left_on='variant_id',
                                       right_on = 'tag_variant_id')
    '''
    time_start = time.time()
    with tiledb.open(os.path.join('opentarget_arrays', 'study_tldb')) as study_array:
        study_df = study_array.df[:].reset_index()
        # pd.DataFrame(study_array.multi_index[:, category])
    with tiledb.open(os.path.join('opentarget_arrays', 'credset_tldb')) as credset_array:
        credset_df = credset_array.df[:].reset_index()
    with tiledb.open(os.path.join('opentarget_arrays', 'variant_tldb')) as variant_array:
        variant_df = variant_array.df[:].reset_index()

    # joined operation using pandas merge function
    results = variant_df.merge(study_df.merge(credset_df,on='study_id'),
                               left_on='variant_id',
                               right_on='tag_variant_id')
    results = results[results_cols_keep]
    results['chr_id'] = results['chr_id'].astype('str')

    time_end = time.time()
    finish_time = time_end - time_start

elif db_choice == 'MongoDB':
    client = MongoClient("mongodb://localhost:27017/")
    db = client["opentarget"]
    credset = db.credset

    code = '''
        pipeline = [{'$lookup':{'from': 'study','localField': 'study_id','foreignField': 'study_id','as': 'study_credset'}},
                    {'$unwind': '$study_credset'},
                    {'$lookup':{'from': 'variant','localField': 'tag_variant_id','foreignField': 'variant_id','as': 'variant_credset'}},
                    {'$unwind': '$variant_credset'},
                    {'$project':
                         {'_id': 0, 'postprob': 1, 'study_credset.study_id': 1, 'study_credset.trait_category': 1,
                          'study_credset.postprob': 1, 'study_credset.type': 1, 'variant_credset.chr_id': 1,
                          'variant_credset.position': 1, 'variant_credset.ref_allele': 1, 'variant_credset.alt_allele': 1,
                          'variant_credset.rs_id': 1, 'variant_credset.most_severe_consequence': 1}}]
        joined_collection = (credset.aggregate(pipeline))
    '''

    pipeline = [{'$lookup':{'from': 'study','localField': 'study_id','foreignField': 'study_id','as': 'study_credset'}},
                {'$unwind': '$study_credset'},
                {'$lookup':{'from': 'variant','localField': 'tag_variant_id','foreignField': 'variant_id','as': 'variant_credset'}},
                {'$unwind': '$variant_credset'},
                {'$project':
                     {'_id': 0, 'postprob': 1, 'study_credset.study_id': 1, 'study_credset.trait_category': 1,
                      'study_credset.postprob': 1, 'study_credset.type': 1, 'variant_credset.chr_id': 1,
                      'variant_credset.position': 1, 'variant_credset.ref_allele': 1, 'variant_credset.alt_allele': 1,
                      'variant_credset.rs_id': 1, 'variant_credset.most_severe_consequence': 1}}]
    time_start = time.time()
    joined_collection = (credset.aggregate(pipeline))
    allresults = ({ **doc['study_credset'], **doc['variant_credset'],'postprob': doc['postprob']} for doc in
               joined_collection)
    results = []
    n = 100
    for i in allresults:
        n -= 1
        results.append(i)
        if n == 0:
            break
    mongo_results = pd.DataFrame.from_records(results)
    time_end = time.time()
    finish_time = time_end - time_start
    # print(results.head())

else:
    spark = SparkSession.builder \
        .master('local') \
        .appName('Data Prep') \
        .getOrCreate()

    code = '''
        study.createOrReplaceTempView('study')
        credset.createOrReplaceTempView('credset')
        variant.createOrReplaceTempView('variant') 
        results = spark.sql(\'\'\'
                SELECT s.study_id, s.trait_category, v.chr_id, v.position, v.ref_allele, v.alt_allele, v.rs_id,
                       v.most_severe_consequence, c.postprob, c.type
                FROM study s 
                JOIN credset c ON c.study_id = s.study_id
                JOIN variant v ON v.variant_id = c.tag_variant_id
            \'\'\')
    '''

    time_start = time.time()
    # Read Mongo collection into pySpark Dataframe
    study = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                .option('uri','mongodb://127.0.0.1/opentarget.study')\
                .load()
    credset = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                .option('uri','mongodb://127.0.0.1/opentarget.credset')\
                .load()
    variant = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                .option('uri','mongodb://127.0.0.1/opentarget.variant')\
                .load()

    study.createOrReplaceTempView('study')
    credset.createOrReplaceTempView('credset')
    variant.createOrReplaceTempView('variant')
    results = spark.sql('''
    
            SELECT s.study_id, s.trait_category, v.chr_id, v.position, v.ref_allele, v.alt_allele, v.rs_id,
                   v.most_severe_consequence, c.postprob, c.type
            FROM study s 
            JOIN credset c ON c.study_id = s.study_id
            JOIN variant v ON v.variant_id = c.tag_variant_id
    
    ''')
    time_end = time.time()
    results = results.repartition(1).toPandas()
    finish_time = time_end - time_start

# ====================== Filter results to display based on user input ========================
if db_choice != 'MongoDB':
    if chromosome =='all':
        results = results[((results['postprob'] >= postprob) & (results['trait_category'] == category))]
    else:
        results = results[((results['chr_id'] == chromosome) & (results['trait_category'] == category)
                          & (results['postprob'] >= postprob))]
elif db_choice == 'MongoDB':
    results = mongo_results
else:
    results = results
results = results.reset_index(drop=True)
query_code.code(code, language='SQL')
query_time.markdown(f"<h4 style='text-align: left;'>Query Time: {finish_time:.2f} s ({len(results)} records)</h1>", unsafe_allow_html=True)
query_result.write(results)
