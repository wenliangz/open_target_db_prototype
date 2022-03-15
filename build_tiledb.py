import tiledb
import pandas as pd
import os
import glob


# Build TileDB Arrays if not exists
if not os.path.exists('opentarget_arrays'):
    tiledb.group_create('opentarget_arrays')

if tiledb.array_exists(os.path.join('opentarget_arrays', 'study_tldb')):
    tiledb.remove(os.path.join('opentarget_arrays', 'study_tldb'))
    tiledb.from_csv(os.path.join('opentarget_arrays', 'study_tldb'), 'data/study_sample.csv',
                    index_dims=['study_id', 'trait_category'],
                    chunksize=10 ** 6,
                    sparse=True,
                    fillna={"trait_efos": '',
                            'ancestry_replication': '',
                            'ancestry_initial': '',
                            'pmid':'',
                            'pub_journal':'',
                            'pub_title':''})

if tiledb.array_exists(os.path.join('opentarget_arrays', 'credset_tldb')):
    tiledb.remove(os.path.join('opentarget_arrays', 'credset_tldb'))
    tiledb.from_csv(os.path.join('opentarget_arrays', "credset_tldb"), 'data/credset_sample.csv',
                    index_dims=['tag_variant_id'],
                    sparse=True,
                    chunksize=10 ** 6)

if tiledb.array_exists(os.path.join('opentarget_arrays', 'variant_tldb')):
    tiledb.remove(os.path.join('opentarget_arrays', 'variant_tldb'))
    tiledb.from_csv(os.path.join('opentarget_arrays', 'variant_tldb'), 'data/variant_sample_withkey.csv',
                    index_dims=['variant_id'],
                    fillna={"rs_id": ''},
                    sparse=True,
                    chunksize=10 ** 6)


# read tiledb
with tiledb.open(os.path.join('opentarget_arrays', 'credset_tldb')) as credset_array:
    # credset_df = credset_array.df[:]
    credset_schema = credset_array.schema
    print(credset_schema)

with tiledb.open(os.path.join('opentarget_arrays', 'study_tldb')) as study_array:
    # study_df = study_array.df[:]
    study_schema = study_array.schema
    print(study_schema)

with tiledb.open(os.path.join('opentarget_arrays', 'variant_tldb')) as variant_array:
    variant_df = variant_array.df[:]
    variant_schema = variant_array.schema
    print(variant_schema)