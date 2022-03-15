from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd


# postgres db
conn_string = 'postgresql://opentarget:opentarget@localhost/opentarget'
engine = create_engine(conn_string)
inspector = inspect(engine)

# credset sample
credset_df = pd.read_csv('data/credset_sample.csv')
credset_cols_keep = ['lead_alt', 'lead_chrom', 'lead_pos', 'lead_ref', 'lead_variant_id', 'logabf', 'postprob',
                     'study_id', 'tag_chrom','tag_alt', 'tag_ref', 'tag_variant_id', 'type']
credset_df = credset_df[credset_cols_keep]
credset_df = credset_df.reset_index()
credset_df['tag_chrom'] = credset_df['tag_chrom'].astype('str')
credset_df['lead_chrom'] = credset_df['lead_chrom'].astype('str')
credset_df.to_sql('credset', con=engine, if_exists='replace',index=False)
print(credset_df.head())

# study_sample
study_df = pd.read_csv('data/study_sample.csv')
print('Save study table to opentarget database...')
study_df = study_df.reset_index()
study_df.to_sql('study', con=engine, if_exists='replace',index=False)
print(study_df.head())

# Variant_sample
variant_df = pd.read_csv('data/variant_sample.csv')
variant_df = variant_df.reset_index()
# variant_df = variant_df.drop_duplicates(subset=['chr_id', 'position' ,'ref_allele', 'alt_allele'])
variant_df['chr_id'] = variant_df['chr_id'].astype('str')
variant_df['variant_id'] = variant_df.apply(lambda x: ':'.join([x['chr_id'],str(x['position']),x['ref_allele'],x['alt_allele']]),axis=1)
variant_df = variant_df.drop_duplicates(subset=['variant_id'])
variant_df.to_csv('data/variant_sample_withkey.csv',index=False)
variant_df.to_sql('variant', con=engine, if_exists='replace',index=False)
print(variant_df.head())


# ADD primary and foreign key to the tables
# NOTE: the credset table serve as a bridge table, not need to have primary key
sql = '''
ALTER TABLE study
ADD PRIMARY KEY (study_id);

ALTER TABLE variant
--ADD PRIMARY KEY (chr_id, position ,ref_allele, alt_allele);
ADD PRIMARY KEY (variant_id);

ALTER TABLE credset 
ADD COLUMN credset_id SERIAL PRIMARY KEY;

ALTER TABLE credset
ADD FOREIGN KEY (study_id) REFERENCES study (study_id),
-- ADD FOREIGN KEY (tag_chrom, tag_pos, tag_ref, tag_alt) REFERENCES variant(chr_id, position ,ref_allele, alt_allele);
ADD FOREIGN KEY (tag_variant_id) REFERENCES variant (variant_id);

CREATE INDEX idx_credset_study_id on credset(study_id);
CREATE INDEX idx_credset_tag_variant_id on credset(tag_variant_id);


'''

with engine.connect() as conn:
    conn.execute(sql)