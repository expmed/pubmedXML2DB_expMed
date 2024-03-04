from models.database import store_in_SQL, create_table,fetch_records_in_batches
from affiliation_parser import parse_affil
import pandas as pd
import pdb
affiliations_parsed_columns = {
    'id':'int',
    'list_of_original_ids':'text',
    'full_text': 'text',
    'department': 'text',
    'institution': 'text',
    'location': 'text',
    'country': 'text',
    'zipcode': 'text',
    'email': 'text'
}

affiliations_parsed_table = create_table('AffiliationsParsed', 'affiliations_parsed', affiliations_parsed_columns)

new_aff_id = 0
for each_batch in fetch_records_in_batches('affiliations', batch_size=30000):
    # pdb.set_trace()
    parsed_data = []
    affiliations_list = each_batch['affiliation'].values.tolist()
    affiliation_ids = each_batch['Affiliation_ID'].values.tolist()

    for aff_id, each_affiliation in zip(affiliation_ids, affiliations_list):
        try:
            # Parse the affiliation and store the result
            parsed = parse_affil(each_affiliation)
            parsed['id'] = new_aff_id
            parsed['list_of_original_ids'] = aff_id
            parsed_data.append(parsed)
            new_aff_id += 1
        except AttributeError:
            print('Affiliation is empty or not a string. Removing it')    

    parsed_df = pd.DataFrame(parsed_data)
    store_in_SQL('affiliations_parsed', parsed_df)
    print(parsed_df)

# for this_set in batch: 
#     # Fetch data from 'affiliations' table
#     affiliations_data = get(tableName="affiliations")

#     # Process and insert data into 'affiliations_parsed' table
#     with engine.connect() as connection:
#         parsed_data = []
#         for _, row in affiliations_data.iterrows()[:10]:
#             parsed_data.push(parse_affil(row['affiliation']))
            
#             # insert_stmt = affiliations_parsed_table.insert().values(**parsed_data)
#             # connection.execute(insert_stmt)
#         pdb.set_trace()
#         # connection.commit()