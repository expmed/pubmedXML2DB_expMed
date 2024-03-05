from models.database import store_in_SQL, create_table,fetch_records_in_batches
from affiliation_parser import parse_affil
import pandas as pd
import pdb

# Define the structure of the table for storing parsed affiliations
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

# Create the table for parsed affiliations in the database
affiliations_parsed_table = create_table('AffiliationsParsed', 'affiliations_parsed', affiliations_parsed_columns)

# Initialize a counter for new affiliation IDs
new_aff_id = 0

# Fetch affiliations in batches and process each batch
for each_batch in fetch_records_in_batches('affiliations', batch_size=30000):
    parsed_data = []# Initialize a list to store parsed affiliation data
    # Extract affiliation text and IDs into lists
    affiliations_list = each_batch['affiliation'].values.tolist()
    affiliation_ids = each_batch['Affiliation_ID'].values.tolist()

    # Process each affiliation in the batch
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
    
    # Convert the list of parsed affiliations into a DataFrame and store it in the SQL database
    parsed_df = pd.DataFrame(parsed_data)
    store_in_SQL('affiliations_parsed', parsed_df)
    print(parsed_df)