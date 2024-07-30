import pdb # Import Python debugger
import pandas as pd # Import pandas for data manipulation
import time # Import time for performance metrics
import argparse # Import argparse for command-line parsing

# Import custom functions from local modules
from models.database import store_in_SQL, create_dynamic_tables, transform_pubications_for_SQL, transform_authors_for_SQL
from services.XMLServices import list_XML_files, process_XML, set_XML_path



def process_file(file, count, AuthorIDCounter, AffiliationIDCounter):
    """
    Process a single XML file to extract and store publication, author, and affiliation data in SQL.
    
    Parameters:
    - file (str): The path to the XML file to be processed.
    - count (int): The current count of processed files, used to determine if dynamic tables need creation.
    - AuthorIDCounter (int): A global counter for assigning unique IDs to authors.
    - AffiliationIDCounter (int): A global counter for assigning unique IDs to affiliations.
    
    Returns:
    - AuthorIDCounter (int): Updated author ID counter.
    - AffiliationIDCounter (int): Updated affiliation ID counter.
    """

    # Record the start time
    start_time = time.time()
    
    publications_df, authors_df, affiliations_df,AuthorIDCounter,AffiliationIDCounter = process_XML(file,AuthorIDCounter,AffiliationIDCounter)
    
    if count ==0 :
        create_dynamic_tables(publications_df,authors_df,affiliations_df)
        # pdb.set_trace()
    
    publications_df = transform_pubications_for_SQL(publications_df)
    authors_df = transform_authors_for_SQL(authors_df)
    store_in_SQL('publications',publications_df)
    store_in_SQL('authors',authors_df)
    store_in_SQL('affiliations',affiliations_df)

    count = count + 1
    
    # Record the end time
    end_time = time.time()
    
    # Compute the elapsed time in seconds
    elapsed_time_seconds = end_time - start_time
    
    # Convert the elapsed time to minutes
    elapsed_time_minutes = elapsed_time_seconds / 60
    
    # Print the elapsed time in minutes
    print(f'Elapsed time: {elapsed_time_minutes:.2f} minutes')
    return AuthorIDCounter, AffiliationIDCounter

if __name__ == "__main__":
    #PATH FOR DEBUG: '/storage/geneGinie/ncbi_ftp_data/pubmed/XML'
    parser = argparse.ArgumentParser(description="Pubmed XML parser. Generates a sqlite database from XML pubmed files")
    parser.add_argument('xml_path', type=str, help='Path to pubmed XML files')
    args = parser.parse_args()

    set_XML_path(args.xml_path)
    #Data Id Global Trackers (for Authors and Affiliations) Incremented when a new author or affiliation is added
    AuthorIDCounter = 0
    AffiliationIDCounter = 0

    # Filter only the XML files
    xml_files = list_XML_files()

    count = 0
    #  ['pubmed23n0001.xml','pubmed23n0189.xml']
    for each_XML_file in xml_files[:5]:
        print(count)
        print(each_XML_file)
        print(f'AuthorIDCounter: {AuthorIDCounter}, AffiliationIDCounter: {AffiliationIDCounter}')
        AuthorIDCounter, AffiliationIDCounter = process_file(each_XML_file,count,AuthorIDCounter,AffiliationIDCounter)
        count = count + 1