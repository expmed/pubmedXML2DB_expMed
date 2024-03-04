import xml.etree.ElementTree as ET # Importing for XML parsing
import os # Importing for interacting with the file system
import pandas as pd # Importing pandas for data manipulation
import pdb # Import Python debugger
from transformers import pipeline # Importing from Hugging Face's Transformers for NLP tasks

# XML_path = '/storage/geneGinie/ncbi_ftp_data/pubmed/XML' 

# Initialize the classification pipeline from Hugging Face Transformers
classify = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")


def set_XML_path(new_path):
    """
    Sets the global variable XML_path to a new path.
    
    Parameters:
    - new_path (str): The new path for XML_path global variable.
    """
    global XML_path
    XML_path = new_path

def list_XML_files():
    """
    Lists all XML files in the specified directory set by XML_path.
    
    Returns:
    - List of filenames that end with '.xml'
    """
    all_files = os.listdir(XML_path)
    xml_files = [file for file in all_files if file.endswith('.xml')]
    return xml_files

#LOAD
def load_XML(file='pubmed23n1226.xml'):
    """
    Loads an XML file and returns its root element.
    
    Parameters:
    - file (str): Filename of the XML file to be loaded.
    
    Returns:
    - The root of the XML tree or None if an error occurs.
    """
    try:
        tree = ET.parse(f'{XML_path}/{file}')
        return tree.getroot()
    except Exception as e:
        print(f"Error loading XML: {e}")
        return None
    
#Process
def process_XML( XML,AuthorIDCounter,AffiliationIDCounter,):
    """
    Processes an XML file to extract publications, authors, and affiliations information.
    
    Parameters:
    - XML (str): The path to the XML file.
    - AuthorIDCounter (int): A counter for assigning unique IDs to authors.
    - AffiliationIDCounter (int): A counter for assigning unique IDs to affiliations.
    
    Returns:
    - Tuple containing DataFrames for publications, authors, affiliations, and updated counters.
    """

    # Initialize lists to hold data
    publications_list = []
    affiliations_list = []
    authors_list = []
   
   # Load the XML and get the root
    xml_root = load_XML(XML)

    # Initialize empty DataFrames for publications, authors, and affiliations
    affiliations_df =pd.DataFrame()
    publications_df = pd.DataFrame()
    authors_df = pd.DataFrame()
    
    # Loop through each publication in the XML file
    for pubmed_article in xml_root.iter('PubmedArticle'):
        # Parse and transform the publication data
        pub_dict = parse_publication(pubmed_article)
        pub_dict, affiliations_list, authors_list, AuthorIDCounter , AffiliationIDCounter = transform_XML(pub_dict, affiliations_list, authors_list,XML,AuthorIDCounter,AffiliationIDCounter)
        #Appended parsed data to the list
        publications_list.append(pub_dict)
    
    # Convert lists to DataFrames
    publications_df = pd.DataFrame(publications_list).sort_index(axis=1)
    # Group by PMID to remove duplicates
    publications_df = publications_df.groupby(['PMID'], as_index=False).first() 

    if len(affiliations_list) > 0:
        #For afiliations we group the ids together of the same affilition before storing in database
        affiliations_df = pd.DataFrame(affiliations_list).sort_index(axis=1)
        affiliations_df = affiliations_df.groupby('affiliation')['Affiliation_ID'].apply(lambda x: ','.join(map(str, x))).reset_index()[['Affiliation_ID','affiliation']]
    if len(authors_list) > 0:
        authors_df = pd.DataFrame(authors_list).sort_index(axis=1)
        #Remove Duplicates
        authors_df = authors_df.groupby(['PMID','ForeName','LastName','Initials'], as_index=False).first()
    
    return publications_df, authors_df, affiliations_df, AuthorIDCounter, AffiliationIDCounter
    
#PARSE PUBLICATION DATA
def parse_publication(pub_XML_element):

    """
        Parses publication data from a given XML element into a dictionary.
        
        Parameters:
        - pub_XML_element (Element): The XML element to parse.
        
        Returns:
        - Dictionary containing parsed publication data.
        """

    xml_dict = {}

    ## Get different XML pieces
    xml_dict['date_completed'] = pub_XML_element.find('.//DateCompleted')
    # xml_dict['Abstract'] = pub_XML_element.find('.//Abstract')
    xml_dict['reference_list'] = pub_XML_element.findall('.//ReferenceList//ArticleIdList')

    # Additional XML pieces
    xml_dict['PublicationStatus'] = pub_XML_element.find('.//PublicationStatus')
    # xml_dict['SupplMeshList'] = pub_XML_element.findall('.//SupplMeshList') TODO
    # xml_dict['DateRevised'] = pub_XML_element.find('.//DateRevised') Add to PubvMedArticle Parsing 
    xml_dict['MedlineCitation'] = pub_XML_element.find('MedlineCitation')
    xml_dict['PubmedData'] = pub_XML_element.find('.//PubmedData')
    xml_dict['CoiStatement'] = pub_XML_element.find('.//CoiStatement')
    xml_dict['MedlineJournalInfo'] = pub_XML_element.find('.//MedlineJournalInfo')
    xml_dict['Journal'] = pub_XML_element.find('.//Journal')
    xml_dict['ArticleIdList'] = pub_XML_element.find('.//PubmedData/ArticleIdList')
    xml_dict['DataBankList'] = pub_XML_element.find('.//DataBankList')
    # xml_dict['PMID'] = pub_XML_element.find('.//PMID')
    xml_dict['OtherID'] = pub_XML_element.findall('.//OtherID')  # Note: Update the path based on actual XML structure
    xml_dict['Article'] = pub_XML_element.find('.//Article')

    return xml_dict

#TRANSFORM
def transform_XML(publications_dict, all_affiliations_list, all_authors_list,XML_file_name,AuthorIDCounter,AffiliationIDCounter):
    """
    Transforms detailed publication data from a dictionary into structured formats suitable for database storage or further processing.
    
    Parameters:
    - publications_dict (dict): A dictionary containing detailed publication data extracted from an XML file.
    - all_affiliations_list (list): A list that accumulates all affiliation data processed across different publications.
    - all_authors_list (list): A list that accumulates all author data processed across different publications.
    - XML_file_name (str): The name of the XML file being processed, used for tracking and logging.
    - AuthorIDCounter (int): A counter used to assign unique IDs to each author processed, ensuring data integrity.
    - AffiliationIDCounter (int): Similar to AuthorIDCounter, but for affiliations, ensuring each is uniquely identified.
    
    Returns:
    - A tuple containing:
        - A dictionary with transformed publication data.
        - Updated lists of all affiliations and authors with newly added entries from the current processing.
        - Updated counters for both authors and affiliations, reflecting the latest state after processing.
    """
    
    parsed_output = {}
    

    # Process GeneSymbol
    if publications_dict.get('GeneSymbol') is not None:
        parsed_output['GeneSymbol'] = publications_dict['GeneSymbol'].text

    # Process CoiStatement (Conflict of Interest Statement) if present.
    if publications_dict['CoiStatement'] is not None:
        parsed_output['CoiStatement'] = publications_dict['CoiStatement'].text

    # Process MedlineCitation for essential publication metadata, including the PubMed ID (PMID) and its version.
    if publications_dict['MedlineCitation'] is not None:
        #PMID_VERSION
        pmid_tag = publications_dict['MedlineCitation'].find('./PMID')
        parsed_output['PMID'] = pmid_tag.text
        parsed_output['PMIDVersion'] = pmid_tag.get('Version')
        #dateCompleted

        #dateRevised

        ##Processing for other abstracts, extracting language and type attributes, and concatenating all abstract texts.
        other_abstract = publications_dict['MedlineCitation'].find('./OtherAbstract')
        if other_abstract is not None:
            parsed_output['OtherAbtractLanguage'] = other_abstract.get('Language')
            parsed_output['OtherAbtractType'] = other_abstract.get('Type')
            other_abstract_texts = other_abstract.findall('./AbstractText')
            full_other_abstract = ''            
            for each_abstract in other_abstract_texts:
                #too complex to create categories since the labels are in original language
                #Append  all text together.
                this_abstract_text = each_abstract.text
                full_other_abstract = f'{full_other_abstract}. {this_abstract_text}' 
            parsed_output['OtherAbtract'] = full_other_abstract

    # Process Journal - Inside Article in XML but done here
    # Process Journal metadata, extracting ISSN, journal issue details, and publication date.
    if publications_dict['Journal'] is not None:
        issn = publications_dict['Journal'].find('./ISSN')
        if issn is not None:
            parsed_output['Journal_ISSN'] = issn.text.strip()
            parsed_output['Journal_ISSN_Type'] = issn.get('IssnType')

        journal_issue = publications_dict['Journal'].find('./JournalIssue')
        if journal_issue is not None:
            volume = journal_issue.find('./Volume')
            if volume is not None:
                parsed_output['Journal_JournalIssue_Volume'] = volume.text.strip()

            issue = journal_issue.find('./Issue')
            if issue is not None:
                parsed_output['Journal_JournalIssue_Issue'] = issue.text.strip()

            cited_medium = journal_issue.get('CitedMedium')
            if cited_medium:
                parsed_output['Journal_JournalIssue_CitedMedium'] = cited_medium.strip()

            pub_date = journal_issue.find('./PubDate')
            if pub_date is not None:
                year = pub_date.find('./Year')
                if year is not None:
                    parsed_output['Journal_JournalIssue_PubDate_Year'] = year.text.strip()

                month = pub_date.find('./Month')
                if month is not None:
                    parsed_output['Journal_JournalIssue_PubDate_Month'] = month.text.strip()

                day = pub_date.find('./Day')
                if day is not None:
                    parsed_output['Journal_JournalIssue_PubDate_Day'] = day.text.strip()

                season = pub_date.find('./Season')
                if season is not None:
                    parsed_output['Journal_JournalIssue_PubDate_Season'] = season.text.strip()

                medline_date = pub_date.find('./MedlineDate')
                if medline_date is not None:
                    parsed_output['Journal_JournalIssue_PubDate_MedlineDate'] = medline_date.text.strip()

        title = publications_dict['Journal'].find('./Title')
        if title is not None:
            parsed_output['Journal_Title'] = title.text.strip()

        iso_abbreviation = publications_dict['Journal'].find('./ISOAbbreviation')
        if iso_abbreviation is not None:
            parsed_output['Journal_ISOAbbreviation'] = iso_abbreviation.text.strip()


    #Transform Article
    if publications_dict['Article'] is not None:
        #initialize date if any value is 0 is unknown
        year = 0
        month = 0
        day = 0

        #article title
        #Extract the entire content inside the ArticleTitle tag, including nested tags
        article_title = ET.tostring(publications_dict['Article'].find('./ArticleTitle'), encoding='unicode', method='xml')
        
        # Remove the opening and closing ArticleTitle tags
        article_title = article_title.replace('<ArticleTitle>', '').replace('</ArticleTitle>', '').strip()

        # Clean up escape characters like \n, \t, etc.
        article_title = ' '.join(article_title.split())

        # If the content starts with [ and ends with ], or ends with ]., remove these characters
        # Remove the first opening bracket if it exists
        if article_title.startswith('['):
            article_title = article_title[1:].strip()

        # Check the ending and remove the closing bracket or the closing bracket with a period
        if article_title.endswith(']'):
            article_title = article_title[:-1].strip()
        elif article_title.endswith('].'):
            article_title = article_title[:-2].strip()

        parsed_output['ArticleTitle'] = article_title

        #Language
        language = publications_dict['Article'].find('./Language')
        if language is not None:
            parsed_output['Language'] = language.text
        
        #article publication mode
        parsed_output['Article_PubModel'] = publications_dict['Article'].get('PubModel')
        
        #article date
        article_date = publications_dict['Article'].find('./ArticleDate')
        if article_date is not None:
            year = article_date.find('./Year').text
            month = article_date.find('./Month').text
            day = article_date.find('./Day').text
        if year != 0:
            parsed_output['ArticleDate'] = f'{year}-{month}-{day}'
        
        #MedlinePgn
        MedlinePgn = publications_dict['Article'].find('./Pagination/MedlinePgn')
        if MedlinePgn is not None:
            parsed_output['Article_MedlinePgn'] = MedlinePgn.text
        
        #ELocationID
        ELocationID = publications_dict['Article'].find('./ELocationID')
        if ELocationID is not None:
            parsed_output['Article_ELocationID'] = ELocationID.text
        
        #Abstract
        Abstract = publications_dict['Article'].find('./Abstract')
        if Abstract is not None:
            all_abstract_texts = Abstract.findall('./AbstractText')
            abstract_text = ''
            for each_abstract in all_abstract_texts:
                #TODO STORE IN THE PROPER LABEL WOULD NEED TO CREATE THE PROPER DICTIONARY FOR THIS WITH ALL FILES
                label = each_abstract.get('Label')
                if label:
                    # pdb.set_trace()
                    candidate_labels = ['Introduction','Purpose','Conclusion','Results','Methods','UNLABELLED']
                    key = classify(label, candidate_labels = candidate_labels)
                    key = key['labels'][0]
                    if key == 'UNLABELLED':
                        key = 'Abstract'
                    else:
                        key = f"Abstract_{key}" 
                else:
                    key = 'Abstract'
                parsed_output[key] = each_abstract.text
                

        #Authors
        AuthorList = publications_dict['Article'].find('./AuthorList')
        if AuthorList is not None:
            #List to store author IDs for this publication
            this_publication_author_list = []
            author_list_complete = AuthorList.get('CompleteYN')
            this_publication_num_authors = len(AuthorList)
            this_author_order = 0
            # Extract authors, affiliations and details
            for author in AuthorList:
                AuthorIDCounter += 1  # Increment author ID
                this_author_order += 1 #Increment author Order
                isFirstAu = (this_author_order == 1) #Tag First Author
                isLastAu = (this_author_order == this_publication_num_authors) #Tag Last Author
                try:
                    last_name = author.find('LastName').text
                except AttributeError:
                    last_name = None
                try:
                    fore_name = author.find('ForeName').text
                except AttributeError:
                    fore_name = None
                try:
                    initials = author.find('Initials').text
                except AttributeError:
                    initials = None

                #If we find a name
                if last_name is not None or fore_name is not None or initials is not None:
                    affiliations = author.findall('./AffiliationInfo/Affiliation')
                    if len(affiliations) > 0 :
                        #List to store affiliations for this author in this publication
                        this_author_affiliation_list = []
                        for each_affiliation in affiliations:
                            # pdb.set_trace()
                            AffiliationIDCounter += 1
                            affiliation = each_affiliation.text
                            #Store affiliations details in all_affiliations list
                            all_affiliations_list.append({'Affiliation_ID':AffiliationIDCounter,'affiliation':affiliation})
                            #add id to author list
                            this_author_affiliation_list.append(AffiliationIDCounter)
                    else:
                        this_author_affiliation_list = float("nan")
                    
                    # Store author details in all_author list
                    all_authors_list.append({'PMID': parsed_output['PMID'] ,'Author_ID': AuthorIDCounter, 'LastName': last_name, 'ForeName': fore_name, 'Initials': initials, 'AffiliationList':this_author_affiliation_list, 'AUOrder':this_author_order, 'isFirstAu':isFirstAu, 'isLastAu':isLastAu})
                    # Store author details and their ID in the authors_data list
                    this_publication_author_list.append(AuthorIDCounter)
                #Store values in publication dictionary
                parsed_output['CompleteYN'] = author_list_complete
                parsed_output['AuthorList'] = this_publication_author_list
                parsed_output['Num_Authors'] = this_publication_num_authors
    
    #Transform PubmedData
    if publications_dict['PubmedData'] is not None:
        #History
        history = publications_dict['PubmedData'].findall('./History/PubMedPubDate')
        if history is not None and len(history) > 0:
            # pdb.set_trace()
            for each_date in history:
                key = f'History_{each_date.get("PubStatus")}'
                year = each_date.find('./Year').text
                month = each_date.find('./Month').text
                day = each_date.find('./Day').text
                if year != 0:
                    parsed_output[key] = f'{year}-{month}-{day}'
                
    
    # Transform MedlineJournalInfo
    if publications_dict['MedlineJournalInfo'] is not None:

        # Extracting each piece of information and adding it to parsed_output with a specific key
        country = publications_dict['MedlineJournalInfo'].find('./Country')
        if country is not None:
            parsed_output['MedlineJournalInfo_Country'] = country.text.strip()

        medline_ta = publications_dict['MedlineJournalInfo'].find('./MedlineTA')
        if medline_ta is not None:
            if medline_ta.text is not None:
                parsed_output['MedlineJournalInfo_MedlineTA'] = medline_ta.text.strip()

        nlm_unique_id = publications_dict['MedlineJournalInfo'].find('./NlmUniqueID')
        if nlm_unique_id is not None:
            if nlm_unique_id.text is not None:
                parsed_output['MedlineJournalInfo_NlmUniqueID'] = nlm_unique_id.text.strip()

        issn_linking = publications_dict['MedlineJournalInfo'].find('./ISSNLinking')
        if issn_linking is not None:
            if issn_linking.text is not None:
                parsed_output['MedlineJournalInfo_ISSNLinking'] = issn_linking.text.strip()

    # Transform ArticleIdList
    if publications_dict['ArticleIdList'] is not None:
        # pdb.set_trace()
        for article_id in publications_dict['ArticleIdList'].findall('./ArticleId'):
            id_type = article_id.get('IdType')
            if id_type:
                if article_id.text is not None:
                    key = f'ArticleId_{id_type}'
                    parsed_output[key] = article_id.text.strip()

    # Transform DataBankList
    # if publications_dict['DataBankList'] is not None :
    #     databanklist_completeyn = publications_dict['DataBankList'].get('CompleteYN')
    #     databanks = publications_dict['DataBankList'].findall('./DataBank')
    #     for each_databank in databanks:
    #         databank_name = each_databank.find('./DataBankName').text.replace('.','_')
    #         key = f'DataBank_{databank_name}'
    #         accession_numbers = each_databank.findall('./AccessionNumberList/AccessionNumber')
    #         parsed_output[key] = [num.text for num in accession_numbers]
    # else:
    #     databanklist_completeyn = 'N'

    # parsed_output['DataBankListCompleteYN'] = databanklist_completeyn
    

    # Transform PMID
    # if publications_dict['PMID'] is not None:
    #     parsed_output['PMID'] = publications_dict['PMID'].text

    # Transform OtherID (Assuming placeholder until actual structure is known)
    # if publications_dict['OtherID'] is not None:
    #     other_id_list = [item.text for item in publications_dict['OtherID']]
    #     parsed_output['OtherID'] = other_id_list
    
    if publications_dict['date_completed'] is not None:
        pass

    # pdb.set_trace()
    if len(publications_dict['reference_list']) > 0:
        reference_list = []
        for each_reference in publications_dict['reference_list']:
            reference_list.append([reference.text for reference in each_reference.findall('./*')])
        #Flatten list
        reference_list = [item for sublist in reference_list for item in sublist]
        # pdb.set_trace()
        parsed_output['ReferenceList'] = reference_list

    #Add the file Name to the record
    parsed_output['XML_file_name'] = XML_file_name
    
    return parsed_output, all_affiliations_list, all_authors_list, AuthorIDCounter, AffiliationIDCounter