# models.py
from sqlalchemy import create_engine, Column, Integer, Text, Float, DateTime, ForeignKey, text, MetaData, select, func
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.exc import OperationalError
import pandas as pd
import re
import pdb


# Base class for declarative class definitions.
Base = declarative_base()
db_path_debug = 'PubMed_june_2024.db' #Database path DEBUG

#Database path
db_path = db_path_debug
# Create SQLite engine

# Create an SQLAlchemy engine that provides connectivity to the SQLite database.
engine = create_engine(f'sqlite:///{db_path}')

# Create a sessionmaker object that will create new Session objects bound to the engine.
Session = sessionmaker(bind=engine)

def create_dynamic_model(df, class_name, table_name, primary_key_column, index_column = None, relationships=None):
    """
    Dynamically creates a SQLAlchemy model based on the provided DataFrame structure.
    
    Parameters:
    - df: The DataFrame whose schema will define the SQLAlchemy model's columns.
    - class_name: The name of the dynamically created model class.
    - table_name: The name of the table in the database to map this model to.
    - primary_key_column: The column name in the DataFrame to be set as the primary key in the model.
    - index_column: Optional. A column in the DataFrame to be indexed in the model.
    - relationships: Optional. A list of relationship definitions for the model.
    
    Returns:
    - A dynamically created SQLAlchemy model class.
    """

    def repr_function(self):
        """
        Defines a __repr__ method for the dynamically created model, 
        providing a string representation that includes all column names and their values.
        """

        attributes = ', '.join(f'{col_name}={getattr(self, col_name)}' for col_name in df.columns)
        return f"<{class_name}({attributes})>"
        
    columns = {
        '__tablename__': table_name, # Sets the table name in the database.
        '__repr__': repr_function # Sets the __repr__ method for the model.
    }
    
    # Dynamically add columns to the model based on the DataFrame's schema.
    for col_name, dtype in zip(df.columns, df.dtypes):
        if col_name == "Affiliation_ID": # Special handling for affiliation IDs as text to support merging in custom triggers.
            columns[col_name] = Column(Text, primary_key=(col_name == primary_key_column))
        elif "int" in str(dtype): # Integer columns
            columns[col_name] = Column(Integer, primary_key=(col_name == primary_key_column))
        elif "float" in str(dtype): # Float columns
            columns[col_name] = Column(Float, primary_key=(col_name == primary_key_column))
        elif "datetime" in str(dtype): # Datetime columns
            columns[col_name] = Column(DateTime, primary_key=(col_name == primary_key_column))
        else: # Default to text columns for other types
            columns[col_name] = Column(Text, primary_key=(col_name == primary_key_column))


    # If an index column is specified, add it to the columns dictionary
    if index_column and index_column in df.columns and not index_column == primary_key_column:
        columns[index_column].index = True

    # Add relationships to the model if specified.
    if relationships:
        for rel in relationships:
            columns[rel['name']] = relationship(
                rel['target'], 
                secondary=rel.get('secondary', None),
                back_populates=rel.get('back_populates', None),
                uselist=rel.get('uselist', True),
                foreign_keys=[ForeignKey(rel['foreign_key'])] if 'foreign_key' in rel else None
            )
    # Create the model class dynamically using the type function.
    DynamicModel = type(class_name, (Base,), columns)
    
    return DynamicModel

def create_model(class_name, table_name, columns_dict, primary_key_column='id'):
    """
    Creates a static SQLAlchemy model with predefined columns.
    
    This is a more traditional approach compared to create_dynamic_model, where the schema is known ahead of time.
    
    Parameters:
    - class_name: Name of the class to be created.
    - table_name: Name of the table in the database.
    - columns_dict: A dictionary with column names as keys and their data types as values.
    - primary_key_column: The column to be set as the primary key.
    
    Returns:
    - A SQLAlchemy model class with the specified schema.
    """
    attributes = {
        '__tablename__': table_name,
        '__repr__': lambda self: f"<{class_name}({', '.join([f'{k}={getattr(self, k)}' for k in columns_dict.keys()])})>"
    }

    for column_name, column_type in columns_dict.items():
        is_primary_key = column_name == primary_key_column
        if column_type == 'int':
            attributes[column_name] = Column(Integer, primary_key=is_primary_key)
        elif column_type == 'float':
            attributes[column_name] = Column(Float, primary_key=is_primary_key)
        elif column_type == 'datetime':
            attributes[column_name] = Column(DateTime, primary_key=is_primary_key)
        elif column_type == 'text':
            attributes[column_name] = Column(Text, primary_key=is_primary_key)
        else:
            raise ValueError(f"Unsupported column type: {column_type}")

    return type(class_name, (Base,), attributes)

def create_table(className, tableName, tableColumns):
    AffiliationParsed = create_model(className, tableName, tableColumns)
    #Create tables
    Base.metadata.create_all(engine)


def create_dynamic_tables(publications_df,authors_df,affiliations_df):
    
    """
    Creates database tables dynamically based on DataFrames for publications, authors, and affiliations.
    
    This function first creates models dynamically using the create_dynamic_model function, then
    initializes these tables in the database.
    
    Parameters:
    - publications_df: DataFrame containing publication data.
    - authors_df: DataFrame containing author data.
    - affiliations_df: DataFrame containing affiliation data.
    """

    
    # Create Authors and Affiliations models
    Affiliation = create_dynamic_model(affiliations_df, 'Affiliation', 'affiliations','affiliation')
    Author = create_dynamic_model(authors_df, 'Author', 'authors','Author_ID','PMID')    
    # Define relationships for Publications model
    # publication_relationships = [
    #     {
    #         'name': 'authors',
    #         'target': 'Author',
    #         'back_populates': 'publications',
    #         'foreign_key': 'authors.id',
    #         'uselist': True  # One to Many relationship
    #     },
    #     {
    #         'name': 'reference_list',
    #         'target': 'Publication',  # Self-referential relationship
    #         'back_populates': 'referenced_by',
    #         'foreign_key': 'publications.id',
    #         'uselist': True  # One to Many relationship
    #     }
    # ]
    
    # Create Publications model
    # Publication = create_dynamic_model(publications_df, 'Publication', 'publications', 'PMID', publication_relationships)
    Publication = create_dynamic_model(publications_df, 'Publication', 'publications', 'PMID')
    
    # Define back_populates for other models
    # Author.publications = relationship('Publication', back_populates='authors')
    # Publication.referenced_by = relationship('Publication', back_populates='reference_list')
    
    #Create tables
    Base.metadata.create_all(engine)
    
    #Get the columns for updating the record in the trigger (SQLite is limited in what you can do in triggers, no external functions)
    table = Base.metadata.tables['publications']
    column_names = [column.name for column in table.columns]
    
    create_database_triggers()


def insert_data(engine, table_name, data):
    """
    Inserts data from a pandas DataFrame into a specified table in the database. If a column is missing in the table,
    it dynamically adds the column and retries the insertion.
    
    Parameters:
    - engine: SQLAlchemy engine object connected to the database.
    - table_name: Name of the table where data needs to be inserted.
    - data: pandas DataFrame containing the data to be inserted.
    """

    
    # Standardize column names by replacing hyphens with underscores to ensure SQL compatibility.
    data.rename(columns=lambda x: x.replace('-', '_'), inplace=True)
    
    try:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
    except OperationalError as e:
        # Handle the case where a column in the DataFrame does not exist in the table.
        error_message = str(e)
        match = re.search(r'no column named ([\w-]+)', error_message)
        if match:
            # If the error was due to a missing column, add it to the table.
            missing_column = match.group(1)
            add_missing_column(engine, table_name, missing_column)
            # Retry inserting the data after the missing column has been added.
            insert_data(engine, table_name, data)
        else:
            # If the error is something else, re-raise it
            raise

def add_missing_column(engine, table_name, missing_column):
    """
    Dynamically adds a missing column to a specified table in the database.
    
    Parameters:
    - engine: SQLAlchemy engine object connected to the database.
    - table_name: Name of the table to which the column should be added.
    - missing_column: The name of the column to be added.
    """

    # Open a connection to the database and execute an ALTER TABLE command to add the missing column.
    # The new column is added with a data type of TEXT by default.
    with engine.connect() as conn:
        alter_statement = f'ALTER TABLE {table_name} ADD COLUMN {missing_column} TEXT'
        conn.execute(text(alter_statement))

def create_database_triggers():
    """
    Sets up database triggers to prevent duplicates and manage data consistency.
    
    This function defines and executes SQL statements to create triggers for:
    - Preventing duplicate PMID entries in the 'publications' table.
    - Preventing duplicate Author_ID entries in the 'authors' table.
    - Merging Affiliation_IDs for the same affiliation in the 'affiliations' table.
    """

    #Define the trigger SQL so that on insertions in publications we merge records when there is a duplicate (EXECUTE AFTER TABLES ARE CREATED)
    trigger_dup_publications = """
    CREATE TRIGGER prevent_duplicate_pmids
    BEFORE INSERT ON publications
    FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM publications WHERE PMID = NEW.PMID)
    BEGIN
        SELECT RAISE(IGNORE);
    END;
    """

    trigger_dup_authors = """
    CREATE TRIGGER prevent_duplicate_authorids
    BEFORE INSERT ON authors
    FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM authors WHERE Author_ID = NEW.Author_ID)
    BEGIN
        SELECT RAISE(IGNORE);
    END;
    """

    # trigger_dup_affiliations = """
    # CREATE TRIGGER prevent_duplicate_affiliationids
    # BEFORE INSERT ON affiliations
    # FOR EACH ROW
    # WHEN EXISTS (SELECT 1 FROM affiliations WHERE Affiliation_ID = NEW.Affiliation_ID)
    # BEGIN
    #     SELECT RAISE(IGNORE);
    # END;
    # """

    trigger_merge_affiliations_ids = """
    CREATE TRIGGER update_affiliation_ids
    BEFORE INSERT ON affiliations
    FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM affiliations WHERE affiliation = NEW.affiliation)
    BEGIN
        UPDATE affiliations
        SET Affiliation_ID = (SELECT GROUP_CONCAT(Affiliation_ID, ',') || ',' || NEW.Affiliation_ID
                            FROM affiliations
                            WHERE affiliation = NEW.affiliation)
        WHERE affiliation = NEW.affiliation;
        SELECT RAISE(IGNORE);
    END;

    """

    # Execute the trigger SQL
    with engine.connect() as conn:
        conn.execute(text(trigger_dup_publications))
        conn.execute(text(trigger_dup_authors))
        # conn.execute(text(trigger_dup_affiliations))
        conn.execute(text(trigger_merge_affiliations_ids))


def store_in_SQL(tableName,dfToStore):
    """
    Stores a DataFrame in a specified table in the SQL database. Handles dynamic column addition if necessary.
    
    Parameters:
    - tableName: The name of the table where the DataFrame should be stored.
    - dfToStore: The pandas DataFrame containing data to be inserted into the table.
    """

    insert_data(engine,tableName, dfToStore)


#Transform df to store in SQL
def transform_pubications_for_SQL(df):
    """
    Transforms a publications DataFrame for SQL storage, converting lists to comma-separated strings.

    Parameters:
    - df: The DataFrame containing publications data.

    Returns:
    - The transformed DataFrame with list columns converted to comma-separated strings.
    """

    df['AuthorList'] = df['AuthorList'].apply(list_to_SQL)
    df['ReferenceList'] = df['ReferenceList'].apply(list_to_SQL)
    return df

def transform_authors_for_SQL(df):
    """
    Transforms an authors DataFrame for SQL storage, specifically handling the AffiliationList column.

    Parameters:
    - df: The DataFrame containing authors data.

    Returns:
    - The transformed DataFrame with the AffiliationList column converted to comma-separated strings.
    """
    df['AffiliationList'] = df['AffiliationList'].apply(list_to_SQL)
    return df

def list_to_SQL(x):
    """
    Converts a list to a comma-separated string, or returns the input if it is not a list.

    Parameters:
    - x: The input value, possibly a list.

    Returns:
    - A comma-separated string if the input is a list, otherwise the original input.
    """
    if isinstance(x, list):
        return ', '.join(map(str, x))
    else:
        return x  # keep the value as is, including NaN

#READ
def get(tableName="publications", by=['PMID'], filterLists=[[30103854, 36548]]):
    """
    Queries a table for rows matching specified conditions and returns the results as a DataFrame.

    Parameters:
    - tableName: The name of the table to query.
    - by: A list of column names to filter by.
    - filterLists: A list of lists, where each sublist contains values to filter the corresponding column in 'by'.

    Returns:
    - A pandas DataFrame containing the query results.
    """

    # Connect to the database
    connection = engine.connect()
    
    # Get the metadata and the table
    metadata = MetaData()
    
    # Reflect the table schema
    metadata.reflect(bind=engine)
    
    # Get the table from the reflected metadata
    target_table = metadata.tables.get(tableName)
    
    # Check if 'by' and 'filterLists' have the same length
    if len(by) != len(filterLists):
        raise ValueError("Length of 'by' and 'filterLists' must be the same")
    
    # Build a select statement with multiple WHERE clauses
    select_statement = select(target_table)
    for column_name, ids in zip(by, filterLists):
        select_statement = select_statement.where(getattr(target_table.c, column_name).in_(ids))
    
    # Execute the select statement and fetch the result into a Pandas DataFrame
    result_df = pd.read_sql(select_statement, connection)
    
    # Prepare a list to collect missing ID information
    missing_info = []

    # Check and collect the IDs that were not found for each column
    for column_name, ids in zip(by, filterLists):
        missing_ids = set(ids) - set(result_df[column_name])
        if missing_ids:
            missing_info.append(f"{column_name}: {missing_ids}")

    connection.close()

    return result_df


def fetch_records_in_batches(table_name, batch_size=1000):
    """
    Fetches records from a specified table in batches, yielding each batch as a pandas DataFrame.

    This is useful for processing large tables without loading the entire table into memory.

    Parameters:
    - table_name: The name of the table from which to fetch records.
    - batch_size: The number of records to fetch in each batch.

    Returns:
    - DataFrame containing a batch of records from the table.
    """

    # Reflect the database schema into MetaData object.
    metadata = MetaData()
    metadata.reflect(bind=engine)
    # Retrieve the target table definition from the metadata.
    target_table = metadata.tables.get(table_name)
    # Initialize the offset for pagination.
    offset = 0

    # Loop indefinitely to fetch records in batches.
    while True:
        # Build a query to fetch a batch of records, applying limit and offset for pagination.
        batch_query = select(target_table).limit(batch_size).offset(offset)
        batch_df = pd.read_sql(batch_query, engine)

        # If no records were fetched (i.e., DataFrame is empty), stop the iteration.
        if batch_df.empty:
            break

        yield batch_df  # Yield the current batch of records to the caller.

        # If the fetched batch is smaller than the requested batch_size, it's the last batch.
        if len(batch_df) < batch_size:
            break
        
        # Increment the offset for the next batch.
        offset += batch_size

