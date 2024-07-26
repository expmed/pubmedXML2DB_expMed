# GeneGinie Project

[Project Overview and architecture information](https://drive.google.com/file/d/1LLVk6F5CWMrpBzZxysWPCxlLPgrhoW2i/view?usp=sharing) 

# PubMed XML to Database Converter

This repository is part of the GeneGinie Project. The code is designed to parse XML files from PubMed and store the extracted information into a SQL database.
Parser is not complete but adds more information than the parsers available by default in biopython.

## Features

- Parses XML files to extract publication, author, and affiliation data.
- Transforms data for SQL compatibility.
- Dynamically creates tables for publications, authors, and affiliations.
- Utilizes global counters to assign unique IDs to authors and affiliations.
    - Affiliations have been enhanced and parsed where possible.
    - Authors are not disambiguated.
- Generates a single DB with three tables. Authors, Papers and Affiliations

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Python 3.6 or later.
- pandas
- SQLAlchemy

## Installation

Clone this repository to your local machine:

Navigate to the cloned directory:

`cd pubmed-xml-to-db`

Install the required packages:

`pip install -r requirements.txt`

## Usage

To start parsing XML files and storing data into the database, run:

Replace `[path_to_pubmed_XML_files]` with the path to your directory containing PubMed XML files.

For example:

`python pubmedXML2DB.py '/path/to/XML_files'`

## Affiliation Parsing with `parseAffiliations.py`

The `parseAffiliations.py` script is designed to parse and process affiliation information from PubMed XML files. This script plays a crucial role in extracting detailed information from affiliation text, such as the department, institution, location, country, and contact information. It works by fetching raw affiliation data in batches from the `affiliations` table, parsing each affiliation, and then storing the parsed data in a structured format into the `affiliations_parsed` table.

### Key Features

- **Batch Processing**: Handles large volumes of affiliation data by processing them in manageable batches.
- **Data Enrichment**: Parses the raw affiliation text to extract structured information, including department names, institutions, geographic locations, and contact details.
- **Error Handling**: Gracefully handles and logs errors for affiliations that cannot be parsed.

### Usage

This script is to be executed after the generation of the database. Requires the Affiliations table to exist. It automatically processes all available data and populates the `affiliations_parsed` table with the enriched affiliation information. The script makes use of https://github.com/titipata/affiliation_parser

For developers looking to understand the parsing logic or extend the parsing capabilities, the script provides a clear template for how affiliation data can be extracted, transformed, and stored efficiently.

### External Dependencies

This script relies on `affiliation_parser`, a library not available on PyPI, which is installed directly from its GitHub repository as specified in `requirements.txt`. Ensure you have Git installed and accessible in your path to enable pip to clone and install this dependency.


## Contributing

Contributions to the PubMed XML to Database Converter are welcome. If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

Don't forget to give the project a star! Thanks again!

## Citing

Please cite accordingly if used. 

## Contact

Alberto Gonzalez – [LinkedIN](https://www.linkedin.com/in/agonzamart/)

Project Link: [https://github.com/yourusername/pubmed-xml-to-db](https://github.com/whatwehaveunlearned/pubmed-xml-to-db)
