# PubMed XML to Database Converter

This project is designed to parse XML files from PubMed and store the extracted information into a SQL database. It handles publication, author, and affiliation data, transforming and storing each element efficiently.

## Features

- Parses XML files to extract publication, author, and affiliation data.
- Transforms data for SQL compatibility.
- Dynamically creates tables for publications, authors, and affiliations.
- Utilizes global counters to assign unique IDs to authors and affiliations.

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

pip install -r requirements.txt

## Usage

To start parsing XML files and storing data into the database, run:

Replace `[path_to_pubmed_XML_files]` with the path to your directory containing PubMed XML files.

For example:

python pubmedXML2DB.py '/path/to/XML_files'

## Contributing

Contributions to the PubMed XML to Database Converter are welcome. If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

Don't forget to give the project a star! Thanks again!

## License

Distributed under the MIT License. See `LICENSE` for more information.
Please cite accordingly if used. 

## Contact

Alberto Gonzalez – [LinkedIN](https://www.linkedin.com/in/agonzamart/)

Project Link: [https://github.com/yourusername/pubmed-xml-to-db](https://github.com/whatwehaveunlearned/pubmed-xml-to-db)