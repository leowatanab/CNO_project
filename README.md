# CNO Database

The main propose of this project is build a dataflow to download the .csv files from web url 'https://arquivos.receitafederal.gov.br/dados/cno/cno.zip' and load them to a cloud database. Finally, we can use this data in our applications.

*Keywords: Database, CNO, Motherduck, DuckDB, Pandas, Streamlit*

## main.py

Basically, the main.py is the script that realizes the Extract, Transform and Load (ETL) flow. 
Here, we are using io and requests to access the .zip file from web url; Pandas to 
