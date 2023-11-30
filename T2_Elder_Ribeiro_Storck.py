"""
python_sparQl.py
Autor: Elder Ribeiro Storck
Data: 30 de novembro de 2023
Descrição: Este programa demonstra o uso de SPARQL em Python para consultar dados em um banco de dados RDF.
"""

%%capture
!pip3 install sparql_dataframe
import sparql_dataframe
from datetime import datetime
import pandas as pd


# Função responsável pela obtenção da consulta no formato de DataFrame do Pandas.
def get_df() -> pd.DataFrame:
    return sparql_dataframe.get("http://dbpedia.org/sparql",
        """
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dbo: <http://dbpedia.org/ontology/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX dboW: <http://dbpedia.org/ontology/Work/>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>

            SELECT DISTINCT ?db_titulo ?db_ator ?db_produtora ?db_diretor ?db_dataNascAtor
            WHERE { ?sujeito a  dbo:TelevisionShow ;
            rdfs:label ?db_titulo;
            dboW:runtime ?tempo;
         		dbo:network ?produtora;
            dbo:director ?diretor;
            dbo:starring ?estrelas

    		    OPTIONAL {?sujeito dbo:budget ?orcamento}

  	    	  ?produtora rdfs:label ?db_produtora.
  	    	  ?diretor   rdfs:label ?db_diretor.
  	    	  ?estrelas rdfs:label ?db_ator.
  	    	  ?estrelas dbo:birthDate ?db_dataNascAtor.

        		BIND (xsd:float(?tempo) as ?db_duracao)

    	    	FILTER (lang(?db_ator) = 'en')
  	      	FILTER (lang(?db_titulo) = 'en')
  	      	FILTER (lang(?db_produtora) = 'en')
  	      	FILTER (lang(?db_diretor) = 'en')
            }
        """
      )


# Tipos personalizados para os dados em questão
Info_artist = dict[str, str|list[str]]
Info_album = dict[str, str|list[int]|list[str]]
Info_group = dict[str, list[str]]

#Função retorna Nome e data de nascimento de um Ator
#def get_artists_info(df: pd.DataFrame) -> dict[Info_artist]:
#  return dict([('db_ator', df.db_ator.drop_duplicates().to_list()),('db_dataNascAtor', df.db_dataNascAtor)])

def get_artists_info(df: pd.DataFrame) -> dict:
    artists_info = {'db_ator': df['db_ator'].drop_duplicates().tolist(), 'db_dataNascAtor': df['db_dataNascAtor'].tolist()}
    return artists_info

def get_list_actores(df: pd.DataFrame) -> list([str, str]):
  return list([df.db_ator.drop_duplicates().to_list(), df.db_dataNascAtor])
  



def single_artist_info(artists_info: dict, artist_name: str) -> Info_artist|None:
    try:
        return artists_info.get(artist_name)#[artist_name]
    except:
        print('Artist not found!')
        return None

# %verifica se um determinado artista nasceu antes de um Ano
def aniversario_eh_antes_da_data(artists_info: dict[Info_artist], data: str) -> int:
  # Converter as strings para objetos datetime
  #single_actor : dict[Info_artist] = artists_info["Alan Bates"]
  #print(single_actor)
  data1 = datetime.strptime(data, "%Y-%m-%d")
  data2 = datetime.strptime(artists_info.db_dataNascAtor[1], "%Y-%m-%d")
  # Comparar as datas
  if data2 < data1:
    return 1
  return 0



df: pd.DataFrame = get_df()
#atores_list: list([str, str]) = atores_info_list(df)
artists_info = get_artists_info(df)
print(aniversario_eh_antes_da_data(df, "1958-11-22"))
single_artist_info(df,"Jamie Lee Curtis")

print(artists_info)
#print(atores_list)

