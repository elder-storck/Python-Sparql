"""
python_sparQl.py
Autor: Elder Ribeiro Storck
Data: 30 de novembro de 2023
Descrição: Este programa demonstra o uso de SPARQL em Python para consultar dados em um banco de dados RDF.
"""

%%capture
!pip3 install sparql_dataframe
import sparql_dataframe
from pprint import pprint
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
            LIMIT 10
        """
    )


Info_atores = dict[str, str|list[str]]
Info_album = dict[str, str|list[int]|list[str]]
Info_group = dict[str, list[str]]



def get_info_atores(df: pd.DataFrame) -> dict[Info_atores]:
    mylist = list(zip(df.db_ator.drop_duplicates().to_list(), df.db_dataNascAtor.to_list()))  #lista com tuplas (nome, data_de_nascimento)
    mydict = {tup[0]: tup[1] for tup in mylist}                                               #dicionário com as tuplas
    return mydict


def get_info_ator(atores_info: dict[Info_atores], nome_ator: str) -> Info_atores|None:
    if nome_ator in atores_info:
      return atores_info[nome_ator]
    return None


def ator_nasceu_antes_da_data(atores_info: dict[Info_atores], nome_ator: str, data_comparada: str) -> int:
  if nome_ator in atores_info:
      nasc_ator      = datetime.strptime(atores_info[nome_ator], "%Y-%m-%d")
      data_comparada = datetime.strptime(data_comparada,         "%Y-%m-%d")
      if(nasc_ator < data_comparada): return 1
  return 0


df: pd.DataFrame = get_df()
atores_info: dict[Info_atores] = get_info_atores(df)
print(ator_nasceu_antes_da_data(atores_info,'Jamie Lee Curtis', "1958-11-23"))
teste = get_info_ator(atores_info,'Jamie Lee Curtis')
#pprint(teste)
pprint(atores_info)
