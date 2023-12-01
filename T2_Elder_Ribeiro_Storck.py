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
            LIMIT 1000
        """
    )


#--------------------------------Criando as Estruturas de Dados--------------------------------#


# Retorna dicionário com as tuplas [nome, Data de Nascimento]
def get_info_atores(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_ator.drop_duplicates().to_list(), df.db_dataNascAtor.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict

# Retorna Data de nascimento do ator
def get_info_ator(atores_info: dict[str, str], nome_ator: str) -> Info_atores|None:
    if nome_ator in atores_info:
      return atores_info[nome_ator]
    return None

# Retorna dicionário com as tuplas [nome do ator, nome da produtora que trabalha]
def producer_cast(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_ator.to_list(), df.db_produtora.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict

# Retorna dicionário com as tuplas [nome do ator, nome do programa de televisão que trabalha]
def televisionShow_Cast(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_ator.to_list(), df.db_titulo.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict

# Retorna dicionário com as tuplas [nome do diretor, nome da produtora que trabalha]
def producer_cast_diretores(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_diretor.to_list(), df.db_produtora.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict


#-------------------------------- Criando as Funções --------------------------------#


# Verifica quais artistas distintos já trabalharam na mesma Produtora
def coworker_Producer(produtora: dict[str, str], ator1: str, ator2: str) -> int:
  if (ator1 in produtora) and (ator2 in produtora):
    if (produtora[ator1] == produtora[ator2]):
      return 1
  return 0

# Verifica quais artistas distintos já trabalharam no mesmo Programa de Tv
def coworker_televisionShow(televisionShow: dict[str, str], ator1: str, ator2: str) -> int:
  if (ator1 in televisionShow) and (ator2 in televisionShow):
    if (televisionShow[ator1] == televisionShow[ator2]):
      return 1
  return 0 

# Verifica quais diretores distintos trabalharam na mesma Produtora
def coworker_dir_Prod(produtora: dict[str, str], diretor1: str, diretor2: str) -> int:
  if (diretor1 in produtora) and (diretor2 in produtora):
    if (produtora[diretor1] == produtora[diretor2]):
      return 1
  return 0 

# verifica se um determinado artista nasceu antes de um Ano
def ator_nasceu_antes_da_data(atores_info: dict[str, str], nome_ator: str, data_comparada: str) -> int:
  if nome_ator in atores_info:
      nasc_ator      = datetime.strptime(atores_info[nome_ator], "%Y-%m-%d")
      data_comparada = datetime.strptime(data_comparada,         "%Y-%m-%d")
      if(nasc_ator < data_comparada): return 1
  return 0

# verifica quantidade de artitas trabalham em uma produtora
def amountActorbyNetwork(produtora: dict[str, str], produtora_name: str) -> int :
  i : int = 0
  produtora_values_list = list(produtora.values())
  for prod in produtora_values_list:
    if prod == produtora_name:
      i=i+1
  return i

# verifica qual produtora tem mais artistas 
def maiorNubAtoresPorProdutora(produtora: dict[str, str]) -> str:
  produtora_values_list = list(produtora.values())    #criando lista com nomes das produtoras
  produtoras_distintas_list = []

  for elemento in produtora_values_list:
    if elemento not in produtoras_distintas_list:
        produtoras_distintas_list.append(elemento)
  quantidade_atores = 0
  produtora : str = "nenhum"
  for element in produtoras_distintas_list:
    quantidade2 = amountActorbyNetwork(produtoras_and_atores_list, element)
    if quantidade2 > quantidade_atores:
      quantidade_atores = quantidade2
      produtora = element
  pprint(produtora)
  return quantidade_atores

# verifica qual produtora tem mais artistas 
def maiorNubAtoresPorProdutora(produtora: dict[str, str]) -> int:
  produtora_values_list  = list(produtora.values())                         #criando lista com somente os nomes das produtoras
  quantidade_atores: int = 0
  produtora :        str = "nenhum"

  for element in produtora_values_list:                                     #enquanto houver elementos na lista
    amount_temp = amountActorbyNetwork(produtoras_and_atores_list, element) #verifica a quantidade de atores de uma produtora
    if amount_temp > quantidade_atores:                                     #se a quantidade de atores da produtora for maior o valor é guardado
      quantidade_atores = amount_temp                                       #maior número
      produtora = element                                                   #nome da produtora com maior número (valor não retornado)
  return quantidade_atores

# verifica qual produtora tem Menos artistas 
def menorNubAtoresPorProdutora(produtora: dict[str, str]) -> int:
  produtora_values_list  = list(produtora.values())                         #criando lista com somente os nomes das produtoras
  quantidade_atores: int = 10000000
  produtora :        str = "nenhum"

  for element in produtora_values_list:                                     #enquanto houver elementos na lista
    amount_temp = amountActorbyNetwork(produtoras_and_atores_list, element) #verifica a quantidade de atores de uma produtora
    if amount_temp < quantidade_atores:                                     #se a quantidade de atores da produtora for maior o valor é guardado
      quantidade_atores = amount_temp                                       #maior número
      produtora = element                                                   #nome da produtora com maior número (valor não retornado)
  #print(produtora)
  return quantidade_atores
