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


# Retorna dicionário com as tuplas [nome_do_ator, Data de Nascimento]
def get_info_actors(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_ator.drop_duplicates().to_list(), df.db_dataNascAtor.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict


# Identifica um ator na lista e retorna sua data de nascimento
def get_actor_birth_date(actors_info: dict[str, str], actor_name: str) -> str|None:
    if actor_name in actors_info:
      return actors_info[actor_name]
    return None


# Retorna dicionário com as tuplas [nome_do_ator, nome da produtora que trabalha]
def get_producer_cast(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_ator.to_list(), df.db_produtora.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict


# Retorna dicionário com as tuplas [nome_do_ator, nome do programa de televisão que trabalha]
def get_televisionShow_cast(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_ator.to_list(), df.db_titulo.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict


# Retorna dicionário com as tuplas [nome_do_diretor, nome da produtora que trabalha]
def get_producer_cast_directors(df: pd.DataFrame) -> dict[str, str]:
    mylist = list(zip(df.db_diretor.to_list(), df.db_produtora.to_list()))
    mydict = {tup[0]: tup[1] for tup in mylist}
    return mydict




# Verifica quais artistas distintos já trabalharam na mesma Produtora
def actor_coworker_producer(prod: dict[str, str], actor1: str, actor2: str) -> int:
  if (actor1 in prod) and (actor2 in prod):
    if (prod[actor1] == prod[actor2]):
      return 1
  return 0


# Verifica quais artistas distintos já trabalharam no mesmo Programa de Tv
def actor_coworker_televisionShow(televisionShow: dict[str, str], actor1: str, actor2: str) -> int:
  if (actor1 in televisionShow) and (actor2 in televisionShow):
    if (televisionShow[actor1] == televisionShow[actor2]):
      return 1
  return 0


# Verifica quais diretores distintos trabalharam na mesma Produtora
def director_coworker_producer(prod: dict[str, str], dir1: str, dir2: str) -> int:
  if (dir1 in prod) and (dir2 in prod):
    if (prod[dir1] == prod[dir2]):
      return 1
  return 0


# Verifica se um determinado artista nasceu antes de um Ano
def actor_birth_date_less_date(actor_list: dict[str, str], actor_name: str, date: str) -> int:
  if actor_name in actor_list:
    actor_birth_date      = datetime.strptime(actor_list[actor_name], "%Y-%m-%d")   #convertendo para o formato data
    date                  = datetime.strptime(date,                   "%Y-%m-%d")   #convertendo para o formato data
    if(actor_birth_date < date):
      return 1                                                                      #comparando data [comparação sensível a dia, mês e ano]
  return 0


# Verifica quantidade de artitas trabalham em uma produtora
def amount_actors_producer(prod: dict[str, str], prod_name: str) -> int :
  i : int = 0
  prod_values_list = list(prod.values())   #lista com os nomes das produtoras 
  for prod_aux in prod_values_list:
    if prod_aux == prod_name:
      i=i+1
  return i


# verifica qual produtora tem mais artistas
def maiorNubAtoresPorProdutora(prod: dict[str, str]) -> int:
  prod_values_list  = list(prod.values())    #criando lista com somente os nomes das produtoras
  amount_actor:       int = 0
  prod_name   :       str = "nenhum"

  for element in prod_values_list:                                     #enquanto houver elementos na lista
    amount_temp = amount_actors_producer(prod, element)                #verifica a quantidade de atores de uma produtora
    if amount_temp > amount_actor:                                     #se a quantidade de atores da produtora for maior, o valor é guardado
      amount_actor = amount_temp                                       #maior número
      prod_name = element                                              #nome da produtora com maior número (valor não retornado)

  return amount_actor


# Verifica qual produtora tem Menos artistas
def menorNubAtoresPorProdutora(prod: dict[str, str]) -> int:
  prod_values_list  = list(prod.values())     #criando lista com somente os nomes das produtoras
  amount_actor:       int = 10000000
  prod_name :         str = "nenhum"

  for element in prod_values_list:                        #enquanto houver elementos na lista
    amount_temp = amount_actors_producer(prod, element)   #verifica a quantidade de atores de uma produtora
    if amount_temp < amount_actor:                        #se a quantidade de atores da produtora for menor, o valor é guardado
      amount_actor = amount_temp                          #menor número
      prod_name = element                                 #nome da produtora com menor número (valor não retornado)
  return amount_actor


def main() -> None:

  df: pd.DataFrame = get_df()                                         #pegando dataFrame
  actors_name = ["Emma Lockhart", "Ralph Waite", "Tom Felton"]        #nomes para teste, o 0 e 1 trabalham na mesma produtora e no mesmo programa
  dir_name    = ["Michael Chang", "Danny Antonucci", "Tom Felton"]    #nomes para teste, o 0 e 1 trabalham na mesma programa

  
  #pprint(get_info_actors(df))
  #pprint(get_actor_birth_date(get_info_actors(df), "Megan Follows"))
  #pprint(get_producer_cast(df))
  #pprint(get_televisionShow_cast(df))
  #pprint(get_producer_cast_directors(df))

  
  prod_and_actors_list = get_producer_cast(df)                         #obtendo lista [nome_ator, nome_produtora]
  if(actor_coworker_producer(prod_and_actors_list, actors_name[0], actors_name[1])):
    pprint("Os dois atores já trabalharam na mesma Produtora")
  else:
    pprint("Os dois atores NÃO trabalharam na mesma Produtora")


  televisionShow_and_actors_list = get_televisionShow_cast(df)         #obtendo lista [nome_ator, nome_programa]  
  if(actor_coworker_televisionShow(televisionShow_and_actors_list, actors_name[0], actors_name[1])):
    pprint("Os dois atores já trabalharam no mesmo programa")
  else:
    pprint("Os dois atores NÃO trabalharam no mesmo programa")
  

  prod_and_dir_list = get_producer_cast_directors(df)         #obtendo lista [nome_ator, nome_programa]
  if(director_coworker_producer(prod_and_dir_list, dir_name[0], dir_name[1])):
    pprint("Os dois diretores já trabalharam na mesma produtora")
  else:
    pprint("Os dois diretores NÃO trabalharam na mesma produtora")


  name_and_birthDate_Actores = get_info_actors(df)
  if(actor_birth_date_less_date(name_and_birthDate_Actores,'Jamie Foreman', "1956-07-19")): #['Jamie Foreman': '1956-07-20']
    pprint("Ator nasceu antes da data comparada")
  else:
    pprint("Ator nasceu depois da data comparada")
  
  print("Exemplo de consulta - a produtora ","Cartoon Network"," tem ",amount_actors_producer(prod_and_actors_list,"Cartoon Network"), " atores")
  print("A produtora com mais  atores tem: ", maiorNubAtoresPorProdutora(prod_and_actors_list), " atores")
  print("A produtora com menos atores tem: ", menorNubAtoresPorProdutora(prod_and_actors_list), " atores")


if __name__ == "__main__":
    main()
