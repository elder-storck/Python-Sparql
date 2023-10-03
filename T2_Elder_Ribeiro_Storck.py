import sparql_dataframe

def get_df() -> pd.Dataframe:
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

def main() -> None:
    # Criando o dataframe da query
    df: pd.DataFrame = get_df()


if __name__ == "__main__":
    main()
