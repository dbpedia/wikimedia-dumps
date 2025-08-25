All dumps listed on wikimedia has format 

- Layer 1 is /wikimedia/
- Layer 2 is /enwiki/
- Layer 3 is /20250801/
 
[https://dumps.wikimedia.org/enwiki/20250801](https://dumps.wikimedia.org/enwiki/20250801)

When dumps processed all important data need to process is under 

[https://dumps.wikimedia.org/enwiki/20250801/dumpsstatus.json](https://dumps.wikimedia.org/enwiki/20250801/dumpstatus.json) 

It has a jobs format under which file details mention  that converted in api    

Example 
``` JSON
"xmlstubsdump": {
      "status": "done",
      "updated": "2025-08-01 10:37:39",
      "files": {
        "aawiki-20250801-stub-meta-history.xml.gz": {
          "size": 108519,
          "url": "/aawiki/20250801/aawiki-20250801-stub-meta-history.xml.gz",
          "md5": "e6e0f16771af23ea22469c620c2a8360",
          "sha1": "72c62da3e905504aa3ff2529add34bea20d95326"
        }       
    
```

Key : Value  has all require info need to publish  the all things 

In Databus thing being got in this form 

- account -> DBpedia
- group -> wikimedia
- artifact -> aawikibooks-xmlstubsdump-history 
- version: 20250801


For DATA UI to see the published data

- Layer 1 -> account
- Layer 2 -> wikimedia
- Layer 3 -> aawiki + filename(from JSON)
- Layer 4 -> Version 

Current Dummy Link [https://databus.dbpedia.org/tech0priyanshu/wikimedia/aawikibooks-xmlstubsdump-history/20250801](https://databus.dbpedia.org/tech0priyanshu/wikimedia/aawikibooks-xmlstubsdump-history/20250801)

Job 

``` JSON
"xmlstubsdump": {
      "status": "done",
      "updated": "2025-08-01 10:37:39",
      "files": {
        "aawiki-20250801-stub-meta-history.xml.gz": {
          "size": 108519,
          "url": "/aawiki/20250801/aawiki-20250801-stub-meta-history.xml.gz",
          "md5": "e6e0f16771af23ea22469c620c2a8360",
          "sha1": "72c62da3e905504aa3ff2529add34bea20d95326"
        }
```

SPARQL Query  
```  
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dcat:   <http://www.w3.org/ns/dcat#>
PREFIX dct:    <http://purl.org/dc/terms/>
PREFIX dcv: <https://dataid.dbpedia.org/databus-cv#>
PREFIX databus: <https://dataid.dbpedia.org/databus#>
SELECT ?file WHERE
{
	GRAPH ?g
	{
		?dataset databus:artifact <https://databus.dbpedia.org/tech0priyanshu/wikimedia/aawikibooks-xmlstubsdump-history> .
		{ ?distribution <http://purl.org/dc/terms/hasVersion> '20250801' . }
		?dataset dcat:distribution ?distribution .
		?distribution databus:file ?file .
	}
}
```



