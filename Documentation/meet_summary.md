# Community Bonding 

Meeting Summary – Technical Discussion

## Week 1

Attendees: Milan, Ronit, Priyanshu

Overview :
The meeting began with an introduction to the project, where I walked through the overall flow and my planned approach to executing it in phases. The mentors provided valuable input that helped refine the direction and structure of the upcoming work.

Key task for next meet :
1. Identify the Data to Collect
2. Design the Indexing Strategy
3. Plan for Structured Publication


## Week 2

Attendees: Milan, Ronit, Priyanshu

Overview :  

1.  Identify the Dumps the XML and SQL dumps because they provide the core content (wikitext) and essential metadata (links, categories, redirects) required for structured data extraction, graph construction, and multilingual alignment used by systems like DBpedia and DIEF.
 
 2. Plan for Structured Publication mentioned through example 

    1.  page-articles Dump:
        https://dumps.wikimedia.org/enwiki/20250420/enwiki-20250420-pages-articles-multistream.xml.bz2

        Databus path:
         https://databus.dbpedia.org/wikimedia-dumps/enwiki/page-articles/20250420

         Mapping:

         account: wikimedia-dumps

        group: enwiki

        artifact: page-articles

        version: 20250420

    2.  corpora (Content Translation) Dump:
         https://dumps.wikimedia.org/other/contenttranslation/20250307/cx-corpora.en2fr.html.json.gz

         Databus path:
         https://databus.dbpedia.org/wikimedia-dumps/contenttranslation/corpora/20250307

        Mapping:

        account: wikimedia-dumps

        group: contenttranslation

        artifact: corpora

        version: 20250307

    3. categorylinks (SQL dump) Dump:
     https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-categorylinks.sql.gz

         Expected Databus path:
        https://databus.dbpedia.org/wikimedia-dumps/enwiki/categorylinks/20250520 

        Mapping:

        account: wikimedia-dumps

        group: enwiki

        artifact: categorylinks

        version: 20250520 

Key task for next meet : 
1. Identify the Dataset Used by the Extraction Framework
2. Manually Publish a Dataset for Testing
3. Explore the Extraction Framework’s Files and Codebase

#

# Coding Phase

## Week 1

Attendees: Sebastian,Milan, Ronit, Priyanshu

Overview:
We began by outlining the overall project workflow and identifying key tasks. We explored strategies to improve efficiency and address potential challenges. Milan provided a valuable solution for the issue faced by me  related to publishing data, helping me  move forward effectively.

Key Tasks for the Next Meeting:

Develop a crawler to identify all subdomains

Initiate contact with Wikimedia regarding the project approach

Implement data publishing using the designated API