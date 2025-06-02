# wikimedia-dumps

Project content credit: [Sebastian Hellmann](https://forum.dbpedia.org/t/automatically-adding-wikimedia-dumps-on-the-databus-gsoc-2025/4253)
### Project Description:

Wikimedia publishes their dumps via https://dumps.wikimedia.org . At the moment, these dumps are described via HTML, so the HTML serves as the metadata and it is required to parse to identify whether new dumps are available. To automate retrieval of data, the Databus (and also MOSS as its extension [Databus and MOSS – Search over a flexible, multi-domain, multi-repository metadata catalog](https://zenodo.org/records/14161466)) has a metadata knowledge graph, where one can do queries like “check whether a new version of x is available”. Since DBpedia uses the dumps to create knowledge graphs, it would be good to put the download links for the dumps and the metadata on the Databus.

### Key Objectives

* Build a docker image that we can run daily on our infrastructure to crawl dumps.wikimedia.org  and identify all new finished dumps, then add a new record on the Databus.
* Goal is to allow checking for new dumps via SPARQL. go to OIDC Form_Post Response  , then example queries, then “Latest version of artifact”.
* this would help us to 1. track new releases from wikimedia, so the core team and the community can more systematically convert them to RDF as well as to 2. build more solid applications on top, i.e. DIEF or other
* process wise I would think that having an early prototype is necessary and then plan iterations from this.

