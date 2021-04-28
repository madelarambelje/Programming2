from Bio import Entrez
import sys
import multiprocessing as mp

Entrez.email = "m.a.delarambelje@gmail.com"
Entrez.api_key = "922bc25a747c178a595054ac75fa5b0f8508"

#Setting user input
pmid = str(sys.argv[1])

def get_reference(pmid):
    '''Extracting first 10 references from user given PMID'''
    # Fetching article with ID
    handle = Entrez.efetch(db="pubmed",id=pmid,retmode="xml")
    record = Entrez.read(handle)
    # Extracting all references
    refList = (record["PubmedArticle"][0].get("PubmedData").get("ReferenceList")[0]["Reference"])
    # Add to empty list all refIDs
    RefIdList = []
    for article in refList:
        refId = article["ArticleIdList"][0].strip()
        RefIdList.append(refId)
    return RefIdList[:10]
def download_save(RefID):
    '''Download reference ID yielded by get_reference'''
    handle = Entrez.efetch(db="pubmed", id=RefID, retmode='xml')
    with open(f'output/{RefID}.xml','wb') as f:
        f.write(handle.read())

if __name__ == "__main__":
    RefIds = get_reference(pmid)
    # Get number of amount cpus
    cpus = mp.cpu_count()
    #
    with mp.Pool(cpus) as pool:
        pool.map(download_save, RefIds)








