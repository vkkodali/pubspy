
"""
fetch_pubmed.py

A script to fetch PubMed publications in XML format using the NCBI e-utilities API.

Usage:
    python fetch_pubmed.py --query "search terms" [--retmax 20] [--email you@example.com]

Requirements:
    - requests

References:
    - NCBI E-utilities: https://www.ncbi.nlm.nih.gov/books/NBK25501/
    - PubMed API: https://dataguide.nlm.nih.gov/eutilities/
"""


import argparse
import sys
import requests
import xml.etree.ElementTree as ET
from typing import Optional, Iterator, Dict
from tqdm import tqdm


ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

def fetch_pubmed_xml(pmids: list[str], email: Optional[str] = None) -> str:
    """Fetch PubMed records in XML format for a list of PMIDs."""
    if not pmids:
        return ""
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    if email:
        params["email"] = email
    resp = requests.post(EFETCH_URL, data=params, timeout=30)
    resp.raise_for_status()
    return resp.text

def extract_iisc_publications(xml_data: str) -> Iterator[Dict[str, str]]:
    """
    Yield publication info for articles with 'Indian Institute of Science' in any author affiliation.
    Returns dicts with keys: title, journal, pubdate, pmid.
    """
    root = ET.fromstring(xml_data)
    for article in root.findall('.//PubmedArticle'):
        pmid_elem = article.find('.//PMID')
        pmid = pmid_elem.text if pmid_elem is not None else None
        # Title
        title_elem = article.find('.//ArticleTitle')
        title = title_elem.text if title_elem is not None else None
        # Journal
        journal_elem = article.find('.//Journal/Title')
        journal = journal_elem.text if journal_elem is not None else None
        # Publication date (try Year, or MedlineDate)
        pubdate_elem = article.find('.//JournalIssue/PubDate/Year')
        if pubdate_elem is not None:
            pubdate = pubdate_elem.text
        else:
            medline_date_elem = article.find('.//JournalIssue/PubDate/MedlineDate')
            pubdate = medline_date_elem.text if medline_date_elem is not None else None
        # Affiliations
        found_iisc = False
        for aff in article.findall('.//AffiliationInfo/Affiliation'):
            if aff.text and 'indian institute of science' in aff.text.lower():
                found_iisc = True
                break
        if found_iisc:
            yield {
                'title': title or '',
                'journal': journal or '',
                'pubdate': pubdate or '',
                'pmid': pmid or '',
            }

def search_pubmed(query: str, retmax: int = 500, email: Optional[str] = None) -> tuple[list[str], int]:
    """Search PubMed and return a tuple: (list of PubMed IDs (PMIDs), total count)."""
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": retmax,
        "retmode": "json",
    }
    if email:
        params["email"] = email
    resp = requests.get(ESEARCH_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data["esearchresult"]["idlist"], int(data["esearchresult"].get("count", 0))



def main():
    parser = argparse.ArgumentParser(description="Fetch PubMed publications in XML format using NCBI e-utilities API.")
    parser.add_argument("--query", required=True, help="Query string to search PubMed.")
    parser.add_argument(
        "--retmax", type=int, default=10000,
        help="Maximum number of results to fetch (default: 10000). Keep below 100000 to avoid NCBI limits.")
    parser.add_argument("--email", type=str, default=None, help="Contact email for NCBI API compliance.")
    parser.add_argument("--output", type=str, default=None, help="Output file to save XML (default: stdout).")
    args = parser.parse_args()
    query = args.query

    CHUNK_SIZE = 250
    def chunked(iterable, n=CHUNK_SIZE):
        for i in range(0, len(iterable), n):
            yield iterable[i:i + n]

    try:
        pmids, total_count = search_pubmed(query, args.retmax, args.email)
        print(f"Total results for query: {total_count}")
        if not pmids:
            print("No results found.", file=sys.stderr)
            sys.exit(1)
        chunks = list(chunked(pmids))
        print(f"Fetching {len(pmids)} PMIDs in {len(chunks)} chunk(s) of {CHUNK_SIZE}...")
        pubs = []
        for chunk in tqdm(chunks, desc="Fetching XML", unit="chunk"):
            xml_data = fetch_pubmed_xml(chunk, args.email)
            for pub in extract_iisc_publications(xml_data):
                pubs.append(pub)
        # Output tab-delimited file if requested
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write("Title\tJournal\tDate\tPMID\tPubMed_URL\n")
                for pub in pubs:
                    url = f"https://pubmed.gov/{pub['pmid']}"
                    # Replace tabs/newlines in fields for clean output
                    title = (pub['title'] or '').replace('\t', ' ').replace('\n', ' ')
                    journal = (pub['journal'] or '').replace('\t', ' ').replace('\n', ' ')
                    date = (pub['pubdate'] or '').replace('\t', ' ').replace('\n', ' ')
                    pmid = pub['pmid'] or ''
                    f.write(f"{title}\t{journal}\t{date}\t{pmid}\t{url}\n")
        # Print filtered publications
        print("\nPublications with affiliation 'Indian Institute of Science':\n")
        if pubs:
            for pub in pubs:
                print(f"Title   : {pub['title']}")
                print(f"Journal : {pub['journal']}")
                print(f"Date    : {pub['pubdate']}")
                print(f"PMID    : {pub['pmid']}")
                print(f"URL     : https://pubmed.gov/{pub['pmid']}")
                print("-" * 60)
        else:
            print("No publications found with affiliation 'Indian Institute of Science'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
