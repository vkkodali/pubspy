
import argparse
import sys
import requests
import xml.etree.ElementTree as ET
from typing import Optional
from collections import defaultdict
import time

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

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

def read_institute_names(filepath: str) -> dict:
    """Read tab-delimited institute and country names from a file, ignoring comment lines."""
    institutes = dict()
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            assert len(parts) == 2
            institute, country = parts[0].strip().replace('"', ''), parts[1].strip()
            institutes[institute] = country
    return institutes

def fetch_pubmed_xml(pmids: list[str], email: Optional[str] = None) -> str:
    """Fetch PubMed XML for a list of PMIDs using POST method."""
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


def parse_and_output_publications(all_pmids, all_institutes, email: Optional[str], output_file):
    """Fetch XML for PMIDs, parse and print/write title, journal, pubdate, pmid, url, institutes."""

    CHUNK_SIZE = 250
    def chunked(iterable, n=CHUNK_SIZE):
        for i in range(0, len(iterable), n):
            yield iterable[i:i + n]

    def institute_affiliation_match(qry_institutes, affiliations):
        """Return True if any institute matches the affiliation rules (country-aware)."""
        for inst in qry_institutes:
            country = all_institutes[inst]
            inst_lower = inst.lower()
            if country == "NA" and any(inst_lower in aff for aff in affiliations):
                return True
            elif any(inst_lower in aff and country and country.lower() in aff for aff in affiliations):
                return True
        return False

    pubs = []
    pmid_list = list(all_pmids.keys())

    for chunk in chunked(pmid_list):
        xml_data = fetch_pubmed_xml(chunk, email)
        root = ET.fromstring(xml_data)
        for article in root.findall('.//PubmedArticle'):
            pmid_elem = article.find('.//PMID')
            pmid = pmid_elem.text if pmid_elem is not None else ''
            title_elem = article.find('.//ArticleTitle')
            title = title_elem.text if title_elem is not None else ''
            journal_elem = article.find('.//Journal/Title')
            journal = journal_elem.text if journal_elem is not None else ''
            pubdate_elem = article.find('.//JournalIssue/PubDate/Year')
            if pubdate_elem is not None:
                pubdate = pubdate_elem.text
            else:
                medline_date_elem = article.find('.//JournalIssue/PubDate/MedlineDate')
                pubdate = medline_date_elem.text if medline_date_elem is not None else ''
            url = f"https://pubmed.gov/{pmid}"
            qry_institutes_str = ''
            matched = False
            if pmid in all_pmids:
                qry_institutes = all_pmids[pmid]
                qry_institutes_str = ';'.join(sorted(qry_institutes))
                affiliations = [aff.text.lower() for aff in article.findall('.//AffiliationInfo/Affiliation') if aff.text]
                matched = institute_affiliation_match(qry_institutes, affiliations)
            if matched:
                # Print to screen
                print(f"Title   : {title}")
                print(f"Journal : {journal}")
                print(f"Date    : {pubdate}")
                print(f"PMID    : {pmid}")
                print(f"URL     : {url}")
                print(f"Institutes: {qry_institutes_str}")
                print("-" * 60)
                # Write to file
                pubs.append({
                    'title': title,
                    'journal': journal,
                    'pubdate': pubdate,
                    'pmid': pmid,
                    'url': url,
                    'institutes': qry_institutes_str
                })
        time.sleep(0.25)

    # Print total count of publications to screen 
    print(f"\nNum. publications to review after dropping false-positives: {len(pubs)}\n")

    if output_file:
        with open(output_file, "a", encoding="utf-8") as f:
            for pub in pubs:
                title = (pub['title'] or '').replace('\t', ' ').replace('\n', ' ')
                journal = (pub['journal'] or '').replace('\t', ' ').replace('\n', ' ')
                date = (pub['pubdate'] or '').replace('\t', ' ').replace('\n', ' ')
                pmid = pub['pmid'] or ''
                url = pub['url']
                institutes_str = pub['institutes']
                f.write(f"{title}\t{journal}\t{date}\t{pmid}\t{url}\t{institutes_str}\n")

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

    # Load institutes from file 
    all_institutes = read_institute_names("Section_1286_list.txt")
    print(f"Num. institutes being checked: {len(all_institutes)}")
    try:
        # Write header to output file if requested
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write("Title\tJournal\tDate\tPMID\tPubMed_URL\tInstitutes\n")
        # Collect all PMIDs from all institutes
        all_pmids = defaultdict(set)  # pmid -> set of institute names
        for institute in all_institutes.keys():
            search_query = f"{args.query} AND {institute}[affiliation]"
            pmids, total_count = search_pubmed(search_query, args.retmax, args.email)
            if total_count > 0:
                print(f"Found {total_count} hits for {institute}")
            for pmid in pmids:
                all_pmids[pmid].add(institute)
            time.sleep(0.25) # do not overload NCBI 

        print(f"\nTotal number of PMIDs returned for all institutes: {len(all_pmids)}\n")

        # Chunk and process all PMIDs
        parse_and_output_publications(all_pmids, all_institutes, args.email, args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
