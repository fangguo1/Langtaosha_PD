import json
import re
import time
import uuid
import xml.etree.ElementTree as ET


MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


def uuid_v7():
    ts_ms = int(time.time() * 1000)
    rand_a = uuid.uuid4().int & ((1 << 12) - 1)
    rand_b = uuid.uuid4().int & ((1 << 62) - 1)
    uuid_int = (ts_ms & ((1 << 48) - 1)) << 80
    uuid_int |= 0x7 << 76
    uuid_int |= rand_a << 64
    uuid_int |= 0x2 << 62
    uuid_int |= rand_b
    return str(uuid.UUID(int=uuid_int))


def make_work_id():
    return f"W{uuid_v7()}"


def normalize_text(value):
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def get_text(elem):
    if elem is None:
        return None
    text = "".join(elem.itertext())
    return normalize_text(text)


def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_month(value):
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return int(value)
    match = re.match(r"([A-Za-z]{3})", value)
    if match:
        return MONTH_MAP.get(match.group(1).title())
    return None


def build_date(year, month=None, day=None):
    if year is None:
        return None
    if month is None:
        return f"{year}"
    if day is None:
        return f"{year}-{month:02d}"
    return f"{year}-{month:02d}-{day:02d}"


def parse_date_from_elem(elem):
    if elem is None:
        return None
    year = safe_int(get_text(elem.find("Year")))
    month = parse_month(get_text(elem.find("Month")))
    day = safe_int(get_text(elem.find("Day")))
    return build_date(year, month, day)


def parse_pub_date(pub_date_elem):
    if pub_date_elem is None:
        return None, None, None, None, None
    year_text = get_text(pub_date_elem.find("Year"))
    medline_date = get_text(pub_date_elem.find("MedlineDate"))
    year = safe_int(year_text) if year_text else None
    if year is None and medline_date:
        match = re.search(r"(\\d{4})", medline_date)
        if match:
            year = safe_int(match.group(1))
    month = parse_month(get_text(pub_date_elem.find("Month")))
    day = safe_int(get_text(pub_date_elem.find("Day")))
    return year, month, day, build_date(year, month, day), medline_date


def parse_abstract(abstract_elem):
    if abstract_elem is None:
        return None
    parts = []
    for abs_text in abstract_elem.findall("AbstractText"):
        text = get_text(abs_text)
        if not text:
            continue
        label = abs_text.get("Label") or abs_text.get("NlmCategory")
        if label:
            parts.append(f"{label}: {text}")
        else:
            parts.append(text)
    if not parts:
        return None
    return "\n".join(parts)


def parse_authors(author_list_elem):
    authors = []
    if author_list_elem is None:
        return authors
    for idx, author in enumerate(author_list_elem.findall("Author"), start=1):
        collective = get_text(author.find("CollectiveName"))
        if collective:
            name = collective
        else:
            fore = get_text(author.find("ForeName"))
            last = get_text(author.find("LastName"))
            initials = get_text(author.find("Initials"))
            if fore and last:
                name = f"{fore} {last}"
            elif last and initials:
                name = f"{last} {initials}"
            else:
                name = fore or last or initials
        affiliations = []
        for aff in author.findall("AffiliationInfo"):
            aff_text = get_text(aff.find("Affiliation"))
            if aff_text:
                affiliations.append(aff_text)
        authors.append(
            {
                "sequence": idx,
                "name": name,
                "author_id": None,
                "affiliation": "; ".join(affiliations) if affiliations else None,
            }
        )
    return authors


def extract_article_ids(pubmed_data_elem):
    ids = []
    if pubmed_data_elem is None:
        return ids
    article_id_list = pubmed_data_elem.find("ArticleIdList")
    if article_id_list is None:
        return ids
    for article_id in article_id_list.findall("ArticleId"):
        id_type = article_id.get("IdType")
        value = get_text(article_id)
        if value:
            ids.append({"type": id_type, "id": value})
    return ids


def extract_elocation_ids(article_elem):
    ids = []
    if article_elem is None:
        return ids
    for eloc in article_elem.findall("ELocationID"):
        value = get_text(eloc)
        if value:
            ids.append(
                {
                    "type": eloc.get("EIdType"),
                    "valid": eloc.get("ValidYN"),
                    "value": value,
                }
            )
    return ids


def choose_doi(article_ids, elocation_ids):
    for item in article_ids:
        if item.get("type") == "doi":
            return item.get("id")
    for item in elocation_ids:
        if item.get("type") == "doi":
            return item.get("value")
    return None


def parse_history(history_elem):
    history = []
    if history_elem is None:
        return history
    for pub_date in history_elem.findall("PubMedPubDate"):
        date_str = parse_date_from_elem(pub_date)
        hour = safe_int(get_text(pub_date.find("Hour")))
        minute = safe_int(get_text(pub_date.find("Minute")))
        time_str = None
        if hour is not None and minute is not None:
            time_str = f"{hour:02d}:{minute:02d}"
        history.append(
            {
                "status": pub_date.get("PubStatus"),
                "date": date_str,
                "time": time_str,
            }
        )
    return history


def parse_reference_list(reference_list_elem):
    references = []
    if reference_list_elem is None:
        return references
    for ref in reference_list_elem.findall("Reference"):
        entry = {}
        citation = get_text(ref.find("Citation"))
        if citation:
            entry["citation"] = citation
        article_ids = []
        article_id_list = ref.find("ArticleIdList")
        if article_id_list is not None:
            for article_id in article_id_list.findall("ArticleId"):
                id_type = article_id.get("IdType")
                value = get_text(article_id)
                if value:
                    article_ids.append({"type": id_type, "id": value})
        if article_ids:
            entry["article_ids"] = article_ids
        if entry:
            references.append(entry)
    return references


def parse_keyword_lists(medline_elem):
    keyword_lists = []
    if medline_elem is None:
        return keyword_lists
    for kw_list in medline_elem.findall("KeywordList"):
        owner = kw_list.get("Owner")
        keywords = []
        for kw in kw_list.findall("Keyword"):
            kw_text = get_text(kw)
            if kw_text:
                keywords.append(kw_text)
        if keywords:
            keyword_lists.append({"owner": owner, "keywords": keywords})
    return keyword_lists


def parse_mesh_headings(medline_elem):
    mesh_items = []
    if medline_elem is None:
        return mesh_items
    mesh_list = medline_elem.find("MeshHeadingList")
    if mesh_list is None:
        return mesh_items
    for mesh in mesh_list.findall("MeshHeading"):
        descriptor = mesh.find("DescriptorName")
        descriptor_name = get_text(descriptor)
        descriptor_ui = descriptor.get("UI") if descriptor is not None else None
        descriptor_major = descriptor.get("MajorTopicYN") if descriptor is not None else None
        qualifiers = []
        for qualifier in mesh.findall("QualifierName"):
            qualifiers.append(
                {
                    "name": get_text(qualifier),
                    "ui": qualifier.get("UI"),
                    "major_topic": qualifier.get("MajorTopicYN"),
                }
            )
        mesh_items.append(
            {
                "descriptor_name": descriptor_name,
                "descriptor_ui": descriptor_ui,
                "descriptor_major_topic": descriptor_major,
                "qualifiers": qualifiers,
            }
        )
    return mesh_items


def parse_chemical_list(medline_elem):
    chemicals = []
    if medline_elem is None:
        return chemicals
    chemical_list = medline_elem.find("ChemicalList")
    if chemical_list is None:
        return chemicals
    for chemical in chemical_list.findall("Chemical"):
        name = get_text(chemical.find("NameOfSubstance"))
        chemicals.append(
            {
                "registry_number": get_text(chemical.find("RegistryNumber")),
                "name": name,
                "ui": chemical.find("NameOfSubstance").get("UI")
                if chemical.find("NameOfSubstance") is not None
                else None,
            }
        )
    return chemicals


def parse_grants(article_elem):
    grants = []
    if article_elem is None:
        return grants
    grant_list = article_elem.find("GrantList")
    if grant_list is None:
        return grants
    for grant in grant_list.findall("Grant"):
        grants.append(
            {
                "grant_id": get_text(grant.find("GrantID")),
                "acronym": get_text(grant.find("Acronym")),
                "agency": get_text(grant.find("Agency")),
                "country": get_text(grant.find("Country")),
            }
        )
    return grants


def parse_publication_types(article_elem):
    pub_types = []
    if article_elem is None:
        return pub_types
    pub_list = article_elem.find("PublicationTypeList")
    if pub_list is None:
        return pub_types
    for pub_type in pub_list.findall("PublicationType"):
        name = get_text(pub_type)
        if name:
            pub_types.append({"ui": pub_type.get("UI"), "name": name})
    return pub_types


def extract_references_for_citations(reference_list_elem, pmid_to_work_id):
    references = []
    if reference_list_elem is None:
        return references
    for ref in reference_list_elem.findall("Reference"):
        article_ids = []
        article_id_list = ref.find("ArticleIdList")
        if article_id_list is not None:
            for article_id in article_id_list.findall("ArticleId"):
                id_type = article_id.get("IdType")
                value = get_text(article_id)
                if value:
                    article_ids.append((id_type, value))
        pmid = next((v for t, v in article_ids if t == "pubmed"), None)
        if pmid:
            references.append(pmid_to_work_id.get(pmid, f"PMID:{pmid}"))
            continue
        doi = next((v for t, v in article_ids if t == "doi"), None)
        if doi:
            references.append(f"DOI:{doi}")
            continue
        citation = get_text(ref.find("Citation"))
        if citation:
            references.append(f"CITATION:{citation}")
    return references


def build_record(article_elem, pmid_to_work_id):
    medline = article_elem.find("MedlineCitation")
    pubmed_data = article_elem.find("PubmedData")
    article = medline.find("Article") if medline is not None else None

    pmid = get_text(medline.find("PMID")) if medline is not None else None
    work_id = pmid_to_work_id.get(pmid, make_work_id())

    author_list = parse_authors(article.find("AuthorList") if article is not None else None)
    abstract = parse_abstract(article.find("Abstract") if article is not None else None)

    journal = article.find("Journal") if article is not None else None
    journal_issue = journal.find("JournalIssue") if journal is not None else None
    pub_date_elem = journal_issue.find("PubDate") if journal_issue is not None else None
    year, month, day, publish_time, medline_date = parse_pub_date(pub_date_elem)

    if year is None:
        article_date = article.find("ArticleDate") if article is not None else None
        year = safe_int(get_text(article_date.find("Year"))) if article_date is not None else None

    article_ids = extract_article_ids(pubmed_data)
    elocation_ids = extract_elocation_ids(article)
    doi = choose_doi(article_ids, elocation_ids)
    history = parse_history(pubmed_data.find("History")) if pubmed_data is not None else []
    submitted_date = None
    for entry in history:
        if entry.get("status") in {"received", "accepted", "revised"} and entry.get("date"):
            submitted_date = entry["date"]
            break

    default_info = {
        "title": get_text(article.find("ArticleTitle")) if article is not None else None,
        "abstract": abstract,
        "authors": author_list,
        "identifiers": {
            "arxiv": None,
            "doi": doi,
            "semantic_scholar": None,
            "pubmed": pmid,
        },
        "primary_category": None,
        "categories": [],
        "year": year,
        "submitted_date": submitted_date,
        "updated_date": parse_date_from_elem(medline.find("DateRevised"))
        if medline is not None
        else None,
        "is_preprint": False,
        "is_published": True,
        "pub_info": {
            "venue_name": get_text(journal.find("Title")) if journal is not None else None,
            "venue_type": "journal",
            "publish_time": publish_time,
            "presentation_type": None,
        },
        "pdf_url": None,
        "source_url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None,
    }

    additional_info = {}

    if medline is not None:
        medline_info = {
            "status": medline.get("Status"),
            "indexing_method": medline.get("IndexingMethod"),
            "owner": medline.get("Owner"),
        }
        if any(medline_info.values()):
            additional_info["medline_citation"] = medline_info

    if article is not None and article.get("PubModel"):
        additional_info["pub_model"] = article.get("PubModel")

    date_completed = parse_date_from_elem(medline.find("DateCompleted")) if medline is not None else None
    if date_completed:
        additional_info["date_completed"] = date_completed

    date_revised = parse_date_from_elem(medline.find("DateRevised")) if medline is not None else None
    if date_revised:
        additional_info["date_revised"] = date_revised

    journal_details = {}
    if journal is not None:
        for issn in journal.findall("ISSN"):
            issn_type = issn.get("IssnType")
            issn_value = get_text(issn)
            if issn_type == "Print":
                journal_details["issn_print"] = issn_value
            elif issn_type == "Electronic":
                journal_details["issn_electronic"] = issn_value
        journal_details["iso_abbreviation"] = get_text(journal.find("ISOAbbreviation"))

    if medline is not None:
        medline_journal = medline.find("MedlineJournalInfo")
        if medline_journal is not None:
            journal_details.update(
                {
                    "medline_ta": get_text(medline_journal.find("MedlineTA")),
                    "nlm_unique_id": get_text(medline_journal.find("NlmUniqueID")),
                    "country": get_text(medline_journal.find("Country")),
                    "issn_linking": get_text(medline_journal.find("ISSNLinking")),
                }
            )

    if journal_issue is not None:
        journal_details["volume"] = get_text(journal_issue.find("Volume"))
        journal_details["issue"] = get_text(journal_issue.find("Issue"))
        journal_details["cited_medium"] = journal_issue.get("CitedMedium")
        if pub_date_elem is not None:
            journal_details["pub_date_raw"] = {
                "year": year,
                "month": month,
                "day": day,
                "medline_date": medline_date,
            }

    if any(v for v in journal_details.values()):
        additional_info["journal_details"] = journal_details

    pagination = get_text(article.find("Pagination/MedlinePgn")) if article is not None else None
    if pagination:
        additional_info["pagination"] = pagination

    if article is not None:
        languages = [get_text(lang) for lang in article.findall("Language")]
        languages = [lang for lang in languages if lang]
        if languages:
            additional_info["language"] = languages

    grants = parse_grants(article)
    if grants:
        additional_info["grant_list"] = grants

    pub_types = parse_publication_types(article)
    if pub_types:
        additional_info["publication_types"] = pub_types

    chemicals = parse_chemical_list(medline)
    if chemicals:
        additional_info["chemical_list"] = chemicals

    mesh_headings = parse_mesh_headings(medline)
    if mesh_headings:
        additional_info["mesh_headings"] = mesh_headings

    keyword_lists = parse_keyword_lists(medline)
    if keyword_lists:
        additional_info["keyword_list"] = keyword_lists

    citation_subset = get_text(medline.find("CitationSubset")) if medline is not None else None
    if citation_subset:
        additional_info["citation_subset"] = citation_subset

    if article_ids:
        additional_info["article_ids"] = article_ids
    if elocation_ids:
        additional_info["elocation_ids"] = elocation_ids

    if pubmed_data is not None:
        if history:
            additional_info["history"] = history
        publication_status = get_text(pubmed_data.find("PublicationStatus"))
        if publication_status:
            additional_info["publication_status"] = publication_status
        reference_list = parse_reference_list(pubmed_data.find("ReferenceList"))
        if reference_list:
            additional_info["reference_list"] = reference_list

    author_list_complete = None
    if article is not None:
        author_list_elem = article.find("AuthorList")
        if author_list_elem is not None:
            author_list_complete = author_list_elem.get("CompleteYN")
    if author_list_complete:
        additional_info["author_list_complete"] = author_list_complete

    record = {
        "work_id": work_id,
        "default_info": default_info,
        "additional_info": additional_info,
    }
    return record, pubmed_data, work_id, pmid


def iter_pubmed_articles(xml_path):
    context = ET.iterparse(xml_path, events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == "PubmedArticle":
            yield elem
            root.clear()


def to_json(record):
    return json.dumps(record, ensure_ascii=False, indent=2)
