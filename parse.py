import nltk
import pandas as pd
from bs4 import BeautifulSoup as b

from scrape import scrape


def soupify(html):
    return b(html, "html.parser")


def parse_title(soup):
    for div_id, elem in title_div_ids:
        if title_div := soup.find("div", {"id": div_id}):
            if title_elem := title_div.find(elem):
                if title := title_elem.text:
                    return title


def extract_human_names(text):
    tokens = nltk.tokenize.word_tokenize(text)
    pos_tags = nltk.pos_tag(tokens)
    chunked_tree = nltk.ne_chunk(pos_tags)

    human_names = []
    for subtree in chunked_tree:
        if type(subtree) == nltk.tree.Tree:
            if subtree.label() == "PERSON":
                name = " ".join(e[0] for e in subtree)
                human_names.append(name)

    return human_names


def parse_main_content(soup):
    for id in main_content_div_ids:
        if (content := soup.find("div", {"id": id})) and not (
            soup.find("div", {"id": id}).get_text().strip().startswith("How Do I?")
        ):
            if content.get_text().strip():
                return content.get_text("\n")


def extract_phone(text):
    phone_numbers = nltk.tokenize.regexp_tokenize(text, phone_regex)
    phone_numbers = {
        phone_number.replace(".", "")
        .replace(
            " ",
            "",
        )
        .replace("\n", "")
        .strip()
        for phone_number in phone_numbers
    }
    return phone_numbers


def extract_emails(text):
    emails = nltk.tokenize.regexp_tokenize(text, email_regex)
    return set(emails)


email_regex = "([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)"
phone_regex = "((?:\(?\d{3}\)?)?[- \.,]?\d{3}[- \.,]?\d{4},?\s?[ext]{0,3},?[ \.]? ?\d*)"


title_div_ids = [
    ("ctl00_PlaceHolderMain_editmodepanel2", "h1"),
    ("Column560", "h2"),
    ("Column560", "h3"),
    ("ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField", "h2"),
    ("ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField", "h1"),
    ("ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField", "span"),
    ("ctl00_PlaceHolderMain_ctl02__ControlWrapper_RichHtmlField", "font"),
    ("ctl00_PlaceHolderMain_ctl02__ControlWrapper_RichHtmlField", "h2"),
    ("WebPartWPQ2", "h2"),
    ("ctl00_PlaceHolderMain_Main_Content__ControlWrapper_RichHtmlField", "h2"),
]
title_h1_class_1 = "mdgov-section__heading"

content_id = "ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"

main_content_div_ids = [
    "MSOZoneCell_WebPartWPQ2",
    "WebPartWPQ2",
    "ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField",
    "ctl00_PlaceHolderMain_ctl02__ControlWrapper_RichHtmlField",
    "Column560",
    "ctl00_PlaceHolderMain_Main_Content__ControlWrapper_RichHtmlField",
]


files = [
    "central_maryland.parquet.gzip",
    "eastern_shore.parquet.gzip",
    "western_maryland.parquet.gzip",
    "southern_maryland.parquet.gzip",
]

all_pages = scrape()

if not all_pages:
    for file in files:
        all_pages[file.split(".")[0]] = pd.read_parquet(file)

for region, df in all_pages.items():
    df["soup"] = pd.Series([soupify(row) for row in df["html"]])
    df["title"] = pd.Series([parse_title(row) for row in df["soup"]])
    df["main_content"] = df["soup"].apply(parse_main_content)
    df["names"] = pd.Series([extract_human_names(row) for row in df["main_content"]])
    df["phone_numbers"] = pd.Series([extract_phone(row) for row in df["main_content"]])
    df["emails"] = pd.Series([extract_emails(row) for row in df["html"]])
    df = df.astype(str)
    df.to_parquet(f"{region}.parquet.gzip", index=False, compression="gzip")
