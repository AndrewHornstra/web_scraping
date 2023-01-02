from collections import defaultdict
from time import sleep

import pandas as pd
import requests as r
from bs4 import BeautifulSoup as b


def scrape() -> dict:
    urls = {
        # "eastern_shore": "https://health.maryland.gov/dda/Pages/esro-providers.aspx",
        # "central_maryland": "https://health.maryland.gov/dda/Pages/cmro-providers.aspx",
        # "southern_maryland": "https://health.maryland.gov/dda/Pages/smro-providers.aspx",
        # "western_maryland": "https://health.maryland.gov/dda/Pages/wmro-providers.aspx",
    }

    provider_links = {}

    for region, url in urls.items():
        response = r.get(url)
        doc = b(response.text, "html.parser")
        providers_html = doc.find(
            "div", {"id": "ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField"}
        )
        provider_links[region] = [
            f"https://health.maryland.gov{link.get('href')}"
            for link in providers_html.find_all("a", href=True)
        ]

    pages = defaultdict(list)
    for region, links in provider_links.items():
        for index, link in enumerate(links):
            pages[region].append(r.get(link).text)
            print(region, index)
            sleep(0.5)

    all_pages = {}
    for region in urls:
        all_pages[region] = pd.DataFrame(
            {"html": pages[region], "url": provider_links[region]}
        ).drop_duplicates

    return all_pages
