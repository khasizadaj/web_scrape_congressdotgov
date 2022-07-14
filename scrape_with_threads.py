import csv
from typing import Dict, List, Tuple
from urllib.parse import urlencode
import concurrent.futures

import requests
from bs4 import BeautifulSoup
from loguru import logger
from requests import Response

import config
import time

PAGES_TO_SCRAPE = {
    "110": (1, 3),
    "111": (1, 3),
    "112": (1, 3),
}


def process_page(congress, soup):
    list_of_congreses = []  # it is for saving congress of each collected bill
    bill_names = []
    bill_introduction_years = []
    sponsor_names = []
    sponsor_party_memberships = []
    sponsor_states = []
    sponsor_year_exps = []
    number_of_cosponsors = []
    average_year_of_services = []

    # Find all individual bills segments, and identify the whole number of bills
    bill_list_items = soup.findAll("li", {"class": "compact"})
    number_of_bills = len(bill_list_items)

    # Repeat the process for all bills in each congress
    for index_bill_list_item, bill_list_item in enumerate(bill_list_items):
        list_of_congreses.append(congress)

        # Getting information from the individual bill, and creating a sub-soup
        # for the individual bill:

        # TODO Delete comment frominformation about bill
        # print(
        #     f"\nProgress of current congress ({congress}): {index_bill_list_item + 1} of {number_of_bills} (of page {page_num})."
        # )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # (
            #     curr_bill_name,
            #     curr_bill_introduction_year,
            # ) = process_bill_information(bill_list_item)

            bill_information = executor.submit(process_bill_information, bill_list_item)
            (
                curr_bill_name,
                curr_bill_introduction_year,
            ) = bill_information.result()

            bill_names.append(curr_bill_name)
            bill_introduction_years.append(curr_bill_introduction_year)

            # Fourth Step, getting the sponsor information
            # (
            #     curr_sponsor_name,
            #     curr_sponsor_party_membership,
            #     curr_sponsor_state,
            #     curr_sponsor_year_exp,
            # ) = (
            #     "nan",
            #     "nan",
            #     "nan",
            #     "nan",
            # )
            sponsor_information = executor.submit(
                process_sponsor_information, bill_list_item
            )
            (
                curr_sponsor_name,
                curr_sponsor_party_membership,
                curr_sponsor_state,
                curr_sponsor_year_exp,
            ) = sponsor_information.result()

            sponsor_names.append(curr_sponsor_name)
            sponsor_party_memberships.append(curr_sponsor_party_membership)
            sponsor_states.append(curr_sponsor_state)
            sponsor_year_exps.append(curr_sponsor_year_exp)

            # Fifth Step, getting cosponsor information:
            # Sixth step, obtaining individual characteristics of the cosponsors
            (curr_number_of_cosponsors, curr_average_year_of_services,) = (
                "nan",
                "nan",
            )
            # ) = process_cosponsor_information(bill_list_item)

            number_of_cosponsors.append(curr_number_of_cosponsors)
            average_year_of_services.append(curr_average_year_of_services)

    result = zip(
        list_of_congreses,
        bill_names,
        bill_introduction_years,
        sponsor_names,
        sponsor_party_memberships,
        sponsor_states,
        sponsor_year_exps,
        number_of_cosponsors,
        average_year_of_services,
    )
    return result


def process_congress(congress):
    def get_page_soup(page_url):
        # getting the html text and reading it with beautiful soup
        congress_response: Response = requests.get(page_url, params=params)
        time.sleep(1)
        page_soup = BeautifulSoup(congress_response.text, "lxml")
        return page_soup

    print("Current congress: " + congress)
    # params = {"q": "", "pageSize": 100, "page": 1}
    params = {
        "q": '{"source":"legislation","congress":' + congress + "}",
        "pageSize": 100,
    }
    url_without_page = get_congress_url(params)

    # initializing empty file with headers
    save(init=True)

    results = []
    pages_to_scrape = PAGES_TO_SCRAPE[congress]

    page_urls = [
        url_without_page + f"&page={page_num}"
        for page_num in range(pages_to_scrape[0], pages_to_scrape[1] + 1)
    ]

    page_soups = map(get_page_soup, page_urls)
    process_page_arguments_list = [[congress, soup] for soup in page_soups]

    with concurrent.futures.ThreadPoolExecutor() as executor:

        results = [
            executor.submit(process_page, *args) for args in process_page_arguments_list
        ]  # these are future objects

        for f in concurrent.futures.as_completed(results):
            save(dataset=f.result())


@logger.catch()
def main(congresses: List[str]):

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_congress, congresses)

    # for congress in congresses:
    #     result = process_congress()
    #     save(congress=congress, dataset=result)


def get_congress_url(params: Dict[str, str]) -> str:
    """
    Function generates to url to be scraped for. This url contains bill names.

    Args:
        params: it contains query string, pageSize integer and page number
            e.g. {
                    "q": "{"source":"legislation"}",
                    "pageSize": 100,
                    "page": 1
                 }

    """

    return config.BASE_URL + "/search?" + urlencode(params)


def save(dataset=None, init=False) -> None:
    """
    Function creates file for each congress to save.

    Args:
        result: list of scraped data
        init: boolean value that specifies if function is called to generate
            empty file with headers, if it is not given or equal to False,
            provided dataset is appended to file
    """

    # generate path for each congress dataset
    path_to_save = f"{config.OUTPUT_DIR}/result.{config.OUTPUT_FORMAT}"

    file_mode = "a+"  # append to file, if there is no such file, create it
    with open(path_to_save, file_mode, newline="", encoding="UTF-8") as csv_file:
        writer = csv.writer(csv_file, delimiter=",")

        # if it's first time, add column names
        if init is True:
            writer.writerow(
                [
                    "Congress",
                    "Bill Name",
                    "Year Introduced",
                    "Sponsor",
                    "Sponsor party membership",
                    "Sponsor states",
                    "Sponsors years of service",
                    "Nr of co-sponsors",
                    "Average years of service among co-sponsors",
                ]
            )
        else:
            # If it's not first time, write given dataset to the file
            for result_member in list(dataset):
                writer.writerow(result_member)


def process_bill_information(bill_soup: BeautifulSoup) -> Tuple[str, str]:
    """
    Function scrapes name and introduction year of the bill from the page.

    Args:
        bill_soup: soup (provided by `bs4` library) object for the list item
            that has all the info of bill
    """
    # Getting name of teh bill
    raw_bill_name = bill_soup.find("span", {"class": "result-heading"}).text
    bill_name = raw_bill_name.split(" ")[0]
    # TODO Delete comment
    # print(f"Bill name: {bill_name}")

    # Getting the year of introduction of the bill
    bill_introduction_year = (
        bill_soup.find("a", target="_blank")
        .findNext(string=True)
        .findNext(string=True)
        .split("(")[-1]
        .split("/")[-1]
        .split(")")[0]
    )

    # TODO Delete comment
    # print(f"Year of introduction of the bill: {bill_introduction_year}")

    return bill_name, bill_introduction_year


def get_sponsor_soup_object(bill_soup: BeautifulSoup) -> BeautifulSoup:
    """
    Function returns soup object for sponsor page.

    Args:
        bill_soup: soup (provided by `bs4` library) object for the list item
            that has all the info of bill
    """

    relative_sponsor_link = bill_soup.find("strong").findNext(href=True).get("href")
    sponsor_link = config.BASE_URL + relative_sponsor_link

    sponsor_response: Response = requests.get(sponsor_link)
    time.sleep(1)

    sponsor_soup = BeautifulSoup(sponsor_response.text, "html.parser")
    return sponsor_soup


def process_sponsor_information(bill_soup: BeautifulSoup) -> Tuple[str, str, str, int]:
    """
    Function scrapes sponsor related data from the page which are sponsor name,
    party membership, sponsor state, length of service sponsor.

    Args:
        bill_soup: soup (provided by `bs4` library) object for the list item
            that has all the info of bill
    """

    # getting name of sponsor
    sponsor_name = bill_soup.find("strong", string="Sponsor:").findNext("a").text
    # TODO Delete comment
    # print(f"Sponsor name: {sponsor_name}")

    # getting sposnor soup object to work with
    sponsor_soup: BeautifulSoup = get_sponsor_soup_object(bill_soup)

    # getting party membership
    try:
        party_membership = (
            sponsor_soup.find("th", {"class": "member_party"}).findNext("td").text
        )
    except AttributeError:
        party_membership = "nan"

    # Extracting house column of table, and state
    member_chambers = sponsor_soup.findAll("th", {"class": "member_chamber"})

    if len(member_chambers) == 0:
        # if there is no chamber state return nan
        party_membership, sponsor_state, sponsor_year_exp = "nan", "nan", "nan"
        return sponsor_name, party_membership, sponsor_state, sponsor_year_exp

    elif len(member_chambers) == 1:
        raw_house = member_chambers[0].findNext("td").text
    else:
        raw_house = member_chambers[1].findNext("td").text

    sponsor_state = raw_house.split(",")[0]

    # extracting the range of years of service to calculate career time
    raw_service_years = raw_house.split("(")[1].replace(")", "")
    service_years = raw_service_years.split("-")
    start_year = int(service_years[0])

    if len(service_years) == 2:
        # if it has two elements, it means there is either start and end year,
        # or sponsor is still in service
        if service_years[1].strip() == "Present":
            end_year = config.LAST_YEAR
        else:
            end_year = int(service_years[1])

        sponsor_year_exp = end_year - start_year
    else:
        # sponsor served only one year
        sponsor_year_exp = 1

    return sponsor_name, party_membership, sponsor_state, sponsor_year_exp


def process_cosponsor_information(bill_soup: BeautifulSoup) -> Tuple[float, int]:
    """
    Function scrapes cosponsor related data from the page which are
    number of cosponsors and average year of service of cosponsors.

    Args:
        bill_soup: soup (provided by `bs4` library) object for the list item
            that has all the info of bill
    """

    # Finding co-sponsor tag from the page
    cosponsors_link_tag = bill_soup.find("strong", string="Cosponsors:").findNext("a")

    # as tag text contains number of cosponsors, if there is no cosponsor,
    # return that data accordingly
    if cosponsors_link_tag.text == "0":
        # TODO Delete comment
        # print(f"\n== This bill doesn't have any cosponsors.")
        count_of_cosponsors = 0
        average_year_of_service = float("nan")
        return count_of_cosponsors, average_year_of_service

    # get link to the cosponsors page and create soup object to work with
    cosponsors_link = config.BASE_URL + cosponsors_link_tag.get("href")
    cosponsor_response: Response = requests.get(cosponsors_link)

    time.sleep(1)

    cosponsors_soup = BeautifulSoup(cosponsor_response.text, "lxml")

    # getting table that has cosponsors and scrape cosponsor links
    # Note: this omits cosponsors who were withdrawn from the bill at later time
    cosponsors_table = cosponsors_soup.find("table", {"class": "item_table"})
    cosponsors = cosponsors_table.findAll("td", {"class": "actions"})

    cosponsors_with_href = [
        cosponsor.find(href=True) for _, cosponsor in enumerate(cosponsors)
    ]
    cosponsor_links = [
        cosponsor.get("href") for _, cosponsor in enumerate(cosponsors_with_href)
    ]

    count_of_cosponsors = len(cosponsor_links)

    # TODO Delete comment
    # print(f"\n== This bill has {count_of_cosponsors} cosponsors.")

    cosponsor_years_service = []
    for _, cosponsors_link in enumerate(cosponsor_links):
        (
            cosponsor_name,
            years_of_service,
        ) = get_characteristics_of_cosponsor(cosponsors_link)

        cosponsor_years_service.append(years_of_service)

        # TODO Delete comment
        # print(
        #     f'==== Cosponsor "{cosponsor_name}" had {years_of_service} year(s) of service.'
        # )

    average_year_of_service = sum(cosponsor_years_service) / count_of_cosponsors

    return count_of_cosponsors, average_year_of_service


def get_characteristics_of_cosponsor(cosponsor_link):
    """
    Function scrapes cosponsor related data from the page which are
    cosponsor name and years of service.

    Args:
        bill_soup: soup (provided by `bs4` library) object for the page
            that has all the info of cosponsor
    """

    # Loading html from an individual cosponsor
    cosponsor_response = requests.get(cosponsor_link)
    time.sleep(1)

    cosponsor_soup = BeautifulSoup(cosponsor_response.text, "lxml")

    raw_cosponsor_name = cosponsor_soup.find("h1", {"class": "legDetail"}).text

    # we split name from first parantheses
    # sample raw name: Representative Dan Burton (1938 - )In Congress 1983 - 2013
    cosponsor_name = raw_cosponsor_name.split("(")[0]

    # extracting years of service in the house exactly as before

    house = cosponsor_soup.find("th", {"class": "member_chamber"}).findNext("td").text
    house_split = house.split(",")

    if len(house_split) == 2:
        house_split = house_split[1]

    if len(house_split) == 1:
        house_split = house_split[0]

    service = house_split.split("(")[1]

    # Solving for the exception if sponsor only for one year:
    if len(service.split("-")) == 2:
        start_year = service.split("-")[0]
        start_year = int(start_year)
        end_year = service.split("-")[1].split(")")[0]
    else:
        start_year = service.split(")")[0]
        start_year = int(start_year)
        end_year = start_year + 1

    if end_year == "Present":
        end_year = "2021"

    end_year = int(end_year)

    # transforming the variable into integers and calculating years of service.
    years_of_service = end_year - start_year

    return cosponsor_name, years_of_service


if __name__ == "__main__":
    start = time.perf_counter()
    congresses = ["110"]
    main(congresses)
    end = time.perf_counter()
    print(f"Finsihed in {round(end - start, 2)} seconds.")
