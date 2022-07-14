import csv

import requests
from bs4 import BeautifulSoup


def write_csv_file(data):
    if not isinstance(
        data[0], list
    ):  # если это не список списков то выполняется этот блок (так записываем колонки в csv)
        with open("bills.csv", "a") as file:
            writer = csv.writer(file)
            writer.writerow(data)

    else:
        with open("bills.csv", "a") as file:
            writer = csv.writer(file)
            writer.writerows(data)


def main():
    data = []  # список, где будем хранить bills
    headers_csv = [
        "Bill Name",
        "Congress Number",
        "Year Introduced",
        "Sponsor",
        "Sponsor party membership",
        "Cosponsors",
    ]

    write_csv_file(headers_csv)

    for congress_id in range(110, 116):
        url = f"https://www.congress.gov/search?q=%7B%22source%22%3A%22legislation%22%2C%22congress%22%3A%22{congress_id}%22%7D&pageSize=100&page=1"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "lxml")
        count_pages = int(
            soup("span", {"class": "results-number"})[1].text.strip()[3:]
        )  # получаем кол-во страниц для данного конгресса

        for page_number in range(1, count_pages + 1):
            url = f"https://www.congress.gov/search?q=%7B%22source%22%3A%22legislation%22%2C%22congress%22%3A%22{congress_id}%22%7D&pageSize=100&page={page_number}"
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "lxml")
            bills = soup.find("div", {"id": "main"}).find_all(
                "li", {"class": "expanded"}
            )  # получаем все bills со страницы в виде списка

            print(f"Конгресс {congress_id} страница {page_number}")
            for bill in bills:
                try:
                    if (
                        bill.find("span", {"class": "visualIndicator"}).text.strip()
                        == "BILL"
                    ):
                        bill_name = bill.find("a").text.strip()
                        congress_number = (
                            bill.find("span", {"class": "result-heading"})
                            .text.split("—")[-1]
                            .strip()[:3]
                        )
                        info_bill = bill.find("span", {"class": "result-item"})

                        sponsor_fullname = (
                            " ".join(info_bill.find("a").text.strip().split(" ")[1:-1])
                            .strip()
                            .split("[")[0]
                            .strip()
                        )
                        year_introduced = (
                            info_bill.text.strip().split(" ")[-3].strip()[:-1]
                        )
                        sponsor_party_membership = (
                            info_bill.find("a").text.strip().split(" ")[0].strip()[:-1]
                        )
                        cosponsors = info_bill.find_all("a")[1].text.strip()

                        data.append(
                            [
                                bill_name,
                                congress_number,
                                year_introduced,
                                sponsor_fullname,
                                sponsor_party_membership,
                                cosponsors,
                            ]
                        )

                except Exception as err:
                    continue

                else:
                    continue

            if len(data) > 0:
                write_csv_file(data)
                data = []


if __name__ == "__main__":
    main()
