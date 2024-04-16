import requests
from bs4 import BeautifulSoup
import csv
import os
from concurrent.futures import ThreadPoolExecutor

def fetch_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def prepare_csv_path_and_write(folder_name, file_name, rows):
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, f"{file_name}.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Artist", "Title"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Data written to {file_path}")

def process_chart_year(chart_name, year, source):
    entries = []
    if source == "Acharts":
        url = f"https://acharts.co/{chart_name.lower().replace(' ', '_')}/year/{year}"
    else:  # Billboard
        url = f"https://www.billboard.com/charts/year-end/{year}/{chart_name}"
    soup = fetch_html(url)
    if soup:
        if source == "BillBoard":
            for entry_soup in soup.select("ul.o-chart-results-list-row"):
                title = entry_soup.select_one("#title-of-a-story").text.strip()
                artist = entry_soup.select_one("#title-of-a-story + span.c-label").text.strip()
                entries.append({'Title': title, 'Artist': artist})
        elif source == "Acharts":
            parsed_entries = soup.find_all("tr", itemprop="itemListElement")
            for entry in parsed_entries:
                artist = entry.find("span", itemprop="byArtist").text.strip()
                title = entry.find("span", itemprop="name").text.strip()
                entries.append({"Artist": artist, "Title": title})
        folder_name = os.path.join("Year End Chart Data For Database")
        file_name = f"{chart_name.lower().replace(' ', '_')}-{year}"
        prepare_csv_path_and_write(folder_name, file_name, entries)

if __name__ == "__main__":
    acharts_list = [
        "Australia Singles Top 50",
        "Austria Singles Top 75",
        "Belgium Singles Top 50",
        "Bulgaria Singles Top 40",
        "Canada Singles Top 100",
        "Denmark Singles Top 40",
        "Dutch Top 40",
        "Finland Singles Top 20",
        "France Singles Top 100",
        "Ireland Singles Top 100",
        "Norway Singles Top 20",
        "Portugal Singles Top 50",
        "Sweden Singles Top 100",
        "Swiss Singles Top 100",
        "UK Singles Top 75",
        "US Singles Top 100",
    ]

    billboard_chart_names = [
        "hot-dance-electronic-songs",
        "hot-rock-songs",
        "hot-country-songs",
        "hot-r-and-and-b-songs",
        "hot-rap-songs",
        "smooth-jazz-songs",
        "classical-albums",
        "blues-albums",
        "hot-100-songs",
        "billboard-global-200",
        "pop-songs"
        "hot-hard-rock-songs"
    ]

    year_range = range(2003, 2023)

    with ThreadPoolExecutor(max_workers=os.cpu_count() * 3) as executor:
        futures = []
        for year in year_range:
            for chart_name in acharts_list:
                futures.append(executor.submit(process_chart_year, chart_name, year, "Acharts"))
            for chart_name in billboard_chart_names:
                futures.append(executor.submit(process_chart_year, chart_name, year, "BillBoard"))
        for future in futures:
            future.result()
import requests
from bs4 import BeautifulSoup
import csv
import os
from concurrent.futures import ThreadPoolExecutor

session = requests.Session()

def fetch_html(url):
    try:
        response = session.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def prepare_csv_path_and_write(folder_name, file_name, rows):
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, f"{file_name}.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Artist", "Title"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Data written to {file_path}")

def process_chart_year(chart_name, year, source):
    entries = []
    if source == "Acharts":
        url = f"https://acharts.co/{chart_name.lower().replace(' ', '_')}/year/{year}"
    else:  # Billboard
        url = f"https://www.billboard.com/charts/year-end/{year}/{chart_name}"
    soup = fetch_html(url)
    if soup:
        if source == "BillBoard":
            for entry_soup in soup.select("ul.o-chart-results-list-row"):
                title = entry_soup.select_one("#title-of-a-story").text.strip()
                artist = entry_soup.select_one("#title-of-a-story + span.c-label").text.strip()
                entries.append({'Title': title, 'Artist': artist})
        elif source == "Acharts":
            parsed_entries = soup.find_all("tr", itemprop="itemListElement")
            for entry in parsed_entries:
                artist = entry.find("span", itemprop="byArtist").text.strip()
                title = entry.find("span", itemprop="name").text.strip()
                entries.append({"Artist": artist, "Title": title})
        folder_name = os.path.join("Year End Chart Data For Database For Paul")
        file_name = f"{chart_name.lower().replace(' ', '_')}-{year}"
        prepare_csv_path_and_write(folder_name, file_name, entries)

if __name__ == "__main__":
    acharts_list = [
        "Australia Singles Top 50",
        "Austria Singles Top 75",
        "Belgium Singles Top 50",
        "Bulgaria Singles Top 40",
        "Canada Singles Top 100",
        "Denmark Singles Top 40",
        "Dutch Top 40",
        "Finland Singles Top 20",
        "France Singles Top 100",
        "Ireland Singles Top 100",
        "Norway Singles Top 20",
        "Portugal Singles Top 50",
        "Sweden Singles Top 100",
        "Swiss Singles Top 100",
        "UK Singles Top 75",
        "US Singles Top 100",
    ]

    billboard_chart_names = [
        "hot-dance-electronic-songs",
        "hot-rock-songs",
        "hot-country-songs",
        "hot-r-and-and-b-songs",
        "hot-rap-songs",
        "smooth-jazz-songs",
        "classical-albums",
        "blues-albums",
        "hot-100-songs",
        "billboard-global-200",
        "pop-songs"
        "hot-hard-rock-songs"
    ]

    year_range = range(2003, 2023)

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(process_chart_year, chart_name, year, source)
                   for year in year_range for chart_name, source in
                   [(chart_name, "Acharts") for chart_name in acharts_list] +
                   [(chart_name, "BillBoard") for chart_name in billboard_chart_names]]
        for future in futures:
            future.result()