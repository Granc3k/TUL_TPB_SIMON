# Made by Martin "Granc3k" Šimon, Jiří "Solus" Šeps
import os
import json
import uuid
import requests
from pyquery import PyQuery
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
import time
import random

# Vytvoření session s cookies
session = requests.Session()
session.cookies.set(
    name="euconsent-v2",
    value="CQFwyQAQFwyQAAHABBENBIFgAP_gAAAAAAAAJqIBJC5kBSFCAGJgYNkAIAAWxxAAIAAAABAAgAAAABoAIAgAEAAwAAQABAAAABAAIEIAAABACABAAAAAQAAAAQAAAAAQAAAAAQAAAAAAAiBACAAAAABAAQAAAABAQAAAgAAAAAIAQAAAAAEAgAAAAAAAAAAAABAAAQgAAAAAAAAAAAAAAAAAAAAAAAAAABBAAAAAAAAAAAAAAAAAwAAADBIAMAAQU6HQAYAAgp0SgAwABBTopABgACCnRCADAAEFOi0AGAAIKdAA.f_wAAAAAAAAA",
)
session.cookies.set(
    name="dCMP",
    value="mafra=1111,all=1,reklama=1,part=0,cpex=1,google=1,gemius=1,id5=1,next=0000,onlajny=0000,jenzeny=0000,databazeknih=0000,autojournal=0000,skodahome=0000,skodaklasik=0000,groupm=1,piano=1,seznam=1,geozo=0,czaid=1,click=1,verze=2,",
)

# Fronta pro předávání zpracovaných článků
queue = Queue()


def get_page(url):
    """Získá obsah stránky pomocí requests s použitím cookies."""
    res = session.get(url)
    return res.text


def get_article_urls(html):
    """Získá URL článků pomocí PyQuery."""
    pq = PyQuery(html)
    article_urls = pq("[score-type='Article']").items()
    return article_urls


def scrape_article_data(url):
    """Získá a zpracuje data jednotlivých článků."""
    try:
        print(f"Scrapuju článek: {url}")
        html = get_page(url)
        pq = PyQuery(html)

        title = pq.find("h1").text()
        content = [pq.find(".opener").text()] + [
            PyQuery(e).text() for e in pq.find("#art-text p")
        ]
        tags = [PyQuery(e).text() for e in pq.find("#art-tags > a")]
        photo_count = (
            int(pq.find(".more-gallery span").text()[0])
            if pq.find(".more-gallery span").text()
            else 0
        )
        time = pq.find(".art-info .time span.time-date").attr("content")
        comment_count = (
            int(pq.find("#moot-linkin span").text().split()[0][1:])
            if pq.find("#moot-linkin span").text()
            else 0
        )

        return {
            "url": url,
            "title": title,
            "content": content,
            "tags": tags,
            "photo_count": photo_count,
            "time": time,
            "comment_count": comment_count,
        }
    except Exception as e:
        print(f"Chyba při scrapování článku {url}: {e}")
        return None


def scrapper(page_number, session_uuid):
    """Funkce producenta pro scrapování článků a jejich vkládání do fronty."""
    url = f"https://www.idnes.cz/zpravy/archiv/{page_number}"
    print(f"Scrapuju stránku: {url}")

    try:
        page_html = get_page(url)
        article_urls = get_article_urls(page_html)

        for article_url in article_urls:
            try:
                article_data = scrape_article_data(article_url.attr("href"))
                if article_data:
                    queue.put(article_data)
                # Přidání náhodné pauzy mezi 1-3 sekundy
                time.sleep(random.uniform(0.1, 0.5))
            except Exception as e:
                print(f"Chyba při scrapování článku {article_url.attr('href')}: {e}")
    except Exception as e:
        print(f"Chyba při scrapování stránky {url}: {e}")


def saver(thread_number, session_uuid, max_size_mb, directory="./cv01/data"):
    """Funkce Savera pro zápis článků z fronty do JSON souboru."""
    file_path = os.path.join(directory, f"{session_uuid}_{thread_number}.json")

    os.makedirs(directory, exist_ok=True)

    # Zápis do souboru probíhá v nekonečné smyčce
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump([], f)  # Vytvoření prázdného JSON souboru

    articles = []
    while True:
        article_data = queue.get()
        if article_data is None:
            # Pokud se do fronty vloží None, ukončujeme savery
            print(f"Saver {thread_number} ukončen, dostal None.")
            break

        articles.append(article_data)

        # Zápis do souboru každých 10 článků nebo když je fronta prázdná
        if len(articles) >= 100 or queue.empty():
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(articles, f, ensure_ascii=False, indent=4)

        # Kontrola aktuální velikosti souboru
        current_size_mb = get_file_size(file_path)
        print(f"Saver {thread_number} - aktuální velikost: {current_size_mb:.2f} MB")
        if current_size_mb >= max_size_mb:
            print(
                f"Saver {thread_number} - dosaženo limitu velikosti: {current_size_mb:.2f} MB, ukončování Savera."
            )
            queue.put(None)  # Signalizace pro zastavení producentů
            break


def merge_files(session_uuid, num_threads, directory="./cv01/data"):
    """Sloučí soubory z různých vláken do jednoho souboru."""
    final_file_path = os.path.join(directory, f"{session_uuid}_final.json")
    merged_articles = []

    for thread_number in range(num_threads):
        file_path = os.path.join(directory, f"{session_uuid}_{thread_number}.json")
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                articles = json.load(f)
                merged_articles.extend(articles)

    with open(final_file_path, "w", encoding="utf-8") as f:
        json.dump(merged_articles, f, ensure_ascii=False, indent=4)

    print(f"Soubory sloučeny do: {final_file_path}")


def main(max_size_mb, max_workers=10, num_threads=10):
    """Hlavní funkce pro scrapování článků."""
    session_uuid = str(uuid.uuid4())
    session_directory = os.path.join("./cv01/data", session_uuid)
    os.makedirs(session_directory, exist_ok=True)

    # Spuštění Saverů v samostatných vláknech
    saver_threads = []
    for thread_number in range(num_threads):
        saver_thread = threading.Thread(
            target=saver,
            args=(thread_number, session_uuid, max_size_mb, session_directory),
        )
        saver_thread.start()
        saver_threads.append(saver_thread)

    page_number = 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while (
            get_file_size(os.path.join(session_directory, f"{session_uuid}_0.json"))
            < max_size_mb
        ):
            futures = [
                executor.submit(scrapper, page, session_uuid)
                for page in range(page_number, page_number + 10)
            ]
            page_number += 10

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Chyba v producentovi: {e}")

            if (
                get_file_size(os.path.join(session_directory, f"{session_uuid}_0.json"))
                >= max_size_mb
            ):
                break

    # Signál pro ukončení Saverů
    for _ in range(num_threads):
        queue.put(None)

    for saver_thread in saver_threads:
        saver_thread.join()

    # Sloučení souborů
    merge_files(session_uuid, num_threads, session_directory)

    print(
        f"Scrapování dokončeno. Konečná velikost souboru: {get_file_size(os.path.join(session_directory, f'{session_uuid}_final.json')):.2f} MB"
    )


def get_file_size(filepath):
    """Získá velikost konkrétního souboru v MB."""
    if os.path.exists(filepath):
        return os.path.getsize(filepath) / (1024 * 1024)
    return 0


if __name__ == "__main__":
    #max_size_mb = 12  # nastavení target velikosti pro male soubory pro ukládání
    #main(max_size_mb, 400, 100)
    merge_files("90a9ae9e-1fa6-4e49-b125-6df7e744f626",100, "./cv01/data/90a9ae9e-1fa6-4e49-b125-6df7e744f626")
