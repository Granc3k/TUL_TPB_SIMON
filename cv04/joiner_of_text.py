# made by Martin "Granc3k" Šimon
import json
import os
from concurrent.futures import ThreadPoolExecutor

# Vytvoření složky pro dočasné soubory
temp_dir = "./cv04/data/temp"
os.makedirs(temp_dir, exist_ok=True)


def process_chunk(chunk, index):
    content_string = ""
    for record in chunk:
        content_string += " ".join(record["content"])
    temp_file_path = os.path.join(temp_dir, f"temp_{index}.txt")
    with open(temp_file_path, "w", encoding="utf-8") as file:
        file.write(content_string)
    return temp_file_path


def main():
    """
    Načte data z JSON souboru a následně to uloží do jednoduchého texťáku
    """
    print("načítám data")
    # Načtení souboru
    with open("./cv04/data/idnes_articles.json", "r", encoding="utf-8") as file:
        data = json.load(file)
    print("data načtena")

    # Rozdělení dat na chunks po 30 000 záznamech
    chunk_size = 30000
    chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Zpracování chunks ve vláknech
    temp_files = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_chunk, chunk, i) for i, chunk in enumerate(chunks)
        ]
        for future in futures:
            temp_files.append(future.result())

    # Spojení všech dočasných souborů do jednoho
    print("ukládám do souboru")
    with open(
        "./cv04/data/idnes_articles_text_better.txt", "w", encoding="utf-8"
    ) as final_file:
        for temp_file in temp_files:
            with open(temp_file, "r", encoding="utf-8") as file:
                final_file.write(file.read())
            os.remove(temp_file)  # Odstranění dočasného souboru


if __name__ == "__main__":
    main()
