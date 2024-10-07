import json

# Načtení dat z JSON souboru
def load_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

# Uložení dat do JSON souboru
def save_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# Filtrace dat podle URL
def filter_data(data):
    seen_urls = set()
    filtered_data = []
    duplicates_count = 0

    for item in data:
        url = item.get("url")
        if url in seen_urls:
            duplicates_count += 1
        else:
            seen_urls.add(url)
            filtered_data.append(item)

    return filtered_data, duplicates_count

# Hlavní funkce
def main(input_file, output_file):
    # Načti data
    data = load_json(input_file)
    
    # Filtrovat data a počítat duplicity
    filtered_data, duplicates_count = filter_data(data)
    
    # Uložení vyfiltrovaných dat
    save_json(filtered_data, output_file)
    
    # Výpis počtu duplicit
    print(f"Počet duplicitních záznamů: {duplicates_count}")

if __name__ == "__main__":
    main("./cv01/data/90a9ae9e-1fa6-4e49-b125-6df7e744f626/edited_Data.json", "./cv02/data/filtered_data.json")
