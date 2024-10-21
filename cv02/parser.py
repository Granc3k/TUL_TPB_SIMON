import json

# Načtení dat z původního JSON souboru
with open('./cv02/data/filtered_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Předpokládáme, že data obsahují více objektů, každý jako článek
# Zde vytáhneme hodnoty z pole "content", "tags" a "photo_count"
output_data = []

# Předpokládejme, že data jsou uložena jako seznam článků
if isinstance(data, list):
    for article in data:
        # Získáme hodnoty podle potřeby
        content_values = article.get('content', [])
        tags_values = article.get('tags', [])
        photo_count_value = article.get('photo_count', 0)

        # Uložení do výstupního seznamu
        output_data.extend(content_values)  # přidání obsahu
        output_data.extend(tags_values)      # přidání tagů
        output_data.append(photo_count_value)  # přidání počtu fotek
else:
    # Pokud je struktura jiná (např. jednotlivý objekt místo seznamu)
    content_values = data.get('content', [])
    tags_values = data.get('tags', [])
    photo_count_value = data.get('photo_count', 0)

    output_data.extend(content_values)
    output_data.extend(tags_values)
    output_data.append(photo_count_value)

# Uložení dat do nového JSON souboru, každý prvek na jednom řádku
with open('vystup.json', 'w', encoding='utf-8') as f:
    for item in output_data:
        json.dump(item, f, ensure_ascii=False)
        f.write('\n')  # Přidání nového řádku po každém prvku
