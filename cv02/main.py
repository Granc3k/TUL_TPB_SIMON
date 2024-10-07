import json
from collections import Counter
from datetime import datetime

def parse_time(time_str):
    try:
        if isinstance(time_str, str):
            return datetime.fromisoformat(time_str)
    except ValueError:
        return None
    return None

def count_covid_mentions(text):
    # Normalizujeme text na malá písmena a hledáme různé varianty "covid"
        normalized_text = text.lower()
        return normalized_text.count('covid') + normalized_text.count('covid-19')+ normalized_text.count('covid 19')

def main(file_name):
    # Načtení dat
    with open(file_name, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # 1. Počet článků
    pocet_clanku = len(data)
    print(f'Počet článků: {pocet_clanku}')

    # 2. Počet duplicitních článků (na základě URL)
    unikatni_clanky = set(item['url'] for item in data)
    pocet_duplicit = pocet_clanku - len(unikatni_clanky)
    print(f'Počet duplicitních článků: {pocet_duplicit}')

    # 3. Datum nejstaršího článku (s kontrolou, že čas je platný)
    validni_casy = [item for item in data if parse_time(item['time']) is not None]
    nejstarsi_clanek = min(validni_casy, key=lambda x: parse_time(x['time']))
    datum_nejstarsiho = nejstarsi_clanek['time']
    print(f'Nejstarší článek byl publikován: {datum_nejstarsiho}')

    # 4. Název článku s nejvíce komentáři
    nejvic_komentaru_clanek = max(data, key=lambda x: x['comment_count'])
    nazev_nejvic_komentaru = nejvic_komentaru_clanek['title']
    print(f'Název článku s nejvíce komentáři: {nazev_nejvic_komentaru}')

    # 5. Nejvyšší počet přidaných fotek u článku
    nejvic_fotek = max(item['photo_count'] for item in data)
    print(f'Nejvyšší počet přidaných fotek u článku: {nejvic_fotek}')

    # 6. Počty článků podle roku publikace (jen platné časy)
    clanky_podle_roku = Counter(parse_time(item['time']).year for item in validni_casy)
    print(f'Počty článků podle roku publikace: {dict(clanky_podle_roku)}')

    # 7. Počet unikátních kategorií a počet článků v každé kategorii
    kategorie_pocet = Counter(item['category'] for item in data)
    print(f'Počet unikátních kategorií: {len(kategorie_pocet)}')
    print(f'Počty článků v každé kategorii: {dict(kategorie_pocet)}')

    # 8. 5 nejčastějších slov v názvu článků z roku 2021
    tituly_2021 = [item['title'] for item in validni_casy if parse_time(item['time']).year == 2021]
    slova_2021 = ' '.join(tituly_2021).split()
    nejcastejsi_slova_2021 = Counter(slova_2021).most_common(5)
    print(f'5 nejčastějších slov v názvu článků z roku 2021: {nejcastejsi_slova_2021}')

    # 9. Celkový počet komentářů
    celkem_komentaru = sum(item['comment_count'] for item in data)
    print(f'Celkový počet komentářů: {celkem_komentaru}')

    # 10. Celkový počet slov ve všech článcích
    celkem_slov = sum(len(' '.join(item['content']).split()) for item in data)
    print(f'Celkový počet slov ve všech článcích: {celkem_slov}')
    
    print("-------- BONUS --------")
    
    # Bonusové úlohy
    
    # 1. 8 nejčastějších slov ve všech článcích (odfiltrujeme krátká slova < 6 písmen)
    vsechna_slova = ' '.join(' '.join(item['content']) for item in data).split()
    dlouha_slova = [word for word in vsechna_slova if len(word) >= 6]
    nejcastejsi_dlouha_slova = Counter(dlouha_slova).most_common(8)
    print(f'8 nejčastějších dlouhých slov: {nejcastejsi_dlouha_slova}')
    
    # 2. 3 články s nejvyšším počtem výskytů slova "Covid-19"
    covid_clanky = [(item['title'], count_covid_mentions(' '.join(item['content']))) for item in data]
    covid_clanky_sorted = sorted(covid_clanky, key=lambda x: x[1], reverse=True)[:3]
    print(f'3 články s nejvyšším počtem výskytů "Covid-19": {covid_clanky_sorted}')

    
    # 3. Články s nejvyšším a nejnižším počtem slov
    clanky_podle_slov = [(item['title'], len(' '.join(item['content']).split())) for item in data]
    nejdelsi_clanek = max(clanky_podle_slov, key=lambda x: x[1])
    nejkratsi_clanek = min(clanky_podle_slov, key=lambda x: x[1])
    print(f'Článek s nejvyšším počtem slov: {nejdelsi_clanek}')
    print(f'Článek s nejnižším počtem slov: {nejkratsi_clanek}')
    
    # 4. Průměrná délka slova přes všechny články
    celkova_delka_slov = sum(len(word) for word in vsechna_slova)
    prumerna_delka_slova = celkova_delka_slov / len(vsechna_slova)
    print(f'Průměrná délka slova: {prumerna_delka_slova:.2f}')
    
    # 5. Měsíce s nejvíce a nejméně publikovanými články + počet článků v každém měsíci
    clanky_podle_mesice = Counter((parse_time(item['time']).year, parse_time(item['time']).month) for item in validni_casy)

    # Najdi měsíc s nejvíce a nejméně články
    nejvic_clanku_mesic = max(clanky_podle_mesice, key=lambda x: clanky_podle_mesice[x])
    nejmin_clanku_mesic = min(clanky_podle_mesice, key=lambda x: clanky_podle_mesice[x])

    print(f'Počet článků podle měsíců: {dict(clanky_podle_mesice)}')
    print(f'Měsíc s nejvíce publikovanými články: {nejvic_clanku_mesic} ({clanky_podle_mesice[nejvic_clanku_mesic]} článků)')
    print(f'Měsíc s nejméně publikovanými články: {nejmin_clanku_mesic} ({clanky_podle_mesice[nejmin_clanku_mesic]} článků)')
    

if __name__ == "__main__":
    #main('./cv01/data/90a9ae9e-1fa6-4e49-b125-6df7e744f626/edited_Data.json')
    main('./cv02/data/filtered_data.json')
