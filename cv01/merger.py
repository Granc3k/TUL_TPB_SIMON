import json
import os

class Article:
    def __init__(self, url, title, content, tags, photo_count, time, comment_count):
        self.url = url
        self.title = title
        self.content = content
        self.tags = tags
        self.photo_count = photo_count
        self.time = time
        self.comment_count = comment_count

class NewArticle:
    def __init__(self, article, category):
        self.url = article.get('url', '')
        self.title = article.get('title', '')
        self.content = article.get('content', '')
        self.tags = article.get('tags', [])
        self.photo_count = article.get('photo_count', 0)
        self.time = article.get('time', '')
        self.comment_count = article.get('comment_count', 0)
        self.category = category

def main():    
    file_path = './cv01/data/90a9ae9e-1fa6-4e49-b125-6df7e744f626/90a9ae9e-1fa6-4e49-b125-6df7e744f626_final.json'
    with open(file_path, 'r', encoding='utf-8') as open_file:
        try:
            articles = json.load(open_file)
            print(f"Načteno {len(articles)} článků.")  # Ladicí výstup
        except json.JSONDecodeError as e:
            print(f"Chyba při načítání JSON souboru: {e}")
            articles = []

    new_articles = []
    categories = []

# Iterace přes články a filtrování těch s neprázdným titulkem
    for article in [a for a in articles if 'title' in a and a['title']]:
        url = article['url']
        split = url.split('/')
        print(f"Zpracovávám URL: {url} -> split: {split}")  # Ladicí výstup pro URL a split
        try:
            if len(split) > 3 and split[2] == "www.idnes.cz":
                category = split[3]
                if category == "revue" and len(split) > 5:
                    category = split[4]
                if len(category) > 12:
                    category = split[3]
                new_article = NewArticle(article, category)
                new_articles.append(new_article.__dict__)  # Přidání nového článku s kategorií
                print(f"Přidán článek: {new_article.title}, kategorie: {new_article.category}")  # Ladicí výstup
        except Exception as e:
            print(f"Chyba při zpracování článku: {e}")
            continue

# Uložení nových dat do JSON souboru
    new_data_path = './cv01/data/90a9ae9e-1fa6-4e49-b125-6df7e744f626/newData.json'
    with open(new_data_path, 'w', encoding='utf-8') as create_file:
        json.dump(new_articles, create_file, ensure_ascii=False, indent=4)
        print(f"Uloženo {len(new_articles)} článků do {new_data_path}.")  # Ladicí výstup

if __name__ == '__main__':
    main()
