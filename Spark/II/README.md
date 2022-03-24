1. zjistěte průměrný počet kamarádů podle věku
* k dispozici máte soubory fakefriends-header.csv (data) a spark-sgl-cv.py (kód)
* vhodně rozšiřte kód
* pro řešení můžete použít SOL dotazy nebo funkce přímo nad DataFrame

2. zjistěte celkovou výši objednávek pro každého zákazníka k dispozice máte soubor customer-orders.csv (data)
* vytvořte skript vracející pro každého zákazníka celkovou utracenou částku
  - skript pojmenujte total-spent-by-customer-dataframes.py
  - řešení postavte na DataFramech
* celkovou částku zaokrouhlete na dvě desetinná místa a uložte do
sloupce total spent, výsledný seznam vraťte setříděný podle celkové částky
  - tip: funkce agg
    
3. nalezněte všechny superhrdiny, kteří mají nejméně (1) propojení
* kdispozici máte soubory marvel-graph.txt, marvel-names.txt a heroes.py
* vhodně upravte a rozšiřte kód
* skript pojmenujte most-obscure-superheroes.py a řešte pomocí DataFramů
* řešení navrhněte bez předpokladu minimálního počtu propojení 1
