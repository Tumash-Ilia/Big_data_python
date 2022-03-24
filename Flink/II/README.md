1. zjistěte celkovou výši objednávek pro každého zákazníka

* k dispozici máte soubor customer-orders.csv (data)

* vytvořte skript vracející pro každého zákazníka celkovou utracenou částku
  - skript pojmenujte total-spent-by-customer.py
  - řešte pomocí dávkového zpracování

* celkovou částku zaokrouhlete na dvě desetinná místa
* a uložte do sloupce total spent
* výsledný seznam vraťte setříděný podle celkové částky
* BONUS: úlohu vyřešte i ve stream módu
  - seznam netřidte

2. nalezněte deset filmů s nejvíce nejlepšími hodnoceními (5 *)

* kdispozici máte soubory u.data (hodnocení) a u-mod.items (filmy)
  - data z MovieLens (datové sady), info viz přednášky

* vytvořte skript vracející 10 filmů s nejvíce hodnoceními známkou 5
  - skript pojmenujte most-hyped-movies.py

* výsledný seznam vraťte setříděný podle počtu nejlepších hodnocení
  - počettaké vypisujte

* ze souboru u-mod.items zjistěte k nalezeným filmům pravá jména

* BONUS: k finální tabulce přidejte sloupec počítající poměr nejlepších
hodnocení daného filmu vůči všem hodnocením daného filmu
  - sestupně setřiďte podle tohoto sloupce
