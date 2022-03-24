1. počítejte četnost slov v textu vstupního streamu
* jedná se o rozšířenou verzi úlohy z přednášky na Structured Streaming

* stream bude číst textová data přes TCP socket z vámi vybraného portu
  - data si budete předávat pomocí netcat

* program počítá četnost slov na vstupu
  - slova jsou před zpracováním převedena na malá písmena a je odstraněna interpunkce
  - slova jsou seřazena od nejčetnějšího po nejméně četná

* výstup je předáván na konzoli, kde je i vypisován

* doplňte funkcionalitu pro zpracování posuvným oknem
  - okno délky 30 sekund s posunem 15 sekund
  - bude potřeba nový sloupecs aktuálním časem
  - pro přehlednost zobrazte začátek i konec okna
  - výsledek setřiďte prvně podle začátku okna a následně podle četnosti

2. četnost slov v souborech přidávaných do adresáře
* úlohu z prvního cvičení upravte
* vstupem tentokrát bude každý soubor vložený do vybraného adresáře
* vyhodnocení by mělo být spuštěno po každém vloženém souboru
* okna pro tento příklad ignorujte