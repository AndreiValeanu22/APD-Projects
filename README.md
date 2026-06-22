# APD-Projects — Algoritmi Paraleli și Distribuiți (UPB ACS)

Proiecte personale pentru cursul **APD** (Anul 3, **CB**), an universitar **2025–2026**.

| Folder | Temă | Tehnologii |
|--------|------|------------|
| [**Tema_1**](Tema_1/) | Bază de date paralelă de știri | Java, Threads, Jackson JSON |
| [**Tema_2**](Tema_2/) | CHORD distribuit | C, MPI |

---

## Tema 1 — Procesare paralelă articole (Java Threads)

**Enunț (rezumat):** Construirea unei baze de date de articole de știri procesată **paralel** cu un pool de thread-uri. Fiecare `DatabaseWorker` prelucrează articole din cozi partajate; la final se generează indexări pe limbă și categorie, numărătoare de cuvinte cheie și un raport agregat.

**Structură:**
- `Tema_1/src/` — cod Java (`Main`, `Database`, `DatabaseWorker`, `Article`, `Inputs`, `DatabaseReport`)
- `Tema_1/checker/` — scripturi de testare și date de intrare/ieșire așteptată

**Compilare & rulare:**
```bash
cd Tema_1/src
make build
make run ARGS="<nr_threaduri> <articles.txt> <inputs.txt>"
```

**Testare automată:**
```bash
cd Tema_1/checker
./checker.sh test_small
./checker.sh test_1
```

**Detalii tehnice:**
- Citire articole JSON (Jackson) din fișiere listate în `articles.txt`
- Sincronizare între thread-uri la nivel de `Database`
- Output: fișiere index, raport statistic, articole procesate

---

## Tema 2 — CHORD în MPI

> Enunț oficial (skeleton): *CHORD in MPI*

**Enunț (rezumat):** Implementarea protocolului **CHORD** (Distributed Hash Table) folosind **MPI** — mesaje doar cu `MPI_Send` / `MPI_Recv`. Inelul de noduri, maparea ID→rank și citirea inputului sunt furnizate; se completează finger table, rutarea logaritmică (`closest_preceding_finger`) și lookup-urile distribuite.

**Structură:**
- `Tema_2/src/tema2.c` — implementare CHORD
- `Tema_2/checker/` — teste `test1` … `test8` + `checker.sh`
- `Tema_2/local.sh` — build și rulare locală cu MPI

**Compilare & test:**
```bash
cd Tema_2
./local.sh
cd checker && ./checker.sh
```

**Tag-uri MPI folosite:** `TAG_LOOKUP_REQ`, `TAG_LOOKUP_REP`, `TAG_DONE`

**Parametri CHORD (skeleton):** `M=4` → inel de 16 ID-uri; noduri cu succesor, predecesor și finger table de completat.

---

## Licență & utilizare

Cod sursă propriu, teme academice **UPB ACS**. Poți folosi repo-ul ca portofoliu; te rog menționează cursul APD dacă refolosești structura.
