/************************************************************
 * TEMA 2 – Algoritmi Paraleli și Distribuiți
 * Protocolul CHORD în MPI
 *
 * Studenții trebuie să implementeze:
 *   1. build_finger_table() - construirea finger table CHORD
 *   2. closest_preceding_finger() - rutarea logaritmică
 *   3. handle_lookup_request() - logica de lookup distribuit
 *   4. inițierea lookup-urilor locale
 *   5. service-loop-ul distribuit
 *
 * Ce este deja implementat:
 *   - citirea inputului
 *   - formarea inelului CHORD static
 *   - maparea ID -> rank
 *   - structurile de date CHORD
 *   - primitive pentru succesor, predecesor, ID-uri globale
 *
 * Tema se rezolvă STRICT folosind:
 *   - MPI_Send / MPI_Recv
 *   - mesaje cu tag-urile TAG_LOOKUP_REQ, TAG_LOOKUP_REP și TAG_DONE
 ************************************************************/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define M 4 // 2^m = 16 ID-uri posibile
#define RING_SIZE 16
#define MAX_NODES 32
#define MAX_PATH 32

/************************************************************
 * Structuri CHORD
 ************************************************************/
typedef struct {
    int start;              // start[i] = (id + 2^i) mod 2^m
    int node;               // finger[i] = succesor(start)
} Finger;

typedef struct {
    int id;                 // ID CHORD al nodului curent
    int successor;          // succesorul în inel
    int predecessor;        // predecesorul în inel
    Finger finger[M];       // finger table-ul (de completat de voi)
} NodeState;

typedef struct {
    int initiator_id;       // cine a inițiat lookup-ul
    int current_id;         // nodul care procesează mesajul
    int key;                // cheia căutată
    int path[MAX_PATH];     // traseul lookup-ului
    int path_len;           // lungimea traseului
} LookupMsg;

/************************************************************
 * Tag-uri mesaje MPI
 ************************************************************/
enum {
    TAG_LOOKUP_REQ = 1,
    TAG_LOOKUP_REP,
    TAG_DONE
};

/************************************************************
 * Variabile globale utile
 ************************************************************/
int world_rank, world_size;
NodeState self;

int all_ids[MAX_NODES];      // ID CHORD pentru fiecare rank
int id_to_rank[RING_SIZE];   // mapare inversă: CHORD ID -> MPI rank
int sorted_ids[MAX_NODES];   // ID-uri sortate ale nodurilor din inel
int num_nodes;

/************************************************************
 * Funcții utile pentru interval circular CHORD
 ************************************************************/
int in_interval(int x, int start, int end) {
    if (start < end)
        return (x > start && x <= end);
    if (start > end)
        return (x > start || x <= end);
    return 1;   // intervalul acoperă tot cercul
}

/************************************************************
 * Construirea mapării rank -> id și id -> rank
 ************************************************************/
void build_id_maps() {
    for (int i = 0; i < RING_SIZE; i++)
        id_to_rank[i] = -1;

    for (int r = 0; r < world_size; r++) {
        int id = all_ids[r];
        if (id >= 0 && id < RING_SIZE)
            id_to_rank[id] = r;
    }
}

int rank_from_id(int id) {
    if (id < 0 || id >= RING_SIZE)
        return -1;
    return id_to_rank[id];
}

/************************************************************
 * Construirea inelului CHORD static
 ************************************************************/
int cmp_int(const void *a, const void *b) {
    return (*(int*)a - *(int*)b);
}

void build_global_ring() {
    num_nodes = world_size;

    for (int i = 0; i < num_nodes; i++)
        sorted_ids[i] = all_ids[i];

    qsort(sorted_ids, num_nodes, sizeof(int), cmp_int);

    for (int i = 0; i < num_nodes; i++) {
        int id = sorted_ids[i];
        int succ = sorted_ids[(i + 1) % num_nodes];
        int pred = sorted_ids[(i - 1 + num_nodes) % num_nodes];

        if (id == self.id) {
            self.successor = succ;
            self.predecessor = pred;
        }
    }
}

/************************************************************
 * Construirea finger table-ului – de completat de voi
 *
 * IMPORTANT:
 *  Tema folosește o variantă STATICĂ a CHORD, iar succesorul unei
 *  poziții "start" se caută EXCLUSIV în lista nodurilor existente,
 *  nu în întreg spațiul 0..RING_SIZE-1 (așa cum ar fi în CHORD real).
 *
 *  find_successor_simple() caută succesorul lui start[i] în sorted_ids[],
 *  care conține DOAR nodurile existente.
 ************************************************************/
int find_successor_simple(int key) {
    for (int i = 0; i < num_nodes; i++)
        if (sorted_ids[i] >= key)
            return sorted_ids[i];
    return sorted_ids[0]; // wrap-around
}

void build_finger_table() {
     for (int i = 0; i < M; ++i) {
        Finger *finger = &self.finger[i];
        finger->start = (self.id + (1 << i)) % RING_SIZE;
        finger->node = find_successor_simple(finger->start);
     }
}

/************************************************************
 * closest_preceding_finger – de implementat de voi
 *
 *  Această funcție returnează cel mai "mare" finger care
 *  se află strict în intervalul CHORD (self.id, key).
 *
 * Atenție:
 *    – Intervalul este unul circular și DESCHIS la self.id.
 *    – Finger-ul ales trebuie să ducă lookup-ul mai aproape
 *      de key în mod real.
 *    – Alegerea unui finger care nu produce progres (de ex.
 *      care se întoarce la self.id) poate duce la cicluri
 *      infinite și blocarea rutării.
 *
 *  Dacă niciun finger nu se potrivește, întoarceți succesorul.
 ************************************************************/
int closest_preceding_finger(int key) {
    for (int finger_index = M - 1; finger_index >= 0; --finger_index) {
        const Finger *finger = &self.finger[finger_index];
        if (in_interval(finger->node, self.id, key))
            return finger->node;
    }

    return self.successor;   // fallback
}

/************************************************************
 * handle_lookup_request – de implementat de voi
 *
 *   Aceasta este funcția esențială pentru rutare distribuită.
 *   Pașii corecți CHORD (tema simplificată):
 *
 *   1. Adăugați self.id în path.
 *   2. Dacă succesorul nostru este responsabil de key:
 *           - adăugați succesorul în path
 *           - trimiteți TAG_LOOKUP_REP către inițiator
 *   3. Altfel:
 *           - next = closest_preceding_finger(key)
 *           - trimiteți TAG_LOOKUP_REQ către closest_preceding_finger
 *
 ************************************************************/
void handle_lookup_request(LookupMsg *msg) {
    // 1. Adaugare self.id in msg.paths.
    msg->path[msg->path_len++] = self.id;
    msg->current_id = self.id;

    // 1.5? Desi acest lucru nu este specificat in documentatia functiei,
    // verificam daca cheia corespunde nodului 'self', iar in caz afirmativ
    // trimitem un mesaj de tag 'TAG_LOOKUP_REP' nodului initiator.
    if (in_interval(msg->key, self.predecessor, self.id)) {
        const int initiator_rank = rank_from_id(msg->initiator_id);
        MPI_Send(msg, sizeof(LookupMsg), MPI_BYTE, initiator_rank,
                 TAG_LOOKUP_REP, MPI_COMM_WORLD);
        return;
    }

    // 2. Verificam daca cheia apartine nodului succesor si in caz afirmativ
    // trimitem un mesaj de tip 'TAG_LOOKUP_REP' nodului initiator.
    if (in_interval(msg->key, self.id, self.successor)) {
        msg->path[msg->path_len++] = self.successor;
        msg->current_id = self.successor;
        
        const int initiator_rank = rank_from_id(msg->initiator_id);
        MPI_Send(msg, sizeof(LookupMsg), MPI_BYTE, initiator_rank,
                 TAG_LOOKUP_REP, MPI_COMM_WORLD);
        return;
    }

    // 3.a. Cautam (logaritmic) nodul caruia sa ii facem forward acestui mesaj.
    const int forward_node_id = closest_preceding_finger(msg->key);
    const int forward_node_rank = rank_from_id(forward_node_id);

    // 3.b. Facem forward mesajului de tip 'TAG_LOOKUP_REQ'.
    MPI_Send(msg, sizeof(LookupMsg), MPI_BYTE, forward_node_rank,
             TAG_LOOKUP_REQ, MPI_COMM_WORLD);
}

static void send_done_message_to_everyone() {
    // Mesajul de tag 'TAG_DONE' nu are continut (0 bytes.)
    for (int rank = 0; rank < world_size; ++rank)
        MPI_Send(NULL, 0, MPI_BYTE, rank, TAG_DONE, MPI_COMM_WORLD);
}

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    /******************************************************
     * Citire input
     ******************************************************/
    char fname[32];
    sprintf(fname, "in%d.txt", world_rank);
    FILE *f = fopen(fname, "r");
    if (!f) {
        fprintf(stderr, "Rank %d: cannot open %s\n", world_rank, fname);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int nr_lookups;
    fscanf(f, "%d", &self.id);
    fscanf(f, "%d", &nr_lookups);

    int *lookups = malloc(nr_lookups * sizeof(int));
    for (int i = 0; i < nr_lookups; i++)
        fscanf(f, "%d", &lookups[i]);
    fclose(f);

    /******************************************************
     * Distribuirea ID-urilor tuturor nodurilor
     ******************************************************/
    MPI_Allgather(&self.id, 1, MPI_INT,
                  all_ids, 1, MPI_INT,
                  MPI_COMM_WORLD);

    build_id_maps();
    build_global_ring();
    build_finger_table();

    MPI_Barrier(MPI_COMM_WORLD);

    for (int lookup_index; lookup_index < nr_lookups; ++lookup_index) {
        LookupMsg msg = {};
        msg.initiator_id = self.id;
        msg.current_id = self.id;
        msg.key = lookups[lookup_index];
        msg.path_len = 0;

        MPI_Send(&msg, sizeof(LookupMsg), MPI_BYTE, world_rank,
                 TAG_LOOKUP_REQ, MPI_COMM_WORLD);
    }

    free(lookups);

    int number_of_nodes_done = 0;
    int number_of_lookups_finished = 0;

    // Trebuie sa gestionam in mod special acest caz intrucat atunci cand acest
    // nod nu face niciun lookup request, niciun mesaj de tag 'TAG_LOOKUP_REP'
    // nu va fi primit si aunci codul din loop-ul de mai jos care trimite mesaje
    // de tag 'TAG_DONE' la restul retelei nu va rula.
    if (nr_lookups == 0)
        send_done_message_to_everyone();

    while (number_of_nodes_done < world_size) {
        MPI_Status status;
        LookupMsg msg;
        MPI_Recv(&msg, sizeof(LookupMsg), MPI_BYTE,
                MPI_ANY_SOURCE, MPI_ANY_TAG,
                MPI_COMM_WORLD, &status);

        // Procesam mesajele de tag 'TAG_LOOKUP_REQ'.
        if (status.MPI_TAG == TAG_LOOKUP_REQ) {
            handle_lookup_request(&msg);
            continue;
        }

        // Procesam mesajele de tag 'TAG_LOOKUP_REP'.
        if (status.MPI_TAG == TAG_LOOKUP_REP) {
            // Printeaza path-ul mesajului in retea.
            printf("Lookup %d: ", msg.key);
            for (int path_index = 0; path_index < msg.path_len; ++path_index) {
                if (path_index != 0)
                    printf(" -> ");
                printf("%d", msg.path[path_index]);
            }
            printf("\n");

            ++number_of_lookups_finished;
            if (number_of_lookups_finished == nr_lookups) {
                // Observam ca prin acest proces ne vom trimite singuri
                // un mesaj de tip 'TAG_DONE', ceea ce inseamna ca nu trebuie
                // sa incrementam aici contorul 'number_of_nodes_done'.
                send_done_message_to_everyone();
            }

            continue;
        }

        // Procesam mesajele de tag 'TAG_DONE'.
        if (status.MPI_TAG == TAG_DONE) {
            ++number_of_nodes_done;
            continue;
        }
    }

    MPI_Finalize();
    return 0;
}
