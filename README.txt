Προγραμματισμός Συστήματος 2019-20
Εργασία 3

Λεωνίδας-Παναγιώτης Παπαδάκος, ΑΜ 1115201700117
================================================================================

Test δεδομένα:
- data/ : αρχεία που τροφοδοτούν τα scripts
- db/ : test βάση δεδομένων με χώρες
- queriesX.txt : test queries (X σε αριθμό) για χρήση από τον client

Small interactive client:
while read -p "> " cmd; do printf "$cmd\0" | nc $serverIP $queryPortNum; echo; done

Παραδοχές:

[0] Για τα παρακάτω ισχύει η πολιτική της εργασίας 2:
 - Δομές δεδομένων του worker
 - Records & entry dates
 - Πρωτόκολλο επικοινωνίας (κοινό για pipes και sockets)
 - Signals (μόνο το SIGINT & SIGQUIT)
 - Καταμέτρηση requests, successful και μη

[1] Ο client δημιουργεί numThreads νήματα, και το καθένα χειρίζεται το πολύ 1
query/request. Αν cmd == NULL τότε το thread εξυπηρετεί μόνο το σκοπό της
αφύπνισης των άλλων νημάτων μέσω του barrier.

Ο client επαναλαμβάνει τη διαδικασία (νέα numThreads νήματα) όσο υπάρχουν
γραμμές στο queryFile.

[2] Όπως παρατηρήθηκε στο https://piazza.com/class/k6pgj1tl3da50l?cid=312,
Η εκφώνηση για τις numPatientAdmissions και numPatientDischarges έχει αλλάξει
από την εκδοχή της εργασίας 2 και τώρα κάνει overlap με την diseaseFrequency.
Δόθηκε διευκρίνηση ότι μπορούμε να διατηρήσουμε τη συμπεριφορά της εργασίας 2.

[3] Περιγραφή server:
- Ακούει στη θύρα statisticsPortNum για τα στατιστικά των workers και
- Παράλληλα ακούει στη θύρα queryPortNum για queries προς προώθηση από τον
  client.
- Δέχεται requests "όπως έρχονται" με poll().
- Τοποθετεί το fd της accept() στο κοινόχρηστο circular buffer. Στην ουσία
  το fd είναι ένα job που ανατίθεται στα threads.

- Το thread μαζεύει το fd/job για να το εξυπηρετήσει.
- H φύση του incoming request ξεχωρίζει από το destination port:
  Αν πρόκειται για στατιστικά, διατηρεί τις πληροφορίες επικοινωνίας με τον
  worker και εκτυπώνει στο stdout (με traffic control) τα στατιστικά.

  Αν πρόκειται για query, το προωθεί στους workers, οι οποίοι απαντούν στο ίδιο
  socket που άνοιξε για την προώθηση του ερωτήματος, συλλέγει τις απαντήσεις και
  στέλνει το αποτέλεσμα στον client. Χρησιμοποιεί το buffer "result" για τις
  δικές του, thread-safe εκτυπώσεις των queries.

[4] Ο worker προσμετρά τα requests για χώρες που δεν διαχειρίζεται στα failed
(και στα total) requests. Η συμπεριφορά αυτή μπορεί να αλλάξει με την εισαγωγή
της συνθήκης "if (ret != DA_INVALID_COUNTRY)" στη γραμμή worker.c:293.

[5] Η παράμετρος backlog για τη listen() των workers έχει άμεση σχέση με το
stress που βάζει ο server στους workers μέσω της ταυτόχρονης λειτουργίας των
server/client threads. Παρουσιάζονται προβλήματα όταν ο αριθμός των threads
πλησιάζει αυτή την παράμετρο.

Σε εναρμόνηση με την ερώτηση https://piazza.com/class/k6pgj1tl3da50l?cid=313
έχω ορίσει αυτή την παράμετρο στο πεσσιμιστικό (safe) SOMAXCONN, αλλά το
fine-tuning εξαρτάται και αο το stress test.
