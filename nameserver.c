#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include "common.h"

#define BUF_CMD 1024
#define BUF_DATA 4096

/* Protocol keywords */
#define KW_RES_OK "RES_OK"
#define KW_RES_ERROR "RES_ERROR"
#define KW_RES_DATA_START "RES_DATA_START"
#define KW_RES_DATA_END "RES_DATA_END"
#define KW_RES_LOCATION "RES_LOCATION"

#define KW_ACK_REGISTER "ACK_REGISTER"
#define KW_ACK_CREATE "ACK_CREATE"
#define KW_ACK_DELETE "ACK_DELETE"
#define KW_ACK_CREATEFOLDER "ACK_CREATEFOLDER"
#define KW_ACK_MOVE "ACK_MOVE"
#define KW_ACK_CHECKPOINT "ACK_CHECKPOINT"
#define KW_ACK_REVERT "ACK_REVERT"

#define KW_REGISTER "REGISTER"
#define KW_REGISTER_SS "REGISTER_SS"
#define KW_REGISTER_SS_FILE "REGISTER_SS_FILE"
#define KW_REGISTER_SS_DONE "REGISTER_SS_DONE"

#define KW_CMD_CREATE "CMD_CREATE"
#define KW_CMD_DELETE "CMD_DELETE"
#define KW_CMD_CREATEFOLDER "CMD_CREATEFOLDER"
#define KW_CMD_MOVE "CMD_MOVE"
#define KW_CMD_CHECKPOINT "CMD_CHECKPOINT"
#define KW_CMD_VIEWCHECKPOINT "CMD_VIEWCHECKPOINT"
#define KW_CMD_LISTCHECKPOINTS "CMD_LISTCHECKPOINTS"
#define KW_CMD_REVERT "CMD_REVERT"
#define KW_REQ_READ_NS_INTERNAL "REQ_READ __NS_INTERNAL__"
// Stats request to SS (internal)
#define KW_REQ_STATS_NS_INTERNAL "REQ_STATS __NS_INTERNAL__"
// Metadata sync commands to Storage Server
#define KW_CMD_META_ADD "CMD_META_ADD"
#define KW_CMD_META_REM "CMD_META_REM"
// Access request persistence with Storage Servers
#define KW_CMD_META_REQ_ADD "CMD_META_REQ_ADD"
#define KW_CMD_META_REQ_REM "CMD_META_REQ_REM"

/* Data structures */
typedef struct {
    int socket_fd;
    char username[50];
} ClientInfo;

typedef struct StorageServerInfo {
    int socket_fd;
    char ip[50];
    int client_port;
    pthread_mutex_t socket_lock;
} StorageServerInfo;

typedef struct {
    char owner[50];
    char read_access_users[10][50];
    char write_access_users[10][50];
    int read_count;
    int write_count;
    char last_access_user[50];
    // Pending access requests (persisted on SS). type: 'R' or 'W'
    char req_users[20][50];
    char req_types[20];
    int req_count;
} FileMeta;

typedef struct {
    StorageServerInfo* ss;
} FileLocation;

/* Simple array-based LRU cache for file -> ss */
typedef struct {
    char name[100];
    StorageServerInfo* ss;
    time_t last_used;
} CacheEntry;

static CacheEntry lru[128];
static int lru_count = 0;
static pthread_mutex_t lru_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Global state */
static StorageServerInfo* ss_list[100];
static int ss_count = 0;
static int ss_round_robin = 0; // Round-robin counter for CREATE
static pthread_mutex_t ss_list_mutex = PTHREAD_MUTEX_INITIALIZER;

static FileLocation file_map[1000];
static char file_map_names[1000][100];
static int file_map_count = 0;
static pthread_mutex_t file_map_mutex = PTHREAD_MUTEX_INITIALIZER;

static FileMeta file_meta_map[1000];
static char file_meta_map_names[1000][100];
static int file_meta_map_count = 0;
static pthread_mutex_t file_meta_mutex = PTHREAD_MUTEX_INITIALIZER;

// Folder registry (hierarchical paths ending with '/')
static char folder_names[1000][200];
static char folder_owners[1000][50];
static char folder_created[1000][32];
static char folder_last_access[1000][32];
static int folder_count = 0;
static pthread_mutex_t folder_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Comparator for folder indices (alphabetical by folder_names) */
static int cmp_folder_indices(const void* a, const void* b) {
    int ia = *(const int*)a;
    int ib = *(const int*)b;
    return strcmp(folder_names[ia], folder_names[ib]);
}

// ---- Folder persistence ----
static const char* FOLDERS_META_FILE = "folders.meta";

static void save_folders(void) {
    pthread_mutex_lock(&folder_mutex);
    char tmpname[64]; snprintf(tmpname, sizeof(tmpname), "%s.tmp", FOLDERS_META_FILE);
    FILE* f = fopen(tmpname, "w");
    if (f) {
        for (int i = 0; i < folder_count; ++i) {
            // Persist: name TAB owner TAB created TAB last_access
            fprintf(f, "%s\t%s\t%s\t%s\n", folder_names[i], folder_owners[i], folder_created[i], folder_last_access[i]);
        }
        fclose(f);
        rename(tmpname, FOLDERS_META_FILE);
    }
    pthread_mutex_unlock(&folder_mutex);
}

static void load_folders(void) {
    pthread_mutex_lock(&folder_mutex);
    FILE* f = fopen(FOLDERS_META_FILE, "r");
    if (!f) { pthread_mutex_unlock(&folder_mutex); return; }
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        char* nl = strchr(line, '\n'); if (nl) *nl = '\0';
        // Tokenize by tabs; support legacy 2-field lines.
        char* p = line;
        char* f1 = strsep(&p, "\t");
        char* f2 = strsep(&p, "\t");
        char* f3 = strsep(&p, "\t");
        char* f4 = strsep(&p, "\t");
        if (!f1 || !f2) continue;
        const char* folder = f1;
        const char* owner = f2;
        const char* created = (f3 && *f3) ? f3 : "-";
        const char* lastacc = (f4 && *f4) ? f4 : created;
        if (folder_count < (int)(sizeof(folder_names)/sizeof(folder_names[0]))) {
            strncpy(folder_names[folder_count], folder, sizeof(folder_names[folder_count])-1);
            folder_names[folder_count][sizeof(folder_names[folder_count])-1] = '\0';
            strncpy(folder_owners[folder_count], owner, sizeof(folder_owners[folder_count])-1);
            folder_owners[folder_count][sizeof(folder_owners[folder_count])-1] = '\0';
            strncpy(folder_created[folder_count], created, sizeof(folder_created[folder_count])-1);
            folder_created[folder_count][sizeof(folder_created[folder_count])-1] = '\0';
            strncpy(folder_last_access[folder_count], lastacc, sizeof(folder_last_access[folder_count])-1);
            folder_last_access[folder_count][sizeof(folder_last_access[folder_count])-1] = '\0';
            folder_count++;
        }
    }
    fclose(f);
    pthread_mutex_unlock(&folder_mutex);
}

static char user_list[100][50];
static int user_count = 0;
static pthread_mutex_t user_list_mutex = PTHREAD_MUTEX_INITIALIZER;

// ---- User persistence ----
static const char* USERS_META_FILE = "users.meta";

static void save_users(void) {
    pthread_mutex_lock(&user_list_mutex);
    char tmpname[64]; snprintf(tmpname, sizeof(tmpname), "%s.tmp", USERS_META_FILE);
    FILE* f = fopen(tmpname, "w");
    if (f) {
        for (int i = 0; i < user_count; ++i) {
            fprintf(f, "%s\n", user_list[i]);
        }
        fclose(f);
        rename(tmpname, USERS_META_FILE);
    }
    pthread_mutex_unlock(&user_list_mutex);
}

static void load_users(void) {
    pthread_mutex_lock(&user_list_mutex);
    FILE* f = fopen(USERS_META_FILE, "r");
    if (!f) { pthread_mutex_unlock(&user_list_mutex); return; }
    char line[128];
    user_count = 0; // reset then load
    while (fgets(line, sizeof(line), f)) {
        char* nl = strchr(line, '\n'); if (nl) *nl = '\0';
        if (*line == '\0') continue;
        if (user_count < (int)(sizeof(user_list)/sizeof(user_list[0]))) {
            strncpy(user_list[user_count], line, sizeof(user_list[user_count])-1);
            user_list[user_count][sizeof(user_list[user_count])-1] = '\0';
            user_count++;
        }
    }
    fclose(f);
    pthread_mutex_unlock(&user_list_mutex);
}

/* --- Simple hashmap for filename -> file_map index (separate chaining) --- */
#define HM_SIZE 4096
typedef struct HMNode {
    char key[128];
    int value; // index into file_map / file_meta_map
    struct HMNode* next;
} HMNode;

static HMNode* hm_table[HM_SIZE];
static pthread_mutex_t hm_mutex = PTHREAD_MUTEX_INITIALIZER;

static unsigned long str_hash(const char* s) {
    unsigned long h = 5381;
    while (*s) h = ((h << 5) + h) + (unsigned char)(*s++);
    return h;
}

static void hm_put(const char* key, int val) {
    unsigned long h = str_hash(key) % HM_SIZE;
    pthread_mutex_lock(&hm_mutex);
    HMNode* cur = hm_table[h];
    while (cur) {
        if (strcmp(cur->key, key) == 0) { cur->value = val; pthread_mutex_unlock(&hm_mutex); return; }
        cur = cur->next;
    }
    HMNode* n = (HMNode*)malloc(sizeof(HMNode));
    strncpy(n->key, key, sizeof(n->key)-1); n->key[sizeof(n->key)-1] = '\0';
    n->value = val; n->next = hm_table[h]; hm_table[h] = n;
    pthread_mutex_unlock(&hm_mutex);
}

static int hm_get(const char* key) {
    unsigned long h = str_hash(key) % HM_SIZE;
    pthread_mutex_lock(&hm_mutex);
    HMNode* cur = hm_table[h];
    while (cur) { if (strcmp(cur->key, key) == 0) { int v = cur->value; pthread_mutex_unlock(&hm_mutex); return v; } cur = cur->next; }
    pthread_mutex_unlock(&hm_mutex);
    return -1;
}

static void hm_remove(const char* key) {
    unsigned long h = str_hash(key) % HM_SIZE;
    pthread_mutex_lock(&hm_mutex);
    HMNode* cur = hm_table[h]; HMNode* prev = NULL;
    while (cur) {
        if (strcmp(cur->key, key) == 0) {
            if (prev) prev->next = cur->next; else hm_table[h] = cur->next;
            free(cur);
            break;
        }
        prev = cur; cur = cur->next;
    }
    pthread_mutex_unlock(&hm_mutex);
}

/* Forward declarations */
static int send_line(int fd, const char* s);

/* Unified error sender with numeric code from common.h */
static void send_error(int fd, int code, const char* msg) {
    char buf[512];
    snprintf(buf, sizeof(buf), KW_RES_ERROR " %d %s\n", code, msg);
    send_line(fd, buf);
}

/* More forward declarations */
void log_message(char* message);
void* handle_storage_server(void* arg);
void* handle_client(void* arg);
void cleanup_ss(StorageServerInfo* ss);

/* Client handlers */
void do_view(ClientInfo* ci, char** args);
void do_list_users(ClientInfo* ci, char** args);
void do_info(ClientInfo* ci, char** args);
void do_addaccess(ClientInfo* ci, char** args);
void do_remaccess(ClientInfo* ci, char** args);
void do_create(ClientInfo* ci, char** args);
void do_delete(ClientInfo* ci, char** args);
void do_createfolder(ClientInfo* ci, char** args);
void do_move(ClientInfo* ci, char** args);
void do_viewfolder(ClientInfo* ci, char** args);
void do_listfolders(ClientInfo* ci, char** args);
void do_location(ClientInfo* ci, char** args);
void do_exec(ClientInfo* ci, char** args);
// Bonus: Access Requests
void do_requestaccess(ClientInfo* ci, char** args);
// Checkpoints
void do_checkpoint(ClientInfo* ci, char** args);
void do_listcheckpoints(ClientInfo* ci, char** args);
void do_viewcheckpoint(ClientInfo* ci, char** args);
void do_revertcheckpoint(ClientInfo* ci, char** args);
void do_listrequests(ClientInfo* ci, char** args);
void do_respondrequest(ClientInfo* ci, char** args);

/* Utilities */
static int send_all(int fd, const char* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, buf + total, len - total, 0);
        if (n <= 0) return -1;
        total += (size_t)n;
    }
    return 0;
}

static int send_line(int fd, const char* s) {
    return send_all(fd, s, strlen(s));
}

static void trim_newline(char* s) {
    if (!s) return;
    size_t n = strlen(s);
    if (n && s[n-1] == '\n') s[n-1] = '\0';
}

static int has_user(char list[][50], int count, const char* user) {
    for (int i = 0; i < count; ++i) {
        if (strcmp(list[i], user) == 0) return 1;
    }
    return 0;
}

typedef struct {
    char words[64];
    char chars_[64];
    char sizeb[64];
    char mtime[64];
    char atime[64];
    char created[64];
} FileStats;

// Read from SS socket until we see RES_DATA_END, parse stats lines
static int fetch_stats_from_ss(StorageServerInfo* ss, const char* fname, FileStats* out) {
    if (!ss || !fname || !out) return -1;
    // defaults
    strncpy(out->words, "-", sizeof(out->words)-1); out->words[sizeof(out->words)-1] = '\0';
    strncpy(out->chars_, "-", sizeof(out->chars_)-1); out->chars_[sizeof(out->chars_)-1] = '\0';
    strncpy(out->sizeb, "-", sizeof(out->sizeb)-1); out->sizeb[sizeof(out->sizeb)-1] = '\0';
    strncpy(out->mtime, "-", sizeof(out->mtime)-1); out->mtime[sizeof(out->mtime)-1] = '\0';
    strncpy(out->atime, "-", sizeof(out->atime)-1); out->atime[sizeof(out->atime)-1] = '\0';
    strncpy(out->created, "-", sizeof(out->created)-1); out->created[sizeof(out->created)-1] = '\0';

    char req[BUF_CMD];
    snprintf(req, sizeof(req), KW_REQ_STATS_NS_INTERNAL " %s\n", fname);
    {
        char logb[256]; snprintf(logb, sizeof(logb), "NS->SS REQ_STATS %s", fname); log_message(logb);
    }
    if (send_line(ss->socket_fd, req) != 0) return -1;

    // Set a short recv timeout to avoid hangs
    struct timeval oldtv, tv; socklen_t tvlen = sizeof(oldtv);
    int had_old = (getsockopt(ss->socket_fd, SOL_SOCKET, SO_RCVTIMEO, &oldtv, &tvlen) == 0);
    tv.tv_sec = 0; tv.tv_usec = 300000; // 300ms to keep VIEW responsive
    setsockopt(ss->socket_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // accumulate until we see RES_DATA_END or timeout
    char acc[BUF_DATA * 2 + 1]; size_t used = 0; acc[0] = '\0';
    int iterations = 0;
    while (1) {
        char buf[BUF_DATA+1];
        ssize_t n = recv(ss->socket_fd, buf, BUF_DATA, 0);
        if (n <= 0) break; // timeout or error -> best-effort parse
        buf[n] = '\0';
        size_t space = sizeof(acc) - 1 - used;
        size_t tocopy = (size_t)n < space ? (size_t)n : space;
        memcpy(acc + used, buf, tocopy);
        used += tocopy; acc[used] = '\0';
        if (strstr(acc, KW_RES_DATA_END)) break;
        if (space == 0) break; // prevent overflow; best-effort parse
        if (++iterations > 16) break; // safety cap
    }

    // restore old timeout
    if (had_old) setsockopt(ss->socket_fd, SOL_SOCKET, SO_RCVTIMEO, &oldtv, sizeof(oldtv));

    // parse only the block between RES_DATA_START and RES_DATA_END
    char* start = strstr(acc, KW_RES_DATA_START);
    if (start) {
        start += strlen(KW_RES_DATA_START);
        if (*start == '\n' || *start == ' ') start++;
    } else {
        start = acc;
    }
    char* endm = strstr(start, KW_RES_DATA_END);
    if (endm) *endm = '\0';

    char* p = start; char* line;
    while ((line = strsep(&p, "\n")) != NULL) {
        if (strncmp(line, "WORDS ", 6) == 0) { strncpy(out->words, line+6, sizeof(out->words)-1); out->words[sizeof(out->words)-1] = '\0'; }
        else if (strncmp(line, "CHARS ", 6) == 0) { strncpy(out->chars_, line+6, sizeof(out->chars_)-1); out->chars_[sizeof(out->chars_)-1] = '\0'; }
        else if (strncmp(line, "SIZE ", 5) == 0) { strncpy(out->sizeb, line+5, sizeof(out->sizeb)-1); out->sizeb[sizeof(out->sizeb)-1] = '\0'; }
        else if (strncmp(line, "MTIME ", 6) == 0) { strncpy(out->mtime, line+6, sizeof(out->mtime)-1); out->mtime[sizeof(out->mtime)-1] = '\0'; }
        else if (strncmp(line, "ATIME ", 6) == 0) { strncpy(out->atime, line+6, sizeof(out->atime)-1); out->atime[sizeof(out->atime)-1] = '\0'; }
        else if (strncmp(line, "CREATED ", 8) == 0) { strncpy(out->created, line+8, sizeof(out->created)-1); out->created[sizeof(out->created)-1] = '\0'; }
    }
    {
        char logb[256]; snprintf(logb, sizeof(logb), "NS<-SS REQ_STATS %s done", fname); log_message(logb);
    }
    return 0;
}

static int file_index_by_name(const char* name) {
    return hm_get(name);
}

static int meta_index_by_name(const char* name) {
    return hm_get(name); // meta entries share the same index mapping as file_map
}

static StorageServerInfo* lru_get(const char* name, int* hit) {
    pthread_mutex_lock(&lru_mutex);
    StorageServerInfo* ss = NULL;
    *hit = 0;
    for (int i = 0; i < lru_count; ++i) {
        if (strcmp(lru[i].name, name) == 0) {
            lru[i].last_used = time(NULL);
            ss = lru[i].ss;
            *hit = 1;
            break;
        }
    }
    pthread_mutex_unlock(&lru_mutex);
    return ss;
}

static void lru_put(const char* name, StorageServerInfo* ss) {
    pthread_mutex_lock(&lru_mutex);
    // update if exists
    for (int i = 0; i < lru_count; ++i) {
        if (strcmp(lru[i].name, name) == 0) {
            lru[i].ss = ss;
            lru[i].last_used = time(NULL);
            pthread_mutex_unlock(&lru_mutex);
            return;
        }
    }
    if (lru_count < (int)(sizeof(lru)/sizeof(lru[0]))) {
        strncpy(lru[lru_count].name, name, sizeof(lru[lru_count].name)-1);
        lru[lru_count].name[sizeof(lru[lru_count].name)-1] = '\0';
        lru[lru_count].ss = ss;
        lru[lru_count].last_used = time(NULL);
        lru_count++;
    } else {
        // evict LRU
        int evict = 0;
        for (int i = 1; i < lru_count; ++i) {
            if (lru[i].last_used < lru[evict].last_used) evict = i;
        }
        strncpy(lru[evict].name, name, sizeof(lru[evict].name)-1);
        lru[evict].name[sizeof(lru[evict].name)-1] = '\0';
        lru[evict].ss = ss;
        lru[evict].last_used = time(NULL);
    }
    pthread_mutex_unlock(&lru_mutex);
}

void log_message(char* message) {
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);
    printf("[%s] %s\n", ts, message);
    fflush(stdout);
    FILE* lf = fopen("ns.log", "a");
    if (lf) { fprintf(lf, "[%s] %s\n", ts, message); fclose(lf); }
}

static void* client_thread_entry(void* arg) { return handle_client(arg); }
static void* ss_thread_entry(void* arg) { return handle_storage_server(arg); }

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: ./nameserver <port>\n");
        return EXIT_FAILURE;
    }

    int port = atoi(argv[1]);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return EXIT_FAILURE;
    }

    if (listen(listen_fd, 128) < 0) {
        perror("listen");
        close(listen_fd);
        return EXIT_FAILURE;
    }

    // Load persisted registries before serving
    load_folders();
    load_users();
    log_message("Name Server listening...");

    while (1) {
        struct sockaddr_in cli;
        socklen_t clilen = sizeof(cli);
        int conn_fd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
        if (conn_fd < 0) {
            perror("accept");
            continue;
        }

        char first[BUF_CMD + 1];
        ssize_t n = recv(conn_fd, first, BUF_CMD, 0);
        if (n <= 0) {
            close(conn_fd);
            continue;
        }
        first[n] = '\0';

        if (strncmp(first, KW_REGISTER " ", strlen(KW_REGISTER) + 1) == 0) {
            // Client
            ClientInfo* ci = (ClientInfo*)calloc(1, sizeof(ClientInfo));
            ci->socket_fd = conn_fd;
            // Store the first line inside ci for parsing in thread
            pthread_t tid;
            // We pass a small buffer containing the first line and fd packed; instead, reuse ci and read again in thread: store first line in username temporarily.
            // To keep it simple, put first line into username; handler will parse and overwrite.
            strncpy(ci->username, first, sizeof(ci->username)-1);
            {
                char ipbuf[64]; const char* cip = inet_ntop(AF_INET, &cli.sin_addr, ipbuf, sizeof(ipbuf)) ? ipbuf : "?";
                char logb[256]; snprintf(logb, sizeof(logb), "Client REGISTER from %s:%d fd=%d", cip, ntohs(cli.sin_port), conn_fd); log_message(logb);
            }
            pthread_create(&tid, NULL, client_thread_entry, (void*)ci);
            pthread_detach(tid);
        } else if (strncmp(first, KW_REGISTER_SS " ", strlen(KW_REGISTER_SS) + 1) == 0) {
            // Storage Server
            StorageServerInfo* ss = (StorageServerInfo*)calloc(1, sizeof(StorageServerInfo));
            ss->socket_fd = conn_fd;
            // Temporarily use ip buffer to stash first line for thread to parse
            strncpy(ss->ip, first, sizeof(ss->ip)-1);
            {
                char ipbuf[64]; const char* sip = inet_ntop(AF_INET, &cli.sin_addr, ipbuf, sizeof(ipbuf)) ? ipbuf : "?";
                char logb[256]; snprintf(logb, sizeof(logb), "SS REGISTER from %s:%d fd=%d", sip, ntohs(cli.sin_port), conn_fd); log_message(logb);
            }
            pthread_t tid;
            pthread_create(&tid, NULL, ss_thread_entry, (void*)ss);
            pthread_detach(tid);
        } else {
            // Unknown; close
            send_error(conn_fd, ERR_UNKNOWN_COMMAND, "Unknown first message");
            close(conn_fd);
        }
    }

    close(listen_fd);
    return EXIT_SUCCESS;
}

void* handle_storage_server(void* arg) {
    StorageServerInfo* ss = (StorageServerInfo*)arg;

    // Parse first message stored in ss->ip
    char first[BUF_CMD+1];
    strncpy(first, ss->ip, sizeof(first)-1);
    first[sizeof(first)-1] = '\0';

    char* saveptr = NULL;
    char* tok = strtok_r(first, " \n", &saveptr); // REGISTER_SS
    tok = strtok_r(NULL, " \n", &saveptr); // ip
    if (!tok) {
        close(ss->socket_fd);
        free(ss);
        pthread_exit(NULL);
    }
    strncpy(ss->ip, tok, sizeof(ss->ip)-1);
    ss->ip[sizeof(ss->ip)-1] = '\0';
    tok = strtok_r(NULL, " \n", &saveptr); // port
    if (!tok) {
        close(ss->socket_fd);
        free(ss);
        pthread_exit(NULL);
    }
    ss->client_port = atoi(tok);

    pthread_mutex_init(&ss->socket_lock, NULL);

    // Add to ss_list
    pthread_mutex_lock(&ss_list_mutex);
    if (ss_count < (int)(sizeof(ss_list)/sizeof(ss_list[0]))) {
        ss_list[ss_count++] = ss;
    }
    pthread_mutex_unlock(&ss_list_mutex);

    send_line(ss->socket_fd, KW_ACK_REGISTER "\n");

    char logbuf[256];
    snprintf(logbuf, sizeof(logbuf), "SS connected: %s:%d fd=%d", ss->ip, ss->client_port, ss->socket_fd);
    log_message(logbuf);

    // Receive file registrations using a line-buffered reader to avoid
    // losing messages when multiple lines arrive in a single recv()
    char acc[BUF_CMD * 8]; size_t used = 0; int done = 0;
    while (!done) {
        if (used >= sizeof(acc)) used = 0; // reset on overflow risk
        ssize_t n = recv(ss->socket_fd, acc + used, sizeof(acc) - used - 1, 0);
        if (n <= 0) {
            log_message("SS disconnected during registration.");
            cleanup_ss(ss);
            pthread_exit(NULL);
        }
        used += (size_t)n; acc[used] = '\0';

        char* start = acc; char* nl;
        while ((nl = strchr(start, '\n')) != NULL) {
            *nl = '\0';
            char line[BUF_CMD * 2];
            strncpy(line, start, sizeof(line)-1); line[sizeof(line)-1] = '\0';
            start = nl + 1;

            if (strncmp(line, KW_REGISTER_SS_FILE " ", strlen(KW_REGISTER_SS_FILE) + 1) == 0) {
                char* sp = NULL;
                strtok_r(line, " \n", &sp); // REGISTER_SS_FILE
                char* fname = strtok_r(NULL, " \n", &sp);
                // Optional extended metadata: OWNER <owner> R <rcount> <r...> W <wcount> <w...> [Q <qcount> <user:type>...]
                char owner[50] = {0};
                int rcount = 0, wcount = 0;
                char rusers[10][50];
                char wusrs[10][50];
                int qcount = 0; char qusers[20][50]; char qtypes[20];
                for (int i = 0; i < 10; ++i) { rusers[i][0] = '\0'; wusrs[i][0] = '\0'; }
                char* tok = strtok_r(NULL, " \n", &sp);
                if (tok && strcmp(tok, "OWNER") == 0) {
                    char* own = strtok_r(NULL, " \n", &sp);
                    if (own) { strncpy(owner, own, sizeof(owner)-1); owner[sizeof(owner)-1] = '\0'; }
                    tok = strtok_r(NULL, " \n", &sp);
                }
                if (tok && strcmp(tok, "R") == 0) {
                    char* rc = strtok_r(NULL, " \n", &sp); rcount = rc ? atoi(rc) : 0;
                    for (int i = 0; i < rcount && i < 10; ++i) {
                        char* u = strtok_r(NULL, " \n", &sp);
                        if (!u) break; strncpy(rusers[i], u, 49); rusers[i][49] = '\0';
                    }
                    tok = strtok_r(NULL, " \n", &sp);
                }
                if (tok && strcmp(tok, "W") == 0) {
                    char* wc = strtok_r(NULL, " \n", &sp); wcount = wc ? atoi(wc) : 0;
                    for (int i = 0; i < wcount && i < 10; ++i) {
                        char* u = strtok_r(NULL, " \n", &sp);
                        if (!u) break; strncpy(wusrs[i], u, 49); wusrs[i][49] = '\0';
                    }
                    tok = strtok_r(NULL, " \n", &sp);
                }
                if (tok && strcmp(tok, "Q") == 0) {
                    char* qc = strtok_r(NULL, " \n", &sp); qcount = qc ? atoi(qc) : 0;
                    for (int i = 0; i < qcount && i < 20; ++i) {
                        char* pair = strtok_r(NULL, " \n", &sp);
                        if (!pair) break;
                        // pair format: user:type
                        char* colon = strrchr(pair, ':');
                        if (colon && colon[1]) {
                            *colon = '\0';
                            strncpy(qusers[i], pair, 49); qusers[i][49] = '\0';
                            qtypes[i] = colon[1];
                        } else {
                            qusers[i][0] = '\0'; qtypes[i] = 0;
                        }
                    }
                }

                if (fname) {
                    // Register file location and metadata (keep indices synchronized!)
                    pthread_mutex_lock(&file_map_mutex);
                    pthread_mutex_lock(&file_meta_mutex);
                    
                    int existing_idx = hm_get(fname);
                    if (existing_idx >= 0) {
                        // File already registered, just update metadata
                        file_map[existing_idx].ss = ss;
                        if (existing_idx < file_meta_map_count) {
                            FileMeta* fm = &file_meta_map[existing_idx];
                            if (owner[0]) { strncpy(fm->owner, owner, sizeof(fm->owner)-1); fm->owner[sizeof(fm->owner)-1] = '\0'; }
                            fm->read_count = (rcount > 10 ? 10 : rcount);
                            for (int i = 0; i < fm->read_count; ++i) { strncpy(fm->read_access_users[i], rusers[i], 49); fm->read_access_users[i][49] = '\0'; }
                            fm->write_count = (wcount > 10 ? 10 : wcount);
                            for (int i = 0; i < fm->write_count; ++i) { strncpy(fm->write_access_users[i], wusrs[i], 49); fm->write_access_users[i][49] = '\0'; }
                            fm->last_access_user[0] = '\0';
                            fm->req_count = (qcount > 20 ? 20 : qcount);
                            for (int i = 0; i < fm->req_count; ++i) { strncpy(fm->req_users[i], qusers[i], 49); fm->req_users[i][49] = '\0'; fm->req_types[i] = qtypes[i]; }
                        }
                    } else {
                        // New file - add to both arrays at the SAME index
                        if (file_map_count < (int)(sizeof(file_map)/sizeof(file_map[0])) && 
                            file_meta_map_count < (int)(sizeof(file_meta_map)/sizeof(file_meta_map[0]))) {
                            int idx = file_map_count;
                            
                            // Add to file_map
                            strncpy(file_map_names[idx], fname, sizeof(file_map_names[idx])-1);
                            file_map_names[idx][sizeof(file_map_names[idx])-1] = '\0';
                            file_map[idx].ss = ss;
                            file_map_count++;
                            
                            // Add to file_meta_map at the SAME index
                            strncpy(file_meta_map_names[idx], fname, sizeof(file_meta_map_names[idx])-1);
                            file_meta_map_names[idx][sizeof(file_meta_map_names[idx])-1] = '\0';
                            FileMeta* fm = &file_meta_map[idx];
                            memset(fm, 0, sizeof(*fm));
                            if (owner[0]) { strncpy(fm->owner, owner, sizeof(fm->owner)-1); fm->owner[sizeof(fm->owner)-1] = '\0'; }
                            fm->read_count = (rcount > 10 ? 10 : rcount);
                            for (int i = 0; i < fm->read_count; ++i) { strncpy(fm->read_access_users[i], rusers[i], 49); fm->read_access_users[i][49] = '\0'; }
                            fm->write_count = (wcount > 10 ? 10 : wcount);
                            for (int i = 0; i < fm->write_count; ++i) { strncpy(fm->write_access_users[i], wusrs[i], 49); fm->write_access_users[i][49] = '\0'; }
                            fm->last_access_user[0] = '\0';
                            fm->req_count = (qcount > 20 ? 20 : qcount);
                            for (int i = 0; i < fm->req_count; ++i) { strncpy(fm->req_users[i], qusers[i], 49); fm->req_users[i][49] = '\0'; fm->req_types[i] = qtypes[i]; }
                            file_meta_map_count++;
                            
                            // Update hashmap mapping name -> idx (same for both arrays)
                            hm_put(fname, idx);
                        }
                    }
                    
                    pthread_mutex_unlock(&file_meta_mutex);
                    pthread_mutex_unlock(&file_map_mutex);
                }
            } else if (strncmp(line, KW_REGISTER_SS_DONE, strlen(KW_REGISTER_SS_DONE)) == 0) {
                log_message("SS registration complete.");
                done = 1; break;
            } else {
                // ignore other lines in registration phase
            }
        }
        // Move leftover
        size_t rem = (size_t)(acc + used - start);
        if (rem > 0 && start != acc) memmove(acc, start, rem);
        used = rem;
    }

    // Do not read from the SS socket here to avoid racing with request handlers
    // that also read responses from the same socket under ss->socket_lock.
    // We rely on send/recv failures during actual requests to detect disconnects
    // and handle cleanup on-demand.
    log_message("SS registration thread exiting (handover to request handlers).\n");
    pthread_exit(NULL);
}

void cleanup_ss(StorageServerInfo* ss) {
    // Remove from ss_list
    pthread_mutex_lock(&ss_list_mutex);
    int pos = -1;
    for (int i = 0; i < ss_count; ++i) {
        if (ss_list[i] == ss) { pos = i; break; }
    }
    if (pos >= 0) {
        for (int i = pos; i < ss_count - 1; ++i) ss_list[i] = ss_list[i+1];
        ss_count--;
    }
    pthread_mutex_unlock(&ss_list_mutex);

    // Remove file_map entries and related metadata
    pthread_mutex_lock(&file_map_mutex);
    pthread_mutex_lock(&file_meta_mutex);

    int i = 0;
    while (i < file_map_count) {
        if (file_map[i].ss == ss) {
            // remove metadata if exists
            int mi = meta_index_by_name(file_map_names[i]);
            if (mi >= 0) {
                for (int k = mi; k < file_meta_map_count - 1; ++k) {
                    file_meta_map[k] = file_meta_map[k+1];
                    strcpy(file_meta_map_names[k], file_meta_map_names[k+1]);
                }
                file_meta_map_count--;
            }
            // remove hashmap entry for this filename
            hm_remove(file_map_names[i]);
            // remove file_map entry and shift remaining; update hashmap indices for shifted items
            for (int j = i; j < file_map_count - 1; ++j) {
                file_map[j] = file_map[j+1];
                strcpy(file_map_names[j], file_map_names[j+1]);
                // update hashmap for moved name
                hm_put(file_map_names[j], j);
            }
            file_map_count--;
        } else {
            i++;
        }
    }

    pthread_mutex_unlock(&file_meta_mutex);
    pthread_mutex_unlock(&file_map_mutex);

    char logbuf[256];
    snprintf(logbuf, sizeof(logbuf), "Cleaned up SS %s:%d", ss->ip, ss->client_port);
    log_message(logbuf);

    close(ss->socket_fd);
    pthread_mutex_destroy(&ss->socket_lock);
    free(ss);
}

void* handle_client(void* arg) {
    ClientInfo* ci = (ClientInfo*)arg;

    // Parse first message stored in ci->username
    char first[BUF_CMD+1];
    strncpy(first, ci->username, sizeof(first)-1);
    first[sizeof(first)-1] = '\0';

    char* saveptr = NULL;
    char* tok = strtok_r(first, " \n", &saveptr); // REGISTER
    tok = strtok_r(NULL, " \n", &saveptr); // username
    if (!tok) {
        close(ci->socket_fd);
        free(ci);
        pthread_exit(NULL);
    }
    strncpy(ci->username, tok, sizeof(ci->username)-1);
    ci->username[sizeof(ci->username)-1] = '\0';

    // Add to user list (deduplicate by username)
    pthread_mutex_lock(&user_list_mutex);
    if (!has_user(user_list, user_count, ci->username)) {
        if (user_count < (int)(sizeof(user_list)/sizeof(user_list[0]))) {
            strncpy(user_list[user_count], ci->username, sizeof(user_list[user_count])-1);
            user_list[user_count][sizeof(user_list[user_count])-1] = '\0';
            user_count++;
            // Persist immediately after adding new user
            // Note: save_users acquires user_list_mutex again; release before calling to avoid deadlock
            pthread_mutex_unlock(&user_list_mutex);
            save_users();
            // Re-acquire for consistency in this scope before proceeding
            pthread_mutex_lock(&user_list_mutex);
        }
    }
    pthread_mutex_unlock(&user_list_mutex);

    send_line(ci->socket_fd, KW_RES_OK " Welcome!\n");

    char logbuf[256];
    snprintf(logbuf, sizeof(logbuf), "Client connected: %s fd=%d", ci->username, ci->socket_fd);
    log_message(logbuf);

    char buf[BUF_CMD+1];
    while (1) {
        ssize_t n = recv(ci->socket_fd, buf, BUF_CMD, 0);
        if (n <= 0) {
            snprintf(logbuf, sizeof(logbuf), "Client disconnected: %s fd=%d", ci->username, ci->socket_fd);
            log_message(logbuf);
            close(ci->socket_fd);
            free(ci);
            pthread_exit(NULL);
        }
        buf[n] = '\0';

        snprintf(logbuf, sizeof(logbuf), "Received from %s: %s", ci->username, buf);
        log_message(logbuf);

        char* sp = NULL;
        char* cmd = strtok_r(buf, " \n", &sp);
        if (!cmd) continue;

        if (strcmp(cmd, "REQ_VIEW") == 0) {
            // Expect: REQ_VIEW <username> <flags>
            char* user = strtok_r(NULL, " \n", &sp);
            char* flag = strtok_r(NULL, " \n", &sp);
            (void)user; // username from ci is authoritative
            char* flags[2] = {flag, NULL};
            do_view(ci, flags);
        } else if (strcmp(cmd, "REQ_LIST_USERS") == 0) {
            do_list_users(ci, NULL);
        } else if (strcmp(cmd, "REQ_VIEWFOLDER") == 0) {
            // REQ_VIEWFOLDER <user> <folder> <flag>
            char* user = strtok_r(NULL, " \n", &sp);
            char* folder = strtok_r(NULL, " \n", &sp);
            char* flag = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[3] = {folder, flag, NULL};
            do_viewfolder(ci, a);
        } else if (strcmp(cmd, "REQ_LISTFOLDERS") == 0) {
            // REQ_LISTFOLDERS <user> <flag>
            char* user = strtok_r(NULL, " \n", &sp);
            char* flag = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[2] = {flag, NULL};
            do_listfolders(ci, a);
        } else if (strcmp(cmd, "REQ_INFO") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            (void)user; // username already in ci
            char* a[2] = {fname, NULL};
            do_info(ci, a);
        } else if (strcmp(cmd, "REQ_ADDACCESS") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* target = strtok_r(NULL, " \n", &sp);
            char* flag = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[4] = {fname, target, flag, NULL};
            do_addaccess(ci, a);
        } else if (strcmp(cmd, "REQ_REMACCESS") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* target = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[3] = {fname, target, NULL};
            do_remaccess(ci, a);
        } else if (strcmp(cmd, "REQ_CREATE") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[2] = {fname, NULL};
            do_create(ci, a);
        } else if (strcmp(cmd, "REQ_CREATEFOLDER") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* folder = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[2] = {folder, NULL};
            do_createfolder(ci, a);
        } else if (strcmp(cmd, "REQ_DELETE") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[2] = {fname, NULL};
            do_delete(ci, a);
        } else if (strcmp(cmd, "REQ_MOVE") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* oldn = strtok_r(NULL, " \n", &sp);
            char* newn = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[3] = {oldn, newn, NULL};
            do_move(ci, a);
        } else if (strcmp(cmd, "REQ_LOCATION") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* op = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[3] = {fname, op, NULL};
            do_location(ci, a);
        } else if (strcmp(cmd, "REQ_EXEC") == 0) {
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[2] = {fname, NULL};
            do_exec(ci, a);
        } else if (strcmp(cmd, "REQ_REQUESTACCESS") == 0) {
            // REQ_REQUESTACCESS <user> <file> <R|W>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* typ = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[3] = {fname, typ, NULL};
            do_requestaccess(ci, a);
        } else if (strcmp(cmd, "REQ_LISTREQUESTS") == 0) {
            // REQ_LISTREQUESTS <user> <file>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[2] = {fname, NULL};
            do_listrequests(ci, a);
        } else if (strcmp(cmd, "REQ_RESPONDREQUEST") == 0) {
            // REQ_RESPONDREQUEST <user> <file> <target_user> <approve|deny> <R|W>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* target = strtok_r(NULL, " \n", &sp);
            char* action = strtok_r(NULL, " \n", &sp);
            char* typ = strtok_r(NULL, " \n", &sp);
            (void)user;
            char* a[5] = {fname, target, action, typ, NULL};
            do_respondrequest(ci, a);
        } else if (strcmp(cmd, "REQ_CHECKPOINT") == 0) {
            // REQ_CHECKPOINT <user> <file> <tag>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* tag = strtok_r(NULL, " \n", &sp);
            (void)user; char* a[3] = {fname, tag, NULL};
            do_checkpoint(ci, a);
        } else if (strcmp(cmd, "REQ_LISTCHECKPOINTS") == 0) {
            // REQ_LISTCHECKPOINTS <user> <file>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            (void)user; char* a[2] = {fname, NULL};
            do_listcheckpoints(ci, a);
        } else if (strcmp(cmd, "REQ_VIEWCHECKPOINT") == 0) {
            // REQ_VIEWCHECKPOINT <user> <file> <tag>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* tag = strtok_r(NULL, " \n", &sp);
            (void)user; char* a[3] = {fname, tag, NULL};
            do_viewcheckpoint(ci, a);
        } else if (strcmp(cmd, "REQ_REVERTCHECKPOINT") == 0) {
            // REQ_REVERTCHECKPOINT <user> <file> <tag>
            char* user = strtok_r(NULL, " \n", &sp);
            char* fname = strtok_r(NULL, " \n", &sp);
            char* tag = strtok_r(NULL, " \n", &sp);
            (void)user; char* a[3] = {fname, tag, NULL};
            do_revertcheckpoint(ci, a);
        } else {
            send_error(ci->socket_fd, ERR_UNKNOWN_COMMAND, "Unknown command");
        }
    }
}

/* Handler implementations */
static int user_has_read(const FileMeta* fm, const char* user) {
    if (strcmp(fm->owner, user) == 0) return 1;
    if (has_user((char(*)[50])fm->read_access_users, fm->read_count, user)) return 1;
    if (has_user((char(*)[50])fm->write_access_users, fm->write_count, user)) return 1;
    return 0;
}

static int user_has_write(const FileMeta* fm, const char* user) {
    if (strcmp(fm->owner, user) == 0) return 1;
    if (has_user((char(*)[50])fm->write_access_users, fm->write_count, user)) return 1;
    return 0;
}

void do_view(ClientInfo* ci, char** args) {
    const char* flag = (args && args[0]) ? args[0] : "NULL";

    typedef struct { char name[100]; char owner[50]; StorageServerInfo* ss; int show; } Entry;
    Entry list[1000]; int count = 0; int detailed = (strcmp(flag, "-l") == 0 || strcmp(flag, "-al") == 0);

    // Take a snapshot under locks (no network I/O while holding mutexes)
    pthread_mutex_lock(&file_map_mutex);
    pthread_mutex_lock(&file_meta_mutex);
    for (int i = 0; i < file_map_count && count < (int)(sizeof(list)/sizeof(list[0])); ++i) {
        // Hide undo/temp artifacts from view
        const char* fname = file_map_names[i];
        size_t ln = strlen(fname);
        if ((ln > 5 && strcmp(fname + ln - 5, ".undo") == 0) ||
            (ln > 4 && strcmp(fname + ln - 4, ".tmp") == 0)) {
            continue;
        }
        int show = 0;
        if (strcmp(flag, "-a") == 0 || strcmp(flag, "-al") == 0) {
            show = 1;
        } else {
            int mi = meta_index_by_name(file_map_names[i]);
            if (mi >= 0 && user_has_read(&file_meta_map[mi], ci->username)) show = 1;
        }
        if (!show) continue;
        strncpy(list[count].name, file_map_names[i], sizeof(list[count].name)-1);
        list[count].name[sizeof(list[count].name)-1] = '\0';
    int fi = file_index_by_name(file_map_names[i]);
    list[count].ss = (fi >= 0) ? file_map[fi].ss : NULL;
    if (list[count].ss) { lru_put(list[count].name, list[count].ss); }
        int mi = meta_index_by_name(file_map_names[i]);
        if (mi >= 0) { strncpy(list[count].owner, file_meta_map[mi].owner, sizeof(list[count].owner)-1); list[count].owner[sizeof(list[count].owner)-1] = '\0'; }
        else { strncpy(list[count].owner, "-", sizeof(list[count].owner)-1); list[count].owner[sizeof(list[count].owner)-1] = '\0'; }
        list[count].show = 1;
        count++;
    }
    pthread_mutex_unlock(&file_meta_mutex);
    pthread_mutex_unlock(&file_map_mutex);

    // Output
    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    if (detailed) {
        /* Fixed-width columns with vertical separators.
           Size(KB) now shown with two decimals (bytes/1024.0). */
        send_line(ci->socket_fd, "Filename                       | Words | Chars | Size(KB)   | Created             | Last Access Time     | Owner\n");
        send_line(ci->socket_fd, "------------------------------ | ------ | ------ | ---------- | ------------------- | ------------------- | ----------------\n");
    }
    for (int i = 0; i < count; ++i) {
        if (!detailed) {
            char line[256];
            snprintf(line, sizeof(line), "--> %s\n", list[i].name);
            send_line(ci->socket_fd, line);
        } else {
            FileStats fs; memset(&fs, 0, sizeof(fs));
            // Defaults to '-' if stats fetch fails
            strncpy(fs.words, "-", sizeof(fs.words)-1); fs.words[sizeof(fs.words)-1] = '\0';
            strncpy(fs.chars_, "-", sizeof(fs.chars_)-1); fs.chars_[sizeof(fs.chars_)-1] = '\0';
            strncpy(fs.atime, "-", sizeof(fs.atime)-1); fs.atime[sizeof(fs.atime)-1] = '\0';
            if (list[i].ss) {
                pthread_mutex_lock(&list[i].ss->socket_lock);
                (void)fetch_stats_from_ss(list[i].ss, list[i].name, &fs);
                pthread_mutex_unlock(&list[i].ss->socket_lock);
            }
            char fname_buf[31]; char owner_buf[17];
            /* Truncate and pad filename (30 chars) and owner (16 chars) */
            snprintf(fname_buf, sizeof(fname_buf), "%-30.30s", list[i].name);
            snprintf(owner_buf, sizeof(owner_buf), "%-16.16s", list[i].owner);
            /* Compute size in KB as floating point with two decimals */
            char sizekb[16] = "-";
            if (fs.sizeb[0]) {
                double bytes = atof(fs.sizeb);
                double kb = bytes / 1024.0;
                snprintf(sizekb, sizeof(sizekb), "%10.3f", kb);
            } else {
                snprintf(sizekb, sizeof(sizekb), "%10s", "-");
            }
            char out[256];
            snprintf(out, sizeof(out), "%s | %-6.6s | %-6.6s | %-10.10s | %-19.19s | %-19.19s | %s\n", fname_buf, fs.words, fs.chars_, sizekb, fs.created[0]?fs.created:"-", fs.atime[0]?fs.atime:"-", owner_buf);
            send_line(ci->socket_fd, out);
        }
    }
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");
}

void do_list_users(ClientInfo* ci, char** args) {
    (void)args;
    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    pthread_mutex_lock(&user_list_mutex);
    for (int i = 0; i < user_count; ++i) {
        char line[64];
        snprintf(line, sizeof(line), "--> %s\n", user_list[i]);
        send_line(ci->socket_fd, line);
    }
    pthread_mutex_unlock(&user_list_mutex);
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");
}

void do_info(ClientInfo* ci, char** args) {
    const char* fname = (args && args[0]) ? args[0] : NULL;
    if (!fname) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_INFO <user> <file>"); return; }

    pthread_mutex_lock(&file_map_mutex);
    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    int fi = file_index_by_name(fname);
    if (mi < 0 || fi < 0) {
        char logb[256]; snprintf(logb, sizeof(logb), "INFO miss: file='%s' mi=%d fi=%d user=%s", fname, mi, fi, ci->username); log_message(logb);
        pthread_mutex_unlock(&file_meta_mutex);
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found");
        return;
    }
    if (!user_has_read(&file_meta_map[mi], ci->username)) {
        char logb[256]; snprintf(logb, sizeof(logb), "INFO denied: file='%s' user=%s", fname, ci->username); log_message(logb);
        pthread_mutex_unlock(&file_meta_mutex);
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Access denied");
        return;
    }

    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    char line[256];
    snprintf(line, sizeof(line), "--> File: %s\n", fname); send_line(ci->socket_fd, line);
    snprintf(line, sizeof(line), "--> Owner: %s\n", file_meta_map[mi].owner); send_line(ci->socket_fd, line);
    // Fetch stats from SS
    int fi2 = file_index_by_name(fname); StorageServerInfo* ss = (fi2 >= 0) ? file_map[fi2].ss : NULL;
    if (ss) { lru_put(fname, ss); }
    FileStats fs; memset(&fs, 0, sizeof(fs));
    if (ss) {
        pthread_mutex_lock(&ss->socket_lock);
        (void)fetch_stats_from_ss(ss, fname, &fs);
        pthread_mutex_unlock(&ss->socket_lock);
    }
    snprintf(line, sizeof(line), "--> Created: %s\n", fs.created); send_line(ci->socket_fd, line);
    snprintf(line, sizeof(line), "--> Last Modified: %s\n", fs.mtime); send_line(ci->socket_fd, line);
    snprintf(line, sizeof(line), "--> Size: %s bytes\n", fs.sizeb); send_line(ci->socket_fd, line);
    // Show access lists (owner, Writers, Readers) and effective access for the requester
    const FileMeta* fm = &file_meta_map[mi];
    char accbuf[BUF_CMD]; size_t off = 0; accbuf[0] = '\0';
    off += (size_t)snprintf(accbuf + off, sizeof(accbuf) - off, "--> Access: %s (owner)", fm->owner[0] ? fm->owner : "-");
    // Writers (exclude owner duplicate)
    for (int i = 0; i < fm->write_count && off < sizeof(accbuf) - 16; ++i) {
        if (fm->owner[0] && strcmp(fm->write_access_users[i], fm->owner) == 0) continue;
        off += (size_t)snprintf(accbuf + off, sizeof(accbuf) - off, ", %s (W)", fm->write_access_users[i]);
    }
    // Readers (exclude owner and anyone already listed as writer)
    for (int i = 0; i < fm->read_count && off < sizeof(accbuf) - 16; ++i) {
        int skip = 0;
        if (fm->owner[0] && strcmp(fm->read_access_users[i], fm->owner) == 0) skip = 1;
        for (int j = 0; j < fm->write_count; ++j) { if (strcmp(fm->write_access_users[j], fm->read_access_users[i]) == 0) { skip = 1; break; } }
        if (skip) continue;
        off += (size_t)snprintf(accbuf + off, sizeof(accbuf) - off, ", %s (R)", fm->read_access_users[i]);
    }
    off += (size_t)snprintf(accbuf + off, sizeof(accbuf) - off, "\n");
    send_line(ci->socket_fd, accbuf);

    const char* acc = user_has_write(fm, ci->username) ? "RW" : (user_has_read(fm, ci->username) ? "R" : "-");
    snprintf(line, sizeof(line), "--> Access (you): %s (%s)\n", ci->username, acc); send_line(ci->socket_fd, line);
    if (file_meta_map[mi].last_access_user[0]) {
        snprintf(line, sizeof(line), "--> Last Accessed: %s by %s\n", fs.atime, file_meta_map[mi].last_access_user);
    } else {
        snprintf(line, sizeof(line), "--> Last Accessed: %s\n", fs.atime);
    }
    send_line(ci->socket_fd, line);
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");

    pthread_mutex_unlock(&file_meta_mutex);
    pthread_mutex_unlock(&file_map_mutex);
}

void do_addaccess(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    const char* target = args ? args[1] : NULL;
    const char* flag = args ? args[2] : NULL;
    if (!fname || !target || !flag) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_ADDACCESS <user> <file> <target> <flag>"); return; }

    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    if (mi < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    FileMeta* fm = &file_meta_map[mi];
    if (strcmp(fm->owner, ci->username) != 0) {
        pthread_mutex_unlock(&file_meta_mutex);
        send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Only the owner can change permissions");
        return;
    }
    if (strcmp(flag, "-R") == 0) {
        if (fm->read_count < 10) strncpy(fm->read_access_users[fm->read_count++], target, 49);
    } else if (strcmp(flag, "-W") == 0) {
        if (fm->write_count < 10) strncpy(fm->write_access_users[fm->write_count++], target, 49);
    } else {
        pthread_mutex_unlock(&file_meta_mutex);
        send_error(ci->socket_fd, ERR_INVALID_ARGS, "Invalid flag");
        return;
    }
    pthread_mutex_unlock(&file_meta_mutex);

    // Persist to SS meta
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (ss) {
        pthread_mutex_lock(&ss->socket_lock);
        char cmd[BUF_CMD];
        // CMD_META_ADD <file> <R|W> <user>
        char rw = (strcmp(flag, "-R") == 0) ? 'R' : 'W';
        snprintf(cmd, sizeof(cmd), KW_CMD_META_ADD " %s %c %s\n", fname, rw, target);
        send_line(ss->socket_fd, cmd);
        pthread_mutex_unlock(&ss->socket_lock);
    }

    log_message("Permission updated (ADDACCESS)");
    send_line(ci->socket_fd, KW_RES_OK " Access granted.\n");
}

void do_remaccess(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    const char* target = args ? args[1] : NULL;
    if (!fname || !target) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_REMACCESS <user> <file> <target>"); return; }

    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    if (mi < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    FileMeta* fm = &file_meta_map[mi];
    if (strcmp(fm->owner, ci->username) != 0) {
        pthread_mutex_unlock(&file_meta_mutex);
        send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Only the owner can change permissions");
        return;
    }
    // remove from read and write lists
// Checkpoints
void do_checkpoint(ClientInfo* ci, char** args);
void do_listcheckpoints(ClientInfo* ci, char** args);
void do_viewcheckpoint(ClientInfo* ci, char** args);
void do_revert(ClientInfo* ci, char** args);
    for (int i = 0; i < fm->read_count; ++i) {
        if (strcmp(fm->read_access_users[i], target) == 0) {
            for (int j = i; j < fm->read_count - 1; ++j)
                strcpy(fm->read_access_users[j], fm->read_access_users[j+1]);
            fm->read_count--;
            break;
        }
    }
    for (int i = 0; i < fm->write_count; ++i) {
        if (strcmp(fm->write_access_users[i], target) == 0) {
            for (int j = i; j < fm->write_count - 1; ++j)
                strcpy(fm->write_access_users[j], fm->write_access_users[j+1]);
            fm->write_count--;
            break;
        }
    }
    pthread_mutex_unlock(&file_meta_mutex);

    // Persist to SS meta (remove from both R and W)
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (ss) {
        pthread_mutex_lock(&ss->socket_lock);
        char cmd[BUF_CMD];
        snprintf(cmd, sizeof(cmd), KW_CMD_META_REM " %s R %s\n", fname, target);
        send_line(ss->socket_fd, cmd);
        snprintf(cmd, sizeof(cmd), KW_CMD_META_REM " %s W %s\n", fname, target);
        send_line(ss->socket_fd, cmd);
        pthread_mutex_unlock(&ss->socket_lock);
    }

    log_message("Permission updated (REMACCESS)");
    send_line(ci->socket_fd, KW_RES_OK " Access removed.\n");
}

void do_create(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    if (!fname) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_CREATE <user> <file>"); return; }

    pthread_mutex_lock(&file_map_mutex);
    if (file_index_by_name(fname) >= 0) {
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_FILE_EXISTS, "File already exists");
        return;
    }

    // Pick an SS (round-robin)
    pthread_mutex_lock(&ss_list_mutex);
    if (ss_count == 0) {
        pthread_mutex_unlock(&ss_list_mutex);
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_NO_SS_AVAILABLE, "No Storage Server available");
        return;
    }
    StorageServerInfo* ss = ss_list[ss_round_robin % ss_count];
    ss_round_robin = (ss_round_robin + 1) % ss_count;
    pthread_mutex_unlock(&ss_list_mutex);

    // Borrow SS socket
    pthread_mutex_lock(&ss->socket_lock);
    char cmd[BUF_CMD];
    snprintf(cmd, sizeof(cmd), KW_CMD_CREATE " %s %s\n", fname, ci->username);
    {
        char logb[256]; snprintf(logb, sizeof(logb), "NS->SS CMD_CREATE %s owner=%s", fname, ci->username); log_message(logb);
    }
    if (send_line(ss->socket_fd, cmd) < 0) {
        pthread_mutex_unlock(&ss->socket_lock);
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server");
        return;
    }
    
    char ack[BUF_CMD+1];
    int retry_count = 0;
    ssize_t n = -1;
    
    // Retry logic for receiving ACK
    while (retry_count < MAX_RETRIES && n <= 0) {
        n = recv_with_timeout(ss->socket_fd, ack, BUF_CMD, RECV_TIMEOUT_SEC);
        if (n <= 0) {
            retry_count++;
            if (retry_count < MAX_RETRIES) {
                // Resend command on timeout
                send_line(ss->socket_fd, cmd);
            }
        }
    }
    
    pthread_mutex_unlock(&ss->socket_lock);
    
    if (n <= 0) {
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Storage Server not responding after retries");
        return;
    }
    ack[n] = '\0';
    {
        char logb[256]; snprintf(logb, sizeof(logb), "NS<-SS ACK_CREATE raw: %.*s", (int)n, ack); log_message(logb);
    }
    if (strncmp(ack, KW_ACK_CREATE, strlen(KW_ACK_CREATE)) != 0) {
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Storage Server failed");
        return;
    }

    // Update both maps at the SAME index (keep synchronized!)
    pthread_mutex_lock(&file_meta_mutex);
    if (file_map_count < (int)(sizeof(file_map)/sizeof(file_map[0])) &&
        file_meta_map_count < (int)(sizeof(file_meta_map)/sizeof(file_meta_map[0]))) {
        int idx = file_map_count;
        
        // Add to file_map
        strncpy(file_map_names[idx], fname, sizeof(file_map_names[idx])-1);
        file_map_names[idx][sizeof(file_map_names[idx])-1] = '\0';
        file_map[idx].ss = ss;
        file_map_count++;
        
        // Add to file_meta_map at the SAME index
        strncpy(file_meta_map_names[idx], fname, sizeof(file_meta_map_names[idx])-1);
        file_meta_map_names[idx][sizeof(file_meta_map_names[idx])-1] = '\0';
    FileMeta* fm = &file_meta_map[idx];
        memset(fm, 0, sizeof(*fm));
        strncpy(fm->owner, ci->username, sizeof(fm->owner)-1);
        fm->owner[sizeof(fm->owner)-1] = '\0';
    fm->last_access_user[0] = '\0';
        file_meta_map_count++;
        
        // Update hashmap mapping name -> idx (same for both arrays)
        hm_put(fname, idx);
    }
    pthread_mutex_unlock(&file_meta_mutex);
    pthread_mutex_unlock(&file_map_mutex);

    send_line(ci->socket_fd, KW_RES_OK " File Created.\n");
}

void do_createfolder(ClientInfo* ci, char** args) {
    if (!args[0]) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Missing folder path"); return; }
    const char* folder = args[0];
    // Choose a storage server (round-robin like CREATE)
    pthread_mutex_lock(&ss_list_mutex);
    if (ss_count == 0) { pthread_mutex_unlock(&ss_list_mutex); send_error(ci->socket_fd, ERR_NO_SS_AVAILABLE, "No SS"); return; }
    StorageServerInfo* ss = ss_list[ss_round_robin % ss_count]; ss_round_robin++;
    pthread_mutex_unlock(&ss_list_mutex);
    char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_CREATEFOLDER " %s\n", folder);
    pthread_mutex_lock(&ss->socket_lock);
    send_line(ss->socket_fd, cmd);
    char ack[BUF_CMD]; ssize_t n = recv(ss->socket_fd, ack, BUF_CMD, 0);
    pthread_mutex_unlock(&ss->socket_lock);
    if (n <= 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "No ACK_CREATEFOLDER"); return; }
    ack[n] = '\0';
    if (strncmp(ack, KW_ACK_CREATEFOLDER, strlen(KW_ACK_CREATEFOLDER)) != 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Bad ACK_CREATEFOLDER"); return; }
    // Record folder (store path normalized ending with '/')
    char norm[256]; strncpy(norm, folder, sizeof(norm)-2); norm[sizeof(norm)-2] = '\0';
    size_t ln = strlen(norm);
    if (ln == 0 || norm[ln-1] != '/') { norm[ln] = '/'; norm[ln+1] = '\0'; }
    pthread_mutex_lock(&folder_mutex);
    int exists = 0;
    for (int i = 0; i < folder_count; ++i) { if (strcmp(folder_names[i], norm) == 0) { exists = 1; break; } }
    if (!exists && folder_count < (int)(sizeof(folder_names)/sizeof(folder_names[0]))) {
        strncpy(folder_names[folder_count], norm, sizeof(folder_names[folder_count])-1); folder_names[folder_count][sizeof(folder_names[folder_count])-1] = '\0';
        strncpy(folder_owners[folder_count], ci->username, sizeof(folder_owners[folder_count])-1); folder_owners[folder_count][sizeof(folder_owners[folder_count])-1] = '\0';
        // Timestamp creation
        time_t now = time(NULL); struct tm tmv; localtime_r(&now, &tmv);
        strftime(folder_created[folder_count], sizeof(folder_created[folder_count]), "%Y-%m-%d %H:%M:%S", &tmv);
        strncpy(folder_last_access[folder_count], folder_created[folder_count], sizeof(folder_last_access[folder_count])-1);
        folder_last_access[folder_count][sizeof(folder_last_access[folder_count])-1] = '\0';
        folder_count++;
    }
    pthread_mutex_unlock(&folder_mutex);
    // Persist folders registry immediately
    save_folders();
    send_line(ci->socket_fd, KW_RES_OK " Folder created.\n");
}
void do_listfolders(ClientInfo* ci, char** args) {
    const char* flag = (args && args[0]) ? args[0] : "NULL";
    int all = (strcmp(flag, "-a") == 0 || strcmp(flag, "-al") == 0);
    int detailed = (strcmp(flag, "-l") == 0 || strcmp(flag, "-al") == 0);
    pthread_mutex_lock(&folder_mutex);
    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    int owned_count = 0;
    for (int i = 0; i < folder_count; ++i) { if (strcmp(folder_owners[i], ci->username) == 0) owned_count++; }
    // Build list of indices matching filter, then sort alphabetically
    int idxs[1000]; int sel_count = 0;
    for (int i = 0; i < folder_count; ++i) {
        if (!all && strcmp(folder_owners[i], ci->username) != 0) continue;
        idxs[sel_count++] = i;
    }
    if (sel_count > 1) { qsort(idxs, sel_count, sizeof(int), cmp_folder_indices); }
    if (!detailed) {
        if (sel_count == 0 && owned_count == 0) {
            send_line(ci->socket_fd, "No folders found!\n");
        } else {
            for (int k = 0; k < sel_count; ++k) {
                int i = idxs[k];
                char line[256]; snprintf(line, sizeof(line), "--> %s\n", folder_names[i]); send_line(ci->socket_fd, line);
                time_t now = time(NULL); struct tm tmv; localtime_r(&now, &tmv);
                strftime(folder_last_access[i], sizeof(folder_last_access[i]), "%Y-%m-%d %H:%M:%S", &tmv);
            }
            if (sel_count == 0 && owned_count > 0) {
                // Filter removed all due to 'all' flag false and ownership mismatch
                send_line(ci->socket_fd, "No folders found!\n");
            }
        }
    } else {
        send_line(ci->socket_fd, "Foldername                  | Owner            | Files | Created             | Last Access Time     | Size(KB)\n");
        send_line(ci->socket_fd, "--------------------------- | ---------------- | ----- | ------------------- | ------------------- | ---------\n");
        for (int k = 0; k < sel_count; ++k) {
            int i = idxs[k];
            int file_count = 0; double total_bytes = 0.0;
            pthread_mutex_lock(&file_map_mutex);
            for (int j = 0; j < file_map_count; ++j) {
                const char* fname = file_map_names[j];
                if (strncmp(fname, folder_names[i], strlen(folder_names[i])) == 0) {
                    file_count++;
                    int fi = file_index_by_name(fname);
                    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
                    if (ss) {
                        FileStats fs; memset(&fs, 0, sizeof(fs));
                        pthread_mutex_lock(&ss->socket_lock);
                        fetch_stats_from_ss(ss, fname, &fs);
                        pthread_mutex_unlock(&ss->socket_lock);
                        if (fs.sizeb[0]) total_bytes += atof(fs.sizeb);
                    }
                }
            }
            pthread_mutex_unlock(&file_map_mutex);
            double size_kb = total_bytes / 1024.0;
            char folder_buf[28]; snprintf(folder_buf, sizeof(folder_buf), "%-27.27s", folder_names[i]);
            char owner_buf[17]; snprintf(owner_buf, sizeof(owner_buf), "%-16.16s", folder_owners[i]);
            time_t now = time(NULL); struct tm tmv; localtime_r(&now, &tmv);
            strftime(folder_last_access[i], sizeof(folder_last_access[i]), "%Y-%m-%d %H:%M:%S", &tmv);
            char line[300]; snprintf(line, sizeof(line), "%s | %s | %-5d | %-19.19s | %-19.19s | %9.3f\n", folder_buf, owner_buf, file_count, folder_created[i][0]?folder_created[i]:"-", folder_last_access[i][0]?folder_last_access[i]:"-", size_kb);
            send_line(ci->socket_fd, line);
        }
        if (sel_count == 0) {
            send_line(ci->socket_fd, "(No folders)\n");
        }
    }
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");
    pthread_mutex_unlock(&folder_mutex);
}

void do_move(ClientInfo* ci, char** args) {
    if (!args[0] || !args[1]) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: MOVE <filename> <folder>"); return; }
    const char* oldn = args[0]; const char* folder = args[1];
    int idx = hm_get(oldn);
    if (idx < 0) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "Source not found"); return; }
    // Verify target folder exists in registry
    char norm[256]; strncpy(norm, folder, sizeof(norm)-2); norm[sizeof(norm)-2] = '\0';
    size_t ln = strlen(norm); if (ln == 0) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Folder required"); return; }
    if (norm[ln-1] != '/') { norm[ln] = '/'; norm[ln+1] = '\0'; ln++; }
    pthread_mutex_lock(&folder_mutex);
    int folder_found = 0; for (int i = 0; i < folder_count; ++i) { if (strcmp(folder_names[i], norm) == 0) { folder_found = 1; break; } }
    pthread_mutex_unlock(&folder_mutex);
    if (!folder_found) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "Folder not found"); return; }
    // Permission: only owner can move
    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(oldn);
    if (mi < 0 || strcmp(file_meta_map[mi].owner, ci->username) != 0) {
        pthread_mutex_unlock(&file_meta_mutex);
        send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Only the owner can MOVE");
        return;
    }
    pthread_mutex_unlock(&file_meta_mutex);
    // Destination path = folder/ + basename(oldn)
    const char* base = strrchr(oldn, '/'); base = base ? base + 1 : oldn;
    char newn[300]; snprintf(newn, sizeof(newn), "%s%s", norm, base);
    // Prevent overwrite
    if (hm_get(newn) >= 0) { send_error(ci->socket_fd, ERR_FILE_EXISTS, "Destination exists"); return; }
    StorageServerInfo* ss = file_map[idx].ss;
    char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_MOVE " %s %s\n", oldn, newn);
    
    pthread_mutex_lock(&ss->socket_lock);
    send_line(ss->socket_fd, cmd);
    
    char ack[BUF_CMD];
    int retry_count = 0;
    ssize_t n = -1;
    
    // Retry logic for receiving ACK
    while (retry_count < MAX_RETRIES && n <= 0) {
        n = recv_with_timeout(ss->socket_fd, ack, BUF_CMD, RECV_TIMEOUT_SEC);
        if (n <= 0) {
            retry_count++;
            if (retry_count < MAX_RETRIES) {
                // Resend command on timeout
                send_line(ss->socket_fd, cmd);
            }
        }
    }
    
    pthread_mutex_unlock(&ss->socket_lock);
    
    if (n <= 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "No ACK_MOVE after retries"); return; }
    ack[n] = '\0';
    if (strncmp(ack, KW_ACK_MOVE, strlen(KW_ACK_MOVE)) != 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Bad ACK_MOVE"); return; }
    // Parse status token at end
    char* sp = NULL; char* tok = strtok_r(ack, " \n", &sp); // ACK_MOVE
    tok = strtok_r(NULL, " \n", &sp); // old
    tok = strtok_r(NULL, " \n", &sp); // new
    char* status = strtok_r(NULL, " \n", &sp);
    if (!status || strcmp(status, "OK") != 0) { send_error(ci->socket_fd, ERR_INTERNAL, "Move failed"); return; }
    // Update maps & hash with new filename
    pthread_mutex_lock(&file_map_mutex);
    strncpy(file_map_names[idx], newn, sizeof(file_map_names[idx])-1); file_map_names[idx][sizeof(file_map_names[idx])-1] = '\0';
    pthread_mutex_unlock(&file_map_mutex);
    pthread_mutex_lock(&file_meta_mutex);
    // Also update meta name parallel array
    for (int i = 0; i < file_meta_map_count; ++i) {
        if (strcmp(file_meta_map_names[i], oldn) == 0) {
            strncpy(file_meta_map_names[i], newn, sizeof(file_meta_map_names[i])-1);
            file_meta_map_names[i][sizeof(file_meta_map_names[i])-1] = '\0';
            break;
        }
    }
    pthread_mutex_unlock(&file_meta_mutex);
    hm_remove(oldn); hm_put(newn, idx);
    send_line(ci->socket_fd, KW_RES_OK " Moved.\n");
}

void do_viewfolder(ClientInfo* ci, char** args) {
    const char* folder = (args && args[0]) ? args[0] : NULL;
    const char* flag = (args && args[1]) ? args[1] : "NULL";
    if (!folder) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_VIEWFOLDER <user> <folder> [flag]"); return; }

    char prefix[256];
    strncpy(prefix, folder, sizeof(prefix)-1); prefix[sizeof(prefix)-1] = '\0';
    size_t lp = strlen(prefix);
    if (lp == 0) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Folder required"); return; }
    if (prefix[lp-1] != '/') { if (lp + 1 < sizeof(prefix)) { prefix[lp] = '/'; prefix[lp+1] = '\0'; lp++; } }

    typedef struct { char name[100]; StorageServerInfo* ss; int show; char owner[50]; } Entry;
    Entry list[1000]; int count = 0; int detailed = (strcmp(flag, "-l") == 0 || strcmp(flag, "-al") == 0);
    int show_all = (strcmp(flag, "-a") == 0 || strcmp(flag, "-al") == 0);

    pthread_mutex_lock(&file_map_mutex);
    pthread_mutex_lock(&file_meta_mutex);
    for (int i = 0; i < file_map_count && count < (int)(sizeof(list)/sizeof(list[0])); ++i) {
        const char* fname = file_map_names[i];
        if (strncmp(fname, prefix, lp) != 0) continue;
        // Skip artifacts
        size_t ln = strlen(fname);
        if ((ln > 5 && strcmp(fname + ln - 5, ".undo") == 0) || (ln > 4 && strcmp(fname + ln - 4, ".tmp") == 0)) continue;
        int show = 0;
        if (show_all) show = 1; else {
            int mi = meta_index_by_name(fname);
            if (mi >= 0 && user_has_read(&file_meta_map[mi], ci->username)) show = 1;
        }
        if (!show) continue;
        strncpy(list[count].name, fname, sizeof(list[count].name)-1); list[count].name[sizeof(list[count].name)-1] = '\0';
        int fi = file_index_by_name(fname);
        list[count].ss = (fi >= 0) ? file_map[fi].ss : NULL;
        int mi = meta_index_by_name(fname);
        if (mi >= 0) { strncpy(list[count].owner, file_meta_map[mi].owner, sizeof(list[count].owner)-1); list[count].owner[sizeof(list[count].owner)-1] = '\0'; }
        else { list[count].owner[0] = '\0'; }
        count++;
    }
    pthread_mutex_unlock(&file_meta_mutex);
    pthread_mutex_unlock(&file_map_mutex);

    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    if (detailed) {
        send_line(ci->socket_fd, "Filename                       | Words | Chars | Size(KB)   | Created             | Last Access Time     | Owner\n");
        send_line(ci->socket_fd, "------------------------------ | ------ | ------ | ---------- | ------------------- | ------------------- | ----------------\n");
    }
    for (int i = 0; i < count; ++i) {
        if (!detailed) {
            char line[256]; snprintf(line, sizeof(line), "--> %s\n", list[i].name); send_line(ci->socket_fd, line);
        } else {
            FileStats fs; memset(&fs, 0, sizeof(fs));
            strncpy(fs.words, "-", sizeof(fs.words)-1); fs.words[sizeof(fs.words)-1] = '\0';
            strncpy(fs.chars_, "-", sizeof(fs.chars_)-1); fs.chars_[sizeof(fs.chars_)-1] = '\0';
            strncpy(fs.atime, "-", sizeof(fs.atime)-1); fs.atime[sizeof(fs.atime)-1] = '\0';
            if (list[i].ss) { pthread_mutex_lock(&list[i].ss->socket_lock); (void)fetch_stats_from_ss(list[i].ss, list[i].name, &fs); pthread_mutex_unlock(&list[i].ss->socket_lock); }
            char fname_buf[31]; char owner_buf[17];
            snprintf(fname_buf, sizeof(fname_buf), "%-30.30s", list[i].name);
            snprintf(owner_buf, sizeof(owner_buf), "%-16.16s", list[i].owner[0]?list[i].owner:"-");
            char sizekb[16] = "-";
            if (fs.sizeb[0]) { double bytes = atof(fs.sizeb); double kb = bytes/1024.0; snprintf(sizekb, sizeof(sizekb), "%10.2f", kb); } else { snprintf(sizekb, sizeof(sizekb), "%10s", "-"); }
            char out[256]; snprintf(out, sizeof(out), "%s | %-6.6s | %-6.6s | %-10.10s | %-19.19s | %-19.19s | %s\n", fname_buf, fs.words, fs.chars_, sizekb, fs.created[0]?fs.created:"-", fs.atime[0]?fs.atime:"-", owner_buf);
            send_line(ci->socket_fd, out);
        }
    }
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");
}

void do_delete(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    if (!fname) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_DELETE <user> <file>"); return; }

    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    if (fi < 0) {
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found");
        return;
    }

    // check owner
    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    if (mi < 0 || strcmp(file_meta_map[mi].owner, ci->username) != 0) {
        pthread_mutex_unlock(&file_meta_mutex);
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Only owner can delete");
        return;
    }
    pthread_mutex_unlock(&file_meta_mutex);

    StorageServerInfo* ss = file_map[fi].ss;

    // Borrow socket
    pthread_mutex_lock(&ss->socket_lock);
    char cmd[BUF_CMD];
    snprintf(cmd, sizeof(cmd), KW_CMD_DELETE " %s %s\n", fname, ci->username);
    if (send_line(ss->socket_fd, cmd) < 0) {
        pthread_mutex_unlock(&ss->socket_lock);
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server");
        return;
    }
    
    char ack[BUF_CMD+1];
    int retry_count = 0;
    ssize_t n = -1;
    
    // Retry logic for receiving ACK
    while (retry_count < MAX_RETRIES && n <= 0) {
        n = recv_with_timeout(ss->socket_fd, ack, BUF_CMD, RECV_TIMEOUT_SEC);
        if (n <= 0) {
            retry_count++;
            if (retry_count < MAX_RETRIES) {
                // Resend command on timeout
                send_line(ss->socket_fd, cmd);
            }
        }
    }
    
    pthread_mutex_unlock(&ss->socket_lock);
    
    if (n <= 0) {
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Storage Server not responding after retries");
        return;
    }
    ack[n] = '\0';
    if (strncmp(ack, KW_ACK_DELETE, strlen(KW_ACK_DELETE)) != 0) {
        pthread_mutex_unlock(&file_map_mutex);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Storage Server failed");
        return;
    }
    
    // Check if deletion failed (e.g., due to locked sentences)
    if (strstr(ack, "FAIL")) {
        pthread_mutex_unlock(&file_map_mutex);
        // Extract error message from ack
        char* msg_start = strstr(ack, "FAIL");
        if (msg_start) {
            msg_start += 5; // skip "FAIL "
            char* newline = strchr(msg_start, '\n');
            if (newline) *newline = '\0';
            send_error(ci->socket_fd, ERR_LOCKED, msg_start);
        } else {
            send_error(ci->socket_fd, ERR_LOCKED, "Cannot delete file");
        }
        return;
    }

    // Remove from both arrays at the SAME index (they must stay synchronized!)
    pthread_mutex_lock(&file_meta_mutex);
    
    // Remove hashmap entry for the deleted file
    hm_remove(file_map_names[fi]);
    
    // Shift both arrays together to maintain same indices
    for (int j = fi; j < file_map_count - 1; ++j) {
        file_map[j] = file_map[j+1];
        strcpy(file_map_names[j], file_map_names[j+1]);
        // Update hashmap index for shifted entry
        hm_put(file_map_names[j], j);
    }
    file_map_count--;
    
    // Shift metadata array at the SAME positions
    for (int j = fi; j < file_meta_map_count - 1; ++j) {
        file_meta_map[j] = file_meta_map[j+1];
        strcpy(file_meta_map_names[j], file_meta_map_names[j+1]);
    }
    file_meta_map_count--;
    
    pthread_mutex_unlock(&file_meta_mutex);
    pthread_mutex_unlock(&file_map_mutex);

    send_line(ci->socket_fd, KW_RES_OK " File Deleted.\n");
}

void do_location(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    const char* op = args ? args[1] : NULL; // READ/WRITE/STREAM
    if (!fname || !op) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_LOCATION <user> <file> <op>"); return; }

    int cache_hit = 0;
    StorageServerInfo* ss = lru_get(fname, &cache_hit);
    if (!ss) {
        pthread_mutex_lock(&file_map_mutex);
        int fi = file_index_by_name(fname);
        if (fi >= 0) ss = file_map[fi].ss;
        pthread_mutex_unlock(&file_map_mutex);
    if (!ss) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
        lru_put(fname, ss);
    }

    // Permission check
    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    if (mi < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    int allowed = 0;
    if (strcmp(op, "READ") == 0 || strcmp(op, "STREAM") == 0) allowed = user_has_read(&file_meta_map[mi], ci->username);
    else if (strcmp(op, "WRITE") == 0) allowed = user_has_write(&file_meta_map[mi], ci->username);

    // Record last access user for READ/STREAM requests
    if (allowed && (strcmp(op, "READ") == 0 || strcmp(op, "STREAM") == 0)) {
        strncpy(file_meta_map[mi].last_access_user, ci->username, sizeof(file_meta_map[mi].last_access_user)-1);
        file_meta_map[mi].last_access_user[sizeof(file_meta_map[mi].last_access_user)-1] = '\0';
    }
    pthread_mutex_unlock(&file_meta_mutex);

    if (!allowed) { send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Access denied"); return; }

    char line[128];
    snprintf(line, sizeof(line), KW_RES_LOCATION " %s %d\n", ss->ip, ss->client_port);
    send_line(ci->socket_fd, line);

    char logbuf[128];
    snprintf(logbuf, sizeof(logbuf), "LOCATION %s: %s\n", cache_hit ? "HIT" : "MISS", fname);
    log_message(logbuf);
}

void do_exec(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    if (!fname) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_EXEC <user> <file>"); return; }

    // Check access and locate SS
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (!ss) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }

    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    int ok = (mi >= 0) ? user_has_read(&file_meta_map[mi], ci->username) : 0;
    pthread_mutex_unlock(&file_meta_mutex);
    if (!ok) { send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Access denied"); return; }

    // Borrow SS socket and fetch content
    pthread_mutex_lock(&ss->socket_lock);
    char req[BUF_CMD];
    snprintf(req, sizeof(req), KW_REQ_READ_NS_INTERNAL " %s\n", fname);
    if (send_line(ss->socket_fd, req) < 0) {
        pthread_mutex_unlock(&ss->socket_lock);
        send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server");
        return;
    }

    // Receive until RES_DATA_END
    char data[BUF_DATA*4];
    size_t used = 0;
    while (1) {
        char buf[BUF_DATA+1];
        ssize_t n = recv(ss->socket_fd, buf, BUF_DATA, 0);
        if (n <= 0) {
            pthread_mutex_unlock(&ss->socket_lock);
            send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Storage Server disconnected");
            return;
        }
        buf[n] = '\0';
        char* end = strstr(buf, KW_RES_DATA_END);
        if (end) {
            *end = '\0';
            size_t len = strlen(buf);
            if (used + len < sizeof(data)) {
                memcpy(data + used, buf, len);
                used += len;
            }
            break;
        } else {
            if (used + (size_t)n < sizeof(data)) {
                memcpy(data + used, buf, (size_t)n);
                used += (size_t)n;
            }
        }
    }
    pthread_mutex_unlock(&ss->socket_lock);

    // Write to temp file
    char tmp[128];
    snprintf(tmp, sizeof(tmp), "/tmp/exec_%s.sh", ci->username);
    FILE* fp = fopen(tmp, "w");
    if (!fp) { send_error(ci->socket_fd, ERR_INTERNAL, "Failed to create temp file"); return; }
    fwrite(data, 1, used, fp);
    fclose(fp);

    // Execute and stream back
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "sh %s", tmp);
    FILE* pipe = popen(cmd, "r");
    if (!pipe) {
        remove(tmp);
        send_error(ci->socket_fd, ERR_INTERNAL, "Execution failed");
        return;
    }
    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    char line[512];
    while (fgets(line, sizeof(line), pipe)) {
        send_line(ci->socket_fd, line);
    }
    pclose(pipe);
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");
    remove(tmp);

    log_message("EXEC completed");
}

/* ---------------- Bonus Feature: Access Requests ---------------- */
static int find_meta_index(const char* fname) { return meta_index_by_name(fname); }

void do_requestaccess(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    const char* typ = args ? args[1] : NULL; // R or W
    if (!fname || !typ || !(typ[0] == 'R' || typ[0] == 'W')) {
        send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_REQUESTACCESS <user> <file> <R|W>");
        return;
    }
    pthread_mutex_lock(&file_meta_mutex);
    int mi = find_meta_index(fname);
    if (mi < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    FileMeta* fm = &file_meta_map[mi];
    // Already has access?
    int already = (typ[0] == 'R') ? user_has_read(fm, ci->username) : user_has_write(fm, ci->username);
    if (already) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_INVALID_ARGS, "Already has access"); return; }
    // Check not duplicated
    for (int i = 0; i < fm->req_count; ++i) {
        if (strcmp(fm->req_users[i], ci->username) == 0 && fm->req_types[i] == typ[0]) {
            pthread_mutex_unlock(&file_meta_mutex);
            send_error(ci->socket_fd, ERR_INVALID_ARGS, "Request already pending");
            return;
        }
    }
    if (fm->req_count >= 20) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_INTERNAL, "Too many pending requests"); return; }
    strncpy(fm->req_users[fm->req_count], ci->username, 49); fm->req_users[fm->req_count][49] = '\0';
    fm->req_types[fm->req_count] = typ[0]; fm->req_count++;
    pthread_mutex_unlock(&file_meta_mutex);

    // Persist on SS
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (ss) {
        pthread_mutex_lock(&ss->socket_lock);
        char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_META_REQ_ADD " %s %c %s\n", fname, typ[0], ci->username);
        send_line(ss->socket_fd, cmd);
        pthread_mutex_unlock(&ss->socket_lock);
    }
    send_line(ci->socket_fd, KW_RES_OK " Request submitted.\n");
}

void do_listrequests(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    if (!fname) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_LISTREQUESTS <user> <file>"); return; }
    pthread_mutex_lock(&file_meta_mutex);
    int mi = find_meta_index(fname);
    if (mi < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    FileMeta* fm = &file_meta_map[mi];
    if (strcmp(fm->owner, ci->username) != 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Only owner may view requests"); return; }
    send_line(ci->socket_fd, KW_RES_DATA_START "\n");
    for (int i = 0; i < fm->req_count; ++i) {
        char line[256]; snprintf(line, sizeof(line), "--> %s : %c\n", fm->req_users[i], fm->req_types[i]);
        send_line(ci->socket_fd, line);
    }
    send_line(ci->socket_fd, KW_RES_DATA_END "\n");
    pthread_mutex_unlock(&file_meta_mutex);
}

void do_respondrequest(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    const char* target = args ? args[1] : NULL;
    const char* action = args ? args[2] : NULL; // approve|deny
    const char* typ = args ? args[3] : NULL; // R|W
    if (!fname || !target || !action || !typ || !(typ[0] == 'R' || typ[0] == 'W')) {
        send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REQ_RESPONDREQUEST <user> <file> <target> <approve|deny> <R|W>");
        return;
    }
    pthread_mutex_lock(&file_meta_mutex);
    int mi = find_meta_index(fname);
    if (mi < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    FileMeta* fm = &file_meta_map[mi];
    if (strcmp(fm->owner, ci->username) != 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Only owner may respond"); return; }
    int req_index = -1;
    for (int i = 0; i < fm->req_count; ++i) {
        if (strcmp(fm->req_users[i], target) == 0 && fm->req_types[i] == typ[0]) { req_index = i; break; }
    }
    if (req_index < 0) { pthread_mutex_unlock(&file_meta_mutex); send_error(ci->socket_fd, ERR_INVALID_ARGS, "No such pending request"); return; }
    int approve = (strcmp(action, "approve") == 0);
    // Remove request first
    for (int j = req_index; j < fm->req_count - 1; ++j) {
        strcpy(fm->req_users[j], fm->req_users[j+1]);
        fm->req_types[j] = fm->req_types[j+1];
    }
    fm->req_count--;
    pthread_mutex_unlock(&file_meta_mutex);

    // Persist removal
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (ss) {
        pthread_mutex_lock(&ss->socket_lock);
        char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_META_REQ_REM " %s %c %s\n", fname, typ[0], target);
        send_line(ss->socket_fd, cmd);
        pthread_mutex_unlock(&ss->socket_lock);
    }

    if (approve) {
        // Grant access (adds to R or W list and persists)
        pthread_mutex_lock(&file_meta_mutex);
        fm = &file_meta_map[mi];
        if (typ[0] == 'R') {
            if (!user_has_read(fm, target) && fm->read_count < 10) { strncpy(fm->read_access_users[fm->read_count], target, 49); fm->read_access_users[fm->read_count][49] = '\0'; fm->read_count++; }
        } else if (typ[0] == 'W') {
            if (!user_has_write(fm, target) && fm->write_count < 10) { strncpy(fm->write_access_users[fm->write_count], target, 49); fm->write_access_users[fm->write_count][49] = '\0'; fm->write_count++; }
        }
        pthread_mutex_unlock(&file_meta_mutex);
        if (ss) {
            pthread_mutex_lock(&ss->socket_lock);
            char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_META_ADD " %s %c %s\n", fname, typ[0], target);
            send_line(ss->socket_fd, cmd);
            pthread_mutex_unlock(&ss->socket_lock);
        }
        send_line(ci->socket_fd, KW_RES_OK " Request approved; access granted.\n");
    } else {
        send_line(ci->socket_fd, KW_RES_OK " Request denied.\n");
    }
}

/* ---------------- Bonus: Checkpoints ---------------- */
void do_checkpoint(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    const char* tag = args ? args[1] : NULL;
    if (!fname || !tag) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: CHECKPOINT <file> <tag>"); return; }
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname);
    StorageServerInfo* ss = (fi >= 0) ? file_map[fi].ss : NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (!ss) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname);
    int allowed = (mi >= 0) ? user_has_write(&file_meta_map[mi], ci->username) : 0; // require write permission to checkpoint
    pthread_mutex_unlock(&file_meta_mutex);
    if (!allowed) { send_error(ci->socket_fd, ERR_ACCESS_DENIED, "No write access"); return; }
    pthread_mutex_lock(&ss->socket_lock);
    char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_CHECKPOINT " %s %s\n", fname, tag);
    if (send_line(ss->socket_fd, cmd) < 0) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server"); return; }
    
    char ack[BUF_CMD];
    int retry_count = 0;
    ssize_t n = -1;
    
    // Retry logic for receiving ACK
    while (retry_count < MAX_RETRIES && n <= 0) {
        n = recv_with_timeout(ss->socket_fd, ack, sizeof(ack)-1, RECV_TIMEOUT_SEC);
        if (n <= 0) {
            retry_count++;
            if (retry_count < MAX_RETRIES) {
                // Resend command on timeout
                send_line(ss->socket_fd, cmd);
            }
        }
    }
    
    pthread_mutex_unlock(&ss->socket_lock);
    
    if (n <= 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "No ACK_CHECKPOINT after retries"); return; }
    ack[n] = '\0';
    if (strncmp(ack, KW_ACK_CHECKPOINT, strlen(KW_ACK_CHECKPOINT)) != 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Bad ACK_CHECKPOINT"); return; }
    // Forward simplified result to client
    if (strstr(ack, " OK")) send_line(ci->socket_fd, KW_RES_OK " Checkpoint created.\n"); else send_error(ci->socket_fd, ERR_INVALID_ARGS, "Checkpoint failed or exists");
}

void do_listcheckpoints(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL;
    if (!fname) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: LISTCHECKPOINTS <file>"); return; }
    pthread_mutex_lock(&file_map_mutex);
    int fi = file_index_by_name(fname); StorageServerInfo* ss = (fi >= 0)?file_map[fi].ss:NULL;
    pthread_mutex_unlock(&file_map_mutex);
    if (!ss) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    pthread_mutex_lock(&file_meta_mutex);
    int mi = meta_index_by_name(fname); int allowed = (mi>=0)?user_has_read(&file_meta_map[mi], ci->username):0;
    pthread_mutex_unlock(&file_meta_mutex);
    if (!allowed) { send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Access denied"); return; }
    pthread_mutex_lock(&ss->socket_lock);
    char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_LISTCHECKPOINTS " %s\n", fname);
    if (send_line(ss->socket_fd, cmd) < 0) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server"); return; }
    // Stream response until RES_DATA_END
    char buf[BUF_DATA+1];
    int started = 0;
    while (1) {
        ssize_t n = recv(ss->socket_fd, buf, BUF_DATA, 0);
        if (n <= 0) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Disconnected"); return; }
        buf[n] = '\0';
        if (!started) { if (strncmp(buf, KW_RES_DATA_START, strlen(KW_RES_DATA_START)) == 0) { send_line(ci->socket_fd, KW_RES_DATA_START "\n"); started = 1; }
            else { // unexpected
                pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Malformed checkpoint list"); return; }
            char* after = strstr(buf, "\n"); if (after) { after++; if (*after) { char* end = strstr(after, KW_RES_DATA_END); if (end) { *end='\0'; send_line(ci->socket_fd, after); send_line(ci->socket_fd, KW_RES_DATA_END "\n"); pthread_mutex_unlock(&ss->socket_lock); return; } else send_line(ci->socket_fd, after); }
            continue; }
        }
        char* end = strstr(buf, KW_RES_DATA_END);
        if (end) { *end='\0'; if (*buf) send_line(ci->socket_fd, buf); send_line(ci->socket_fd, KW_RES_DATA_END "\n"); pthread_mutex_unlock(&ss->socket_lock); return; }
        if (*buf) send_line(ci->socket_fd, buf);
    }
}

void do_viewcheckpoint(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL; const char* tag = args ? args[1] : NULL;
    if (!fname || !tag) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: VIEWCHECKPOINT <file> <tag>"); return; }
    pthread_mutex_lock(&file_map_mutex); int fi = file_index_by_name(fname); StorageServerInfo* ss = (fi>=0)?file_map[fi].ss:NULL; pthread_mutex_unlock(&file_map_mutex);
    if (!ss) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    pthread_mutex_lock(&file_meta_mutex); int mi = meta_index_by_name(fname); int allowed = (mi>=0)?user_has_read(&file_meta_map[mi], ci->username):0; pthread_mutex_unlock(&file_meta_mutex);
    if (!allowed) { send_error(ci->socket_fd, ERR_ACCESS_DENIED, "Access denied"); return; }
    pthread_mutex_lock(&ss->socket_lock);
    char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_VIEWCHECKPOINT " %s %s\n", fname, tag);
    if (send_line(ss->socket_fd, cmd) < 0) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server"); return; }
    // Relay data frame
    char buf[BUF_DATA+1]; int started=0;
    while (1) {
        ssize_t n = recv(ss->socket_fd, buf, BUF_DATA, 0);
        if (n <= 0) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Disconnected"); return; }
        buf[n]='\0';
    if (strncmp(buf, KW_RES_ERROR, strlen(KW_RES_ERROR)) == 0) { pthread_mutex_unlock(&ss->socket_lock); send_line(ci->socket_fd, buf); return; }
        if (!started) {
            char* p = strstr(buf, KW_RES_DATA_START);
            if (!p) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Malformed checkpoint view"); return; }
            send_line(ci->socket_fd, KW_RES_DATA_START "\n");
            p += strlen(KW_RES_DATA_START);
            if (*p=='\n') p++;
            char* end = strstr(p, KW_RES_DATA_END);
            if (end) { *end='\0'; if (*p) send_line(ci->socket_fd, p); send_line(ci->socket_fd, KW_RES_DATA_END "\n"); pthread_mutex_unlock(&ss->socket_lock); return; }
            if (*p) send_line(ci->socket_fd, p);
            started=1; continue;
        }
        char* end = strstr(buf, KW_RES_DATA_END);
        if (end) { *end='\0'; if (*buf) send_line(ci->socket_fd, buf); send_line(ci->socket_fd, KW_RES_DATA_END "\n"); pthread_mutex_unlock(&ss->socket_lock); return; }
        if (*buf) send_line(ci->socket_fd, buf);
    }
}

void do_revertcheckpoint(ClientInfo* ci, char** args) {
    const char* fname = args ? args[0] : NULL; const char* tag = args ? args[1] : NULL;
    if (!fname || !tag) { send_error(ci->socket_fd, ERR_INVALID_ARGS, "Usage: REVERTCHECKPOINT <file> <tag>"); return; }
    pthread_mutex_lock(&file_map_mutex); int fi = file_index_by_name(fname); StorageServerInfo* ss = (fi>=0)?file_map[fi].ss:NULL; pthread_mutex_unlock(&file_map_mutex);
    if (!ss) { send_error(ci->socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    pthread_mutex_lock(&file_meta_mutex); int mi = meta_index_by_name(fname); int allowed = (mi>=0)?user_has_write(&file_meta_map[mi], ci->username):0; pthread_mutex_unlock(&file_meta_mutex);
    if (!allowed) { send_error(ci->socket_fd, ERR_ACCESS_DENIED, "No write access"); return; }
    pthread_mutex_lock(&ss->socket_lock);
    char cmd[BUF_CMD]; snprintf(cmd, sizeof(cmd), KW_CMD_REVERT " %s %s\n", fname, tag);
    if (send_line(ss->socket_fd, cmd) < 0) { pthread_mutex_unlock(&ss->socket_lock); send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Failed to contact Storage Server"); return; }
    char ack[BUF_CMD]; ssize_t n = recv(ss->socket_fd, ack, sizeof(ack)-1, 0); pthread_mutex_unlock(&ss->socket_lock);
    if (n <= 0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "No ACK_REVERT"); return; }
    ack[n]='\0'; if (strncmp(ack, KW_ACK_REVERT, strlen(KW_ACK_REVERT))!=0) { send_error(ci->socket_fd, ERR_SS_COMMUNICATION, "Bad ACK_REVERT"); return; }
    if (strstr(ack, " OK")) send_line(ci->socket_fd, KW_RES_OK " Reverted to checkpoint.\n"); else send_error(ci->socket_fd, ERR_INVALID_ARGS, "Checkpoint revert failed");
}
