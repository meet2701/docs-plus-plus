// Clean, corrected storageserver.c replacement
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "common.h"

#define BUF_CMD 1024
#define BUF_DATA 4096

/* Protocol keywords */
#define KW_RES_OK "RES_OK"
#define KW_RES_ERROR "RES_ERROR"
#define KW_RES_DATA_START "RES_DATA_START"
#define KW_RES_DATA_END "RES_DATA_END"
#define KW_DATA_STREAM_WORD "DATA_STREAM_WORD"
#define KW_DATA_STREAM_END "DATA_STREAM_END"

#define KW_REGISTER_SS "REGISTER_SS"
#define KW_REGISTER_SS_FILE "REGISTER_SS_FILE"
#define KW_REGISTER_SS_DONE "REGISTER_SS_DONE"
#define KW_ACK_REGISTER "ACK_REGISTER"
#define KW_ACK_CREATE "ACK_CREATE"
#define KW_ACK_DELETE "ACK_DELETE"
#define KW_ACK_CREATEFOLDER "ACK_CREATEFOLDER"
#define KW_ACK_MOVE "ACK_MOVE"
#define KW_ACK_CHECKPOINT "ACK_CHECKPOINT"
#define KW_ACK_REVERT "ACK_REVERT"
#define KW_CMD_CREATE "CMD_CREATE"
#define KW_CMD_DELETE "CMD_DELETE"
#define KW_CMD_CREATEFOLDER "CMD_CREATEFOLDER"
#define KW_CMD_MOVE "CMD_MOVE"
#define KW_CMD_CHECKPOINT "CMD_CHECKPOINT"
#define KW_CMD_REVERT "CMD_REVERT"
#define KW_CMD_VIEWCHECKPOINT "CMD_VIEWCHECKPOINT"
#define KW_CMD_LISTCHECKPOINTS "CMD_LISTCHECKPOINTS"
#define KW_CMD_META_ADD "CMD_META_ADD"
#define KW_CMD_META_REM "CMD_META_REM"
// Access request persistence from NS
#define KW_CMD_META_REQ_ADD "CMD_META_REQ_ADD"
#define KW_CMD_META_REQ_REM "CMD_META_REQ_REM"

typedef struct {
    char filename[100];
    int sentence_num;
} SentenceLock;

static SentenceLock g_active_locks[100];
static int g_active_lock_count = 0;
static pthread_mutex_t g_lock_list_mutex = PTHREAD_MUTEX_INITIALIZER;
// Helper: check if a sentence of a file is currently locked
static int is_locked(const char* filename, int sentence_num) {
    for (int i = 0; i < g_active_lock_count; ++i) {
        if (strcmp(g_active_locks[i].filename, filename) == 0 &&
            g_active_locks[i].sentence_num == sentence_num) {
            return 1;
        }
    }
    return 0;
}

// Helper: check if ANY sentence of a file is currently locked
static int has_any_lock(const char* filename) {
    for (int i = 0; i < g_active_lock_count; ++i) {
        if (strcmp(g_active_locks[i].filename, filename) == 0) {
            return 1;
        }
    }
    return 0;
}

// Session-scoped write context to keep a locked sentence stable during a write session
typedef struct {
    char filename[100];
    int sentence_num;
    char* pre;   // text before the locked sentence
    char* cur;   // locked sentence text (active sentence within the session)
    char* post_orig;  // original text after the locked sentence (snapshot at lock)
    char* post_extra; // additional sentences created from the locked sentence during the session
    char* cur_orig; // original sentence snapshot at lock time for rebasing on release
    char user[50]; // locker username for permission revalidation on release
} WriteSession;

static WriteSession g_sessions[100];
static int g_session_count = 0;
static pthread_mutex_t g_session_mutex = PTHREAD_MUTEX_INITIALIZER;

// helpers for sessions
static int find_session(const char* filename, int sentence_num);
static void free_session(WriteSession* s);
static void remove_session_by_index(int idx);
static char* concat_range(char** arr, int start, int end);

static char g_data_dir[256];
static int g_ns_socket_fd = -1;
static int g_client_listen_port = 0;
static char g_advertised_ip[50] = {0};
static pthread_mutex_t g_file_io_mutex = PTHREAD_MUTEX_INITIALIZER; // serialize final writes

/* Forward decls */
static void log_message(char* message);
static void build_path(char* buffer, size_t buf_size, const char* dir, const char* file);
static int copy_file(const char* src_path, const char* dest_path);
static int ensure_dirs(void);
static int ensure_parent_dirs(const char* base_dir, const char* rel_path);
static void register_files_recursive(int sockfd, const char* base_dir, const char* rel_prefix);

static int connect_and_register_to_ns(const char* ns_ip, int ns_port);
static void* ns_listener_thread(void* arg);
static void* handle_client_request(void* arg);

/* File operations */
static void do_create(const char* filename, const char* owner);
static void do_delete(const char* filename);
static void do_createfolder(const char* folderpath);
static void do_move(const char* oldname, const char* newname);
static void do_read(int client_socket_fd, const char* filename);
static void do_read_for_ns(const char* filename);
static void do_stream(int client_socket_fd, const char* filename);
static void do_undo(int client_socket_fd, const char* filename);
static void do_checkpoint(const char* filename, const char* tag);
static void do_viewcheckpoint(const char* filename, const char* tag);
static void do_listcheckpoints(const char* filename);
static void do_revertcheckpoint(const char* filename, const char* tag);

/* Write flow */
static int do_write_lock(int client_socket_fd, const char* filename, int sentence_num, const char* user);
static void write_session_loop(int client_socket_fd, const char* filename, int sentence_num);
static int do_write_update(const char* filename, int sentence_num, int word_index, const char* content);
static int do_write_release(const char* filename, int sentence_num);

/* Utils */
static char* read_whole_file_preferring_tmp(const char* filename, size_t* out_len);
static char* read_whole_file_base_only(const char* filename, size_t* out_len);
static int write_whole_tmp(const char* filename, const char* content, size_t len);
static char** split_sentences(const char* text, int* count);
static void free_str_array(char** arr, int count);
static char* join_sentences(char** arr, int count, size_t* out_len);
static char** split_words(const char* sentence, int* count);
static char* join_words(char** words, int count);
static void build_meta_path(char* meta_path, size_t meta_path_size, const char* filename);
static void format_time(time_t t, char* out, size_t out_sz);
static int meta_load(const char* filename, char* owner_out, size_t owner_out_sz,
                     char read_users[][50], int* read_count,
                     char write_users[][50], int* write_count,
                     char req_users[][50], char req_types[], int* req_count);
static int meta_write(const char* filename, const char* owner,
                      char read_users[][50], int read_count,
                      char write_users[][50], int write_count,
                      char req_users[][50], char req_types[], int req_count);
static int meta_add_user(const char* filename, char type, const char* user);
static int meta_remove_user(const char* filename, char type, const char* user);
static int meta_add_request(const char* filename, char type, const char* user);
static int meta_remove_request(const char* filename, char type, const char* user);

static int send_all(int fd, const char* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, buf + total, len - total, 0);
        if (n <= 0) return -1;
        total += (size_t)n;
    }
    return 0;
}

/* Fallback error code definitions in case common.h isn't available to the build
 * (keeps this file self-contained for static checks). These match the values
 * defined in common.h.
 */
#ifndef ERR_OK
#define ERR_OK 0
#define ERR_UNKNOWN_COMMAND 1
#define ERR_FILE_NOT_FOUND 2
#define ERR_ACCESS_DENIED 3
#define ERR_FILE_EXISTS 4
#define ERR_NO_SS_AVAILABLE 5
#define ERR_SS_COMMUNICATION 6
#define ERR_INVALID_ARGS 7
#define ERR_LOCKED 8
#define ERR_INVALID_INDEX 9
#define ERR_INTERNAL 10
#endif

static int send_line(int fd, const char* s) {
    return send_all(fd, s, strlen(s));
}

static void send_error_ss(int fd, int code, const char* msg) {
    char buf[512];
    snprintf(buf, sizeof(buf), KW_RES_ERROR " %d %s\n", code, msg);
    send_line(fd, buf);
}

static void log_message(char* message) {
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);
    // Print to console for live visibility
    printf("[%s] %s\n", ts, message);
    fflush(stdout);
    // Append to log file
    FILE* lf = fopen("ss.log", "a");
    if (lf) {
        fprintf(lf, "[%s] %s\n", ts, message);
        fclose(lf);
    }
}

static void build_path(char* buffer, size_t buf_size, const char* dir, const char* file) {
    if (!dir || !file) { if (buffer && buf_size) buffer[0] = '\0'; return; }
    size_t ld = strlen(dir);
    if (ld > 0 && (dir[ld-1] == '/' || dir[ld-1] == '\\')) {
        snprintf(buffer, buf_size, "%s%s", dir, file);
    } else {
        snprintf(buffer, buf_size, "%s/%s", dir, file);
    }
}

static int copy_file(const char* src_path, const char* dest_path) {
    FILE* in = fopen(src_path, "rb");
    if (!in) return -1;
    FILE* out = fopen(dest_path, "wb");
    if (!out) { fclose(in); return -1; }
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, n, out) != n) { fclose(in); fclose(out); return -1; }
    }
    fclose(in);
    fclose(out);
    return 0;
}

static int ensure_dirs(void) {
    char files_dir[512], meta_dir[512];
    build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    build_path(meta_dir, sizeof(meta_dir), g_data_dir, "meta");
    mkdir(g_data_dir, 0755);
    mkdir(files_dir, 0755);
    mkdir(meta_dir, 0755);
    // IMPORTANT: Do NOT delete .tmp files on startup. They may hold uncommitted writes.
    // Startup should preserve these so that subsequent reads prefer .tmp if present.
    return 0;
}

static int ensure_parent_dirs(const char* base_dir, const char* rel_path) {
    if (!rel_path || !*rel_path) return 0;
    char path[1024];
    build_path(path, sizeof(path), base_dir, rel_path);
    // Iterate through path and create each intermediate directory
    for (size_t i = 0; i < strlen(path); ++i) {
        if (path[i] == '/') {
            path[i] = '\0';
            mkdir(path, 0755);
            path[i] = '/';
        }
    }
    return 0;
}

static void register_files_recursive(int sockfd, const char* base_dir, const char* rel_prefix) {
    char dirpath[1024];
    build_path(dirpath, sizeof(dirpath), base_dir, rel_prefix);
    DIR* d = opendir(dirpath);
    if (!d) return;
    struct dirent* de;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        char rel[1024];
        if (rel_prefix[0]) snprintf(rel, sizeof(rel), "%s/%s", rel_prefix, de->d_name);
        else snprintf(rel, sizeof(rel), "%s", de->d_name);
        // If directory, recurse
        char fullpath[1024]; build_path(fullpath, sizeof(fullpath), base_dir, rel);
        struct stat st; if (stat(fullpath, &st) == 0 && S_ISDIR(st.st_mode)) {
            register_files_recursive(sockfd, base_dir, rel);
            continue;
        }
        // Skip artifacts
        size_t ln = strlen(rel);
        if ((ln > 5 && strcmp(rel + ln - 5, ".undo") == 0) || (ln > 4 && strcmp(rel + ln - 4, ".tmp") == 0)) continue;
        // Metadata
        char owner[64] = {0}; char rusers[10][50]; int rcount=0; char wusrs[10][50]; int wcount=0; char qusers[20][50]; char qtypes[20]; int qcount=0;
        meta_load(rel, owner, sizeof(owner), rusers, &rcount, wusrs, &wcount, qusers, qtypes, &qcount);
        char msg[BUF_CMD]; int off = snprintf(msg, sizeof(msg), KW_REGISTER_SS_FILE " %s OWNER %s R %d", rel, owner[0]?owner:"", rcount);
        for (int i=0;i<rcount && off < (int)sizeof(msg)-2;++i) off += snprintf(msg+off, sizeof(msg)-(size_t)off, " %s", rusers[i]);
        off += snprintf(msg+off, sizeof(msg)-(size_t)off, " W %d", wcount);
        for (int i=0;i<wcount && off < (int)sizeof(msg)-2;++i) off += snprintf(msg+off, sizeof(msg)-(size_t)off, " %s", wusrs[i]);
        off += snprintf(msg+off, sizeof(msg)-(size_t)off, " Q %d", qcount);
        for (int i=0;i<qcount && off < (int)sizeof(msg)-4;++i) off += snprintf(msg+off, sizeof(msg)-(size_t)off, " %s:%c", qusers[i], qtypes[i]);
        off += snprintf(msg+off, sizeof(msg)-(size_t)off, "\n");
        send_line(sockfd, msg);
    }
    closedir(d);
}

/* ---------- Metadata (.meta) helpers ---------- */
static void build_meta_path(char* meta_path, size_t meta_path_size, const char* filename) {
    char meta_dir[512];
    build_path(meta_dir, sizeof(meta_dir), g_data_dir, "meta");
    build_path(meta_path, meta_path_size, meta_dir, filename);
    strncat(meta_path, ".meta", meta_path_size - strlen(meta_path) - 1);
}

static void format_time(time_t t, char* out, size_t out_sz) {
    struct tm tmv; localtime_r(&t, &tmv);
    strftime(out, out_sz, "%Y-%m-%d %H:%M:%S", &tmv);
}

static int meta_load(const char* filename, char* owner_out, size_t owner_out_sz,
                     char read_users[][50], int* read_count,
                     char write_users[][50], int* write_count,
                     char req_users[][50], char req_types[], int* req_count) {
    if (owner_out && owner_out_sz) owner_out[0] = '\0';
    if (read_count) *read_count = 0; if (write_count) *write_count = 0; if (req_count) *req_count = 0;

    char meta_path[1024];
    build_meta_path(meta_path, sizeof(meta_path), filename);
    FILE* m = fopen(meta_path, "r");
    if (!m) return -1; // meta may not exist yet
    char line[256];
    while (fgets(line, sizeof(line), m)) {
        if (strncmp(line, "owner=", 6) == 0) {
            char* v = line + 6; size_t n = strcspn(v, "\r\n"); v[n] = '\0';
            if (owner_out) strncpy(owner_out, v, owner_out_sz - 1), owner_out[owner_out_sz - 1] = '\0';
        } else if (strncmp(line, "read=", 5) == 0) {
            char* v = line + 5; size_t n = strcspn(v, "\r\n"); v[n] = '\0';
            if (read_users && read_count && *read_count < 10) {
                strncpy(read_users[*read_count], v, 49);
                read_users[*read_count][49] = '\0';
                (*read_count)++;
            }
        } else if (strncmp(line, "write=", 6) == 0) {
            char* v = line + 6; size_t n = strcspn(v, "\r\n"); v[n] = '\0';
            if (write_users && write_count && *write_count < 10) {
                strncpy(write_users[*write_count], v, 49);
                write_users[*write_count][49] = '\0';
                (*write_count)++;
            }
        } else if (strncmp(line, "request=", 8) == 0) {
            char* v = line + 8; size_t n = strcspn(v, "\r\n"); v[n] = '\0';
            if (req_users && req_types && req_count && *req_count < 20) {
                char* colon = strrchr(v, ':');
                if (colon && colon[1]) {
                    *colon = '\0';
                    strncpy(req_users[*req_count], v, 49);
                    req_users[*req_count][49] = '\0';
                    req_types[*req_count] = colon[1];
                    (*req_count)++;
                }
            }
        }
    }
    fclose(m);
    return 0;
}

static int meta_write(const char* filename, const char* owner,
                      char read_users[][50], int read_count,
                      char write_users[][50], int write_count,
                      char req_users[][50], char req_types[], int req_count) {
    char meta_path[1024];
    build_meta_path(meta_path, sizeof(meta_path), filename);
    FILE* m = fopen(meta_path, "w");
    if (!m) return -1;
    if (owner && *owner) fprintf(m, "owner=%s\n", owner);
    else fprintf(m, "owner=\n");
    for (int i = 0; i < read_count; ++i) fprintf(m, "read=%s\n", read_users[i]);
    for (int i = 0; i < write_count; ++i) fprintf(m, "write=%s\n", write_users[i]);
    for (int i = 0; i < req_count; ++i) fprintf(m, "request=%s:%c\n", req_users[i], req_types[i]);
    fclose(m);
    return 0;
}

static int meta_add_user(const char* filename, char type, const char* user) {
    char owner[64] = {0};
    char rusers[10][50]; int rcount = 0;
    char wusrs[10][50]; int wcount = 0;
    char qusers[20][50]; char qtypes[20]; int qcount = 0;
    meta_load(filename, owner, sizeof(owner), rusers, &rcount, wusrs, &wcount, qusers, qtypes, &qcount);
    if (type == 'R') {
        if (rcount >= 10) return -1;
        for (int i = 0; i < rcount; ++i) if (strcmp(rusers[i], user) == 0) return 0;
        strncpy(rusers[rcount], user, 49); rusers[rcount][49] = '\0';
        rcount++;
    } else if (type == 'W') {
        if (wcount >= 10) return -1;
        for (int i = 0; i < wcount; ++i) if (strcmp(wusrs[i], user) == 0) return 0;
        strncpy(wusrs[wcount], user, 49); wusrs[wcount][49] = '\0';
        wcount++;
    } else {
        return -1;
    }
    return meta_write(filename, owner, rusers, rcount, wusrs, wcount, qusers, qtypes, qcount);
}

static int meta_remove_user(const char* filename, char type, const char* user) {
    char owner[64] = {0};
    char rusers[10][50]; int rcount = 0;
    char wusrs[10][50]; int wcount = 0;
    char qusers[20][50]; char qtypes[20]; int qcount = 0;
    meta_load(filename, owner, sizeof(owner), rusers, &rcount, wusrs, &wcount, qusers, qtypes, &qcount);
    if (type == 'R') {
        int j = 0; for (int i = 0; i < rcount; ++i) if (strcmp(rusers[i], user) != 0) { if (j != i) strcpy(rusers[j], rusers[i]); j++; }
        rcount = j;
    } else if (type == 'W') {
        int j = 0; for (int i = 0; i < wcount; ++i) if (strcmp(wusrs[i], user) != 0) { if (j != i) strcpy(wusrs[j], wusrs[i]); j++; }
        wcount = j;
    } else {
        return -1;
    }
    return meta_write(filename, owner, rusers, rcount, wusrs, wcount, qusers, qtypes, qcount);
}

static int meta_add_request(const char* filename, char type, const char* user) {
    char owner[64] = {0};
    char rusers[10][50]; int rcount = 0;
    char wusrs[10][50]; int wcount = 0;
    char qusers[20][50]; char qtypes[20]; int qcount = 0;
    meta_load(filename, owner, sizeof(owner), rusers, &rcount, wusrs, &wcount, qusers, qtypes, &qcount);
    if (qcount >= 20) return -1;
    for (int i = 0; i < qcount; ++i) if (strcmp(qusers[i], user) == 0 && qtypes[i] == type) return 0;
    strncpy(qusers[qcount], user, 49); qusers[qcount][49] = '\0'; qtypes[qcount] = type; qcount++;
    return meta_write(filename, owner, rusers, rcount, wusrs, wcount, qusers, qtypes, qcount);
}

static int meta_remove_request(const char* filename, char type, const char* user) {
    char owner[64] = {0};
    char rusers[10][50]; int rcount = 0;
    char wusrs[10][50]; int wcount = 0;
    char qusers[20][50]; char qtypes[20]; int qcount = 0;
    meta_load(filename, owner, sizeof(owner), rusers, &rcount, wusrs, &wcount, qusers, qtypes, &qcount);
    int j = 0; for (int i = 0; i < qcount; ++i) {
        if (!(strcmp(qusers[i], user) == 0 && qtypes[i] == type)) {
            if (j != i) { strcpy(qusers[j], qusers[i]); qtypes[j] = qtypes[i]; }
            j++;
        }
    }
    qcount = j;
    return meta_write(filename, owner, rusers, rcount, wusrs, wcount, qusers, qtypes, qcount);
}

static int connect_and_register_to_ns(const char* ns_ip, int ns_port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return -1; }
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)ns_port);
    addr.sin_addr.s_addr = inet_addr(ns_ip);
    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("connect"); close(sockfd); return -1; }

    g_ns_socket_fd = sockfd;

    char reg[BUF_CMD];
    snprintf(reg, sizeof(reg), KW_REGISTER_SS " %s %d\n", g_advertised_ip, g_client_listen_port);
    if (send_line(sockfd, reg) < 0) { fprintf(stderr, "Failed to send REGISTER_SS\n"); return -1; }

    char ack[BUF_CMD+1];
    ssize_t n = recv(sockfd, ack, BUF_CMD, 0);
    if (n <= 0) { fprintf(stderr, "No ACK_REGISTER\n"); return -1; }
    ack[n] = '\0';
    if (strncmp(ack, KW_ACK_REGISTER, strlen(KW_ACK_REGISTER)) != 0) {
        fprintf(stderr, "Bad ACK: %s\n", ack);
        return -1;
    }

    // Scan files dir recursively and register existing files (with metadata)
    char files_dir[512];
    build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    register_files_recursive(sockfd, files_dir, "");
    send_line(sockfd, KW_REGISTER_SS_DONE "\n");
    log_message("Storage Server registered with Name Server.");
    return 0;
}

static void* ns_listener_thread(void* arg) {
    (void)arg;
    pthread_detach(pthread_self());
    // Persistent line buffer across recv() calls
    char acc[BUF_CMD * 8];
    size_t used = 0;
    while (1) {
        if (used >= sizeof(acc)) used = 0; // prevent overflow by resetting
        ssize_t n = recv(g_ns_socket_fd, acc + used, sizeof(acc) - used - 1, 0);
        if (n <= 0) {
            log_message("Name Server disconnected.");
            close(g_ns_socket_fd);
            g_ns_socket_fd = -1;
            pthread_exit(NULL);
        }
        used += (size_t)n;
        acc[used] = '\0';

        // Process complete lines
        char* start = acc;
        char* nl;
        while ((nl = strchr(start, '\n')) != NULL) {
            *nl = '\0';
            char linebuf[BUF_CMD * 2];
            strncpy(linebuf, start, sizeof(linebuf)-1);
            linebuf[sizeof(linebuf)-1] = '\0';

            // advance start past this line
            start = nl + 1;

            // Trim leading/trailing spaces
            char* line = linebuf;
            while (*line == ' ' || *line == '\t' || *line == '\r') line++;
            if (*line == '\0') continue;

            char* sp = NULL;
            char* cmd = strtok_r(line, " \n", &sp);
            if (!cmd) continue;

            if (strcmp(cmd, KW_CMD_CREATE) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* owner = strtok_r(NULL, " \n", &sp);
                if (fname && owner) {
                    char lb[256]; snprintf(lb, sizeof(lb), "SS: CMD_CREATE %s owner=%s", fname, owner); log_message(lb);
                    do_create(fname, owner);
                }
            } else if (strcmp(cmd, KW_CMD_DELETE) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                if (fname) do_delete(fname);
            } else if (strcmp(cmd, KW_CMD_CREATEFOLDER) == 0) {
                char* folder = strtok_r(NULL, " \n", &sp);
                if (folder) do_createfolder(folder);
            } else if (strcmp(cmd, KW_CMD_MOVE) == 0) {
                char* oldn = strtok_r(NULL, " \n", &sp);
                char* newn = strtok_r(NULL, " \n", &sp);
                if (oldn && newn) do_move(oldn, newn);
            } else if (strcmp(cmd, KW_CMD_META_ADD) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* typ = strtok_r(NULL, " \n", &sp);
                char* user = strtok_r(NULL, " \n", &sp);
                if (fname && typ && user && (typ[0] == 'R' || typ[0] == 'W')) meta_add_user(fname, typ[0], user);
            } else if (strcmp(cmd, KW_CMD_META_REM) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* typ = strtok_r(NULL, " \n", &sp);
                char* user = strtok_r(NULL, " \n", &sp);
                if (fname && typ && user && (typ[0] == 'R' || typ[0] == 'W')) meta_remove_user(fname, typ[0], user);
            } else if (strcmp(cmd, "REQ_READ") == 0) {
                char* internal = strtok_r(NULL, " \n", &sp);
                if (internal && strcmp(internal, "__NS_INTERNAL__") == 0) {
                    char* fname = strtok_r(NULL, " \n", &sp);
                    if (fname) do_read_for_ns(fname);
                }
            } else if (strcmp(cmd, "REQ_STATS") == 0) {
                char* internal = strtok_r(NULL, " \n", &sp);
                if (internal && strcmp(internal, "__NS_INTERNAL__") == 0) {
                    char* fname = strtok_r(NULL, " \n", &sp);
                    if (!fname) {
                        log_message("SS: REQ_STATS missing filename; sending empty frame");
                        // Still respond with empty stats to unblock NS
                        const char* empty = KW_RES_DATA_START "\n" KW_RES_DATA_END "\n";
                        send_all(g_ns_socket_fd, empty, strlen(empty));
                        continue;
                    }
                    {
                        char lb[256]; snprintf(lb, sizeof(lb), "SS: REQ_STATS %s", fname); log_message(lb);
                    }
                    // Compute stats
                    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
                    char file_path[1024]; build_path(file_path, sizeof(file_path), files_dir, fname);
                    char tmppath[1024]; build_path(tmppath, sizeof(tmppath), files_dir, fname); strncat(tmppath, ".tmp", sizeof(tmppath)-strlen(tmppath)-1);
                    const char* path = NULL; struct stat st; memset(&st, 0, sizeof(st));
                    if (stat(tmppath, &st) == 0) path = tmppath; else if (stat(file_path, &st) == 0) path = file_path; else path = file_path;
                    size_t clen = 0; char* content = read_whole_file_preferring_tmp(fname, &clen);
                    size_t words = 0; int inw = 0;
                    for (size_t i = 0; i < clen; ++i) {
                        if (content[i] == ' ' || content[i] == '\n' || content[i] == '\t' || content[i] == '\r' || content[i] == '\f' || content[i] == '\v') { if (inw) { words++; inw = 0; } }
                        else { inw = 1; }
                    }
                    if (inw) words++;
                    free(content);
                    char mtime_s[32] = "-", atime_s[32] = "-", created_s[32] = "-";
                    if (path && stat(path, &st) == 0) { format_time(st.st_mtime, mtime_s, sizeof(mtime_s)); format_time(st.st_atime, atime_s, sizeof(atime_s)); }
                    char meta_path[1024]; build_meta_path(meta_path, sizeof(meta_path), fname);
                    FILE* m = fopen(meta_path, "r");
                    if (m) { char lbuf[256]; while (fgets(lbuf, sizeof(lbuf), m)) { if (strncmp(lbuf, "created=", 8) == 0) { char* v = lbuf + 8; size_t nn = strcspn(v, "\r\n"); v[nn] = '\0'; strncpy(created_s, v, sizeof(created_s)-1); created_s[sizeof(created_s)-1] = '\0'; } } fclose(m); }
                    char block[1024]; size_t off = 0;
                    off += (size_t)snprintf(block + off, sizeof(block) - off, KW_RES_DATA_START "\n");
                    off += (size_t)snprintf(block + off, sizeof(block) - off, "WORDS %zu\n", words);
                    off += (size_t)snprintf(block + off, sizeof(block) - off, "CHARS %zu\n", (size_t)st.st_size);
                    off += (size_t)snprintf(block + off, sizeof(block) - off, "SIZE %zu\n", (size_t)st.st_size);
                    off += (size_t)snprintf(block + off, sizeof(block) - off, "MTIME %s\n", mtime_s);
                    off += (size_t)snprintf(block + off, sizeof(block) - off, "ATIME %s\n", atime_s);
                    off += (size_t)snprintf(block + off, sizeof(block) - off, "CREATED %s\n", created_s);
                    off += (size_t)snprintf(block + off, sizeof(block) - off, KW_RES_DATA_END "\n");
                    send_all(g_ns_socket_fd, block, off);
                    {
                        char lb[256]; snprintf(lb, sizeof(lb), "SS: REQ_STATS %s sent", fname); log_message(lb);
                    }
                }
            } else if (strcmp(cmd, KW_CMD_META_REQ_ADD) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* typ = strtok_r(NULL, " \n", &sp);
                char* user = strtok_r(NULL, " \n", &sp);
                if (fname && typ && user && (typ[0] == 'R' || typ[0] == 'W')) meta_add_request(fname, typ[0], user);
            } else if (strcmp(cmd, KW_CMD_META_REQ_REM) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* typ = strtok_r(NULL, " \n", &sp);
                char* user = strtok_r(NULL, " \n", &sp);
                if (fname && typ && user && (typ[0] == 'R' || typ[0] == 'W')) meta_remove_request(fname, typ[0], user);
            } else if (strcmp(cmd, KW_CMD_CHECKPOINT) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* tag = strtok_r(NULL, " \n", &sp);
                if (fname && tag) do_checkpoint(fname, tag);
            } else if (strcmp(cmd, KW_CMD_VIEWCHECKPOINT) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* tag = strtok_r(NULL, " \n", &sp);
                if (fname && tag) do_viewcheckpoint(fname, tag);
            } else if (strcmp(cmd, KW_CMD_LISTCHECKPOINTS) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                if (fname) do_listcheckpoints(fname);
            } else if (strcmp(cmd, KW_CMD_REVERT) == 0) {
                char* fname = strtok_r(NULL, " \n", &sp);
                char* tag = strtok_r(NULL, " \n", &sp);
                if (fname && tag) do_revertcheckpoint(fname, tag);
            }
        }

        // Move any leftover (partial) line to the beginning
        size_t rem = (size_t)(acc + used - start);
        if (rem > 0 && start != acc) memmove(acc, start, rem);
        used = rem;
    }
}

static void do_create(const char* filename, const char* owner) {
    ensure_dirs();
    char files_dir[512], meta_dir[512];
    build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    build_path(meta_dir, sizeof(meta_dir), g_data_dir, "meta");
    char file_path[1024], meta_path[1024], tmppath[1024], undopath[1024];
    // Ensure parent directory structure exists for nested paths
    ensure_parent_dirs(files_dir, filename);
    ensure_parent_dirs(meta_dir, filename);
    build_path(file_path, sizeof(file_path), files_dir, filename);
    build_path(meta_path, sizeof(meta_path), meta_dir, filename);
    strncat(meta_path, ".meta", sizeof(meta_path)-strlen(meta_path)-1);
    // Clean up any stale temp/undo files from prior sessions
    build_path(tmppath, sizeof(tmppath), files_dir, filename);
    strncat(tmppath, ".tmp", sizeof(tmppath)-strlen(tmppath)-1);
    build_path(undopath, sizeof(undopath), files_dir, filename);
    strncat(undopath, ".undo", sizeof(undopath)-strlen(undopath)-1);
    remove(tmppath);
    remove(undopath);
    FILE* f = fopen(file_path, "wb"); if (f) fclose(f);
    FILE* m = fopen(meta_path, "wb");
    if (m) { 
        fprintf(m, "owner=%s\n", owner ? owner : ""); 
        time_t now = time(NULL); 
        char ts[32]; 
        format_time(now, ts, sizeof(ts)); 
        fprintf(m, "created=%s\n", ts); 
        fclose(m); 
    }
    {
        char lb[256]; snprintf(lb, sizeof(lb), "SS: ACK_CREATE %s -> NS", filename); log_message(lb);
    }
    char ack[BUF_CMD]; snprintf(ack, sizeof(ack), KW_ACK_CREATE " %s OK\n", filename); send_line(g_ns_socket_fd, ack);
}

static void do_delete(const char* filename) {
    // Check if any sentence of the file is locked
    pthread_mutex_lock(&g_lock_list_mutex);
    int locked = has_any_lock(filename);
    pthread_mutex_unlock(&g_lock_list_mutex);
    
    if (locked) {
        // File has active write locks, cannot delete
        char ack[BUF_CMD]; 
        snprintf(ack, sizeof(ack), KW_ACK_DELETE " %s FAIL Sentence is locked, cannot delete\n", filename); 
        send_line(g_ns_socket_fd, ack);
        return;
    }
    
    char files_dir[512], meta_dir[512];
    build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    build_path(meta_dir, sizeof(meta_dir), g_data_dir, "meta");
    char file_path[1024], meta_path[1024], tmppath[1024], undopath[1024];
    build_path(file_path, sizeof(file_path), files_dir, filename);
    build_path(meta_path, sizeof(meta_path), meta_dir, filename);
    strncat(meta_path, ".meta", sizeof(meta_path)-strlen(meta_path)-1);
    build_path(tmppath, sizeof(tmppath), files_dir, filename);
    strncat(tmppath, ".tmp", sizeof(tmppath)-strlen(tmppath)-1);
    build_path(undopath, sizeof(undopath), files_dir, filename);
    strncat(undopath, ".undo", sizeof(undopath)-strlen(undopath)-1);
    remove(file_path);
    remove(meta_path);
    remove(tmppath);
    remove(undopath);
    char ack[BUF_CMD]; snprintf(ack, sizeof(ack), KW_ACK_DELETE " %s OK\n", filename); send_line(g_ns_socket_fd, ack);
}

static void do_createfolder(const char* folderpath) {
    if (!folderpath || !*folderpath) return;
    ensure_dirs();
    char files_dir[512], meta_dir[512];
    build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    build_path(meta_dir, sizeof(meta_dir), g_data_dir, "meta");
    char f_full[1024], m_full[1024];
    build_path(f_full, sizeof(f_full), files_dir, folderpath);
    build_path(m_full, sizeof(m_full), meta_dir, folderpath);
    // Create nested directory structure (mkdir -p style)
    ensure_parent_dirs(files_dir, folderpath);
    ensure_parent_dirs(meta_dir, folderpath);
    mkdir(f_full, 0755);
    mkdir(m_full, 0755);
    char ack[BUF_CMD]; snprintf(ack, sizeof(ack), KW_ACK_CREATEFOLDER " %s OK\n", folderpath); send_line(g_ns_socket_fd, ack);
    char lb[256]; snprintf(lb, sizeof(lb), "SS: CREATEFOLDER %s", folderpath); log_message(lb);
}

static void do_move(const char* oldname, const char* newname) {
    if (!oldname || !newname || !*oldname || !*newname) return;
    char files_dir[512], meta_dir[512];
    build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    build_path(meta_dir, sizeof(meta_dir), g_data_dir, "meta");
    char old_file[1024], new_file[1024];
    char old_meta[1024], new_meta[1024];
    char old_tmp[1024], new_tmp[1024];
    char old_undo[1024], new_undo[1024];
    build_path(old_file, sizeof(old_file), files_dir, oldname);
    build_path(new_file, sizeof(new_file), files_dir, newname);
    build_path(old_meta, sizeof(old_meta), meta_dir, oldname); strncat(old_meta, ".meta", sizeof(old_meta)-strlen(old_meta)-1);
    build_path(new_meta, sizeof(new_meta), meta_dir, newname); strncat(new_meta, ".meta", sizeof(new_meta)-strlen(new_meta)-1);
    build_path(old_tmp, sizeof(old_tmp), files_dir, oldname); strncat(old_tmp, ".tmp", sizeof(old_tmp)-strlen(old_tmp)-1);
    build_path(new_tmp, sizeof(new_tmp), files_dir, newname); strncat(new_tmp, ".tmp", sizeof(new_tmp)-strlen(new_tmp)-1);
    build_path(old_undo, sizeof(old_undo), files_dir, oldname); strncat(old_undo, ".undo", sizeof(old_undo)-strlen(old_undo)-1);
    build_path(new_undo, sizeof(new_undo), files_dir, newname); strncat(new_undo, ".undo", sizeof(new_undo)-strlen(new_undo)-1);
    // Ensure target parent dirs
    ensure_parent_dirs(files_dir, newname);
    ensure_parent_dirs(meta_dir, newname);
    // Only move if source is a regular file
    struct stat st; memset(&st, 0, sizeof(st));
    int ok = 1;
    if (stat(old_file, &st) != 0 || !S_ISREG(st.st_mode)) {
        ok = 0;
    } else {
    // Perform renames; ignore errors individually but track overall success
    if (rename(old_file, new_file) != 0) ok = 0;
    // Metadata & artifacts (only if original exists)
    rename(old_meta, new_meta);
    rename(old_tmp, new_tmp);
    rename(old_undo, new_undo);
    }
    char ack[BUF_CMD]; snprintf(ack, sizeof(ack), KW_ACK_MOVE " %s %s %s\n", oldname, newname, ok ? "OK" : "FAIL"); send_line(g_ns_socket_fd, ack);
    char lb[256]; snprintf(lb, sizeof(lb), "SS: MOVE %s -> %s %s", oldname, newname, ok?"OK":"FAIL"); log_message(lb);
}

static void do_read_common(int fd, const char* filename) {
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char file_path[1024]; build_path(file_path, sizeof(file_path), files_dir, filename);
    FILE* f = fopen(file_path, "rb");
    if (!f) { send_error_ss(fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    char lb[256]; snprintf(lb, sizeof(lb), "SS: READ %s -> fd=%d", filename, fd); log_message(lb);
    send_line(fd, KW_RES_DATA_START "\n");
    char buf[BUF_DATA]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) { if (send_all(fd, buf, n) < 0) break; }
    fclose(f);
    send_line(fd, KW_RES_DATA_END "\n");
}

static void do_read(int client_socket_fd, const char* filename) { do_read_common(client_socket_fd, filename); }
static void do_read_for_ns(const char* filename) { do_read_common(g_ns_socket_fd, filename); }

static void do_stream(int client_socket_fd, const char* filename) {
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char file_path[1024]; build_path(file_path, sizeof(file_path), files_dir, filename);
    FILE* f = fopen(file_path, "r"); if (!f) { send_error_ss(client_socket_fd, ERR_FILE_NOT_FOUND, "File not found"); return; }
    { char lb[256]; snprintf(lb, sizeof(lb), "SS: STREAM %s -> fd=%d", filename, client_socket_fd); log_message(lb); }
    char word[512];
    while (fscanf(f, "%511s", word) == 1) { char line[BUF_CMD + 600]; snprintf(line, sizeof(line), KW_DATA_STREAM_WORD " %s\n", word); if (send_line(client_socket_fd, line) < 0) break; usleep(100000); }
    fclose(f); send_line(client_socket_fd, KW_DATA_STREAM_END "\n");
}

static void do_undo(int client_socket_fd, const char* filename) {
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char file_path[1024], undo_path[1024];
    build_path(file_path, sizeof(file_path), files_dir, filename);
    build_path(undo_path, sizeof(undo_path), files_dir, filename);
    strncat(undo_path, ".undo", sizeof(undo_path)-strlen(undo_path)-1);
    if (rename(undo_path, file_path) == 0) {
        log_message("SS: UNDO applied"); send_line(client_socket_fd, KW_RES_OK " Undo Successful!\n");
    } else { send_error_ss(client_socket_fd, ERR_INVALID_ARGS, "Nothing to undo"); }
}


/* ---------------- Checkpoint Functions ---------------- */
// Sanitize a checkpoint tag: allow [A-Za-z0-9_-], replace others with '_'
static void sanitize_tag(char* tag) {
    for (char* p = tag; *p; ++p) {
        if (!( (*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || *p == '-' || *p == '_' )) {
            *p = '_';
        }
    }
}

static void build_checkpoint_dir(char* out, size_t out_sz, const char* filename) {
    char base[512]; build_path(base, sizeof(base), g_data_dir, "checkpoints");
    // Ensure base exists
    mkdir(base, 0755);
    // If filename has folders, create nested path under checkpoints mirroring structure
    ensure_parent_dirs(base, filename); // creates intermediate dirs
    build_path(out, out_sz, base, filename);
    mkdir(out, 0755); // ensure leaf directory exists
}

static void checkpoint_file_paths(const char* filename, const char* tag, char* ckpt_path, size_t ckpt_sz) {
    char dir[1024]; build_checkpoint_dir(dir, sizeof(dir), filename);
    char safe_tag[128]; strncpy(safe_tag, tag, sizeof(safe_tag)-1); safe_tag[sizeof(safe_tag)-1] = '\0'; sanitize_tag(safe_tag);
    snprintf(ckpt_path, ckpt_sz, "%s/%s.ckpt", dir, safe_tag);
}

static int copy_file_atomic(const char* src, const char* dst) {
    char tmp[1024]; snprintf(tmp, sizeof(tmp), "%s.tmp", dst);
    int ok = (copy_file(src, tmp) == 0);
    if (!ok) { remove(tmp); return -1; }
    if (rename(tmp, dst) != 0) { remove(tmp); return -1; }
    return 0;
}

static void do_checkpoint(const char* filename, const char* tag) {
    if (!filename || !tag) return;
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char file_path[1024]; build_path(file_path, sizeof(file_path), files_dir, filename);
    struct stat st;
    if (stat(file_path, &st) != 0) {
        char ack[BUF_CMD];
        snprintf(ack, sizeof(ack), KW_ACK_CHECKPOINT " %s %s FAIL\n", filename, tag);
        send_line(g_ns_socket_fd, ack);
        return;
    }
    char ckpt_path[1024]; checkpoint_file_paths(filename, tag, ckpt_path, sizeof(ckpt_path));
    // Refuse overwrite of existing checkpoint
    if (stat(ckpt_path, &st) == 0) {
        char ack[BUF_CMD];
        snprintf(ack, sizeof(ack), KW_ACK_CHECKPOINT " %s %s FAIL\n", filename, tag);
        send_line(g_ns_socket_fd, ack);
        return;
    }
    int ok = (copy_file_atomic(file_path, ckpt_path) == 0);
    char ack[BUF_CMD]; snprintf(ack, sizeof(ack), KW_ACK_CHECKPOINT " %s %s %s\n", filename, tag, ok?"OK":"FAIL");
    send_line(g_ns_socket_fd, ack);
    char lb[256]; snprintf(lb, sizeof(lb), "SS: CHECKPOINT %s tag=%s %s", filename, tag, ok?"OK":"FAIL"); log_message(lb);
}

static void do_viewcheckpoint(const char* filename, const char* tag) {
    char ckpt_path[1024]; checkpoint_file_paths(filename, tag, ckpt_path, sizeof(ckpt_path));
    FILE* f = fopen(ckpt_path, "rb");
    if (!f) { send_error_ss(g_ns_socket_fd, ERR_FILE_NOT_FOUND, "Checkpoint not found"); return; }
    send_line(g_ns_socket_fd, KW_RES_DATA_START "\n");
    char buf[BUF_DATA]; size_t n; while ((n = fread(buf,1,sizeof(buf),f))>0) { if (send_all(g_ns_socket_fd, buf, n) < 0) break; }
    fclose(f); send_line(g_ns_socket_fd, KW_RES_DATA_END "\n");
    char lb[256]; snprintf(lb, sizeof(lb), "SS: VIEWCHECKPOINT %s tag=%s", filename, tag); log_message(lb);
}

static void do_listcheckpoints(const char* filename) {
    char dir[1024]; build_checkpoint_dir(dir, sizeof(dir), filename);
    DIR* d = opendir(dir);
    send_line(g_ns_socket_fd, KW_RES_DATA_START "\n");
    if (!d) { send_line(g_ns_socket_fd, KW_RES_DATA_END "\n"); return; }
    struct dirent* de; while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        size_t ln = strlen(de->d_name);
        if (ln > 5 && strcmp(de->d_name + ln - 5, ".ckpt") == 0) {
            char tag[128]; strncpy(tag, de->d_name, ln - 5); tag[ln - 5] = '\0';
            char line[256]; snprintf(line, sizeof(line), "TAG %s\n", tag); send_line(g_ns_socket_fd, line);
        }
    }
    closedir(d);
    send_line(g_ns_socket_fd, KW_RES_DATA_END "\n");
    char lb[256]; snprintf(lb, sizeof(lb), "SS: LISTCHECKPOINTS %s", filename); log_message(lb);
}

static void do_revertcheckpoint(const char* filename, const char* tag) {
    if (!filename || !tag) return;
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char file_path[1024]; build_path(file_path, sizeof(file_path), files_dir, filename);
    char ckpt_path[1024]; checkpoint_file_paths(filename, tag, ckpt_path, sizeof(ckpt_path));
    struct stat st;
    int ok = 0;
    if (stat(file_path, &st) == 0 && stat(ckpt_path, &st) == 0) {
        ok = (copy_file_atomic(ckpt_path, file_path) == 0);
    }
    char ack[BUF_CMD]; snprintf(ack, sizeof(ack), KW_ACK_REVERT " %s %s %s\n", filename, tag, ok?"OK":"FAIL"); send_line(g_ns_socket_fd, ack);
    char lb[256]; snprintf(lb, sizeof(lb), "SS: REVERT %s tag=%s %s", filename, tag, ok?"OK":"FAIL"); log_message(lb);
}

static int do_write_lock(int client_socket_fd, const char* filename, int sentence_num, const char* user) {
    size_t len = 0; int scount = 0; 
    char* full = read_whole_file_preferring_tmp(filename, &len);
    char** sentences = split_sentences(full, &scount);
    free(full);
    if (sentence_num == scount && scount > 0) {
        char* last = sentences[scount - 1]; size_t L = strlen(last);
    if (!(L > 0 && (last[L-1] == '.' || last[L-1] == '!' || last[L-1] == '?'))) { free_str_array(sentences, scount); send_error_ss(client_socket_fd, ERR_INVALID_INDEX, "Sentence index out of range"); return -1; }
    }
    if (sentence_num < 0 || sentence_num > scount) { free_str_array(sentences, scount); send_error_ss(client_socket_fd, ERR_INVALID_INDEX, "Invalid sentence index"); return -1; }
    pthread_mutex_lock(&g_lock_list_mutex);
    if (is_locked(filename, sentence_num)) { pthread_mutex_unlock(&g_lock_list_mutex); free_str_array(sentences, scount); send_error_ss(client_socket_fd, ERR_LOCKED, "Sentence locked by other user"); return -1; }
    if (g_active_lock_count < (int)(sizeof(g_active_locks)/sizeof(g_active_locks[0]))) {
        strncpy(g_active_locks[g_active_lock_count].filename, filename, sizeof(g_active_locks[g_active_lock_count].filename)-1);
        g_active_locks[g_active_lock_count].filename[sizeof(g_active_locks[g_active_lock_count].filename)-1] = '\0';
        g_active_locks[g_active_lock_count].sentence_num = sentence_num; g_active_lock_count++;
    }
    pthread_mutex_unlock(&g_lock_list_mutex);
    // Initialize session state to keep segmentation stable during this write session
    pthread_mutex_lock(&g_session_mutex);
    if (g_session_count < (int)(sizeof(g_sessions)/sizeof(g_sessions[0]))) {
        WriteSession* s = &g_sessions[g_session_count++];
        strncpy(s->filename, filename, sizeof(s->filename)-1); s->filename[sizeof(s->filename)-1] = '\0';
        s->sentence_num = sentence_num;
        if (user) { strncpy(s->user, user, sizeof(s->user)-1); s->user[sizeof(s->user)-1] = '\0'; } else { s->user[0] = '\0'; }
        s->pre = concat_range(sentences, 0, sentence_num - 1);
        s->cur = (sentence_num < scount) ? strdup(sentences[sentence_num]) : strdup("");
        s->post_orig = concat_range(sentences, sentence_num + 1, scount - 1);
        s->post_extra = strdup("");
        s->cur_orig = s->cur ? strdup(s->cur) : strdup("");
    }
    pthread_mutex_unlock(&g_session_mutex);
    free_str_array(sentences, scount);
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char file_path[1024], undo_path[1024];
    build_path(file_path, sizeof(file_path), files_dir, filename);
    build_path(undo_path, sizeof(undo_path), files_dir, filename);
    strncat(undo_path, ".undo", sizeof(undo_path)-strlen(undo_path)-1);
    copy_file(file_path, undo_path);
    { char lb[256]; snprintf(lb, sizeof(lb), "SS: LOCK %s[%d] by %s", filename, sentence_num, user ? user : "?"); log_message(lb); }
    send_line(client_socket_fd, KW_RES_OK " Sentence locked.\n");
    return 0;
}

static char* read_whole_file_preferring_tmp(const char* filename, size_t* out_len) {
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char path[1024], tmppath[1024];
    build_path(path, sizeof(path), files_dir, filename);
    build_path(tmppath, sizeof(tmppath), files_dir, filename);
    strncat(tmppath, ".tmp", sizeof(tmppath)-strlen(tmppath)-1);
    const char* p = tmppath;
    FILE* f = fopen(tmppath, "rb");
    if (!f) { p = path; f = fopen(path, "rb"); }
    if (!f) { *out_len = 0; return strdup(""); }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); *out_len = 0; return strdup(""); }
    char* buf = (char*)malloc((size_t)sz + 1); if (!buf) { fclose(f); *out_len = 0; return strdup(""); }
    size_t n = fread(buf, 1, (size_t)sz, f); buf[n] = '\0'; fclose(f); *out_len = n; (void)p; return buf;
}

// Read from base file only (ignore any .tmp). Used for merge-on-release to
// preserve other sessions' committed changes.
static char* read_whole_file_base_only(const char* filename, size_t* out_len) {
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char path[1024]; build_path(path, sizeof(path), files_dir, filename);
    FILE* f = fopen(path, "rb"); if (!f) { *out_len = 0; return strdup(""); }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); *out_len = 0; return strdup(""); }
    char* buf = (char*)malloc((size_t)sz + 1); if (!buf) { fclose(f); *out_len = 0; return strdup(""); }
    size_t n = fread(buf, 1, (size_t)sz, f); buf[n] = '\0'; fclose(f); *out_len = n; return buf;
}

static int write_whole_tmp(const char* filename, const char* content, size_t len) {
    char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
    char tmppath[1024]; build_path(tmppath, sizeof(tmppath), files_dir, filename); strncat(tmppath, ".tmp", sizeof(tmppath)-strlen(tmppath)-1);
    FILE* f = fopen(tmppath, "wb"); if (!f) return -1; size_t w = fwrite(content, 1, len, f); fclose(f); return (w == len) ? 0 : -1;
}

static char** split_sentences(const char* text, int* count) {
    // Split on every '.', '!' or '?' as sentence delimiters (even inside words/quotes).
    size_t n = strlen(text);
    char* copy = (char*)malloc(n + 1);
    strcpy(copy, text);
    char** arr = (char**)malloc(sizeof(char*) * 4096);
    int cnt = 0;
    char* start = copy;
    for (size_t i = 0; i < n; ++i) {
        if (copy[i] == '.' || copy[i] == '!' || copy[i] == '?') {
            size_t len = (copy + i + 1) - start;
            char* s = (char*)malloc(len + 1);
            memcpy(s, start, len);
            s[len] = '\0';
            arr[cnt++] = s;
            start = copy + i + 1;
        }
    }
    if (*start) arr[cnt++] = strdup(start);
    *count = cnt;
    free(copy);
    return arr;
}

static void free_str_array(char** arr, int count) { for (int i = 0; i < count; ++i) free(arr[i]); free(arr); }

static char* join_sentences(char** arr, int count, size_t* out_len) {
    // Join sentence fragments, inserting a single space between fragments
    // when neither the previous fragment ends with whitespace nor the next
    // fragment begins with whitespace. This avoids words/sentences sticking.
    size_t total = 0;
    for (int i = 0; i < count; ++i) {
        total += strlen(arr[i]);
        if (i > 0) {
            const char* prev = arr[i-1]; size_t lp = strlen(prev);
            const char* next = arr[i];
            int need_space = 0;
            if (lp > 0 && next && next[0] != '\0') {
                unsigned char lastc = (unsigned char)prev[lp - 1];
                unsigned char firstc = (unsigned char)next[0];
                if (!isspace(lastc) && !isspace(firstc)) need_space = 1;
            }
            if (need_space) total += 1;
        }
    }
    char* out = (char*)malloc(total + 1);
    size_t pos = 0;
    for (int i = 0; i < count; ++i) {
        if (i > 0) {
            const char* prev = arr[i-1]; size_t lp = strlen(prev);
            const char* next = arr[i];
            int need_space = 0;
            if (lp > 0 && next && next[0] != '\0') {
                unsigned char lastc = (unsigned char)prev[lp - 1];
                unsigned char firstc = (unsigned char)next[0];
                if (!isspace(lastc) && !isspace(firstc)) need_space = 1;
            }
            if (need_space) out[pos++] = ' ';
        }
        size_t l = strlen(arr[i]);
        memcpy(out + pos, arr[i], l);
        pos += l;
    }
    out[pos] = '\0';
    if (out_len) *out_len = pos;
    return out;
}

static char** split_words(const char* sentence, int* count) {
    char* copy = strdup(sentence); char* p = copy; char** arr = (char**)malloc(sizeof(char*) * 4096);
    int cnt = 0; char* tok; while ((tok = strsep(&p, " \t\r\n")) != NULL) { if (*tok == '\0') continue; arr[cnt++] = strdup(tok); }
    free(copy); *count = cnt; return arr;
}

static char* join_words(char** words, int count) {
    size_t total = (count > 0 ? (size_t)(count - 1) : 0); for (int i = 0; i < count; ++i) total += strlen(words[i]);
    char* out = (char*)malloc(total + 1); size_t pos = 0;
    for (int i = 0; i < count; ++i) { size_t l = strlen(words[i]); memcpy(out + pos, words[i], l); pos += l; if (i + 1 < count) out[pos++] = ' '; }
    out[pos] = '\0'; return out;
}

static int do_write_update(const char* filename, int sentence_num, int word_index, const char* content) {
    // Strip trailing whitespace from content
    char* content_copy = strdup(content);
    size_t clen = strlen(content_copy);
    while (clen > 0 && (content_copy[clen-1] == ' ' || content_copy[clen-1] == '\t' || 
                        content_copy[clen-1] == '\r' || content_copy[clen-1] == '\n')) {
        content_copy[--clen] = '\0';
    }
    
    // Prefer session-aware update to keep the locked sentence stable across updates
    int rc = -1;
    pthread_mutex_lock(&g_session_mutex);
    int sidx = find_session(filename, sentence_num);
    if (sidx >= 0) {
        WriteSession* s = &g_sessions[sidx];
        // operate on s->cur only
        int wcount = 0; char** words = split_words(s->cur ? s->cur : "", &wcount);
        if (word_index == -1) { word_index = wcount; }
        if (word_index < 0 || word_index > wcount) {
            for (int i = 0; i < wcount; ++i) free(words[i]); free(words);
            pthread_mutex_unlock(&g_session_mutex);
            free(content_copy);
            return -1;
        }
        // Split content into words and insert preserving order
        int insc = 0; char** ins_words = split_words(content_copy, &insc);
        char** new_words = (char**)malloc(sizeof(char*) * (wcount + insc + 2)); int pos = 0;
        for (int i = 0; i < word_index; ++i) new_words[pos++] = strdup(words[i]);
        for (int i = 0; i < insc; ++i) new_words[pos++] = strdup(ins_words[i]);
        for (int i = word_index; i < wcount; ++i) new_words[pos++] = strdup(words[i]);
        int new_wcount = pos;
        for (int i = 0; i < wcount; ++i) free(words[i]); free(words);
        for (int i = 0; i < insc; ++i) free(ins_words[i]); free(ins_words);
        char* new_sentence = join_words(new_words, new_wcount);
        for (int i = 0; i < new_wcount; ++i) free(new_words[i]); free(new_words);
        if (s->cur) free(s->cur); s->cur = new_sentence;

        // Re-segment s->cur by sentence delimiters and move trailing fragments into post_extra
        int fragc = 0; char** frags = split_sentences(s->cur, &fragc);
        if (fragc > 1) {
            char* first = strdup(frags[0]);
            size_t extra_len = 0; char* extra = NULL;
            if (fragc - 1 > 0) extra = join_sentences(&frags[1], fragc - 1, &extra_len); else extra = strdup("");
            free(s->cur); s->cur = first;
            if (extra && *extra) {
                if (s->post_extra && *s->post_extra) {
                    size_t a = strlen(s->post_extra), b = strlen(extra);
                    char* combined = (char*)malloc(a + 1 + b + 1);
                    memcpy(combined, s->post_extra, a); combined[a] = ' ';
                    memcpy(combined + a + 1, extra, b); combined[a + 1 + b] = '\0';
                    free(s->post_extra); s->post_extra = combined;
                } else {
                    if (s->post_extra) free(s->post_extra);
                    s->post_extra = strdup(extra);
                }
            }
            for (int i = 0; i < fragc; ++i) free(frags[i]);
            free(frags);
            free(extra);
        } else {
            for (int i = 0; i < fragc; ++i) free(frags[i]);
            free(frags);
        }

        pthread_mutex_unlock(&g_session_mutex);
        // Do not write .tmp during session; commit happens on release with merge
        free(content_copy);
        return 0;
    }
    pthread_mutex_unlock(&g_session_mutex);

    // Fallback to original behavior (should be rare)
    size_t len; char* full = read_whole_file_preferring_tmp(filename, &len);
    int scount = 0; char** sentences = split_sentences(full, &scount); free(full);
    if (sentence_num < 0 || sentence_num > scount) { free_str_array(sentences, scount); free(content_copy); return -1; }
    const char* base_sentence = ""; if (sentence_num < scount) base_sentence = sentences[sentence_num];
    int wcount = 0; char** words = split_words(base_sentence, &wcount);
    if (word_index < 0 || word_index > wcount) { for (int i = 0; i < wcount; ++i) free(words[i]); free(words); free_str_array(sentences, scount); free(content_copy); return -1; }
    // Treat content as a SINGLE word (don't split it)
    char** new_words = (char**)malloc(sizeof(char*) * (wcount + 2)); int pos = 0;
    for (int i = 0; i < word_index; ++i) new_words[pos++] = strdup(words[i]);
    new_words[pos++] = strdup(content_copy); // Insert content as single word
    for (int i = word_index; i < wcount; ++i) new_words[pos++] = strdup(words[i]);
    int new_wcount = pos;
    for (int i = 0; i < wcount; ++i) free(words[i]); free(words);
    char* new_sentence = join_words(new_words, new_wcount); for (int i = 0; i < new_wcount; ++i) free(new_words[i]); free(new_words);

    // Replace in-place without re-splitting
    int total_new = (sentence_num < scount ? scount : scount + 1);
    char** merged = (char**)malloc(sizeof(char*) * (total_new));
    int m = 0; for (int i = 0; i < sentence_num && i < scount; ++i) merged[m++] = strdup(sentences[i]);
    merged[m++] = strdup(new_sentence);
    if (sentence_num < scount) { for (int i = sentence_num + 1; i < scount; ++i) merged[m++] = strdup(sentences[i]); }
    free_str_array(sentences, scount);
    size_t out_len; char* rebuilt = join_sentences(merged, m, &out_len); for (int i = 0; i < m; ++i) free(merged[i]); free(merged); free(new_sentence);
    // Fallback path (no session found): write directly to tmp
    rc = write_whole_tmp(filename, rebuilt, out_len); free(rebuilt); free(content_copy); return rc;
}

static int do_write_release(const char* filename, int sentence_num) {
    // Merge-on-release: rebase updated sentence onto latest file to preserve other sessions' changes
    pthread_mutex_lock(&g_session_mutex);
    int sidx = find_session(filename, sentence_num);
    WriteSession* s = (sidx >= 0 ? &g_sessions[sidx] : NULL);
    char* cur_updated = NULL; char* cur_anchor = NULL; int orig_index = sentence_num;
    if (s) { cur_updated = s->cur ? strdup(s->cur) : strdup(""); cur_anchor = s->cur_orig ? strdup(s->cur_orig) : strdup(""); orig_index = s->sentence_num; }
    pthread_mutex_unlock(&g_session_mutex);

    // Permission revalidation: user must still have write access
    int allowed = 1;
    if (s) {
    char owner[64] = {0}; char rusers[10][50]; int rcnt = 0; char wusers[10][50]; int wcnt = 0; char qusers[20][50]; char qtypes[20]; int qcnt = 0;
    if (meta_load(filename, owner, sizeof(owner), rusers, &rcnt, wusers, &wcnt, qusers, qtypes, &qcnt) == 0) {
            allowed = 0;
            if (s->user[0] != '\0') {
                if (strcmp(owner, s->user) == 0) allowed = 1; else {
                    for (int i = 0; i < wcnt; ++i) { if (strcmp(wusers[i], s->user) == 0) { allowed = 1; break; } }
                }
            }
        }
    }
    if (!allowed) {
        // Drop session without writing, release lock
        pthread_mutex_unlock(&g_session_mutex);
        pthread_mutex_lock(&g_lock_list_mutex);
        for (int i = 0; i < g_active_lock_count; ++i) {
            if (strcmp(g_active_locks[i].filename, filename) == 0 && g_active_locks[i].sentence_num == sentence_num) {
                for (int j = i; j < g_active_lock_count - 1; ++j) g_active_locks[j] = g_active_locks[j+1];
                g_active_lock_count--; break; }
        }
        pthread_mutex_unlock(&g_lock_list_mutex);
        pthread_mutex_lock(&g_session_mutex);
        int sidx2 = find_session(filename, sentence_num);
        if (sidx2 >= 0) remove_session_by_index(sidx2);
        pthread_mutex_unlock(&g_session_mutex);
        return -1;
    }

    size_t base_len = 0; char* base = read_whole_file_base_only(filename, &base_len);
    int scount = 0; char** sentences = split_sentences(base, &scount); free(base);
    // Find anchor sentence in latest content
    int target_idx = -1;
    if (cur_anchor && *cur_anchor) {
        for (int i = 0; i < scount; ++i) { if (strcmp(sentences[i], cur_anchor) == 0) { target_idx = i; break; } }
    }
    if (target_idx < 0) {
        // Fallback to original index if available
        if (orig_index >= 0 && orig_index < scount) target_idx = orig_index; else target_idx = scount; // append if out of range
    }
    // Build new sentences array with replacement at target_idx and inject extra fragments created during this session
    int extra_cnt = 0; char** extra_arr = NULL;
    pthread_mutex_lock(&g_session_mutex);
    sidx = find_session(filename, sentence_num);
    if (sidx >= 0 && g_sessions[sidx].post_extra && *g_sessions[sidx].post_extra) {
        extra_arr = split_sentences(g_sessions[sidx].post_extra, &extra_cnt);
    }
    pthread_mutex_unlock(&g_session_mutex);
    int new_count = scount + extra_cnt;
    char** merged = (char**)malloc(sizeof(char*) * (new_count + 2));
    int m = 0;
    for (int i = 0; i < target_idx && i < scount; ++i) merged[m++] = strdup(sentences[i]);
    merged[m++] = cur_updated ? strdup(cur_updated) : strdup("");
    for (int i = 0; i < extra_cnt; ++i) merged[m++] = strdup(extra_arr[i]);
    if (target_idx < scount) { for (int i = target_idx + 1; i < scount; ++i) merged[m++] = strdup(sentences[i]); }
    free_str_array(sentences, scount);
    if (extra_arr) { for (int i = 0; i < extra_cnt; ++i) free(extra_arr[i]); free(extra_arr); }
    size_t out_len = 0; char* rebuilt = join_sentences(merged, m, &out_len);
    for (int i = 0; i < m; ++i) free(merged[i]); free(merged);
    if (cur_updated) free(cur_updated); if (cur_anchor) free(cur_anchor);

    // Serialize final write to avoid clobber with other releases
    pthread_mutex_lock(&g_file_io_mutex);
    int rc = write_whole_tmp(filename, rebuilt, out_len);
    if (rc == 0) {
        char files_dir[512]; build_path(files_dir, sizeof(files_dir), g_data_dir, "files");
        char file_path[1024], tmppath[1024];
        build_path(file_path, sizeof(file_path), files_dir, filename);
        build_path(tmppath, sizeof(tmppath), files_dir, filename);
        strncat(tmppath, ".tmp", sizeof(tmppath)-strlen(tmppath)-1);
        rename(tmppath, file_path);
    }
    pthread_mutex_unlock(&g_file_io_mutex);
    free(rebuilt);

    // Remove session state and release sentence lock
    pthread_mutex_lock(&g_session_mutex);
    sidx = find_session(filename, sentence_num);
    if (sidx >= 0) remove_session_by_index(sidx);
    pthread_mutex_unlock(&g_session_mutex);
    pthread_mutex_lock(&g_lock_list_mutex);
    for (int i = 0; i < g_active_lock_count; ++i) {
        if (strcmp(g_active_locks[i].filename, filename) == 0 && g_active_locks[i].sentence_num == sentence_num) {
            for (int j = i; j < g_active_lock_count - 1; ++j) g_active_locks[j] = g_active_locks[j+1];
            g_active_lock_count--;
            break;
        }
    }
    pthread_mutex_unlock(&g_lock_list_mutex);
    return 0;
}

static void write_session_loop(int client_socket_fd, const char* filename, int sentence_num) {
    char buf[BUF_CMD + BUF_DATA];
    while (1) {
        ssize_t n = recv(client_socket_fd, buf, sizeof(buf)-1, 0);
    if (n <= 0) { (void)do_write_release(filename, sentence_num); break; }
        buf[n] = '\0';
        char* sp = NULL; char* cmd = strtok_r(buf, " \n", &sp);
        if (!cmd) continue;
        if (strcmp(cmd, "DATA_WRITE_UPDATE") == 0) {
            char* idxs = strtok_r(NULL, " \n", &sp); char* content = sp; if (!idxs || !content) { send_error_ss(client_socket_fd, ERR_INVALID_ARGS, "Invalid format"); continue; }
            while (*content == ' ') content++;
            int widx = 0; int use_end = 0; int valid = 1;
            if (strcmp(idxs, "END") == 0) { use_end = 1; widx = -1; }
            else if (*idxs == '\0') { valid = 0; }
            else {
                for (char* p = idxs; *p; ++p) { if (*p < '0' || *p > '9') { valid = 0; break; } }
                if (valid) widx = atoi(idxs);
            }
            if (!valid) { send_error_ss(client_socket_fd, ERR_INVALID_INDEX, "Invalid indices or write error"); continue; }
            if (do_write_update(filename, sentence_num, widx, content) == 0) send_line(client_socket_fd, KW_RES_OK " Update applied.\n");
            else send_error_ss(client_socket_fd, ERR_INVALID_INDEX, "Invalid indices or write error");
        } else if (strcmp(cmd, "CMD_WRITE_RELEASE") == 0) {
            int rc = do_write_release(filename, sentence_num);
            if (rc == 0) send_line(client_socket_fd, KW_RES_OK " Write Successful!\n");
            else send_error_ss(client_socket_fd, ERR_ACCESS_DENIED, "Write denied: access revoked");
            break;
        }
    }
}

static void* handle_client_request(void* arg) {
    int client_fd = (int)(intptr_t)arg; char buf[BUF_CMD+1];
    ssize_t n = recv(client_fd, buf, BUF_CMD, 0); if (n <= 0) { close(client_fd); pthread_exit(NULL); }
    buf[n] = '\0';
    char* sp = NULL; char* cmd = strtok_r(buf, " \n", &sp); if (!cmd) { close(client_fd); pthread_exit(NULL); }
    if (strcmp(cmd, "REQ_READ") == 0) { char* user = strtok_r(NULL, " \n", &sp); char* fname = strtok_r(NULL, " \n", &sp); (void)user; do_read(client_fd, fname); }
    else if (strcmp(cmd, "REQ_STREAM") == 0) { char* user = strtok_r(NULL, " \n", &sp); char* fname = strtok_r(NULL, " \n", &sp); (void)user; do_stream(client_fd, fname); }
    else if (strcmp(cmd, "REQ_UNDO") == 0) { char* user = strtok_r(NULL, " \n", &sp); char* fname = strtok_r(NULL, " \n", &sp); (void)user; do_undo(client_fd, fname); }
    else if (strcmp(cmd, "REQ_WRITE_LOCK") == 0) { char* user = strtok_r(NULL, " \n", &sp); char* fname = strtok_r(NULL, " \n", &sp); char* sidx = strtok_r(NULL, " \n", &sp); int sentence_num = sidx ? atoi(sidx) : -1; if (fname && sentence_num >= 0 && do_write_lock(client_fd, fname, sentence_num, user ? user : "") == 0) { write_session_loop(client_fd, fname, sentence_num); } }
    else { send_error_ss(client_fd, ERR_UNKNOWN_COMMAND, "Unknown command"); }
    close(client_fd); pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    if (argc != 5 && argc != 6) { 
        fprintf(stderr, "Usage: ./storageserver <ns_ip> <ns_port> <client_listen_port> <data_dir> [advertised_ip]\n"); 
        return EXIT_FAILURE; 
    }
    const char* ns_ip = argv[1]; 
    int ns_port = atoi(argv[2]); 
    g_client_listen_port = atoi(argv[3]);
    strncpy(g_data_dir, argv[4], sizeof(g_data_dir)-1); 
    g_data_dir[sizeof(g_data_dir)-1] = '\0';
    
    // Use advertised IP if provided, otherwise use ns_ip as the advertised IP
    if (argc == 6) {
        strncpy(g_advertised_ip, argv[5], sizeof(g_advertised_ip)-1);
    } else {
        strncpy(g_advertised_ip, ns_ip, sizeof(g_advertised_ip)-1);
    }
    g_advertised_ip[sizeof(g_advertised_ip)-1] = '\0';
    pthread_mutex_init(&g_lock_list_mutex, NULL); ensure_dirs();
    if (connect_and_register_to_ns(ns_ip, ns_port) != 0) { fprintf(stderr, "Failed to register with Name Server.\n"); return EXIT_FAILURE; }
    pthread_t ns_tid; pthread_create(&ns_tid, NULL, ns_listener_thread, NULL);
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0); if (listen_fd < 0) { perror("socket"); return EXIT_FAILURE; }
    int opt = 1; setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr)); addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons((uint16_t)g_client_listen_port);
    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); close(listen_fd); return EXIT_FAILURE; }
    if (listen(listen_fd, 128) < 0) { perror("listen"); close(listen_fd); return EXIT_FAILURE; }
    char logbuf[128]; snprintf(logbuf, sizeof(logbuf), "Storage Server listening on %d", g_client_listen_port); log_message(logbuf);
    while (1) { int cfd = accept(listen_fd, NULL, NULL); if (cfd < 0) { perror("accept"); continue; } pthread_t tid; pthread_create(&tid, NULL, handle_client_request, (void*)(intptr_t)cfd); pthread_detach(tid); }
    close(listen_fd); return EXIT_SUCCESS;
}

// ---- Session helper implementations ----
static int find_session(const char* filename, int sentence_num) {
    for (int i = 0; i < g_session_count; ++i) {
        if (g_sessions[i].sentence_num == sentence_num && strcmp(g_sessions[i].filename, filename) == 0) return i;
    }
    return -1;
}

static void free_session(WriteSession* s) {
    if (!s) return;
    if (s->pre) { free(s->pre); s->pre = NULL; }
    if (s->cur) { free(s->cur); s->cur = NULL; }
    if (s->post_orig) { free(s->post_orig); s->post_orig = NULL; }
    if (s->post_extra) { free(s->post_extra); s->post_extra = NULL; }
    if (s->cur_orig) { free(s->cur_orig); s->cur_orig = NULL; }
}

static void remove_session_by_index(int idx) {
    if (idx < 0 || idx >= g_session_count) return;
    free_session(&g_sessions[idx]);
    for (int j = idx; j < g_session_count - 1; ++j) g_sessions[j] = g_sessions[j + 1];
    g_session_count--;
}

static char* concat_range(char** arr, int start, int end) {
    if (!arr) return strdup("");
    if (end < start) return strdup("");
    size_t total = 0;
    for (int i = start; i <= end; ++i) total += strlen(arr[i]);
    char* out = (char*)malloc(total + 1);
    size_t pos = 0;
    for (int i = start; i <= end; ++i) { size_t l = strlen(arr[i]); memcpy(out + pos, arr[i], l); pos += l; }
    out[pos] = '\0';
    return out;
}