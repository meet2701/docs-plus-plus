#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <strings.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include "common.h"

/* Global constants */
#define BUF_CMD 1024
#define BUF_RESP 1024
#define BUF_DATA 4096

/* Protocol keywords */
#define KW_RES_OK "RES_OK"
#define KW_RES_ERROR "RES_ERROR"
#define KW_RES_DATA_START "RES_DATA_START"
#define KW_RES_DATA_END "RES_DATA_END"
#define KW_RES_LOCATION "RES_LOCATION"
#define KW_DATA_STREAM_WORD "DATA_STREAM_WORD"
#define KW_DATA_STREAM_END "DATA_STREAM_END"

/* Global username for graceful exits and history save */
static char g_username[128] = "";

/* Forward decl for history save so we can call it on exit */
static void history_save(const char* username);

/* Raw-mode state must be declared before use in immediate_exit */
static struct termios orig_termios;
static int raw_enabled = 0;

/* Exit immediately: leave raw mode, persist history, and terminate */
static void immediate_exit(void) {
    if (raw_enabled) {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &orig_termios);
        raw_enabled = 0;
    }
    if (g_username[0]) {
        history_save(g_username);
    }
    write(STDOUT_FILENO, "\n", 1);
    _exit(0);
}

/* Forward declarations */
int connect_to_server(char* ip, int port);
int get_ss_location(int ns_socket_fd, char* username, char* filename, char* operation, char* ip_buffer, int* port_buffer);
void run_shell(int ns_socket_fd, char* username);
void handle_command(int ns_socket_fd, char* username, char* command, char** args);

/* Handlers */
void handle_view(int ns_socket_fd, char* username, char** args);
void handle_list(int ns_socket_fd, char* username, char** args);
void handle_info(int ns_socket_fd, char* username, char** args);
void handle_exec(int ns_socket_fd, char* username, char** args);

void handle_create(int ns_socket_fd, char* username, char** args);
void handle_delete(int ns_socket_fd, char* username, char** args);
void handle_createfolder(int ns_socket_fd, char* username, char** args);
void handle_move(int ns_socket_fd, char* username, char** args);
void handle_viewfolder(int ns_socket_fd, char* username, char** args);
void handle_listfolders(int ns_socket_fd, char* username, char** args);
void handle_addaccess(int ns_socket_fd, char* username, char** args);
void handle_remaccess(int ns_socket_fd, char* username, char** args);
// Bonus: Access Requests
void handle_requestaccess(int ns_socket_fd, char* username, char** args);
void handle_listrequests(int ns_socket_fd, char* username, char** args);
void handle_respondrequest(int ns_socket_fd, char* username, char** args);
// Bonus: Checkpoints
void handle_checkpoint(int ns_socket_fd, char* username, char** args);
void handle_listcheckpoints(int ns_socket_fd, char* username, char** args);
void handle_viewcheckpoint(int ns_socket_fd, char* username, char** args);
void handle_revertcheckpoint(int ns_socket_fd, char* username, char** args);

void handle_read(int ns_socket_fd, char* username, char** args);
void handle_stream(int ns_socket_fd, char* username, char** args);
void handle_undo(int ns_socket_fd, char* username, char** args);
void handle_write(int ns_socket_fd, char* username, char** args);
void handle_easteregg(void);
void handle_help(void);

/* Utility helpers */
static int send_all(int sockfd, const char* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(sockfd, buf + total, len - total, 0);
        if (n <= 0) return -1;
        total += (size_t)n;
    }
    return 0;
}

static int send_line(int sockfd, const char* line) {
    return send_all(sockfd, line, strlen(line));
}

static int starts_with(const char* s, const char* pfx) {
    size_t ls = strlen(s), lp = strlen(pfx);
    return ls >= lp && strncmp(s, pfx, lp) == 0;
}

static void strip_newline(char* s) {
    if (!s) return;
    size_t n = strlen(s);
    if (n > 0 && s[n-1] == '\n') s[n-1] = '\0';
}

/* ---------------- Minimal line editor with history and arrow keys ---------------- */
typedef struct {
    char items[128][BUF_CMD];
    int count;
} CmdHistory;

static CmdHistory g_history;


static void disable_raw_mode(void) {
    if (raw_enabled) {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &orig_termios);
        raw_enabled = 0;
    }
}

static int enable_raw_mode(void) {
    if (tcgetattr(STDIN_FILENO, &orig_termios) == -1) return -1;
    atexit(disable_raw_mode);
    struct termios raw = orig_termios;
    raw.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
    raw.c_iflag &= ~(IXON | ICRNL);
    raw.c_oflag |= OPOST; // keep post-processing for \n -> \r\n behavior
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) == -1) return -1;
    raw_enabled = 1;
    return 0;
}

static void clear_line_and_prompt(const char* prompt, const char* buf, size_t curpos) {
    // Move to line start, print prompt, buffer, clear to end, then position cursor
    write(STDOUT_FILENO, "\r", 1);
    write(STDOUT_FILENO, prompt, strlen(prompt));
    write(STDOUT_FILENO, buf, strlen(buf));
    // Clear to end of line
    const char* clr = "\x1b[K";
    write(STDOUT_FILENO, clr, strlen(clr));
    // Move cursor back if not at end
    size_t buflen = strlen(buf);
    if (buflen > curpos) {
        char seq[64];
        snprintf(seq, sizeof(seq), "\x1b[%zuD", buflen - curpos);
        write(STDOUT_FILENO, seq, strlen(seq));
    }
    fflush(stdout);
}

static void history_add(const char* line) {
    if (!line || !*line) return; // ignore empty
    if (g_history.count > 0 && strcmp(g_history.items[g_history.count-1], line) == 0) return;
    if (g_history.count < (int)(sizeof(g_history.items)/sizeof(g_history.items[0]))) {
        strncpy(g_history.items[g_history.count], line, BUF_CMD-1);
        g_history.items[g_history.count][BUF_CMD-1] = '\0';
        g_history.count++;
    } else {
        // simple FIFO: drop oldest
        for (int i = 1; i < g_history.count; ++i) strcpy(g_history.items[i-1], g_history.items[i]);
        strncpy(g_history.items[g_history.count-1], line, BUF_CMD-1);
        g_history.items[g_history.count-1][BUF_CMD-1] = '\0';
    }
}

static int ensure_dir_exists(const char* dir) {
    struct stat st;
    if (stat(dir, &st) == 0) {
        if (S_ISDIR(st.st_mode)) return 0;
        return -1; // exists but not a dir
    }
    if (mkdir(dir, 0755) == 0) return 0;
    if (errno == EEXIST) return 0;
    return -1;
}

static void history_get_path_for_user(const char* username, char* out, size_t outsz) {
    // Store per-username history in project folder .history/.docspp_history_<username>
    const char* base = ".history";
    // We don't create here; creation is done on save to avoid errors in read-only environments.
    snprintf(out, outsz, "%s/.docspp_history_%s", base, username);
}

static void history_load(const char* username) {
    char path[512]; history_get_path_for_user(username, path, sizeof(path));
    FILE* f = fopen(path, "r"); if (!f) return;
    char line[BUF_CMD];
    while (fgets(line, sizeof(line), f)) {
        size_t n = strlen(line); if (n && line[n-1] == '\n') line[n-1] = '\0';
        if (*line == '\0') continue;
        history_add(line);
    }
    fclose(f);
}

static void history_save(const char* username) {
    // Ensure .history directory exists
    if (ensure_dir_exists(".history") != 0) {
        // Best-effort: if cannot create, skip saving
        return;
    }
    char path[512]; history_get_path_for_user(username, path, sizeof(path));
    char tmp[544]; snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE* f = fopen(tmp, "w"); if (!f) return;
    for (int i = 0; i < g_history.count; ++i) {
        fputs(g_history.items[i], f); fputc('\n', f);
    }
    fclose(f);
    rename(tmp, path);
}

// Returns 1 if a line was read, 0 on EOF (Ctrl-D on empty line), -1 on error
// Returns 2 if interrupted by Ctrl-C (line cleared, prompt refresh)
static int read_line_edited(const char* prompt, char* out, size_t outsz) {
    out[0] = '\0';
    if (enable_raw_mode() < 0) return -1;

    char buf[BUF_CMD]; size_t len = 0; size_t cur = 0;
    buf[0] = '\0';
    int hist_index = g_history.count; // points past last; up decrements

    // Show prompt
    write(STDOUT_FILENO, prompt, strlen(prompt));
    fflush(stdout);

    while (1) {
        unsigned char c;
        ssize_t n = read(STDIN_FILENO, &c, 1);
        if (n <= 0) { disable_raw_mode(); return -1; }

        if (c == '\r' || c == '\n') {
            write(STDOUT_FILENO, "\r\n", 2);
            buf[len] = '\0';
            strncpy(out, buf, outsz-1); out[outsz-1] = '\0';
            disable_raw_mode();
            return 1;
        } else if (c == 3) { // Ctrl-C -> exit terminal immediately
            write(STDOUT_FILENO, "^C", 2);
            disable_raw_mode();
            immediate_exit();
            return -1; // not reached
        } else if (c == 4 /* Ctrl-D */) {
            if (len == 0) {
                // Cancel current line (swap semantics with Ctrl-C)
                write(STDOUT_FILENO, "^D\r\n", 4);
                buf[0] = '\0'; len = cur = 0;
                disable_raw_mode();
                out[0] = '\0';
                return 2;
            }
            // treat as delete at cursor
            if (cur < len) {
                memmove(&buf[cur], &buf[cur+1], len - cur - 1);
                len--; buf[len] = '\0';
                clear_line_and_prompt(prompt, buf, cur);
            }
        } else if (c == 127) { // Backspace
            if (cur > 0) {
                memmove(&buf[cur-1], &buf[cur], len - (cur - 1));
                cur--; len--; buf[len] = '\0';
                clear_line_and_prompt(prompt, buf, cur);
            }
        } else if (c == 8) { // Ctrl-Backspace (often Ctrl-H) -> delete previous word
            if (cur > 0) {
                // Find previous non-space then previous space
                size_t i = (cur > 0) ? cur : 0;
                while (i > 0 && buf[i-1] == ' ') i--;
                while (i > 0 && buf[i-1] != ' ') i--;
                size_t del = cur - i;
                memmove(&buf[i], &buf[cur], len - cur);
                len -= del; cur = i; buf[len] = '\0';
                clear_line_and_prompt(prompt, buf, cur);
            }
        } else if (c == 27) { // ESC sequence
            unsigned char seq1;
            if (read(STDIN_FILENO, &seq1, 1) != 1) continue;
            if (seq1 == '[') {
                // Read remaining CSI bytes if available
                char csibuf[8]; int k = 0; unsigned char b;
                // Non-blocking-ish small read: attempt up to 6 more bytes
                while (k < (int)sizeof(csibuf)-1) {
                    if (read(STDIN_FILENO, &b, 1) != 1) break;
                    csibuf[k++] = (char)b;
                    if ((b >= 'A' && b <= 'Z') || b == '~') break;
                }
                csibuf[k] = '\0';
                if (k == 1 && csibuf[0] == 'D') { // left
                    if (cur > 0) { cur--; write(STDOUT_FILENO, "\x1b[D", 3); }
                } else if (k == 1 && csibuf[0] == 'C') { // right
                    if (cur < len) { cur++; write(STDOUT_FILENO, "\x1b[C", 3); }
                } else if (k == 1 && csibuf[0] == 'A') { // up -> prev history
                    if (g_history.count > 0 && hist_index > 0) {
                        hist_index--;
                        strncpy(buf, g_history.items[hist_index], sizeof(buf)-1);
                        buf[sizeof(buf)-1] = '\0';
                        len = strlen(buf); cur = len;
                        clear_line_and_prompt(prompt, buf, cur);
                    }
                } else if (k == 1 && csibuf[0] == 'B') { // down -> next history
                    if (g_history.count > 0 && hist_index < g_history.count) {
                        hist_index++;
                        if (hist_index == g_history.count) { buf[0] = '\0'; len = 0; cur = 0; }
                        else {
                            strncpy(buf, g_history.items[hist_index], sizeof(buf)-1);
                            buf[sizeof(buf)-1] = '\0';
                            len = strlen(buf); cur = len;
                        }
                        clear_line_and_prompt(prompt, buf, cur);
                    }
                } else if (strcmp(csibuf, "3;5~") == 0) {
                    // Ctrl+Backspace in some terminals -> delete previous word
                    if (cur > 0) {
                        size_t i = cur;
                        while (i > 0 && buf[i-1] == ' ') i--;
                        while (i > 0 && buf[i-1] != ' ') i--;
                        size_t del = cur - i;
                        memmove(&buf[i], &buf[cur], len - cur);
                        len -= del; cur = i; buf[len] = '\0';
                        clear_line_and_prompt(prompt, buf, cur);
                    }
                }
            }
        } else if (c == 23) { // Ctrl-W -> delete previous word
            if (cur > 0) {
                size_t i = cur;
                while (i > 0 && buf[i-1] == ' ') i--;
                while (i > 0 && buf[i-1] != ' ') i--;
                size_t del = cur - i;
                memmove(&buf[i], &buf[cur], len - cur);
                len -= del; cur = i; buf[len] = '\0';
                clear_line_and_prompt(prompt, buf, cur);
            }
        } else if (c == 1) { // Ctrl-A home
            cur = 0; clear_line_and_prompt(prompt, buf, cur);
        } else if (c == 5) { // Ctrl-E end
            cur = len; clear_line_and_prompt(prompt, buf, cur);
        } else if (c >= 32 && c <= 126) { // printable ASCII
            if (len + 1 < sizeof(buf)) {
                if (cur == len) {
                    buf[len++] = (char)c; buf[len] = '\0'; cur = len;
                } else {
                    memmove(&buf[cur+1], &buf[cur], len - cur);
                    buf[cur] = (char)c; len++; cur++;
                }
                clear_line_and_prompt(prompt, buf, cur);
            }
        } else {
            // ignore other control chars
        }
    }
}

/* Helper to parse and display error with code */
static void print_error_message(const char* err_line) {
    // Expected format: "RES_ERROR <code> <message>\n"
    int code = -1;
    char msg[512] = "";
    
    const char* after_keyword = err_line + strlen(KW_RES_ERROR);
    while (*after_keyword == ' ') after_keyword++;
    
    if (sscanf(after_keyword, "%d", &code) == 1) {
        // Skip code and extract message
        const char* msg_start = after_keyword;
        while (*msg_start && (*msg_start >= '0' && *msg_start <= '9')) msg_start++;
        while (*msg_start == ' ') msg_start++;
        strncpy(msg, msg_start, sizeof(msg)-1);
        msg[sizeof(msg)-1] = '\0';
        // Remove trailing newline
        size_t len = strlen(msg);
        if (len > 0 && msg[len-1] == '\n') msg[len-1] = '\0';
        
        fprintf(stderr, "ERROR [%d]: %s\n", code, msg);
    } else {
        // Fallback: no numeric code found, print as-is
        fputs(err_line, stderr);
    }
}

static void print_until_end_marker_initial(int sockfd) {
    char buf[BUF_DATA + 1];
    int first_chunk = 1;
    for (;;) {
        ssize_t n = recv(sockfd, buf, BUF_DATA, 0);
        if (n <= 0) {
            fprintf(stderr, "ERROR: Connection closed while receiving data.\n");
            break;
        }
        buf[n] = '\0';

        if (first_chunk) {
            if (starts_with(buf, KW_RES_ERROR)) {
                // Ensure we read the full error line even if TCP splits it
                char acc[BUF_RESP * 8 + 1]; size_t used = 0;
                size_t len = strlen(buf);
                if (len > sizeof(acc) - 1) len = sizeof(acc) - 1;
                memcpy(acc, buf, len); used = len; acc[used] = '\0';
                // If we didn't receive a full line yet, keep reading until a newline
                while (strchr(acc, '\n') == NULL && used < sizeof(acc) - 1) {
                    ssize_t m = recv(sockfd, buf, BUF_DATA, 0);
                    if (m <= 0) break;
                    size_t copy = (size_t)m;
                    if (copy > sizeof(acc) - 1 - used) copy = sizeof(acc) - 1 - used;
                    memcpy(acc + used, buf, copy); used += copy; acc[used] = '\0';
                }
                print_error_message(acc);
                break;
            }
            // Skip RES_DATA_START if present
            char* p = strstr(buf, KW_RES_DATA_START);
            if (p) {
                p += strlen(KW_RES_DATA_START);
                if (*p == '\n' || *p == ' ') p++;
            } else {
                p = buf; // no explicit start marker present
            }
            char* end = strstr(p, KW_RES_DATA_END);
            if (end) {
                // Print up to before end marker
                *end = '\0';
                fputs(p, stdout);
                break;
            } else {
                fputs(p, stdout);
            }
            first_chunk = 0;
        } else {
            char* end = strstr(buf, KW_RES_DATA_END);
            if (end) {
                *end = '\0';
                fputs(buf, stdout);
                break;
            } else {
                fputs(buf, stdout);
            }
        }
    }
}

static void recv_one_response(int sockfd) {
    char resp[BUF_RESP + 1];
    ssize_t n = recv(sockfd, resp, BUF_RESP, 0);
    if (n <= 0) {
        fprintf(stderr, "ERROR: No response from server.\n");
        return;
    }
    resp[n] = '\0';
    fputs(resp, stdout);
}

int connect_to_server(char* ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = inet_addr(ip);

    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }
    return sockfd;
}

int get_ss_location(int ns_socket_fd, char* username, char* filename, char* operation, char* ip_buffer, int* port_buffer) {
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_LOCATION %s %s %s\n", username, filename, operation);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_LOCATION.\n");
        return -1;
    }
    char resp[BUF_RESP + 1];
    ssize_t n = recv(ns_socket_fd, resp, BUF_RESP, 0);
    if (n <= 0) {
        fprintf(stderr, "ERROR: No response to REQ_LOCATION.\n");
        return -1;
    }
    resp[n] = '\0';
    if (starts_with(resp, KW_RES_ERROR)) {
        print_error_message(resp);
        return -1;
    }
    if (starts_with(resp, KW_RES_LOCATION)) {
        // Expect: RES_LOCATION <ip> <port>
        char ip[128];
        int port = 0;
        if (sscanf(resp, "RES_LOCATION %127s %d", ip, &port) == 2) {
            strcpy(ip_buffer, ip);
            *port_buffer = port;
            return 0;
        }
    }
    fprintf(stderr, "ERROR: Malformed RES_LOCATION: %s\n", resp);
    return -1;
}

void handle_view(int ns_socket_fd, char* username, char** args) {
    char flags[8] = "NULL";
    if (args[0]) {
        strncpy(flags, args[0], sizeof(flags)-1);
        flags[sizeof(flags)-1] = '\0';
    }
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_VIEW %s %s\n", username, flags);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_VIEW.\n");
        return;
    }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_list(int ns_socket_fd, char* username, char** args) {
    (void)args;
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_LIST_USERS %s\n", username);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_LIST_USERS.\n");
        return;
    }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_info(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: INFO <filename>\n");
        return;
    }
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_INFO %s %s\n", username, args[0]);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_INFO.\n");
        return;
    }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_exec(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: EXEC <filename>\n");
        return;
    }
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_EXEC %s %s\n", username, args[0]);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_EXEC.\n");
        return;
    }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_create(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: CREATE <filename>\n");
        return;
    }
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_CREATE %s %s\n", username, args[0]);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_CREATE.\n");
        return;
    }
    recv_one_response(ns_socket_fd);
}

void handle_delete(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: DELETE <filename>\n");
        return;
    }
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_DELETE %s %s\n", username, args[0]);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_DELETE.\n");
        return;
    }
    recv_one_response(ns_socket_fd);
}

void handle_addaccess(int ns_socket_fd, char* username, char** args) {
    // Expect: ADDACCESS -R|-W <filename> <target_user>
    if (!args[0] || !args[1] || !args[2]) {
        fprintf(stderr, "Usage: ADDACCESS -R|-W <filename> <username>\n");
        return;
    }
    char flag[8];
    strncpy(flag, args[0], sizeof(flag)-1);
    flag[sizeof(flag)-1] = '\0';
    char req[BUF_CMD];
    // Note: order: username filename target_username flag
    snprintf(req, sizeof(req), "REQ_ADDACCESS %s %s %s %s\n", username, args[1], args[2], flag);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_ADDACCESS.\n");
        return;
    }
    recv_one_response(ns_socket_fd);
}

void handle_remaccess(int ns_socket_fd, char* username, char** args) {
    // Expect: REMACCESS <filename> <target_user>
    if (!args[0] || !args[1]) {
        fprintf(stderr, "Usage: REMACCESS <filename> <username>\n");
        return;
    }
    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_REMACCESS %s %s %s\n", username, args[0], args[1]);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_REMACCESS.\n");
        return;
    }
    recv_one_response(ns_socket_fd);
}

void handle_requestaccess(int ns_socket_fd, char* username, char** args) {
    // REQUESTACCESS <filename> <R|W>
    if (!args[0] || !args[1]) {
        fprintf(stderr, "Usage: REQUESTACCESS <filename> <R|W>\n");
        return;
    }
    char typ = args[1][0];
    if (!(typ == 'R' || typ == 'W')) { fprintf(stderr, "ERROR: type must be R or W\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_REQUESTACCESS %s %s %c\n", username, args[0], typ);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_REQUESTACCESS.\n"); return; }
    recv_one_response(ns_socket_fd);
}

void handle_listrequests(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) { fprintf(stderr, "Usage: LISTREQUESTS <filename>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_LISTREQUESTS %s %s\n", username, args[0]);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_LISTREQUESTS.\n"); return; }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_respondrequest(int ns_socket_fd, char* username, char** args) {
    // RESPONDREQUEST <filename> <user> <approve|deny> <R|W>
    if (!args[0] || !args[1] || !args[2] || !args[3]) {
        fprintf(stderr, "Usage: RESPONDREQUEST <filename> <user> <approve|deny> <R|W>\n");
        return;
    }
    char typ = args[3][0]; if (!(typ == 'R' || typ == 'W')) { fprintf(stderr, "ERROR: type must be R or W\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_RESPONDREQUEST %s %s %s %s %c\n", username, args[0], args[1], args[2], typ);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_RESPONDREQUEST.\n"); return; }
    recv_one_response(ns_socket_fd);
}

void handle_read(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: READ <filename>\n");
        return;
    }
    char ip[128]; int port = 0;
    if (get_ss_location(ns_socket_fd, username, args[0], "READ", ip, &port) < 0) return;
    int ssfd = connect_to_server(ip, port);
    if (ssfd < 0) return;

    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_READ %s %s\n", username, args[0]);
    if (send_line(ssfd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_READ.\n");
        close(ssfd);
        return;
    }
    print_until_end_marker_initial(ssfd);
    close(ssfd);
}

void handle_stream(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: STREAM <filename>\n");
        return;
    }
    char ip[128]; int port = 0;
    if (get_ss_location(ns_socket_fd, username, args[0], "STREAM", ip, &port) < 0) return;
    int ssfd = connect_to_server(ip, port);
    if (ssfd < 0) return;

    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_STREAM %s %s\n", username, args[0]);
    if (send_line(ssfd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_STREAM.\n");
        close(ssfd);
        return;
    }

    char resp[BUF_RESP + 1];
    for (;;) {
        ssize_t n = recv(ssfd, resp, BUF_RESP, 0);
        if (n <= 0) {
            fprintf(stderr, "ERROR: Storage Server disconnected mid-stream.\n");
            break;
        }
        resp[n] = '\0';
        if (strstr(resp, KW_DATA_STREAM_END)) {
            break;
        }
        if (starts_with(resp, KW_DATA_STREAM_WORD)) {
            // Expect: DATA_STREAM_WORD <word>\n
            char *p = resp + strlen(KW_DATA_STREAM_WORD);
            while (*p == ' ' || *p == '\t') p++;
            // Print word as-is
            fputs(p, stdout);
            fflush(stdout);
            usleep(100000); // 0.1s
        } else {
            // Unknown chunk; print as-is for debugging
            fputs(resp, stdout);
        }
    }

    close(ssfd);
}

void handle_undo(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) {
        fprintf(stderr, "Usage: UNDO <filename>\n");
        return;
    }
    char ip[128]; int port = 0;
    if (get_ss_location(ns_socket_fd, username, args[0], "WRITE", ip, &port) < 0) return;
    int ssfd = connect_to_server(ip, port);
    if (ssfd < 0) return;

    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_UNDO %s %s\n", username, args[0]);
    if (send_line(ssfd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_UNDO.\n");
        close(ssfd);
        return;
    }
    recv_one_response(ssfd);
    close(ssfd);
}

void handle_write(int ns_socket_fd, char* username, char** args) {
    if (!args[0] || !args[1]) {
        fprintf(stderr, "Usage: WRITE <filename> <sentence_number>\n");
        return;
    }
    // Validate that sentence_number is a non-negative integer
    const char* sidx = args[1];
    if (*sidx == '\0') { fprintf(stderr, "Usage: WRITE <filename> <sentence_number>\n"); return; }
    for (const char* p = sidx; *p; ++p) {
        if (*p < '0' || *p > '9') { fprintf(stderr, "ERROR: sentence_number must be a non-negative integer.\n"); return; }
    }
    char ip[128]; int port = 0;
    if (get_ss_location(ns_socket_fd, username, args[0], "WRITE", ip, &port) < 0) return;
    int ssfd = connect_to_server(ip, port);
    if (ssfd < 0) return;

    char req[BUF_CMD];
    snprintf(req, sizeof(req), "REQ_WRITE_LOCK %s %s %s\n", username, args[0], args[1]);
    if (send_line(ssfd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_WRITE_LOCK.\n");
        close(ssfd);
        return;
    }

    char resp[BUF_RESP + 1];
    ssize_t n = recv(ssfd, resp, BUF_RESP, 0);
    if (n <= 0) {
        fprintf(stderr, "ERROR: No response to REQ_WRITE_LOCK.\n");
        close(ssfd);
        return;
    }
    resp[n] = '\0';
    if (starts_with(resp, KW_RES_ERROR)) {
        print_error_message(resp);
        close(ssfd);
        return;
    }

    // Got OK, enter update loop
    fputs(resp, stdout); // Print confirmation
    printf("Sentence locked. Enter updates:\n");

    for (;;) {
        char inner_line[BUF_CMD];
        int rr = read_line_edited("> ", inner_line, sizeof(inner_line));
        if (rr == 0) {
            // Ctrl-D on empty -> treat as release
            strcpy(inner_line, "ETIRW");
        } else if (rr < 0) {
            // Fallback to fgets
            printf("> "); fflush(stdout);
            if (!fgets(inner_line, sizeof(inner_line), stdin)) { strcpy(inner_line, "ETIRW"); }
            strip_newline(inner_line);
        }
        if (strcmp(inner_line, "ETIRW") == 0) {
            const char* rel = "CMD_WRITE_RELEASE\n";
            if (send_line(ssfd, rel) < 0) {
                fprintf(stderr, "ERROR: Failed to send CMD_WRITE_RELEASE.\n");
            }
            break;
        }
        char* content = strchr(inner_line, ' ');
        if (!content || *(content + 1) == '\n' || *(content + 1) == '\0') {
            fprintf(stderr, "Invalid format. Usage: <word_index> <content>\n");
            continue;
        }
        *content = '\0';
        content++;
        // Validate word_index is a non-negative integer
        int valid = 1; if (*inner_line == '\0') valid = 0; else {
            for (char* p = inner_line; *p; ++p) { if (*p < '0' || *p > '9') { valid = 0; break; } }
        }
        if (!valid) {
            fprintf(stderr, "ERROR: word_index must be a non-negative integer.\n");
            continue;
        }
        int word_index = atoi(inner_line);
        char update[BUF_CMD + 32];
        snprintf(update, sizeof(update), "DATA_WRITE_UPDATE %d %s\n", word_index, content);
        if (send_line(ssfd, update) < 0) {
            fprintf(stderr, "ERROR: Failed to send DATA_WRITE_UPDATE.\n");
            break;
        }
        // Read and print response
        recv_one_response(ssfd);
    }

    // Final ACK for release
    recv_one_response(ssfd);
    close(ssfd);
}

void handle_command(int ns_socket_fd, char* username, char* command, char** args) {
    if (strcmp(command, "VIEW") == 0) {
        handle_view(ns_socket_fd, username, args);
    } else if (strcmp(command, "LIST") == 0) {
        handle_list(ns_socket_fd, username, args);
    } else if (strcmp(command, "CREATE") == 0) {
        handle_create(ns_socket_fd, username, args);
    } else if (strcmp(command, "DELETE") == 0) {
        handle_delete(ns_socket_fd, username, args);
    } else if (strcmp(command, "CREATEFOLDER") == 0) {
        handle_createfolder(ns_socket_fd, username, args);
    } else if (strcmp(command, "MOVE") == 0) {
        handle_move(ns_socket_fd, username, args);
    } else if (strcmp(command, "VIEWFOLDER") == 0) {
        handle_viewfolder(ns_socket_fd, username, args);
    } else if (strcmp(command, "LISTFOLDERS") == 0) {
        handle_listfolders(ns_socket_fd, username, args);
    } else if (strcmp(command, "INFO") == 0) {
        handle_info(ns_socket_fd, username, args);
    } else if (strcmp(command, "ADDACCESS") == 0) {
        handle_addaccess(ns_socket_fd, username, args);
    } else if (strcmp(command, "REMACCESS") == 0) {
        handle_remaccess(ns_socket_fd, username, args);
    } else if (strcmp(command, "EXEC") == 0) {
        handle_exec(ns_socket_fd, username, args);
    } else if (strcmp(command, "CHECKPOINT") == 0) {
        handle_checkpoint(ns_socket_fd, username, args);
    } else if (strcmp(command, "LISTCHECKPOINTS") == 0) {
        handle_listcheckpoints(ns_socket_fd, username, args);
    } else if (strcmp(command, "VIEWCHECKPOINT") == 0) {
        handle_viewcheckpoint(ns_socket_fd, username, args);
    } else if (strcmp(command, "REVERT") == 0) { // reusing name but maps to REVERTCHECKPOINT
        handle_revertcheckpoint(ns_socket_fd, username, args);
    } else if (strcmp(command, "READ") == 0) {
        handle_read(ns_socket_fd, username, args);
    } else if (strcmp(command, "WRITE") == 0) {
        handle_write(ns_socket_fd, username, args);
    } else if (strcmp(command, "STREAM") == 0) {
        handle_stream(ns_socket_fd, username, args);
    } else if (strcmp(command, "UNDO") == 0) {
        handle_undo(ns_socket_fd, username, args);
    } else if (strcmp(command, "REQUESTACCESS") == 0) {
        handle_requestaccess(ns_socket_fd, username, args);
    } else if (strcmp(command, "LISTREQUESTS") == 0) {
        handle_listrequests(ns_socket_fd, username, args);
    } else if (strcmp(command, "RESPONDREQUEST") == 0) {
        handle_respondrequest(ns_socket_fd, username, args);
    } else if (strcmp(command, "HELP") == 0) {
        handle_help();
    } else if (strcasecmp(command, "EASTEREGG") == 0) {
        handle_easteregg();
    } else {
        fprintf(stderr, "ERROR: Unknown command '%s'\n", command);
    }
}

static int file_exists(const char* path) {
    struct stat st; return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static void escape_path(const char* in, char* out, size_t outsz) {
    // Basic shell escaping: wrap in single quotes, escape existing single quotes
    size_t j = 0; out[0] = '\0';
    if (j < outsz) out[j++] = '\'';
    for (size_t i = 0; in[i] && j + 4 < outsz; ++i) {
        if (in[i] == '\'') { // close, escape, reopen: '\''
            out[j++] = '\''; out[j++] = '\\'; out[j++] = '\''; out[j++] = '\'';
        } else {
            out[j++] = in[i];
        }
    }
    if (j < outsz) out[j++] = '\'';
    if (j <= outsz) out[j] = '\0';
}

void handle_help(void) {
    // Print a concise command reference. Keep widths consistent for readability.
    printf("\n=== HELP: Command Reference ===\n");
    printf("Core:\n");
    printf("  VIEW [-a|-l|-al]                 List files (all, long, all+long).\n");
    printf("  LIST                            List users currently connected.\n");
    printf("  CREATE <file>                   Create empty file (owner only).\n");
    printf("  DELETE <file>                   Delete a file (owner only).\n");
    printf("  INFO <file>                     Show ACL + basic stats for file.\n");
    printf("  READ <file>                     Read full file contents.\n");
    printf("  STREAM <file>                   Stream words one-by-one.\n");
    printf("  WRITE <file> <sentence_idx>     Lock sentence; then '<word_idx> <content>' lines; end with ETIRW.\n");
    printf("  UNDO <file>                     Revert last WRITE (if available).\n");
    printf("  EXEC <file>                     Execute a script (owner + exec permission).\n");
    printf("\nAccess Control:\n");
    printf("  ADDACCESS -R|-W <file> <user>   Grant read or write access.\n");
    printf("  REMACCESS <file> <user>         Remove all access for user.\n");
    printf("  REQUESTACCESS <file> <R|W>      Request R/W access from owner.\n");
    printf("  LISTREQUESTS <file>             List pending access requests.\n");
    printf("  RESPONDREQUEST <file> <user> <approve|deny> <R|W>  Owner responds to request.\n");
    printf("\nFolders:\n");
    printf("  CREATEFOLDER <path>             Create a folder path (owner becomes creator).\n");
    printf("  MOVE <file> <folder>            Move file into folder (owner only).\n");
    printf("  VIEWFOLDER <folder> [-l|-a|-al] View files inside folder.\n");
    printf("  LISTFOLDERS [-a|-l]             List folders (all or long).\n");
    printf("\nCheckpoints:\n");
    printf("  CHECKPOINT <file> <tag>         Create checkpoint snapshot.\n");
    printf("  LISTCHECKPOINTS <file>          List available checkpoint tags.\n");
    printf("  VIEWCHECKPOINT <file> <tag>     Show file content at checkpoint.\n");
    printf("  REVERT <file> <tag>             Revert file to checkpoint.\n");
    printf("\nMisc:\n");
    printf("  HELP                            Show this reference.\n");
    printf("  EASTEREGG                       Surprise.\n");
    printf("  exit                            Quit the client (Ctrl-D also works).\n");
    printf("\nNotes:\n");
    printf("  - Sentence and word indices are 0-based.\n");
    printf("  - VIEW -l / -al columns: Filename, Words, Chars, Size(KB), Created, Last Access Time, Owner.\n");
    printf("  - Use TAB/Arrow keys for basic history navigation.\n");
    printf("\n=================================\n\n");
}

static int try_exec_terminal(const char* cmd) {
    // Try a list of common terminal emulators, preferring gnome-terminal
    // Return only on failure; on success, this replaces the process image.
    execlp("gnome-terminal", "gnome-terminal", "--maximize", "--", "bash", "-lc", cmd, (char*)NULL);
    execlp("x-terminal-emulator", "x-terminal-emulator", "-e", "bash", "-lc", cmd, (char*)NULL);
    execlp("xterm", "xterm", "-fa", "Monospace", "-fs", "12", "-fullscreen", "-e", "bash", "-lc", cmd, (char*)NULL);
    execlp("konsole", "konsole", "-e", "bash", "-lc", cmd, (char*)NULL);
    execlp("xfce4-terminal", "xfce4-terminal", "--maximize", "-e", "bash", "-lc", cmd, (char*)NULL);
    execlp("mate-terminal", "mate-terminal", "--maximize", "--", "bash", "-lc", cmd, (char*)NULL);
    execlp("tilix", "tilix", "-e", "bash", "-lc", cmd, (char*)NULL);
    execlp("alacritty", "alacritty", "-e", "bash", "-lc", cmd, (char*)NULL);
    execlp("kitty", "kitty", "bash", "-lc", cmd, (char*)NULL);
    execlp("urxvt", "urxvt", "-e", "bash", "-lc", cmd, (char*)NULL);
    return -1;
}

void handle_easteregg(void) {
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) strcpy(cwd, ".");
    char mp3_path[PATH_MAX];
    snprintf(mp3_path, sizeof(mp3_path), "%s/%s", cwd, "media/rick_roll.MP3");

    int have_mp3 = file_exists(mp3_path);

    char mp3_path_escaped[PATH_MAX * 2 + 16];
    escape_path(mp3_path, mp3_path_escaped, sizeof(mp3_path_escaped));

    char mp3_uri[PATH_MAX + 16];
    snprintf(mp3_uri, sizeof(mp3_uri), "file://%s", mp3_path);
    char mp3_uri_escaped[PATH_MAX * 2 + 16];
    escape_path(mp3_uri, mp3_uri_escaped, sizeof(mp3_uri_escaped));

    char cmd[4096];
            if (have_mp3) {
                    // Build a shell that starts audio (prefer hiding Rhythmbox so it doesn't obstruct) and runs ASCII; cleanup on Ctrl-C/exit
                    snprintf(cmd, sizeof(cmd),
                            "set -m; on_int() { "
                                "if command -v rhythmbox-client >/dev/null 2>&1; then rhythmbox-client --stop >/dev/null 2>&1; rhythmbox-client --pause >/dev/null 2>&1; fi; "
                                "if jobs -p >/dev/null 2>&1; then kill $(jobs -p) 2>/dev/null; fi; "
                            "}; trap on_int EXIT INT TERM; "
                            "if command -v rhythmbox-client >/dev/null 2>&1; then "
                                "(rhythmbox-client --no-start --play-uri=%s >/dev/null 2>&1 || rhythmbox-client --play-uri=%s >/dev/null 2>&1); "
                                "rhythmbox-client --hide >/dev/null 2>&1; "
                            "elif command -v mpg123 >/dev/null 2>&1; then mpg123 -q %s & "
                            "elif command -v cvlc >/dev/null 2>&1; then cvlc --play-and-exit --quiet %s & "
                            "elif command -v ffplay >/dev/null 2>&1; then ffplay -nodisp -autoexit -loglevel quiet %s & "
                            "elif command -v paplay >/dev/null 2>&1; then paplay %s & "
                            "elif command -v aplay >/dev/null 2>&1; then aplay %s & fi; "
                            "echo 'Enjoy! Press Ctrl-C to stop audio and close.'; "
                            "curl -s https://ascii.live/rick",
                            mp3_uri_escaped,
                            mp3_uri_escaped,
                            mp3_path_escaped,
                            mp3_path_escaped,
                            mp3_path_escaped,
                            mp3_path_escaped,
                            mp3_path_escaped);
    } else {
        snprintf(cmd, sizeof(cmd),
            "echo 'rick_roll.MP3 not found; playing ASCII only.'; echo; curl -s https://ascii.live/rick; read -p 'Press Enter to close...' _");
    }

    pid_t pid = fork();
    if (pid == 0) {
        // Child: launch terminal
        (void)try_exec_terminal(cmd);
        fprintf(stderr, "ERROR: No supported terminal emulator found to run easteregg.\n");
        _exit(127);
    } else if (pid < 0) {
        perror("fork");
    } else {
        printf("Easter egg launched in a new terminal.\n");
    }
}

void handle_createfolder(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) { fprintf(stderr, "Usage: CREATEFOLDER <path>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_CREATEFOLDER %s %s\n", username, args[0]);
    send_line(ns_socket_fd, req);
    recv_one_response(ns_socket_fd);
}

void handle_move(int ns_socket_fd, char* username, char** args) {
    if (!args[0] || !args[1]) { fprintf(stderr, "Usage: MOVE <filename> <folder>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_MOVE %s %s %s\n", username, args[0], args[1]);
    send_line(ns_socket_fd, req);
    recv_one_response(ns_socket_fd);
}

void handle_viewfolder(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) { fprintf(stderr, "Usage: VIEWFOLDER <folder> [-l|-a|-al]\n"); return; }
    const char* folder = args[0];
    const char* flag = args[1] ? args[1] : "NULL";
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_VIEWFOLDER %s %s %s\n", username, folder, flag);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_VIEWFOLDER.\n");
        return;
    }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_listfolders(int ns_socket_fd, char* username, char** args) {
    const char* flag = args[0] ? args[0] : "NULL"; // allow -a or -l later
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_LISTFOLDERS %s %s\n", username, flag);
    if (send_line(ns_socket_fd, req) < 0) {
        fprintf(stderr, "ERROR: Failed to send REQ_LISTFOLDERS.\n");
        return;
    }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_checkpoint(int ns_socket_fd, char* username, char** args) {
    if (!args[0] || !args[1]) { fprintf(stderr, "Usage: CHECKPOINT <filename> <tag>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_CHECKPOINT %s %s %s\n", username, args[0], args[1]);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_CHECKPOINT.\n"); return; }
    recv_one_response(ns_socket_fd);
}

void handle_listcheckpoints(int ns_socket_fd, char* username, char** args) {
    if (!args[0]) { fprintf(stderr, "Usage: LISTCHECKPOINTS <filename>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_LISTCHECKPOINTS %s %s\n", username, args[0]);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_LISTCHECKPOINTS.\n"); return; }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_viewcheckpoint(int ns_socket_fd, char* username, char** args) {
    if (!args[0] || !args[1]) { fprintf(stderr, "Usage: VIEWCHECKPOINT <filename> <tag>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_VIEWCHECKPOINT %s %s %s\n", username, args[0], args[1]);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_VIEWCHECKPOINT.\n"); return; }
    print_until_end_marker_initial(ns_socket_fd);
}

void handle_revertcheckpoint(int ns_socket_fd, char* username, char** args) {
    if (!args[0] || !args[1]) { fprintf(stderr, "Usage: REVERT <filename> <tag>\n"); return; }
    char req[BUF_CMD]; snprintf(req, sizeof(req), "REQ_REVERTCHECKPOINT %s %s %s\n", username, args[0], args[1]);
    if (send_line(ns_socket_fd, req) < 0) { fprintf(stderr, "ERROR: Failed to send REQ_REVERTCHECKPOINT.\n"); return; }
    recv_one_response(ns_socket_fd);
}
/* Parse command line with quote support (both " and ') */
static int parse_command_line(char* line, char** command, char* args[10]) {
    char* p = line;
    int idx = 0;
    int is_command = 1;
    
    // Skip leading whitespace
    while (*p && (*p == ' ' || *p == '\t')) p++;
    
    while (*p && idx < 10) {
        // Skip whitespace between tokens
        while (*p && (*p == ' ' || *p == '\t')) p++;
        if (!*p) break;
        
        char* token_start = p;
        char quote = 0;
        
        // Check if token starts with quote
        if (*p == '"' || *p == '\'') {
            quote = *p;
            p++;
            token_start = p;
            // Find closing quote
            while (*p && *p != quote) p++;
            if (*p == quote) {
                *p = '\0'; // Terminate the token
                p++;
            }
        } else {
            // Regular token - read until space or newline
            while (*p && *p != ' ' && *p != '\t' && *p != '\n') p++;
            if (*p) {
                *p = '\0';
                p++;
            }
        }
        
        if (is_command) {
            *command = token_start;
            is_command = 0;
        } else {
            args[idx++] = token_start;
        }
    }
    
    args[idx] = NULL;
    return idx;
}

void run_shell(int ns_socket_fd, char* username) {
    char line[BUF_CMD];
    while (1) {
        char prompt[256]; snprintf(prompt, sizeof(prompt), "%s@docs++ > ", username);
        int r = read_line_edited(prompt, line, sizeof(line));
        if (r == 0) { // Ctrl-D on empty line -> exit
            break;
        }
        if (r == 2) { // Ctrl-C interrupt -> new prompt
            continue;
        }
        if (r < 0) {
            // Fallback to fgets if raw mode failed
            printf("%s", prompt); fflush(stdout);
            if (!fgets(line, sizeof(line), stdin)) break;
        }
        
        // Preserve original command line for history before parsing mutates it
        char line_orig[BUF_CMD];
        strncpy(line_orig, line, sizeof(line_orig)-1); line_orig[sizeof(line_orig)-1] = '\0';

        char* command = NULL;
        char* args[10];
        char line_for_parse[BUF_CMD];
        strncpy(line_for_parse, line, sizeof(line_for_parse)-1); line_for_parse[sizeof(line_for_parse)-1] = '\0';
        parse_command_line(line_for_parse, &command, args);
        
    if (!command || strlen(command) == 0) continue; // empty input
    if (strcmp(command, "exit") == 0 || strcmp(command, "quit") == 0) break;
        history_add(line_orig);
        
        handle_command(ns_socket_fd, username, command, args);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: ./client <ns_ip> <ns_port>\n");
        return EXIT_FAILURE;
    }

    char username[128];
    printf("Enter username: ");
    fflush(stdout);
    if (!fgets(username, sizeof(username), stdin)) {
        fprintf(stderr, "ERROR: Failed to read username.\n");
        return EXIT_FAILURE;
    }
    strip_newline(username);
    // record globally for exit handlers/history saves
    strncpy(g_username, username, sizeof(g_username)-1);
    g_username[sizeof(g_username)-1] = '\0';
    // Load per-username history after we know the username
    history_load(username);

    char* ns_ip = argv[1];
    int ns_port = atoi(argv[2]);

    int nsfd = connect_to_server(ns_ip, ns_port);
    if (nsfd < 0) {
        fprintf(stderr, "ERROR: Could not connect to Name Server.\n");
        return EXIT_FAILURE;
    }

    char reg[BUF_CMD];
    snprintf(reg, sizeof(reg), "REGISTER %s\n", username);
    if (send_line(nsfd, reg) < 0) {
        fprintf(stderr, "ERROR: Failed to send REGISTER.\n");
        close(nsfd);
        return EXIT_FAILURE;
    }

    // Read and display initial ACK from Name Server to avoid mixing it with the first command's response
    char init_resp[BUF_RESP + 1];
    ssize_t rn = recv(nsfd, init_resp, BUF_RESP, 0);
    if (rn > 0) {
        init_resp[rn] = '\0';
        fputs(init_resp, stdout);
    }

    run_shell(nsfd, username);

    printf("Disconnecting...\n");
    // Save history on clean exit
    history_save(username);
    close(nsfd);
    return EXIT_SUCCESS;
}
