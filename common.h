#ifndef COMMON_H
#define COMMON_H

#include <sys/time.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

/* Common protocol error codes and helpers used across NameServer/StorageServer/Client */

/* Numeric error codes (universal) */
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

/* Helper macro to stringify codes when needed */
#define STR(x) #x

/* Network retry configuration */
#define RECV_TIMEOUT_SEC 5
#define MAX_RETRIES 3

/* Recv with timeout: returns bytes received or -1 on error/timeout */
static inline ssize_t recv_with_timeout(int sockfd, void* buf, size_t len, int timeout_sec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        return -1; // failed to set timeout
    }
    
    ssize_t n = recv(sockfd, buf, len, 0);
    
    // Clear timeout (set back to blocking)
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return -1; // timeout
    }
    
    return n;
}

#endif /* COMMON_H */
