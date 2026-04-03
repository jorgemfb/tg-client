/**
 * Bot TDLib en C para descarga automática de vídeos.
 * Compilación:
 *   make
 *
 * Variables requeridas:
 *   API_ID
 *   API_HASH
 *   BOT_TOKEN
 *
 * Variables opcionales:
 *   TDLIB_DB
 *   TDLIB_FILES
 *   DOWNLOAD_DIR
 * Version: v0.0.6 build:03042026
 */

#define _GNU_SOURCE
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <td/telegram/td_json_client.h>
#include <td/telegram/td_log.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_DB_DIR "./tdlib"
#define DEFAULT_FILES_DIR "./tdlib/files"
#define DEFAULT_DOWNLOAD_DIR "./videos_descargados"
#define DOWNLOAD_PRIORITY 32
#define MAX_TRACKED_FILES 1000
#define PROGRESS_UPDATE_SECONDS 2
#define PROGRESS_UPDATE_PERCENT_STEP 5

static volatile sig_atomic_t running = 1;
static void *client = NULL;
static char download_dir[PATH_MAX] = {0};
static char *last_sent_payload = NULL;
static FILE *app_log_file = NULL;
static FILE *tdlib_log_file = NULL;
static char logs_dir[PATH_MAX] = {0};
static time_t last_log_cleanup = 0;

typedef struct {
    int file_id;
    long long chat_id;
    long long temp_progress_message_id;
    long long progress_message_id;
    long long total_size;
    int last_reported_percent;
    time_t started_at;
    time_t last_progress_update;
} PendingDownload;

typedef struct {
    const char *message_type;
    const char *mime_substring;
    const char *container_pattern;
    const char *file_pattern;
    const char *nested_file_pattern;
    const char *size_prefix;
} MediaPattern;

static PendingDownload pending_downloads[MAX_TRACKED_FILES] = {0};
static int pending_count = 0;

static const MediaPattern media_patterns[] = {
    {
        "messageVideo",
        NULL,
        "\"video\":{",
        "\"video\":{\"@type\":\"file\",\"id\":",
        "\"video\":{\"video\":{\"id\":",
        "\"video\":{\"video\":{"
    },
    {
        "messageDocument",
        "\"mime_type\":\"video/",
        "\"document\":{",
        "\"document\":{\"@type\":\"file\",\"id\":",
        "\"document\":{\"document\":{\"id\":",
        "\"document\":{\"document\":{"
    },
    {
        "messageAnimation",
        NULL,
        "\"animation\":{",
        "\"animation\":{\"@type\":\"file\",\"id\":",
        "\"animation\":{\"animation\":{\"id\":",
        "\"animation\":{\"animation\":{"
    },
    {
        "messageVideoNote",
        NULL,
        "\"video_note\":{",
        "\"video\":{\"@type\":\"file\",\"id\":",
        "\"video_note\":{\"video\":{\"id\":",
        "\"video_note\":{\"video\":{"
    }
};

static const size_t media_pattern_count = sizeof(media_patterns) / sizeof(media_patterns[0]);

static int mkdir_p(const char *path);

static FILE *get_log_target(void) {
    return app_log_file ? app_log_file : stderr;
}

static void log_vline(const char *level, const char *fmt, va_list args) {
    FILE *target = get_log_target();

    fprintf(target, "[%s] ", level);
    vfprintf(target, fmt, args);
    fputc('\n', target);
    fflush(target);
}

static void format_time_label(time_t value, char *buffer, size_t bufsize) {
    struct tm local_tm;

    if (!buffer || bufsize == 0) {
        return;
    }

    if (value == (time_t)-1 || localtime_r(&value, &local_tm) == NULL) {
        snprintf(buffer, bufsize, "desconocida");
        return;
    }

    if (strftime(buffer, bufsize, "%Y-%m-%d %H:%M:%S", &local_tm) == 0) {
        snprintf(buffer, bufsize, "desconocida");
    }
}

static PendingDownload *find_pending_download(int file_id) {
    for (int i = 0; i < pending_count; ++i) {
        if (pending_downloads[i].file_id == file_id) {
            return &pending_downloads[i];
        }
    }
    return NULL;
}

static int get_executable_dir(char *buffer, size_t buffer_size) {
    char exe_path[PATH_MAX];
    ssize_t length;
    char *last_slash;

    if (!buffer || buffer_size == 0) {
        errno = EINVAL;
        return -1;
    }

    length = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (length < 0) {
        if (!getcwd(buffer, buffer_size)) {
            return -1;
        }
        return 0;
    }

    exe_path[length] = '\0';
    last_slash = strrchr(exe_path, '/');
    if (!last_slash) {
        if (!getcwd(buffer, buffer_size)) {
            return -1;
        }
        return 0;
    }

    *last_slash = '\0';
    if (snprintf(buffer, buffer_size, "%s", exe_path) >= (int)buffer_size) {
        errno = ENAMETOOLONG;
        return -1;
    }

    return 0;
}

static void cleanup_old_logs(const char *directory, time_t now) {
    DIR *dir;
    struct dirent *entry;

    if (!directory || directory[0] == '\0') {
        return;
    }

    dir = opendir(directory);
    if (!dir) {
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        char path[PATH_MAX];
        struct stat st;

        if (entry->d_name[0] == '.') {
            continue;
        }

        if (snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name) >= (int)sizeof(path)) {
            continue;
        }

        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
            continue;
        }

        if (now - st.st_mtime >= 24 * 60 * 60) {
            unlink(path);
        }
    }

    closedir(dir);
}

static void tdlib_log_callback(int verbosity_level, const char *message) {
    (void)verbosity_level;

    if (!tdlib_log_file || !message) {
        return;
    }

    fprintf(tdlib_log_file, "%s\n", message);
    fflush(tdlib_log_file);
}

static int setup_logging(void) {
    char executable_dir[PATH_MAX];
    char timestamp[32];
    char app_log_path[PATH_MAX];
    char tdlib_log_path[PATH_MAX];
    int null_fd;
    time_t now;
    struct tm local_tm;

    if (get_executable_dir(executable_dir, sizeof(executable_dir)) != 0) {
        return -1;
    }

    if (snprintf(logs_dir, sizeof(logs_dir), "%s/logs", executable_dir) >= (int)sizeof(logs_dir)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    if (mkdir_p(logs_dir) != 0) {
        return -1;
    }

    now = time(NULL);
    if (localtime_r(&now, &local_tm) == NULL) {
        return -1;
    }

    cleanup_old_logs(logs_dir, now);
    last_log_cleanup = now;

    if (strftime(timestamp, sizeof(timestamp), "%Y%m%d-%H%M%S", &local_tm) == 0) {
        errno = EINVAL;
        return -1;
    }

    if (snprintf(app_log_path, sizeof(app_log_path), "%s/bot-%s.log", logs_dir, timestamp) >=
        (int)sizeof(app_log_path)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    if (snprintf(tdlib_log_path, sizeof(tdlib_log_path), "%s/tdlib-%s.log", logs_dir, timestamp) >=
        (int)sizeof(tdlib_log_path)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    app_log_file = fopen(app_log_path, "a");
    if (!app_log_file) {
        return -1;
    }

    tdlib_log_file = fopen(tdlib_log_path, "a");
    if (!tdlib_log_file) {
        fclose(app_log_file);
        app_log_file = NULL;
        return -1;
    }

    setvbuf(app_log_file, NULL, _IOLBF, 0);
    setvbuf(tdlib_log_file, NULL, _IOLBF, 0);
    null_fd = open("/dev/null", O_WRONLY);
    if (null_fd < 0) {
        return -1;
    }

    if (dup2(null_fd, STDOUT_FILENO) < 0 || dup2(null_fd, STDERR_FILENO) < 0) {
        close(null_fd);
        return -1;
    }
    close(null_fd);

    td_set_log_message_callback(5, tdlib_log_callback);
    return 0;
}

static char *trim_whitespace(char *value) {
    char *end;

    while (*value == ' ' || *value == '\t' || *value == '\n' || *value == '\r') {
        ++value;
    }

    if (*value == '\0') {
        return value;
    }

    end = value + strlen(value) - 1;
    while (end > value &&
           (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
        *end = '\0';
        --end;
    }

    return value;
}

static void load_dotenv_file(const char *filename) {
    FILE *file;
    char line[4096];

    file = fopen(filename, "r");
    if (!file) {
        return;
    }

    while (fgets(line, sizeof(line), file)) {
        char *content = trim_whitespace(line);
        char *separator;
        char *key;
        char *value;
        size_t value_len;

        if (*content == '\0' || *content == '#') {
            continue;
        }

        separator = strchr(content, '=');
        if (!separator) {
            continue;
        }

        *separator = '\0';
        key = trim_whitespace(content);
        value = trim_whitespace(separator + 1);

        if (*key == '\0' || getenv(key)) {
            continue;
        }

        value_len = strlen(value);
        if (value_len >= 2 &&
            ((value[0] == '"' && value[value_len - 1] == '"') ||
             (value[0] == '\'' && value[value_len - 1] == '\''))) {
            value[value_len - 1] = '\0';
            ++value;
        }

        setenv(key, value, 0);
    }

    fclose(file);
}

static const char *getenv_any(const char *first, const char *second) {
    const char *value = getenv(first);

    if (value && value[0] != '\0') {
        return value;
    }

    if (!second) {
        return NULL;
    }

    value = getenv(second);
    if (value && value[0] != '\0') {
        return value;
    }

    return NULL;
}

static void log_line(const char *level, const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    log_vline(level, fmt, args);
    va_end(args);
}

static void log_info(const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    log_vline("INFO", fmt, args);
    va_end(args);
}

static void log_error(const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    log_vline("ERROR", fmt, args);
    va_end(args);
}

static int mkdir_p(const char *path) {
    char temp[PATH_MAX];
    size_t len;

    if (!path || !path[0]) {
        errno = EINVAL;
        return -1;
    }

    len = strnlen(path, sizeof(temp));
    if (len == 0 || len >= sizeof(temp)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    memcpy(temp, path, len + 1);

    for (char *p = temp + 1; *p; ++p) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(temp, 0700) != 0 && errno != EEXIST) {
                return -1;
            }
            *p = '/';
        }
    }

    if (mkdir(temp, 0700) != 0 && errno != EEXIST) {
        return -1;
    }

    return 0;
}

static int make_absolute_path(const char *input, char *output, size_t output_size) {
    char cwd[PATH_MAX];
    int needed;

    if (!input || !output || output_size == 0) {
        return -1;
    }

    if (input[0] == '/') {
        needed = snprintf(output, output_size, "%s", input);
    } else {
        if (!getcwd(cwd, sizeof(cwd))) {
            return -1;
        }
        needed = snprintf(output, output_size, "%s/%s", cwd, input);
    }

    if (needed < 0 || (size_t)needed >= output_size) {
        errno = ENAMETOOLONG;
        return -1;
    }

    return 0;
}

static int json_escape_string(const char *input, char *output, size_t output_size) {
    size_t out = 0;

    if (!input || !output || output_size == 0) {
        errno = EINVAL;
        return -1;
    }

    while (*input) {
        const char *replacement = NULL;
        char unicode_escape[7];
        unsigned char ch = (unsigned char)*input++;

        switch (ch) {
            case '"':
                replacement = "\\\"";
                break;
            case '\\':
                replacement = "\\\\";
                break;
            case '\b':
                replacement = "\\b";
                break;
            case '\f':
                replacement = "\\f";
                break;
            case '\n':
                replacement = "\\n";
                break;
            case '\r':
                replacement = "\\r";
                break;
            case '\t':
                replacement = "\\t";
                break;
            default:
                if (ch < 0x20) {
                    snprintf(unicode_escape, sizeof(unicode_escape), "\\u%04x", ch);
                    replacement = unicode_escape;
                }
                break;
        }

        if (replacement) {
            size_t replacement_len = strlen(replacement);

            if (out + replacement_len >= output_size) {
                errno = ENAMETOOLONG;
                return -1;
            }

            memcpy(output + out, replacement, replacement_len);
            out += replacement_len;
            continue;
        }

        if (out + 1 >= output_size) {
            errno = ENAMETOOLONG;
            return -1;
        }

        output[out++] = (char)ch;
    }

    output[out] = '\0';
    return 0;
}

static int json_extract_string_after(const char *start, const char *pattern, char *buffer, size_t bufsize) {
    const char *pos;
    size_t out = 0;

    if (!start || !pattern || !buffer || bufsize == 0) {
        return 0;
    }

    pos = strstr(start, pattern);
    if (!pos) {
        return 0;
    }

    pos += strlen(pattern);
    while (*pos && *pos != '"' && out + 1 < bufsize) {
        if (*pos == '\\') {
            ++pos;
            if (*pos == '\0') {
                break;
            }

            switch (*pos) {
                case '"':
                case '\\':
                case '/':
                    buffer[out++] = *pos++;
                    break;
                case 'b':
                    buffer[out++] = '\b';
                    ++pos;
                    break;
                case 'f':
                    buffer[out++] = '\f';
                    ++pos;
                    break;
                case 'n':
                    buffer[out++] = '\n';
                    ++pos;
                    break;
                case 'r':
                    buffer[out++] = '\r';
                    ++pos;
                    break;
                case 't':
                    buffer[out++] = '\t';
                    ++pos;
                    break;
                case 'u': {
                    unsigned value = 0;

                    ++pos;
                    for (int i = 0; i < 4; ++i) {
                        char ch = pos[i];

                        value <<= 4;
                        if (ch >= '0' && ch <= '9') {
                            value |= (unsigned)(ch - '0');
                        } else if (ch >= 'a' && ch <= 'f') {
                            value |= (unsigned)(ch - 'a' + 10);
                        } else if (ch >= 'A' && ch <= 'F') {
                            value |= (unsigned)(ch - 'A' + 10);
                        } else {
                            buffer[out++] = '?';
                            value = 0;
                            break;
                        }
                    }

                    if (value != 0) {
                        if (value <= 0x7F && out + 1 < bufsize) {
                            buffer[out++] = (char)value;
                        } else if (value <= 0x7FF && out + 2 < bufsize) {
                            buffer[out++] = (char)(0xC0 | (value >> 6));
                            buffer[out++] = (char)(0x80 | (value & 0x3F));
                        } else if (out + 3 < bufsize) {
                            buffer[out++] = (char)(0xE0 | (value >> 12));
                            buffer[out++] = (char)(0x80 | ((value >> 6) & 0x3F));
                            buffer[out++] = (char)(0x80 | (value & 0x3F));
                        }
                    }

                    pos += 4;
                    break;
                }
                default:
                    buffer[out++] = *pos++;
                    break;
            }
        } else {
            buffer[out++] = *pos++;
        }
    }

    buffer[out] = '\0';
    return *pos == '"';
}

static int parse_long_long_after_pattern(const char *start, const char *pattern, long long *out) {
    char *endptr = NULL;
    long long value;
    const char *pos;

    if (!start || !pattern || !out) {
        return 0;
    }

    pos = strstr(start, pattern);
    if (!pos) {
        return 0;
    }

    pos += strlen(pattern);
    errno = 0;
    value = strtoll(pos, &endptr, 10);
    if (errno != 0 || endptr == pos) {
        return 0;
    }

    *out = value;
    return 1;
}

static int json_extract_int_after(const char *start, const char *pattern, int *out) {
    long value;
    long long parsed_value;

    if (!start || !pattern || !out) {
        return 0;
    }

    if (!parse_long_long_after_pattern(start, pattern, &parsed_value)) {
        return 0;
    }

    value = (long)parsed_value;
    if (parsed_value <= 0 || parsed_value > INT_MAX || (long long)value != parsed_value) {
        return 0;
    }

    *out = (int)value;
    return 1;
}

static int json_extract_long_long_after(const char *start, const char *pattern, long long *out) {
    long long value;

    if (!parse_long_long_after_pattern(start, pattern, &value) || value <= 0) {
        return 0;
    }

    *out = value;
    return 1;
}

static int json_extract_error_message(const char *json, char *buffer, size_t bufsize) {
    return json_extract_string_after(json, "\"message\":\"", buffer, bufsize);
}

static int extract_nested_file_id(
    const char *start,
    const char *container_pattern,
    const char *file_pattern,
    int *file_id
) {
    const char *container;

    if (!start || !container_pattern || !file_pattern || !file_id) {
        return 0;
    }

    container = strstr(start, container_pattern);
    if (!container) {
        return 0;
    }

    return json_extract_int_after(container, file_pattern, file_id);
}

static int extract_exact_file_size(const char *start, const char *prefix, int file_id, long long *size) {
    char pattern[128];

    if (!start || !prefix || !size || file_id <= 0) {
        return 0;
    }

    snprintf(pattern, sizeof(pattern), "%s\"@type\":\"file\",\"id\":%d,\"size\":", prefix, file_id);
    if (json_extract_long_long_after(start, pattern, size)) {
        return 1;
    }

    snprintf(pattern, sizeof(pattern), "%s\"@type\":\"file\",\"id\":%d,\"expected_size\":", prefix, file_id);
    if (json_extract_long_long_after(start, pattern, size)) {
        return 1;
    }

    snprintf(pattern, sizeof(pattern), "%s\"id\":%d,\"size\":", prefix, file_id);
    if (json_extract_long_long_after(start, pattern, size)) {
        return 1;
    }

    snprintf(pattern, sizeof(pattern), "%s\"id\":%d,\"expected_size\":", prefix, file_id);
    return json_extract_long_long_after(start, pattern, size);
}

static int extract_size_for_file_id(const char *start, int file_id, long long *size) {
    char marker[64];
    const char *file;

    if (!start || !size || file_id <= 0) {
        return 0;
    }

    snprintf(marker, sizeof(marker), "\"@type\":\"file\",\"id\":%d", file_id);
    file = strstr(start, marker);
    if (!file) {
        return 0;
    }

    return json_extract_long_long_after(file, "\"size\":", size) ||
           json_extract_long_long_after(file, "\"expected_size\":", size);
}

static int extract_message_content_type(const char *update_json, char *buffer, size_t bufsize) {
    const char *content;

    content = strstr(update_json, "\"content\":{");
    if (!content) {
        return 0;
    }

    return json_extract_string_after(content, "\"@type\":\"", buffer, bufsize);
}

static int extract_chat_id(const char *update_json, long long *chat_id) {
    return parse_long_long_after_pattern(update_json, "\"chat_id\":", chat_id);
}

static int extract_message_id(const char *update_json, long long *message_id) {
    const char *pos;
    const char *pattern;

    if (!update_json || !message_id) {
        return 0;
    }

    pos = strstr(update_json, "\"message\":{\"id\":");
    if (pos) {
        pattern = "\"message\":{\"id\":";
    } else {
        pos = strstr(update_json, "\"id\":");
        if (!pos) {
            return 0;
        }
        pattern = "\"id\":";
    }

    return parse_long_long_after_pattern(pos, pattern, message_id);
}

static int extract_old_message_id(const char *update_json, long long *message_id) {
    return parse_long_long_after_pattern(update_json, "\"old_message_id\":", message_id);
}

static int extract_extra(const char *update_json, char *buffer, size_t bufsize) {
    return json_extract_string_after(update_json, "\"@extra\":\"", buffer, bufsize);
}

static const MediaPattern *find_media_pattern(const char *update_json, const char **message_start) {
    for (size_t i = 0; i < media_pattern_count; ++i) {
        const MediaPattern *pattern = &media_patterns[i];
        const char *candidate = strstr(update_json, pattern->message_type);

        if (!candidate) {
            continue;
        }

        if (pattern->mime_substring && !strstr(candidate, pattern->mime_substring)) {
            continue;
        }

        if (message_start) {
            *message_start = candidate;
        }

        return pattern;
    }

    if (message_start) {
        *message_start = NULL;
    }

    return NULL;
}

static int extract_video_file_id(const char *update_json, int *file_id, char *detected_type, size_t detected_type_size) {
    const char *message_start = NULL;
    const MediaPattern *pattern = find_media_pattern(update_json, &message_start);

    if (!pattern || !message_start) {
        return 0;
    }

    if (!(extract_nested_file_id(message_start, pattern->container_pattern, pattern->file_pattern, file_id) ||
          json_extract_int_after(message_start, pattern->nested_file_pattern, file_id) ||
          json_extract_int_after(message_start, pattern->file_pattern, file_id))) {
        return 0;
    }

    if (detected_type && detected_type_size > 0) {
        snprintf(detected_type, detected_type_size, "%s", pattern->message_type);
    }

    return 1;
}

static int extract_message_total_size(const char *update_json, int file_id, long long *size) {
    const char *message_start = NULL;
    const MediaPattern *pattern;

    if (!update_json || !size) {
        return 0;
    }

    pattern = find_media_pattern(update_json, &message_start);
    if (!pattern || !message_start) {
        return 0;
    }

    return extract_size_for_file_id(message_start, file_id, size) ||
           extract_exact_file_size(message_start, pattern->size_prefix, file_id, size);
}

static int extract_update_file_id(const char *update_json, int *file_id) {
    return json_extract_int_after(update_json, "\"file\":{\"id\":", file_id);
}

static int extract_file_size(const char *update_json, long long *size) {
    const char *file = strstr(update_json, "\"file\":{");
    const char *video = strstr(update_json, "\"video\":{");
    const char *document = strstr(update_json, "\"document\":{");
    const char *animation = strstr(update_json, "\"animation\":{");
    const char *video_note = strstr(update_json, "\"video_note\":{");

    if (file) {
        if (json_extract_long_long_after(file, "\"size\":", size) ||
            json_extract_long_long_after(file, "\"expected_size\":", size)) {
            return 1;
        }
    }

    if (video) {
        if (json_extract_long_long_after(video, "\"video\":{\"@type\":\"file\",\"size\":", size) ||
            json_extract_long_long_after(video, "\"video\":{\"size\":", size) ||
            json_extract_long_long_after(video, "\"expected_size\":", size)) {
            return 1;
        }
    }

    if (document) {
        if (json_extract_long_long_after(document, "\"document\":{\"@type\":\"file\",\"size\":", size) ||
            json_extract_long_long_after(document, "\"document\":{\"size\":", size) ||
            json_extract_long_long_after(document, "\"expected_size\":", size)) {
            return 1;
        }
    }

    if (animation) {
        if (json_extract_long_long_after(animation, "\"animation\":{\"@type\":\"file\",\"size\":", size) ||
            json_extract_long_long_after(animation, "\"animation\":{\"size\":", size) ||
            json_extract_long_long_after(animation, "\"expected_size\":", size)) {
            return 1;
        }
    }

    if (video_note) {
        if (json_extract_long_long_after(video_note, "\"video\":{\"@type\":\"file\",\"size\":", size) ||
            json_extract_long_long_after(video_note, "\"video\":{\"size\":", size) ||
            json_extract_long_long_after(video_note, "\"expected_size\":", size)) {
            return 1;
        }
    }

    return json_extract_long_long_after(update_json, "\"size\":", size) ||
           json_extract_long_long_after(update_json, "\"expected_size\":", size);
}

static int extract_downloaded_size(const char *update_json, long long *size) {
    const char *local = strstr(update_json, "\"local\":{");

    if (!local) {
        return 0;
    }

    return json_extract_long_long_after(local, "\"downloaded_size\":", size) ||
           json_extract_long_long_after(local, "\"downloaded_prefix_size\":", size);
}

static int extract_local_file_path(const char *update_json, char *buffer, size_t bufsize) {
    return json_extract_string_after(update_json, "\"local\":{\"path\":\"", buffer, bufsize);
}

static int is_pending(int file_id) {
    return find_pending_download(file_id) != NULL;
}

static void init_pending_download(PendingDownload *pending, int file_id, long long chat_id, time_t started_at) {
    if (!pending) {
        return;
    }

    memset(pending, 0, sizeof(*pending));
    pending->file_id = file_id;
    pending->chat_id = chat_id;
    pending->last_reported_percent = -1;
    pending->started_at = started_at;
}

static void remember_pending(int file_id, long long chat_id) {
    PendingDownload *pending;

    pending = find_pending_download(file_id);
    if (pending) {
        pending->chat_id = chat_id;
        return;
    }

    if (pending_count < MAX_TRACKED_FILES) {
        init_pending_download(&pending_downloads[pending_count], file_id, chat_id, time(NULL));
        ++pending_count;
        return;
    }

    log_error("Se alcanzó el límite de archivos rastreados (%d)", MAX_TRACKED_FILES);
}

static void forget_pending(int file_id) {
    for (int i = 0; i < pending_count; ++i) {
        if (pending_downloads[i].file_id == file_id) {
            pending_downloads[i] = pending_downloads[pending_count - 1];
            memset(&pending_downloads[pending_count - 1], 0, sizeof(pending_downloads[pending_count - 1]));
            --pending_count;
            return;
        }
    }
}

static PendingDownload *remember_pending_and_get(int file_id, long long chat_id) {
    PendingDownload *pending = find_pending_download(file_id);

    if (pending) {
        pending->chat_id = chat_id;
        return pending;
    }

    remember_pending(file_id, chat_id);
    return find_pending_download(file_id);
}

static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

static void bot_send(const char *json_str) {
    if (client && json_str) {
        char *payload = strdup(json_str);

        if (!payload) {
            log_error("No se pudo reservar memoria para enviar una petición a TDLib");
            running = 0;
            return;
        }

        free(last_sent_payload);
        last_sent_payload = payload;
        td_json_client_send(client, last_sent_payload);
    }
}

static void send_text_message(long long chat_id, const char *text, const char *extra) {
    char payload[2048];
    char escaped_text[1024];
    char escaped_extra[128];
    int written;

    if (json_escape_string(text ? text : "", escaped_text, sizeof(escaped_text)) != 0) {
        log_error("No se pudo escapar el texto de sendMessage para chat_id=%lld", chat_id);
        return;
    }

    if (extra && extra[0] != '\0' &&
        json_escape_string(extra, escaped_extra, sizeof(escaped_extra)) != 0) {
        log_error("No se pudo escapar @extra de sendMessage para chat_id=%lld", chat_id);
        return;
    }

    written = snprintf(
        payload,
        sizeof(payload),
        extra && extra[0] != '\0' ?
            "{\"@type\":\"sendMessage\",\"chat_id\":%lld,\"@extra\":\"%s\","
            "\"input_message_content\":{\"@type\":\"inputMessageText\","
            "\"text\":{\"@type\":\"formattedText\",\"text\":\"%s\"},"
            "\"disable_web_page_preview\":true,\"clear_draft\":false}}" :
            "{\"@type\":\"sendMessage\",\"chat_id\":%lld,"
            "\"input_message_content\":{\"@type\":\"inputMessageText\","
            "\"text\":{\"@type\":\"formattedText\",\"text\":\"%s\"},"
            "\"disable_web_page_preview\":true,\"clear_draft\":false}}",
        chat_id,
        escaped_extra,
        escaped_text
    );

    if (written < 0 || (size_t)written >= sizeof(payload)) {
        log_error("No se pudo construir un sendMessage para chat_id=%lld", chat_id);
        return;
    }

    bot_send(payload);
}

static void edit_text_message(long long chat_id, long long message_id, const char *text) {
    char payload[2048];
    char escaped_text[1024];
    int written;

    if (json_escape_string(text ? text : "", escaped_text, sizeof(escaped_text)) != 0) {
        log_error("No se pudo escapar el texto de editMessageText para message_id=%lld", message_id);
        return;
    }

    written = snprintf(
        payload,
        sizeof(payload),
        "{\"@type\":\"editMessageText\",\"chat_id\":%lld,\"message_id\":%lld,"
        "\"input_message_content\":{\"@type\":\"inputMessageText\","
        "\"text\":{\"@type\":\"formattedText\",\"text\":\"%s\"},"
        "\"disable_web_page_preview\":true,\"clear_draft\":false}}",
        chat_id,
        message_id,
        escaped_text
    );

    if (written < 0 || (size_t)written >= sizeof(payload)) {
        log_error("No se pudo construir un editMessageText para message_id=%lld", message_id);
        return;
    }

    bot_send(payload);
}

static void send_tdlib_parameters(int api_id, const char *api_hash, const char *db_dir, const char *files_dir) {
    char params[4096];
    char escaped_api_hash[512];
    char escaped_db_dir[PATH_MAX * 2];
    char escaped_files_dir[PATH_MAX * 2];
    int written;

    if (json_escape_string(api_hash ? api_hash : "", escaped_api_hash, sizeof(escaped_api_hash)) != 0 ||
        json_escape_string(db_dir ? db_dir : "", escaped_db_dir, sizeof(escaped_db_dir)) != 0 ||
        json_escape_string(files_dir ? files_dir : "", escaped_files_dir, sizeof(escaped_files_dir)) != 0) {
        log_error("No se pudieron escapar los parámetros de TDLib");
        running = 0;
        return;
    }

    written = snprintf(
        params,
        sizeof(params),
        "{\"@type\":\"setTdlibParameters\","
        "\"use_test_dc\":false,"
        "\"database_directory\":\"%s\","
        "\"files_directory\":\"%s\","
        "\"use_file_database\":true,"
        "\"use_chat_info_database\":false,"
        "\"use_message_database\":false,"
        "\"use_secret_chats\":false,"
        "\"api_id\":%d,"
        "\"api_hash\":\"%s\","
        "\"system_language_code\":\"es\","
        "\"device_model\":\"tg-client\","
        "\"system_version\":\"Linux\","
        "\"application_version\":\"1.0.0\","
        "\"enable_storage_optimizer\":true,"
        "\"ignore_file_names\":false"
        "}",
        escaped_db_dir,
        escaped_files_dir,
        api_id,
        escaped_api_hash
    );

    if (written < 0 || (size_t)written >= sizeof(params)) {
        log_error("No se pudieron serializar los parámetros de TDLib");
        running = 0;
        return;
    }

    bot_send(params);
}

static void send_database_key(const char *database_key) {
    char payload[1024];
    char escaped_database_key[512];
    int written;

    if (json_escape_string(database_key ? database_key : "", escaped_database_key, sizeof(escaped_database_key)) != 0) {
        log_error("La clave de cifrado de la base de datos es demasiado larga");
        running = 0;
        return;
    }

    written = snprintf(
        payload,
        sizeof(payload),
        "{\"@type\":\"checkDatabaseEncryptionKey\",\"encryption_key\":\"%s\"}",
        escaped_database_key
    );

    if (written < 0 || (size_t)written >= sizeof(payload)) {
        log_error("La clave de cifrado de la base de datos es demasiado larga");
        running = 0;
        return;
    }

    bot_send(payload);
}

static void send_bot_token(const char *bot_token) {
    char auth[1024];
    char escaped_bot_token[512];
    int written;

    if (json_escape_string(bot_token ? bot_token : "", escaped_bot_token, sizeof(escaped_bot_token)) != 0) {
        log_error("El BOT_TOKEN es demasiado largo");
        running = 0;
        return;
    }

    written = snprintf(
        auth,
        sizeof(auth),
        "{\"@type\":\"checkAuthenticationBotToken\",\"token\":\"%s\"}",
        escaped_bot_token
    );

    if (written < 0 || (size_t)written >= sizeof(auth)) {
        log_error("El BOT_TOKEN es demasiado largo");
        running = 0;
        return;
    }

    bot_send(auth);
}

static int build_unique_destination(const char *filename, char *final_path, size_t final_path_size) {
    char base[NAME_MAX];
    char ext[NAME_MAX];
    const char *dot;
    int written;
    int counter = 1;

    if (!filename || !filename[0]) {
        errno = EINVAL;
        return -1;
    }

    dot = strrchr(filename, '.');
    if (dot && dot != filename) {
        size_t base_len = (size_t)(dot - filename);
        size_t ext_len = strlen(dot + 1);

        if (base_len >= sizeof(base) || ext_len >= sizeof(ext)) {
            errno = ENAMETOOLONG;
            return -1;
        }

        memcpy(base, filename, base_len);
        base[base_len] = '\0';
        memcpy(ext, dot + 1, ext_len + 1);
    } else {
        size_t filename_len = strlen(filename);
        if (filename_len >= sizeof(base)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        memcpy(base, filename, filename_len + 1);
        ext[0] = '\0';
    }

    written = snprintf(final_path, final_path_size, "%s/%s", download_dir, filename);
    if (written < 0 || (size_t)written >= final_path_size) {
        errno = ENAMETOOLONG;
        return -1;
    }

    while (access(final_path, F_OK) == 0) {
        if (ext[0] != '\0') {
            written = snprintf(final_path, final_path_size, "%s/%s_%d.%s", download_dir, base, counter, ext);
        } else {
            written = snprintf(final_path, final_path_size, "%s/%s_%d", download_dir, base, counter);
        }

        if (written < 0 || (size_t)written >= final_path_size) {
            errno = ENAMETOOLONG;
            return -1;
        }

        ++counter;
    }

    return 0;
}

static void handle_authorization_state(const char *update_json, int api_id, const char *api_hash, const char *db_dir,
                                       const char *files_dir, const char *bot_token, const char *database_key) {
    if (strstr(update_json, "authorizationStateWaitTdlibParameters")) {
        log_info("TDLib solicita parámetros iniciales");
        send_tdlib_parameters(api_id, api_hash, db_dir, files_dir);
        return;
    }

    if (strstr(update_json, "authorizationStateWaitEncryptionKey")) {
        log_info("TDLib solicita clave de base de datos");
        send_database_key(database_key);
        return;
    }

    if (strstr(update_json, "authorizationStateWaitPhoneNumber")) {
        log_info("TDLib listo para autenticar bot");
        send_bot_token(bot_token);
        return;
    }

    if (strstr(update_json, "authorizationStateReady")) {
        log_info("Bot autenticado. Escuchando mensajes...");
        return;
    }

    if (strstr(update_json, "authorizationStateClosed")) {
        log_info("TDLib cerró la sesión");
        running = 0;
    }
}

static void request_download(int file_id, long long chat_id) {
    char download_cmd[256];
    char extra[64];
    char started_at[32];
    int written;
    PendingDownload *pending;

    written = snprintf(
        download_cmd,
        sizeof(download_cmd),
        "{\"@type\":\"downloadFile\",\"file_id\":%d,\"priority\":%d,\"offset\":0,\"limit\":0,\"synchronous\":false}",
        file_id,
        DOWNLOAD_PRIORITY
    );

    if (written < 0 || (size_t)written >= sizeof(download_cmd)) {
        log_error("No se pudo construir la petición de descarga para file_id=%d", file_id);
        return;
    }

    snprintf(extra, sizeof(extra), "progress_init:%d", file_id);
    send_text_message(chat_id, "Iniciando descarga...", extra);
    bot_send(download_cmd);
    pending = remember_pending_and_get(file_id, chat_id);
    format_time_label(pending ? pending->started_at : time(NULL), started_at, sizeof(started_at));
    log_info("Vídeo detectado. Solicitando descarga para file_id=%d. Inicio=%s", file_id, started_at);
}

static void handle_new_message(const char *update_json) {
    int file_id = 0;
    long long total_size = 0;
    char content_type[64];
    char detected_type[64];
    char preview[4096];
    long long chat_id = 0;

    memset(content_type, 0, sizeof(content_type));
    memset(detected_type, 0, sizeof(detected_type));
    extract_message_content_type(update_json, content_type, sizeof(content_type));
    extract_chat_id(update_json, &chat_id);

    log_info(
        "Mensaje recibido en chat_id=%lld con content_type=%s",
        chat_id,
        content_type[0] ? content_type : "desconocido"
    );

    if (!extract_video_file_id(update_json, &file_id, detected_type, sizeof(detected_type))) {
        if (content_type[0] != '\0') {
            log_info("Mensaje ignorado: tipo %s no descargable por la lógica actual", content_type);
            if (strcmp(content_type, "messageDocument") == 0) {
                snprintf(preview, sizeof(preview), "%.500s", update_json);
                log_info("Vista previa messageDocument: %s", preview);
            }
        } else {
            log_info("Mensaje ignorado: no se pudo determinar un file_id de vídeo");
        }
        return;
    }

    if (is_pending(file_id)) {
        log_info("file_id=%d ya estaba pendiente", file_id);
        return;
    }

    log_info("Archivo detectado como %s con file_id=%d", detected_type, file_id);
    request_download(file_id, chat_id);
    if (extract_message_total_size(update_json, file_id, &total_size)) {
        PendingDownload *pending = find_pending_download(file_id);
        if (pending) {
            pending->total_size = total_size;
        }
        log_info("Tamano total detectado para file_id=%d: %lld bytes", file_id, total_size);
    } else {
        log_info("No se pudo detectar el tamano total para file_id=%d", file_id);
    }
}

static void update_progress_message(const char *update_json, int file_id) {
    PendingDownload *pending;
    char text[256];
    long long downloaded_size = 0;
    long long total_size = 0;
    int percent = 0;
    double downloaded_mb;
    double total_mb;
    time_t now;

    pending = find_pending_download(file_id);
    if (!pending || pending->progress_message_id == 0) {
        return;
    }

    if (!extract_downloaded_size(update_json, &downloaded_size)) {
        return;
    }

    extract_file_size(update_json, &total_size);
    if (total_size <= 0 && pending->total_size > 0) {
        total_size = pending->total_size;
    }
    if (total_size > 0) {
        percent = (int)((downloaded_size * 100LL) / total_size);
        if (percent > 100) {
            percent = 100;
        }
    }

    now = time(NULL);
    if (pending->last_reported_percent >= 0 &&
        percent < 100 &&
        percent < pending->last_reported_percent + PROGRESS_UPDATE_PERCENT_STEP &&
        now != (time_t)-1 &&
        pending->last_progress_update != 0 &&
        now - pending->last_progress_update < PROGRESS_UPDATE_SECONDS) {
        return;
    }

    downloaded_mb = (double)downloaded_size / (1024.0 * 1024.0);
    total_mb = (double)total_size / (1024.0 * 1024.0);

    if (total_size > 0) {
        snprintf(
            text,
            sizeof(text),
            "Descargando... %d%% (%.1f/%.1f MB)",
            percent,
            downloaded_mb,
            total_mb
        );
    } else {
        snprintf(
            text,
            sizeof(text),
            "Descargando... %.1f MB",
            downloaded_mb
        );
    }

    edit_text_message(pending->chat_id, pending->progress_message_id, text);
    pending->last_reported_percent = percent;
    if (now != (time_t)-1) {
        pending->last_progress_update = now;
    }
}

static void handle_progress_message_result(const char *update_json) {
    char extra[64];
    PendingDownload *pending;
    long long message_id = 0;
    int file_id = 0;

    if (!extract_extra(update_json, extra, sizeof(extra))) {
        return;
    }

    if (sscanf(extra, "progress_init:%d", &file_id) != 1) {
        return;
    }

    pending = find_pending_download(file_id);
    if (!pending) {
        return;
    }

    if (!extract_message_id(update_json, &message_id)) {
        return;
    }

    pending->temp_progress_message_id = message_id;
    pending->last_reported_percent = 0;
    pending->last_progress_update = time(NULL);
}

static void handle_message_send_succeeded(const char *update_json) {
    long long old_message_id = 0;
    long long new_message_id = 0;

    if (!strstr(update_json, "\"@type\":\"updateMessageSendSucceeded\"")) {
        return;
    }

    if (!extract_old_message_id(update_json, &old_message_id) ||
        !extract_message_id(update_json, &new_message_id)) {
        return;
    }

    for (int i = 0; i < pending_count; ++i) {
        if (pending_downloads[i].temp_progress_message_id == old_message_id) {
            pending_downloads[i].progress_message_id = new_message_id;
            return;
        }
    }
}

static void handle_file_update(const char *update_json) {
    int file_id = 0;
    PendingDownload *pending;
    char started_at[32];
    char finished_at[32];
    long long duration_seconds = -1;
    char src_path[PATH_MAX];
    char final_path[PATH_MAX];
    const char *filename;
    time_t finished_time;

    if (!extract_update_file_id(update_json, &file_id) || !is_pending(file_id)) {
        return;
    }

    pending = find_pending_download(file_id);
    if (strstr(update_json, "\"is_downloading_completed\":true")) {
        memset(src_path, 0, sizeof(src_path));
        if (!extract_local_file_path(update_json, src_path, sizeof(src_path)) || src_path[0] == '\0') {
            log_error("La descarga del file_id=%d terminó sin ruta local válida", file_id);
            forget_pending(file_id);
            return;
        }

        filename = strrchr(src_path, '/');
        filename = filename ? filename + 1 : src_path;

        if (build_unique_destination(filename, final_path, sizeof(final_path)) != 0) {
            log_error("No se pudo preparar el destino para '%s': %s", filename, strerror(errno));
            return;
        }

        if (rename(src_path, final_path) != 0) {
            log_error("No se pudo mover '%s' a '%s': %s", src_path, final_path, strerror(errno));
            if (pending && pending->progress_message_id != 0) {
                edit_text_message(pending->chat_id, pending->progress_message_id, "La descarga terminó, pero no se pudo mover el archivo.");
            }
            return;
        }

        if (pending && pending->progress_message_id != 0) {
            edit_text_message(pending->chat_id, pending->progress_message_id, "Descarga completada.");
        }

        finished_time = time(NULL);
        format_time_label(pending ? pending->started_at : (time_t)-1, started_at, sizeof(started_at));
        format_time_label(finished_time, finished_at, sizeof(finished_at));
        if (pending && pending->started_at != (time_t)-1) {
            duration_seconds = (long long)(finished_time - pending->started_at);
        }
        forget_pending(file_id);
        log_info(
            "Descarga completada: %s | inicio=%s | fin=%s | duracion=%lld s",
            final_path,
            started_at,
            finished_at,
            duration_seconds
        );
        return;
    }

    update_progress_message(update_json, file_id);
}

static void handle_update(const char *update_json, int api_id, const char *api_hash, const char *db_dir,
                          const char *files_dir, const char *bot_token, const char *database_key) {
    char message[512];
    const char *update_type;

    handle_progress_message_result(update_json);
    handle_message_send_succeeded(update_json);

    update_type = strstr(update_json, "\"@type\":\"");
    if (!update_type) {
        return;
    }

    if (strstr(update_type, "\"@type\":\"error\"") == update_type) {
        memset(message, 0, sizeof(message));
        if (json_extract_error_message(update_json, message, sizeof(message))) {
            log_error("%s", message);
            if (strstr(message, "api_id") || strstr(message, "API_ID")) {
                log_error("Revisa API_ID y API_HASH");
            }
        } else {
            log_error("TDLib devolvió un error no parseable");
        }
        return;
    }

    if (strstr(update_type, "\"@type\":\"updateAuthorizationState\"") == update_type) {
        handle_authorization_state(update_json, api_id, api_hash, db_dir, files_dir, bot_token, database_key);
        return;
    }

    if (strstr(update_type, "\"@type\":\"updateNewMessage\"") == update_type) {
        handle_new_message(update_json);
        return;
    }

    if (strstr(update_type, "\"@type\":\"updateFile\"") == update_type) {
        handle_file_update(update_json);
    }
}

int main(void) {
    const char *api_id_str;
    const char *api_hash;
    const char *bot_token;
    const char *database_key;
    const char *db_dir_env;
    const char *files_dir_env = getenv("TDLIB_FILES");
    const char *download_dir_env;
    char abs_db_dir[PATH_MAX];
    char abs_files_dir[PATH_MAX];
    char abs_download_dir[PATH_MAX];
    char *api_id_end = NULL;
    long parsed_api_id;
    int api_id;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    if (setup_logging() != 0) {
        return 1;
    }

    load_dotenv_file(".env");

    api_id_str = getenv_any("API_ID", "TDLIB_API_ID");
    api_hash = getenv_any("API_HASH", "TDLIB_API_HASH");
    bot_token = getenv("BOT_TOKEN");
    database_key = getenv("TDLIB_ENCRYPTION_KEY");
    db_dir_env = getenv_any("TDLIB_DB", "TDLIB_DB_PATH");
    download_dir_env = getenv_any("DOWNLOAD_DIR", "DOWNLOAD_PATH");
    files_dir_env = getenv("TDLIB_FILES");

    if (!api_id_str || !api_hash || !bot_token) {
        log_error("Faltan credenciales. Define API_ID/API_HASH/BOT_TOKEN o TDLIB_API_ID/TDLIB_API_HASH en .env");
        return 1;
    }

    errno = 0;
    parsed_api_id = strtol(api_id_str, &api_id_end, 10);
    if (errno != 0 || api_id_end == api_id_str || *api_id_end != '\0' || parsed_api_id <= 0 || parsed_api_id > INT_MAX) {
        log_error("API_ID no es válido");
        return 1;
    }
    api_id = (int)parsed_api_id;

    if (!db_dir_env) {
        db_dir_env = DEFAULT_DB_DIR;
    }
    if (!files_dir_env) {
        files_dir_env = DEFAULT_FILES_DIR;
    }
    if (!download_dir_env) {
        download_dir_env = DEFAULT_DOWNLOAD_DIR;
    }

    if (make_absolute_path(db_dir_env, abs_db_dir, sizeof(abs_db_dir)) != 0 ||
        make_absolute_path(files_dir_env, abs_files_dir, sizeof(abs_files_dir)) != 0 ||
        make_absolute_path(download_dir_env, abs_download_dir, sizeof(abs_download_dir)) != 0) {
        log_error("No se pudieron resolver las rutas de trabajo: %s", strerror(errno));
        return 1;
    }

    if (mkdir_p(abs_db_dir) != 0 || mkdir_p(abs_files_dir) != 0 || mkdir_p(abs_download_dir) != 0) {
        log_error("No se pudieron crear los directorios requeridos: %s", strerror(errno));
        return 1;
    }

    if (snprintf(download_dir, sizeof(download_dir), "%s", abs_download_dir) >= (int)sizeof(download_dir)) {
        log_error("La ruta de descarga es demasiado larga");
        return 1;
    }

    log_info("DB: %s", abs_db_dir);
    log_info("Files: %s", abs_files_dir);
    log_info("Download: %s", download_dir);
    log_info("API_ID: %d", api_id);

    client = td_json_client_create();
    if (!client) {
        log_error("No se pudo crear el cliente TDLib");
        return 1;
    }

    log_info("Cliente TDLib creado");
    bot_send("{\"@type\":\"getAuthorizationState\"}");
    log_info("Entrando en bucle de eventos...");

    while (running) {
        const char *result = td_json_client_receive(client, 1.0);
        time_t now = time(NULL);

        if (now != (time_t)-1 && now - last_log_cleanup >= 24 * 60 * 60) {
            cleanup_old_logs(logs_dir, now);
            last_log_cleanup = now;
        }

        if (result) {
            handle_update(result, api_id, api_hash, abs_db_dir, abs_files_dir, bot_token, database_key);
        }
    }

    log_info("Cerrando cliente TDLib...");
    td_json_client_destroy(client);
    client = NULL;
    free(last_sent_payload);
    last_sent_payload = NULL;
    log_line("INFO", "Sesión finalizada");
    if (app_log_file) {
        fclose(app_log_file);
        app_log_file = NULL;
    }
    if (tdlib_log_file) {
        fclose(tdlib_log_file);
        tdlib_log_file = NULL;
    }
    return 0;
}
