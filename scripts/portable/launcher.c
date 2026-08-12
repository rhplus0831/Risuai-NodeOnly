/*
 * PocketRisu Windows portable launcher
 * Compiled with: gcc -municode -O2 -o PocketRisu.exe launcher.c launcher.res -lshell32
 * Note: -municode already defines UNICODE and _UNICODE
 */
#include <windows.h>
#include <shellapi.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

static WCHAR *copy_wide_string(const WCHAR *value) {
    size_t size = wcslen(value) + 1;
    WCHAR *copy = (WCHAR *)malloc(size * sizeof(WCHAR));
    if (copy) memcpy(copy, value, size * sizeof(WCHAR));
    return copy;
}

static WCHAR *module_directory(void) {
    DWORD capacity = 512;
    while (capacity <= 32768) {
        WCHAR *buffer = (WCHAR *)malloc((size_t)capacity * sizeof(WCHAR));
        DWORD length;
        if (!buffer) return NULL;
        SetLastError(ERROR_SUCCESS);
        length = GetModuleFileNameW(NULL, buffer, capacity);
        if (length == 0) {
            free(buffer);
            return NULL;
        }
        if (length < capacity - 1
                || (length < capacity && GetLastError() != ERROR_INSUFFICIENT_BUFFER)) {
            WCHAR *separator = wcsrchr(buffer, L'\\');
            if (!separator) {
                free(buffer);
                return NULL;
            }
            *separator = L'\0';
            return buffer;
        }
        free(buffer);
        capacity *= 2;
    }
    SetLastError(ERROR_FILENAME_EXCED_RANGE);
    return NULL;
}

static WCHAR *append_wide(const WCHAR *left, const WCHAR *right) {
    size_t left_length = wcslen(left);
    size_t right_length = wcslen(right);
    WCHAR *result = (WCHAR *)malloc((left_length + right_length + 1) * sizeof(WCHAR));
    if (!result) return NULL;
    memcpy(result, left, left_length * sizeof(WCHAR));
    memcpy(result + left_length, right, (right_length + 1) * sizeof(WCHAR));
    return result;
}

static WCHAR *read_port(void) {
    DWORD required = GetEnvironmentVariableW(L"PORT", NULL, 0);
    WCHAR *port;
    if (required == 0) return copy_wide_string(L"6001");
    port = (WCHAR *)malloc((size_t)required * sizeof(WCHAR));
    if (!port) return NULL;
    if (!GetEnvironmentVariableW(L"PORT", port, required)) {
        free(port);
        return NULL;
    }
    return port;
}

static WCHAR *build_command_line(const WCHAR *node_path, const WCHAR *script_path) {
    size_t size = wcslen(node_path) + wcslen(script_path) + 6;
    WCHAR *command = (WCHAR *)malloc(size * sizeof(WCHAR));
    if (!command) return NULL;
    command[0] = L'"';
    wcscpy(command + 1, node_path);
    wcscat(command, L"\" \"");
    wcscat(command, script_path);
    wcscat(command, L"\"");
    return command;
}

int wmain(void) {
    WCHAR *dir = module_directory();
    WCHAR *port = read_port();
    WCHAR *node_path = NULL;
    WCHAR *script_path = NULL;
    WCHAR *url = NULL;
    WCHAR *command = NULL;
    int result = 1;

    if (!dir || !port) {
        wprintf(L"[Error] Could not resolve the portable application path (error %lu).\n",
                GetLastError());
        goto cleanup;
    }

    SetConsoleTitleW(L"PocketRisu");
    if (!SetCurrentDirectoryW(dir)) {
        wprintf(L"[Error] Could not enter the application directory (error %lu).\n",
                GetLastError());
        goto cleanup;
    }
    SetEnvironmentVariableW(L"PORT", port);

    node_path = append_wide(dir, L"\\bin\\node.exe");
    script_path = append_wide(dir, L"\\server\\node\\server.cjs");
    if (!node_path || !script_path) goto cleanup;
    if (GetFileAttributesW(node_path) == INVALID_FILE_ATTRIBUTES) {
        wprintf(L"[Error] bin\\node.exe not found.\n");
        goto cleanup;
    }

    url = append_wide(L"http://localhost:", port);
    if (url) ShellExecuteW(NULL, L"open", url, NULL, NULL, SW_SHOWNORMAL);

    command = build_command_line(node_path, script_path);
    if (!command) goto cleanup;

    {
        STARTUPINFOW startup_info;
        PROCESS_INFORMATION process_info;
        ZeroMemory(&startup_info, sizeof(startup_info));
        startup_info.cb = sizeof(startup_info);
        ZeroMemory(&process_info, sizeof(process_info));

        if (!CreateProcessW(
                node_path,
                command,
                NULL,
                NULL,
                TRUE,
                0,
                NULL,
                dir,
                &startup_info,
                &process_info)) {
            wprintf(L"[Error] Failed to start server (error %lu).\n", GetLastError());
            goto cleanup;
        }
        WaitForSingleObject(process_info.hProcess, INFINITE);
        CloseHandle(process_info.hProcess);
        CloseHandle(process_info.hThread);
    }

    result = 0;

cleanup:
    free(command);
    free(url);
    free(script_path);
    free(node_path);
    free(port);
    free(dir);
    if (result != 0) system("pause");
    return result;
}
