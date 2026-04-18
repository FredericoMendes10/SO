#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "src/common/io.h"

int freq, fresp, fnotif, freg;

void close_all() {
  close(freq);
  close(fresp);
  close(fnotif);
  close(freg);
}

void unlink_all(char const* req_pipe_path, char const* resp_pipe_path,char const* notif_pipe_path) {
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
}

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* fd_server_pipe_path,
                char const* notif_pipe_path) {
  // create pipes and connect
  if ((freg = open(fd_server_pipe_path, O_WRONLY)) < 0) {
    write_string(STDERR_FILENO, "Failed to open register FIFO: ");
    write_string(STDERR_FILENO, fd_server_pipe_path);
    write_string(STDERR_FILENO, "\n");
  }

  unlink_all(req_pipe_path, resp_pipe_path, notif_pipe_path);

  if (mkfifo(req_pipe_path, 0777) < 0){
    write_string(STDERR_FILENO, "Failed to create request FIFO: ");
    write_string(STDERR_FILENO, req_pipe_path);
    write_string(STDERR_FILENO, "\n");
    close(freg);
    exit(1);
  } 
  if (mkfifo(resp_pipe_path, 0777) < 0){
    write_string(STDERR_FILENO, "Failed to create answer FIFO: ");
    write_string(STDERR_FILENO, resp_pipe_path);
    write_string(STDERR_FILENO, "\n");    
    unlink(req_pipe_path);
    close(freg);
    exit(1);
  }
  if (mkfifo(notif_pipe_path, 0777) < 0){
    write_string(STDERR_FILENO, "Failed to create notification FIFO: ");
    write_string(STDERR_FILENO, notif_pipe_path);
    write_string(STDERR_FILENO, "\n");
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    close(freg);
    exit(1);
  }

  write_all(freg, "1", 1);
  write_all(freg, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  write_all(freg, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  write_all(freg, notif_pipe_path, MAX_PIPE_PATH_LENGTH);

  if ((fresp = open(resp_pipe_path, O_RDONLY)) < 0) {
    write_string(STDERR_FILENO, "Failed to open answer FIFO: ");
    write_string(STDERR_FILENO, resp_pipe_path);
    write_string(STDERR_FILENO, "\n");
    close(freg);
    unlink_all(req_pipe_path, resp_pipe_path, notif_pipe_path);
    exit(1);
  }
  if ((freq = open(req_pipe_path, O_WRONLY)) < 0){
    write_string(STDERR_FILENO, "Failed to open request FIFO: ");
    write_string(STDERR_FILENO, req_pipe_path);
    write_string(STDERR_FILENO, "\n");
    close(freg);
    close(fresp);
    unlink_all(req_pipe_path, resp_pipe_path, notif_pipe_path);
    exit(1);
  }
  if ((fnotif = open(notif_pipe_path, O_RDONLY)) < 0) {
    write_string(STDERR_FILENO, "Failed to open notification FIFO: ");
    write_string(STDERR_FILENO, notif_pipe_path);
    write_string(STDERR_FILENO, "\n");
    close(freg);
    close(freq);
    close(fresp);
    unlink_all(req_pipe_path, resp_pipe_path, notif_pipe_path);
    exit(1);
  }

  char message[2];
  read_all(fresp, message, 2, NULL);
  printf("Server returned %c for operation: connect\n", message[1]);
  if (message[1] == '1'){
    close_all();
    unlink_all(req_pipe_path, resp_pipe_path, notif_pipe_path);
  } 
  return fnotif;
}
 
int kvs_disconnect(char const* req_pipe_path, char const* resp_pipe_path, char const* notif_pipe_path){
  // close pipes and unlink pipe files
  write_all(freq, "2", 1);
  char message[2];
  read_all(fresp, message, 2, NULL);
  printf("Server returned %c for operation: disconnect\n", message[1]);
  close_all();
  unlink_all(req_pipe_path, resp_pipe_path, notif_pipe_path);
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  char buffer[MAX_KEY_SIZE];
  strcpy(buffer, key);
  memset(buffer + strlen(buffer), 0, sizeof(buffer) - strlen(buffer));
  write_all(freq, "3", 1);
  write_all(freq, buffer, MAX_KEY_SIZE);

  char message[2];
  read_all(fresp, message, 2, NULL);
  printf("Server returned %c for operation: subscribe\n", message[1]);
  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe
  char buffer[MAX_KEY_SIZE];
  strcpy(buffer, key);
  memset(buffer + strlen(buffer), 0, sizeof(buffer) - strlen(buffer));
  write_all(freq, "4", 1);
  write_all(freq, buffer, MAX_KEY_SIZE);

  char message[2];
  read_all(fresp, message, 2, NULL);
  printf("Server returned %c for operation: unsubscribe\n", message[1]);
  return 0;
}
