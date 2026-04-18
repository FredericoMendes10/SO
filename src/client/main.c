#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

int fd_notif;
int disconnected = 0;
char req_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/al043req";
char resp_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/al043resp";
char notif_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/al043notif";

// Thread function to handle server notifications
static void* notifications_thread() {
  char opcode[1];
  char buffer[MAX_STRING_SIZE];

  // Continuously read notifications from the server
  while (read(fd_notif, opcode, 1) != 0) {
    switch(opcode[0]){
      case 'w':  // "Write" notification (key-value update)
        read_all(fd_notif, buffer, MAX_STRING_SIZE, NULL);
        printf("(%s,", buffer);
        read_all(fd_notif, buffer, MAX_STRING_SIZE, NULL);
        printf("%s)\n", buffer);
        break;
      case 'd':  // "Delete" notification (key deletion)
        read_all(fd_notif, buffer, MAX_STRING_SIZE, NULL);
        printf("(%s,DELETED)\n", buffer);
        break;
    }
  }

  // If disconnection was not intentional, handle forced disconnection
  if (disconnected == 0){
    close_all();  // Close open file descriptors
    unlink_all(req_pipe_path,resp_pipe_path,notif_pipe_path);  // Remove FIFOs
    printf("Disconnected from server by a SIGUSR1\n");
    exit(0);  // Terminate process
  }
  pthread_exit(NULL);
}


int main(int argc, char* argv[]) {
  // Validate command-line arguments
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }
  
  // Construct the pathname for server communication
  char pathname[MAX_PIPE_PATH_LENGTH] = "/tmp/al043";
  strncat(pathname, argv[2], strlen(argv[2]) * sizeof(char));

  // Append client unique ID to named pipes' paths
  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // Ensure the remaining bytes in the buffer are zeroed
  memset(req_pipe_path + strlen(req_pipe_path), 0, sizeof(req_pipe_path) - strlen(req_pipe_path));
  memset(resp_pipe_path + strlen(resp_pipe_path), 0, sizeof(resp_pipe_path) - strlen(resp_pipe_path));
  memset(notif_pipe_path + strlen(notif_pipe_path), 0, sizeof(notif_pipe_path) - strlen(notif_pipe_path));

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0}; // Store subscribed keys
  unsigned int delay_ms;
  size_t num;
  
  // Attempt to connect to the server
  if ((fd_notif = kvs_connect(req_pipe_path, resp_pipe_path, pathname, notif_pipe_path)) == 1) {
    fprintf(stderr, "Failed to connect to the server\n");
    exit(1);
  }
  
  // Create a separate thread to listen for notifications from the server
  pthread_t notif_thread;
  if (pthread_create(&notif_thread, NULL, notifications_thread, NULL) != 0) {
    fprintf(stderr, "Failed to create notifications thread\n");
    exit(1);
  }

  // Main command-processing loop
  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:  // Handle client disconnection
        disconnected = 1;
        if (kvs_disconnect(req_pipe_path, resp_pipe_path, notif_pipe_path) != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        if (pthread_join(notif_thread, NULL) != 0) {
          fprintf(stderr, "Failed to join notifications thread\n");
        }
        printf("Disconnected from server\n");
        return 0;

      case CMD_SUBSCRIBE:  // Handle subscription request
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
        
        if (kvs_subscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:  // Handle unsubscription request
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
        if (kvs_unsubscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_DELAY:  // Handle delay request
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
          printf("Waiting...\n");
          delay(delay_ms);
        }
        break;

      case CMD_INVALID:  // Invalid command received
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:  // Empty command (ignored)
        break;

      case EOC:  // End of command (exit loop)
        break;
    }
  }
}