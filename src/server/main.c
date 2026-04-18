#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>

#include "kvs.h"
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"

#include <sys/stat.h>
#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"
#include <semaphore.h>
#include <signal.h>

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

int all_fnotif[MAX_SESSION_COUNT] = {0};   // All notifications FIFO file descriptors
int all_fresp[MAX_SESSION_COUNT] = {0};    // All answer FIFO file descriptors
char buffer_produtor_consumidor[MAX_PIPE_PATH_LENGTH*3];
sem_t full, empty;
pthread_mutex_t semMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;
int freg;                      // File descriptor of register FIFO
int sigusr1_received = 0;      // SIGUSR1 flag

// SIGUSR1 handler
void handle_sigusr1(int sig) {
  (void)sig;
  sigusr1_received = 1; // Change SIGUSR1 flag to 1
} 

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        if (pthread_mutex_lock(&n_current_backups_lock)) {
          fprintf(stderr, "Failed to lock n_current_backups_lock\n");
          return -1;
        }
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        if (pthread_mutex_unlock(&n_current_backups_lock)) {
          fprintf(stderr, "Failed to unlock n_current_backups_lock\n");
          return -1;
        }
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  // Block SIGUSR1 signal to prevent interruptions in the thread 
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if ((in_fd )== -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void* get_client() {
  // Block SIGUSR1 signal to prevent interruptions in the thread
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  while(1){
    // Wait for an available client request
    sem_wait(&full);

    // Lock the mutex to safely access shared resources
    if (pthread_mutex_lock(&semMutex) != 0) {
      fprintf(stderr, "Failed to lock semaphore_mutex\n");
      return NULL;
    }

    // Retrieve the FIFO (named pipe) paths from the shared buffer
    char req_pipe_path[MAX_PIPE_PATH_LENGTH];
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
    strncpy(req_pipe_path, buffer_produtor_consumidor, MAX_PIPE_PATH_LENGTH);
    strncpy(resp_pipe_path, &buffer_produtor_consumidor[MAX_PIPE_PATH_LENGTH], MAX_PIPE_PATH_LENGTH);
    strncpy(notif_pipe_path, &buffer_produtor_consumidor[MAX_PIPE_PATH_LENGTH*2], MAX_PIPE_PATH_LENGTH);

    // Unlock the mutex after accessing shared resources
    if (pthread_mutex_unlock(&semMutex) != 0) {
      fprintf(stderr, "Failed to unlock semaphore_mutex\n");
      return NULL;
    }
    
    int fresp, freq, fnotif;

    // Open the response FIFO (write-only)
    if ((fresp = open(resp_pipe_path, O_WRONLY)) < 0){
      write_str(STDERR_FILENO, "Failed to open answer FIFO: ");
      write_str(STDERR_FILENO, resp_pipe_path);
      write_str(STDERR_FILENO, "\n");
      sem_post(&empty); // Release semaphore slot
      continue;
    } 
    // Open the request FIFO (read-only)
    else if ((freq = open(req_pipe_path, O_RDONLY)) < 0){
      write_str(STDERR_FILENO, "Failed to open request FIFO: ");
      write_str(STDERR_FILENO, req_pipe_path);
      write_str(STDERR_FILENO, "\n");
      write_all(fresp, "11", 2); // Send error code to client
      close(fresp);
      sem_post(&empty); // Release semaphore slot
      continue;
    } 
    // Open the notification FIFO (write-only)
    else if ((fnotif = open(notif_pipe_path, O_WRONLY)) < 0){
      write_str(STDERR_FILENO, "Failed to open notification FIFO: ");
      write_str(STDERR_FILENO, notif_pipe_path);
      write_str(STDERR_FILENO, "\n");
      write_all(fresp, "11", 2); // Send error code to client
      close(fresp);
      close(freq);
      sem_post(&empty); // Release semaphore slot
      continue;
    }
    else write_all(fresp, "10", 2); // Send success code to client

    // Store the notification file descriptor in an available slot
    for(int i=0; i<MAX_SESSION_COUNT; i++){
      if(all_fnotif[i] == 0){
        all_fnotif[i] = fnotif;
        break;
      }
    }

    // Store the response file descriptor in an available slot
    for(int i=0; i<MAX_SESSION_COUNT; i++){
      if(all_fresp[i] == 0){
        all_fresp[i] = fresp;
        break;
      }
    }

    char opcode[1];
    int client_subscriptions = 0;
    char client_keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0}; // Store client subscriptions
    
    // Process client requests
    while (read(freq, opcode, 1) != 0) {
      switch (opcode[0] - '0') {
        case OP_CODE_DISCONNECT: { // Client disconnect request
          // Remove client from the notification list
          for(int i=0; i<MAX_SESSION_COUNT; i++){
            if(all_fnotif[i] == fnotif){
              all_fnotif[i] = 0;
              break;
            }
          }
          // Remove client from the response list
          for(int i=0; i<MAX_SESSION_COUNT; i++){
            if(all_fresp[i] == fresp){
              all_fresp[i] = 0;
              break;
            }
          }
          
          write_all(fresp, "20", 2); // Send disconnect confirmation
          close(fresp);
          close(fnotif);

          // Unsubscribe the client from all subscribed keys
          for(int i = 0; i < MAX_NUMBER_SUB; i++){
            if(!strcmp(client_keys[i], "\0")) continue;
            add_remove_subscription(client_keys[i], fnotif, 1);
          }

          sem_post(&empty); // Indicate an empty slot for a new client
          break;
        }
        case OP_CODE_SUBSCRIBE: { // Client subscription request
          char key[MAX_KEY_SIZE];
          read_all(freq, key, MAX_KEY_SIZE, NULL);
          
          // Validate key and subscription limit
          if(key_in_table(key) == 0 || client_subscriptions >= MAX_NUMBER_SUB) 
            write_all(fresp, "30", 2); // Subscription failure
          else {
            if (has_subscription(key, fnotif) == 1){
              client_subscriptions++;
              add_remove_subscription(key, fnotif, 0); // Add subscription

              // Store subscribed key
              for(int i = 0; i < MAX_NUMBER_SUB; i++){
                if(!strcmp(client_keys[i], "\0")){
                  strcpy(client_keys[i], key);
                  break;
                }
              }
            } 
            write_all(fresp, "31", 2); // Subscription success
          }
          break;
        }
        case OP_CODE_UNSUBSCRIBE: { // Client unsubscription request
          char key[MAX_KEY_SIZE];
          read_all(freq, key, MAX_KEY_SIZE, NULL);

          // Validate unsubscription request
          if(has_subscription(key, fnotif) == 0) {
            client_subscriptions--;
            add_remove_subscription(key, fnotif, 1); // Remove subscription

            // Remove key from client's subscription list
            for(int i = 0; i < MAX_NUMBER_SUB; i++){
              if(!strcmp(client_keys[i], key)){
                strcpy(client_keys[i], "\0");
                break;
              }
            }
            write_all(fresp, "40", 2); // Unsubscription success
          }
          else write_all(fresp, "41", 2); // Unsubscription failure
          break;
        }
      }  
    }
    close(freq); // Close request FIFO
  }
  pthread_exit(NULL); // Exit the thread
}


static void dispatch_threads(DIR* dir) {
    // Allocate memory for worker and client threads.
    pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
    pthread_t* clients_threads = malloc(MAX_SESSION_COUNT * sizeof(pthread_t));
    
    // Initialize semaphores for producer-consumer synchronization.
    sem_init(&full, 0, 0);
    sem_init(&empty, 0, MAX_SESSION_COUNT);
    
    // Initialize the mutex for protecting shared resources.
    pthread_mutex_init(&semMutex, NULL);

    if (threads == NULL) {
        fprintf(stderr, "Failed to allocate memory for threads\n");
        return;
    }

    // Prepare shared data structure for threads.
    struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

    // Create worker threads for processing files.
    for (size_t i = 0; i < max_threads; i++) {
        if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
            fprintf(stderr, "Failed to create thread %zu\n", i);
            if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
                fprintf(stderr, "Failed to destroy directory_mutex\n");
            }
            free(threads);
            return;
        }
    }

    // Create client threads for handling producer-consumer buffer.
    for (size_t i = 0; i < MAX_SESSION_COUNT; i++) {
        if (pthread_create(&clients_threads[i], NULL, get_client, buffer_produtor_consumidor) != 0) {
            fprintf(stderr, "Failed to create client thread %zu\n", i);
            if (pthread_mutex_destroy(&semMutex) != 0) {
                fprintf(stderr, "Failed to destroy semaphore_mutex\n");
            }
            free(clients_threads);
            return;
        }
    }

    char opcode[1];    
    int intr = 0;
    
    // Main loop to read from FIFO and handle requests or signals.
    while (read_all(freg, opcode, 1, &intr) != 0) {
        if (sigusr1_received == 1) { // Handle SIGUSR1 signal.
            for (int i = 0; i < MAX_SESSION_COUNT; i++) {
                if (all_fresp[i] != 0) {
                    close(all_fresp[i]);
                    all_fresp[i] = 0;
                }
                if (all_fnotif[i] != 0) {
                    close(all_fnotif[i]);
                    all_fnotif[i] = 0;
                    sem_post(&empty); // Release a slot in the buffer.
                }
            }
            kvs_clear_all_subscriptions(); // Clear all KVS subscriptions.
            sigusr1_received = 0; // Reset the signal flag.
        } else {
            sem_wait(&empty); // Wait for an empty slot in the buffer.
            if (pthread_mutex_lock(&semMutex) != 0) {
                fprintf(stderr, "Failed to lock semaphore_mutex\n");
                return;
            }
            // Read client data into the producer-consumer buffer.
            read_all(freg, buffer_produtor_consumidor, MAX_PIPE_PATH_LENGTH * 3, NULL);
            if (pthread_mutex_unlock(&semMutex) != 0) {
                fprintf(stderr, "Failed to unlock semaphore_mutex\n");
                return;
            }
            sem_post(&full); // Signal that the buffer has new data.
        }
    }

    // Wait for all worker threads to finish.
    for (unsigned int i = 0; i < max_threads; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            fprintf(stderr, "Failed to join thread %u\n", i);
            if (pthread_mutex_destroy(&thread_data.directory_mutex)) {
                fprintf(stderr, "Failed to destroy directory_mutex\n");
            }
            free(threads);
            return;
        }
    }

    // Wait for all client threads to finish.
    for (unsigned int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (pthread_join(clients_threads[i], NULL) != 0) {
            fprintf(stderr, "Failed to join client thread %u\n", i);
            if (pthread_mutex_destroy(&semMutex) != 0) {
                fprintf(stderr, "Failed to destroy semaphore_mutex\n");
            }
            free(clients_threads);
            return;
        }
    }

    // Clean up resources.
    if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
        fprintf(stderr, "Failed to destroy directory_mutex\n");
    }
    if (pthread_mutex_destroy(&semMutex) != 0) {
        fprintf(stderr, "Failed to destroy semaphore_mutex\n");
    }
    if (sem_destroy(&full) != 0) {
        fprintf(stderr, "Failed to destroy full semaphore\n");
    }
    if (sem_destroy(&empty) != 0) {
        fprintf(stderr, "Failed to destroy empty semaphore\n");
    }

    // Free allocated memory.
    free(clients_threads);
    free(threads);
}


int main(int argc, char** argv) {
    struct sigaction sa;
    sa.sa_handler = &handle_sigusr1; // Set the signal handler for SIGUSR1.
    sigemptyset(&sa.sa_mask); // Initialize the signal mask to empty.
    sa.sa_flags = 0; // No special flags are set for signal handling.
    if (sigaction(SIGUSR1, &sa, NULL) == -1) { // Register the signal handler.
        perror("sigaction"); // Print an error message if sigaction fails.
        exit(EXIT_FAILURE); // Exit with failure status.
    }

    if (argc != 5) { // Check if the correct number of arguments is provided.
        write_str(STDERR_FILENO, "Usage: ");
        write_str(STDERR_FILENO, argv[0]); // Program name.
        write_str(STDERR_FILENO, " <jobs_dir>");
        write_str(STDERR_FILENO, " <max_threads>");
        write_str(STDERR_FILENO, " <max_backups>");
        write_str(STDERR_FILENO, " <name_of_register_fifo> \n");
        return 1; // Exit with error if arguments are invalid.
    }

    jobs_directory = argv[1]; // Directory where jobs are stored.

    char pathname[MAX_PIPE_PATH_LENGTH];
    snprintf(pathname, sizeof(pathname), "/tmp/al043%s", argv[4]); // Construct the FIFO path.

    char* endptr;
    max_backups = strtoul(argv[3], &endptr, 10); // Parse max_backups as an unsigned long.

    if (*endptr != '\0') { // Check for invalid input for max_backups.
        fprintf(stderr, "Invalid max_proc value\n");
        return 1;
    }

    max_threads = strtoul(argv[2], &endptr, 10); // Parse max_threads as an unsigned long.

    if (*endptr != '\0') { // Check for invalid input for max_threads.
        fprintf(stderr, "Invalid max_threads value\n");
        return 1;
    }

    if (max_backups <= 0) { // Ensure max_backups is a positive value.
        write_str(STDERR_FILENO, "Invalid number of backups\n");
        return 0;
    }

    if (max_threads <= 0) { // Ensure max_threads is a positive value.
        write_str(STDERR_FILENO, "Invalid number of threads\n");
        return 0;
    }

    if (kvs_init()) { // Initialize the key-value store (KVS).
        write_str(STDERR_FILENO, "Failed to initialize KVS\n");
        return 1;
    }

    unlink(pathname); // Remove any existing FIFO with the same path.
    if (mkfifo(pathname, 0777) < 0) { // Create a named pipe (FIFO).
        write_str(STDERR_FILENO, "Failed to create register FIFO: ");
        write_str(STDERR_FILENO, pathname);
        write_str(STDERR_FILENO, "\n");
        exit(1);
    }
    if ((freg = open(pathname, O_RDWR)) < 0) { // Open the FIFO for reading and writing.
        write_str(STDERR_FILENO, "Failed to open register FIFO: ");
        write_str(STDERR_FILENO, pathname);
        write_str(STDERR_FILENO, "\n");
        exit(1);
    }

    DIR* dir = opendir(argv[1]); // Open the jobs directory.
    if (dir == NULL) { // Check if the directory was opened successfully.
        fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
        return 0;
    }

    dispatch_threads(dir); // Start processing jobs using threads.

    if (closedir(dir) == -1) { // Close the directory and check for errors.
        fprintf(stderr, "Failed to close directory\n");
        return 0;
    }

    while (active_backups > 0) { // Wait for all active backups to finish.
        wait(NULL); // Wait for a child process to terminate.
        active_backups--;
    }

    close(freg); // Close the FIFO file descriptor.
    unlink(pathname); // Remove the FIFO file.

    kvs_terminate(); // Terminate the key-value store.

    return 0; // Exit successfully.
}
