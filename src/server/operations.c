#include "operations.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "kvs.h"
#include "src/common/io.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void kvs_clear_all_subscriptions() {
  // Percorrer a hashtable e remover subscrições
  if (pthread_rwlock_wrlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to lock tablelock\n");
    exit(1);
  }
  for(int j = 0; j < TABLE_SIZE; j++) {
    KeyNode *keyNode = kvs_table->table[j];
    while (keyNode != NULL) {
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (keyNode->sub[i] != 0) {
          keyNode->sub[i] = 0;
        }
      }
      keyNode = keyNode->next;
    }
  }
  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
}

int has_subscription(const char *key, int fd) {
  int index = hash(key);
  if (pthread_rwlock_rdlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to lock tablelock\n");
    exit(1);
  }
  KeyNode *keyNode = kvs_table->table[index];
  while (keyNode != NULL) {
    if (!strcmp(keyNode->key, key)) {
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (keyNode->sub[i] == fd) {
          if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
            fprintf(stderr, "Failed to unlock tablelock\n");
            exit(1);
          }
          return 0;
        }
      }
      if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
        fprintf(stderr, "Failed to unlock tablelock\n");
        exit(1);
      }
      return 1;
    }
    keyNode = keyNode->next;
  }
  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  return 1;
}

int key_in_table(const char *key) {
  int index = hash(key);
  if (pthread_rwlock_rdlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to lock tablelock\n");
    exit(1);
  }
  KeyNode *keyNode = kvs_table->table[index];
  while (keyNode != NULL) {
    if (!strcmp(keyNode->key, key)) {
      if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
        fprintf(stderr, "Failed to unlock tablelock\n");
        exit(1);
      }
      return 1;
    }
    keyNode = keyNode->next;
  }
  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  return 0;
}

int add_remove_subscription(const char *key, int fd, int add_remove) {
  int index = hash(key);
  if (pthread_rwlock_wrlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to lock tablelock\n");
    exit(1);
  }
  KeyNode *keyNode = kvs_table->table[index];
  while (keyNode != NULL) {
    if (!strcmp(keyNode->key, key)) {
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (add_remove == 0){
          if (keyNode->sub[i] == 0) {
            keyNode->sub[i] =  fd;
            if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
              fprintf(stderr, "Failed to unlock tablelock\n");
              exit(1);
            }
            return 0;
          }
        }
        else if (add_remove == 1){
          if (keyNode->sub[i] == fd) {
            keyNode->sub[i] =  0;
            if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
              fprintf(stderr, "Failed to unlock tablelock\n");
              exit(1);
            }
            return 0;
          }
        }
        else {
          if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
            fprintf(stderr, "Failed to unlock tablelock\n");
            exit(1);
          }
          return 1;  
        }
      }
      if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
        fprintf(stderr, "Failed to unlock tablelock\n");
        exit(1);
      }
      return 1;
    }
    keyNode = keyNode->next;
  }
  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  return 1;
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  if (pthread_rwlock_wrlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to lock tablelock\n");
    exit(1);
  }
  char buffer[MAX_STRING_SIZE];
  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
    }
    else{
      KeyNode *keyNode = kvs_table->table[hash(keys[i])];
      while (keyNode != NULL) {
        if (!strcmp(keyNode->key, keys[i])) {
          for (int j = 0; j < MAX_SESSION_COUNT; j++){
            if (keyNode->sub[j] != 0){
              write_all(keyNode->sub[j], "w", 1);
              strcpy(buffer, keys[i]);
              memset(buffer + strlen(keys[i]), 0, sizeof(buffer) - strlen(keys[i]));
              write_all(keyNode->sub[j], buffer, MAX_STRING_SIZE);
              strcpy(buffer, values[i]);
              memset(buffer + strlen(values[i]), 0, sizeof(buffer) - strlen(values[i]));
              write_all(keyNode->sub[j], buffer, MAX_STRING_SIZE);  
            }
          }
        }
        keyNode = keyNode->next;
      }
    }
  }

  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");

  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  if (pthread_rwlock_wrlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to lock tablelock\n");
    exit(1);
  }
  char buffer[MAX_STRING_SIZE];
  int aux = 0;
  
  for (size_t i = 0; i < num_pairs; i++) {
    KeyNode *keyNode = kvs_table->table[hash(keys[i])];
    while (keyNode != NULL) {
      if (!strcmp(keyNode->key, keys[i])) {
        for (int j = 0; j < MAX_SESSION_COUNT; j++){
          if (keyNode->sub[j] != 0){
            write_all(keyNode->sub[j], "d", 1);
            strcpy(buffer, keys[i]);
            memset(buffer + strlen(keys[i]), 0, sizeof(buffer) - strlen(keys[i]));
            write_all(keyNode->sub[j], buffer, MAX_STRING_SIZE);
          }
        }
      }
      keyNode = keyNode->next;
    }
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
    }
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key,
               keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
}

int kvs_backup(size_t num_backup, char *job_filename, char *directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory,
           strtok(job_filename, "."), num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  if (pthread_rwlock_unlock(&kvs_table->tablelock) != 0) {
    fprintf(stderr, "Failed to unlock tablelock\n");
    exit(1);
  }
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->key,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ", ",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->value,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ")\n",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}
