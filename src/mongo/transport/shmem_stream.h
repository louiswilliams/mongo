
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAX_BUFFER_LEN 48 * 1000 * 1000
#define MAX_SHM_KEY_LEN 255

// C standard issues with compiling
int ftruncate(int fd, off_t length);

// Acceptor is identified by 'name'. Client must know the name before opening
// To connect, client takes lock, sets client_control, and signals accept_cond.
// Client waits for ready_cond, server takes lock, creates its control block, sets server_control,
// signals ready_cond. Connection is established.
typedef struct {
	volatile bool ready;
	int fd;
	char name[MAX_SHM_KEY_LEN + 1];
	pthread_mutex_t accept_mutex;
	pthread_cond_t accept_cond;
	pthread_cond_t ready_cond;
	char server_control[MAX_SHM_KEY_LEN + 1];
	char client_control[MAX_SHM_KEY_LEN + 1];
} shmem_acceptor_t;

// Each peer has one control_block per open connection, which acts as its receive buffer.
// Peers write to the control block, and the application reads from it.
typedef struct {
	volatile bool open;
	pthread_mutex_t mutex;
	pthread_cond_t read_cond;
	pthread_cond_t write_cond;
	int length;
	int write_cursor;
    int read_cursor;
	char name[MAX_SHM_KEY_LEN + 1];
	char ring_buffer[MAX_BUFFER_LEN];
} shmem_control_t;

// There is one stream per open connection. There is a local control block for receiving data
// There is a dest control block for sending data to the remote peer.
typedef struct {
	int fd;
	int dest_fd;
	shmem_control_t* control;
	shmem_control_t* dest_control;
} shmem_stream_t;

/* Initialize len bytes of shared memory named 'name'. Sets fd to the file descriptor
   and addr to the address of the segment */
int shmem_create(char* prefix, size_t len, int* fd, void** addr);

/* Opens, but does not initialize a shared memory fragment. */
int shmem_open(char* prefix, size_t len, int* fd, void** addr);

/* Setup acceptor */
int shmem_stream_listen(char* name, shmem_acceptor_t** acceptor);

/* Accept a client connnection on stream */
int shmem_stream_accept(shmem_acceptor_t* acceptor, shmem_stream_t* stream);

/* Attempt to access and create a connection to an acceptor identified by 'name' */
int shmem_stream_connect(char* name, shmem_stream_t* stream);

/* Receive len bytes into buffer from local control block */
int shmem_stream_recv(shmem_stream_t* stream, char* buffer, size_t len);

/* Send len bytes from buffer to remote control block */
int shmem_stream_send(shmem_stream_t* stream, const char* buffer, size_t len);

/* Copy len bytes of buffer into stream */
int shmem_stream_control_write(shmem_control_t* control, const char* buffer, size_t len);

/* Read and copy len bytes into buffer. Advances read cursor */
int shmem_stream_control_read(shmem_control_t* control, char* buffer, size_t len);

/* Points buffer to a region of memory with len bytes of data. Does not adanvce cursor
 * This is useful as a zero-copy mechanism. Must call 'advance' when done with memory */
int shmem_stream_control_peek(shmem_control_t* control, char** buffer, size_t len);

/* Advance cursor len bytes after calling 'peek' */
int shmem_stream_control_advance(shmem_control_t* stream, size_t len);

/* Closes the file descriptor associated with the control block */
int shmem_stream_close(shmem_stream_t*);

/* Call from a server stream to unlink the shared memory segment */
int shmem_stream_shutdown(shmem_acceptor_t*);
