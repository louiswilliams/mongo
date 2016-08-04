
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define SHMEM_MAX_KEY_LEN 255
#define SHMEM_MAX_KEY_BYTES SHMEM_MAX_KEY_LEN + 1
#define SHMEM_BUF_GROW_LEN sysconf(_SC_PAGESIZE)
#define SHMEM_CONTROL_LEN sysconf(_SC_PAGESIZE)
#define SHMEM_MIN_BUF_LEN (SHMEM_BUF_GROW_LEN - sizeof(shmem_control_t))
#define SHMEM_MAX_BUF_LEN 4096

#define SHMEM_OK 0
#define SHMEM_ERR_OPEN 1
#define SHMEM_ERR_MMAP 2
#define SHMEM_ERR_RESIZE 3
#define SHMEM_ERR_CLOSED 4
#define SHMEM_ERR_BUFFER 5


// C standard issues with compiling
int ftruncate(int fd, off_t length);

/** Connection establishment using the acceptor:
 Acceptor is identified by 'name'. Client must know the name before opening.  To connect,
 a client takes the lock, sets client_control to the name of its control block.  The client then
 signals accept_cond. The client waits for ready_cond, the server takes the lock, creates its
 control block, sets server_control to the name of it's control block, and signals ready_cond.
 */
typedef struct {
	volatile bool running; // The server is listening to connections
	int fd; // Server's file descriptor 
	char name[SHMEM_MAX_KEY_BYTES]; // Name (client must know) to connect
	pthread_mutex_t accept_mutex; // Mutex protecting this acceptor
	pthread_cond_t accept_cond; // The client is ready to be accepted
	pthread_cond_t ready_cond; // The server is done accepting and ready to talk
	char server_control[SHMEM_MAX_KEY_BYTES]; // The server sets this to a non-empty name when accepting
	char client_control[SHMEM_MAX_KEY_BYTES]; // The client sets this to a non-empty name when connecting
} shmem_acceptor_t;

// Each peer has one control_block per open connection, which acts as its receive buffer.
// Peers write to the control block, and the application reads from it.
typedef struct {
	volatile bool open; // Used by peers to check if the control block has been initialized
	pthread_mutex_t mutex; // Mutex protecting this control block
	pthread_cond_t read_cond; // Bytes available to be read
	pthread_cond_t write_cond; // Space available for writing
	size_t length;	// The amount of bytes in the buffer
	size_t write_cursor; // The position of the next byte to write
    size_t read_cursor;  // The position of the next byte to read
	char name[SHMEM_MAX_KEY_BYTES];
	char ring_buffer[SHMEM_MAX_BUF_LEN];
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
