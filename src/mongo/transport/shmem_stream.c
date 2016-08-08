
#include "shmem_stream.h"

int _shm_counter = 0;

int shmem_create(char* name, size_t len, int* fd, void** addr) {

    int ec = 0;

    shm_unlink(name);
    int shmem_fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, S_IRWXU | S_IRWXG);
    if (shmem_fd < 0) {
        printf("Error creating shared memory %s: %s\n", name, strerror(errno));
        return SHMEM_ERR_OPEN;
    }
    *fd = shmem_fd;

    if ((ec = ftruncate(shmem_fd, len))) {
        printf("Unable to resize memory to %lu bytes: %s\n", len, strerror(errno));
        return SHMEM_ERR_RESIZE;
    }

    *addr = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, shmem_fd, 0);

    if (*addr == NULL) {
        printf("Unable to mmap memory: %s\n", strerror(errno));
        return SHMEM_ERR_MMAP;
    }
    return SHMEM_OK;
}

int shmem_open(char* name, size_t len, int* fd, void** addr) {
    int shmem_fd = shm_open(name, O_RDWR, S_IRWXU | S_IRWXG);
    if (shmem_fd < 0) {
        printf("Unable to open shared memory %s: %s\n", name, strerror(errno));
        return SHMEM_ERR_OPEN;
    }
    *fd = shmem_fd;
    *addr = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, shmem_fd, 0);

    if (*addr == NULL) {
        printf("Unable to mmap memory: %s\n", strerror(errno));
        return SHMEM_ERR_MMAP;
    }
    return SHMEM_OK;
}

int shmem_stream_listen(char* name, shmem_acceptor_t** acceptor_ptr) {

    int fd;
    int ec;
    if ((ec = shmem_create(name, sizeof(shmem_acceptor_t), &fd, (void**)acceptor_ptr))) {
        return ec;
    }

    shmem_acceptor_t* acceptor = *acceptor_ptr;
    strncpy(acceptor->name, name, SHMEM_MAX_KEY_LEN);

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&acceptor->accept_mutex, &attr);

    pthread_condattr_t condattr;
    pthread_condattr_init(&condattr);
    pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);

    pthread_cond_init(&acceptor->ready_cond, &condattr);
    pthread_cond_init(&acceptor->accept_cond, &condattr);

    pthread_mutex_lock(&acceptor->accept_mutex);
    acceptor->running = true;
    acceptor->client_control[0] = '\0';
    acceptor->server_control[0] = '\0';
    pthread_mutex_unlock(&acceptor->accept_mutex);

    return SHMEM_OK;
}


int shmem_stream_accept(shmem_acceptor_t* acceptor, shmem_stream_t* stream) {

    // Wait for accept_cond
    pthread_mutex_lock(&acceptor->accept_mutex);
    while (strlen(acceptor->client_control) == 0) {
        pthread_cond_wait(&acceptor->accept_cond, &acceptor->accept_mutex);

        /* Exit if the acceptor was shut down */
        if (!acceptor->running) {
        	pthread_mutex_unlock(&acceptor->accept_mutex);
        	return SHMEM_ERR_CLOSED;
        }
    }

    int ec;
    // Open remote control block
    if ((ec = shmem_open(acceptor->client_control,
                   sizeof(shmem_control_t),
                   &stream->dest_fd,
                   (void**)&stream->dest_control))) {
        acceptor->client_control[0] = '\0';
        pthread_mutex_unlock(&acceptor->accept_mutex);
        return ec;
    }

    // Null out for future connections
    acceptor->client_control[0] = '\0';

    // Construct local control block
    char name[SHMEM_MAX_KEY_BYTES];
    sprintf(name, "%s-%d-%d", acceptor->name, getpid(), _shm_counter++);
    if ((ec = shmem_create(name, sizeof(shmem_control_t), &stream->fd, (void**)&stream->control))) {
        printf("Error intializing shared memory for control block\n");
        pthread_mutex_unlock(&acceptor->accept_mutex);
        return ec;
    }
    strncpy(stream->control->name, name, SHMEM_MAX_KEY_LEN);
    strncpy(acceptor->server_control, name, SHMEM_MAX_KEY_LEN);

    shmem_control_t* mem_control = stream->control;

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&mem_control->mutex, &attr);

    pthread_condattr_t condattr;
    pthread_condattr_init(&condattr);
    pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(&mem_control->read_cond, &condattr);
    pthread_cond_init(&mem_control->write_cond, &condattr);

    // Signal to client we are ready
    mem_control->open = true;
    pthread_cond_signal(&acceptor->ready_cond);
    pthread_mutex_unlock(&acceptor->accept_mutex);
    return SHMEM_OK;
}

int shmem_stream_connect(char* server_name, shmem_stream_t* stream) {

    // Open acceptor block
    int fd, ec;
    shmem_acceptor_t* acceptor;
    if ((ec = shmem_open(server_name, sizeof(shmem_acceptor_t), &fd, (void**)&acceptor))) {
        return ec;
    }

    if (!acceptor->running) {
        printf("acceptor control block not ready!");
        return SHMEM_ERR_CLOSED;
    }

    // Take lock, make local control block, set name, signal accept
    pthread_mutex_lock(&acceptor->accept_mutex);

    char name[SHMEM_MAX_KEY_BYTES];
    sprintf(name, "%s-%d-%d", server_name, getpid(), _shm_counter++);
    if ((ec = shmem_create(name, sizeof(shmem_control_t), &stream->fd, (void**)&stream->control))) {
        printf("Error intializing shared memory for control block\n");
        pthread_mutex_unlock(&acceptor->accept_mutex);
        return ec;
    }

    strncpy(acceptor->client_control, name, SHMEM_MAX_KEY_LEN);
    strncpy(stream->control->name, name, SHMEM_MAX_KEY_LEN);

    shmem_control_t* mem_control = stream->control;

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&mem_control->mutex, &attr);

    pthread_condattr_t condattr;
    pthread_condattr_init(&condattr);
    pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(&mem_control->read_cond, &condattr);
    pthread_cond_init(&mem_control->write_cond, &condattr);

    pthread_cond_signal(&acceptor->accept_cond);

    // TODO: add timeout
    while (strlen(acceptor->server_control) == 0) {
        pthread_cond_wait(&acceptor->ready_cond, &acceptor->accept_mutex);
    }

    if ((ec = shmem_open(acceptor->server_control,
                   sizeof(shmem_control_t),
                   &stream->dest_fd,
                   (void**)&stream->dest_control))) {
        pthread_mutex_unlock(&acceptor->accept_mutex);
        acceptor->server_control[0] = '\0';
        return ec;
    }

    mem_control->open = true;
    acceptor->server_control[0] = '\0';
    pthread_mutex_unlock(&acceptor->accept_mutex);

    return SHMEM_OK;
}

int shmem_stream_recv(shmem_stream_t* stream, char* buffer, size_t len) {
    return shmem_stream_control_read(stream->control, buffer, len);
}

int shmem_stream_send(shmem_stream_t* stream, const char* buffer, size_t len) {
    return shmem_stream_control_write(stream->dest_control, buffer, len);
}

int shmem_stream_control_write(shmem_control_t* control, const char* buffer, size_t len) {
    pthread_mutex_lock(&control->mutex);

    size_t bytes_written = 0;
    
    /* While not all data written */
    while (bytes_written < len) {

    	/* Wait until there is space in the buffer */
    	while (control->length == SHMEM_MAX_BUF_LEN ) {
	        pthread_cond_wait(&control->write_cond, &control->mutex);

            if (!control->open) {
	        	pthread_mutex_unlock(&control->mutex);
			printf("Connection has been closed\n");
	        	return SHMEM_ERR_CLOSED;
	        }
    	}

		size_t cursor = control->write_cursor;

		/*	If the read cursor is after the write cursor, the space available for writing
			is the space to the read cursor */
	    size_t write_space;
	    if (control->read_cursor > cursor) {
	    	write_space = control->read_cursor - cursor;
	    } else {
	    	write_space = SHMEM_MAX_BUF_LEN - cursor;
	    }
	    size_t to_write = (write_space < (len - bytes_written)) ? write_space : (len - bytes_written);

	    memcpy(&control->ring_buffer[cursor], &buffer[bytes_written], to_write);
	    bytes_written += to_write;
	    control->length += to_write;
	    cursor = (cursor + to_write) % SHMEM_MAX_BUF_LEN;
	    control->write_cursor = cursor;

    	pthread_cond_signal(&control->read_cond);
    }

    pthread_mutex_unlock(&control->mutex);
    return SHMEM_OK;
}

int shmem_stream_control_read(shmem_control_t* control, char* buffer, size_t len) {
    pthread_mutex_lock(&control->mutex);
    
    /* Read all bytes requested */
    size_t bytes_read = 0;
    while (bytes_read < len) {

    	/* Wait until data is available */
		while ((size_t)control->length == 0) {
	        pthread_cond_wait(&control->read_cond, &control->mutex);

	        if (!control->open) {
	        	pthread_mutex_unlock(&control->mutex);
			printf("Connection has been closed\n");
	        	return SHMEM_ERR_CLOSED;
	        }
	    }
	    size_t cursor = control->read_cursor;
	    size_t read_space;
	    if (cursor < control->write_cursor) {
	    	read_space = control->write_cursor - cursor;
	    } else {
	    	read_space = SHMEM_MAX_BUF_LEN - cursor;
	    }
	    size_t to_read = (read_space < (len - bytes_read)) ? read_space : (len - bytes_read);

	    memcpy(&buffer[bytes_read], &control->ring_buffer[cursor], to_read);
	    bytes_read += to_read;
	    control->length -= to_read;
	    cursor = (cursor + to_read) % SHMEM_MAX_BUF_LEN;
	    control->read_cursor = cursor;

	    pthread_cond_signal(&control->write_cond);
	}
    pthread_mutex_unlock(&control->mutex);
    return SHMEM_OK;
}


int shmem_stream_control_peek(shmem_control_t* control, char** buffer, size_t len) {
    pthread_mutex_lock(&control->mutex);

    while ((size_t)control->length < len) {
        pthread_cond_wait(&control->read_cond, &control->mutex);

        if (!control->open) {
        	pthread_mutex_unlock(&control->mutex);
        	return SHMEM_ERR_CLOSED;
        }
    }
    if (len + control->read_cursor > SHMEM_MAX_BUF_LEN) {
        printf("Oops. Can't read past end of buffer yet. Implement an iovec\n");
        pthread_mutex_unlock(&control->mutex);
        return SHMEM_ERR_BUFFER;
    }
    *buffer = &control->ring_buffer[control->read_cursor];
    pthread_mutex_unlock(&control->mutex);
    return SHMEM_OK;
}

int shmem_stream_control_advance(shmem_control_t* control, size_t len) {
    pthread_mutex_lock(&control->mutex);
    size_t cursor = control->read_cursor + len;
    if (cursor >= SHMEM_MAX_BUF_LEN) {
        cursor -= SHMEM_MAX_BUF_LEN;
    }
    if (cursor > (size_t)control->write_cursor) {
        printf("Oops. Can't advance past write cursor!\n");
        pthread_mutex_unlock(&control->mutex);
        return SHMEM_ERR_BUFFER;
    }
    if (len > (size_t)control->length) {
        printf("can't advance more than length!\n");
        pthread_mutex_unlock(&control->mutex);
        return SHMEM_ERR_BUFFER;
    }
    control->length -= len;
    control->read_cursor = cursor;
    pthread_cond_signal(&control->write_cond);
    pthread_mutex_unlock(&control->mutex);
    return SHMEM_OK;
}

int shmem_stream_shutdown(shmem_acceptor_t* acceptor) {
	pthread_mutex_lock(&acceptor->accept_mutex);
	acceptor->running = false;
	pthread_cond_signal(&acceptor->accept_cond);
	pthread_mutex_unlock(&acceptor->accept_mutex);
    close(acceptor->fd);
    shm_unlink(acceptor->name);
    return SHMEM_OK;
}

int shmem_stream_close(shmem_stream_t* stream) {
	/* Close both local and remote control blocks */
	pthread_mutex_lock(&stream->control->mutex);
	stream->control->open = false;
	pthread_cond_signal(&stream->control->read_cond);
	pthread_cond_signal(&stream->control->write_cond);
	pthread_mutex_unlock(&stream->control->mutex);

	pthread_mutex_lock(&stream->dest_control->mutex);
	stream->dest_control->open = false;
	pthread_cond_signal(&stream->dest_control->read_cond);
	pthread_cond_signal(&stream->dest_control->write_cond);
	pthread_mutex_unlock(&stream->dest_control->mutex);

	/* Close our file descriptors, but remote end should unlink its segment */
    close(stream->fd);
    close(stream->dest_fd);
    shm_unlink(stream->control->name);
    return SHMEM_OK;
}
