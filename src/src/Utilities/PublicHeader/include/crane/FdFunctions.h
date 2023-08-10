#pragma once

namespace util {

// Close file descriptors within [fd_begin, fd_end)
void CloseFdRange(int fd_begin, int fd_end);

// Close file descriptors from fd_begin to the max fd.
void CloseFdFrom(int fd_begin);

// Set close-on-exec flag on [fd_begin, fd_end).
void SetCloseOnExecOnFdRange(int fd_begin, int fd_end);

// Set close-on-exec flag from fd_begin to the max fd.
void SetCloseOnExecFromFd(int fd_begin);

}  // namespace util