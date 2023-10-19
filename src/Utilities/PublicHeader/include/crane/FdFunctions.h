/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

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

bool SetMaxFileDescriptorNumber(unsigned long num);

}  // namespace util