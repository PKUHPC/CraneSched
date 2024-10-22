/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>
#include <uv.h>

#include <cstdio>
#include <cstdlib>

uv_loop_t* loop;

void timer_cb2(uv_timer_t* timer, int status) {
  printf("Timer 2 Status: %d\n", status);
  uv_timer_stop(timer);
  delete timer;
}

void timer_cb1(uv_timer_t* timer, int status) {
  printf("Timer 1 Status: %d\n", status);
  uv_timer_stop(timer);

  auto* timer2 = new uv_timer_t;
  uv_timer_init(loop, timer2);
  uv_timer_start(timer2, (uv_timer_cb)&timer_cb2, 1000, 1000);
}

TEST(Libuv, simple) {
  loop = static_cast<uv_loop_t*>(malloc(sizeof(uv_loop_t)));
  uv_loop_init(loop);

  uv_timer_t timer1;
  uv_timer_init(loop, &timer1);
  uv_timer_start(&timer1, (uv_timer_cb)&timer_cb1, 1000, 1000);

  uv_run(loop, UV_RUN_DEFAULT);
  printf("Now quitting.\n");

  uv_loop_close(loop);
  free(loop);
}