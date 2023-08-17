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