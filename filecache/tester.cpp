#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

const char* msgs[5] = {"Hello from writer 0\n", "Hello from writer 1\n",
                       "Hello from writer 2\n", "Hello from writer 3\n",
                       "Hello from writer 4\n"};
void check(void) {
  if (errno != 0) {
    printf("errno != 0\n");
    exit(1);
  }
  errno = 0;
}

ssize_t full_read(int fd, char* buf) {
  ssize_t reads = 0;
  ssize_t this_read;
  while ((this_read = read(fd, buf + reads, 1024)) > 0) {
    reads += this_read;
  }
  return reads;
}

void wait_prompt(char* next_command) {
  printf("Press Enter to continue. Next command is: %s\n", next_command);
  std::cin.ignore();
}

/**
    Test1:
    @assume server side has 1mb.txt
    proxy client reads twice, first should from server fetch, second should from
    cache
*/
void test_1() {
  errno = 0;
  char buf[1024 * 1024 + 1] = {0};
  int read_fd1 = open("1mb.txt", O_RDONLY);
  check();
  ssize_t reads = full_read(read_fd1, buf);
  check();
  printf("1st reads = %ld bytes\n", reads);
  close(read_fd1);

  // should from cache directly
  memset(buf, 0, sizeof(buf));
  int read_fd2 = open("1mb.txt", O_RDONLY);
  check();
  reads = full_read(read_fd2, buf);
  check();
  printf("2nd reads = %ld bytes\n", reads);
  close(read_fd2);
}

/**
    Test2:
    @assume none
    proxy client creates and write new file on server
    read it back and add something to it.
*/
void test_2() {
  errno = 0;
  char buf[1024 * 1024 + 1] = {0};
  const char* msg = "hello from client";
  int write_fd = open("hello.txt", O_WRONLY | O_CREAT, S_IRWXU);
  check();
  write(write_fd, msg, strlen(msg));
  check();
  close(write_fd);

  // should read back 'hello from client'
  int fd = open("hello.txt", O_RDWR);
  check();
  ssize_t reads = full_read(fd, buf);
  printf("read back content=%s\n", buf);
  memset(buf, 0, sizeof(buf));

  // append a sentence to the end of that file
  const char* msg_ = "\nhello again";
  lseek(fd, 0, SEEK_END);
  check();
  write(fd, msg_, strlen(msg_));
  check();
  close(fd);

  // read the just-written content back
  fd = open("hello.txt", O_RDONLY);
  check();
  reads = full_read(fd, buf);
  printf("after append, read back content=%s\n", buf);
  close(fd);
}

/**
    Test3:
    @assume server side does not have 'no.txt', does have 'yes.txt'
    proxy client want to read an non-existing or create existing should all fail
*/
void test_3() {
  errno = 0;
  int read_fd = open("no.txt", O_RDONLY);
  printf("open non-existing no.txt should fail. fd=%d errno=%d\n", read_fd,
         errno);
  errno = 0;

  int write_fd = open("yes.txt", O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
  printf("open-create existing yes.txt should fail. fd=%d errno=%d\n", write_fd,
         errno);
  close(read_fd);
  close(write_fd);
}

/**
    Test4:
    @assume server side have 'subdir' directory
    proxy client should be able to open existing directory
*/
void test_4() {
  errno = 0;
  int read_fd = open("subdir", O_RDONLY);
  check();
  close(read_fd);
}

/**
    Test5:
    @assume server side have base version file of "base.txt"
    proxy client should be able to see session-semantics with server
 */
void test_5() {
  errno = 0;
  char buf[1024 * 1024 + 1] = {0};
  // open base.txt should download base.txt from server
  int read_fd = open("base.txt", O_RDONLY);
  check();

  int write_fd = open("base.txt", O_RDWR);
  check();
  lseek(write_fd, 0, SEEK_END);
  const char* append_msg = "from writer 1\n";
  ssize_t writes = write(write_fd, append_msg, strlen(append_msg));
  check();
  printf("Writer1 appends %ld bytes into file\n", writes);

  int write_fd2 = open("base.txt", O_RDWR);
  check();
  lseek(write_fd2, 0, SEEK_END);
  const char* append_msg2 = "from writer 2\n";
  writes = write(write_fd2, append_msg2, strlen(append_msg2));
  check();
  printf("Writer2 appends %ld bytes into file\n", writes);

  close(write_fd2);
  close(write_fd);

  // should not read the append content
  ssize_t reads = full_read(read_fd, buf);
  check();
  printf("Old Reader reads %ld bytes of content=%s\n", reads, buf);
  close(read_fd);
  memset(buf, 0, sizeof(buf));

  // new read should read content from write_fd, since write_fd2 should be
  // overwritten
  read_fd = open("base.txt", O_RDONLY);
  check();
  reads = full_read(read_fd, buf);
  check();
  printf("New Reader reads %ld bytes of content=%s\n", reads, buf);
  close(read_fd);
}

/**
    Test: Concurrent Proxy interaction
    @assume none
    launch two clients connected with two different proxy

 */
void test_concurrent_proxy(int id) {
  printf("Welcome. I am client %d\n", id);

  char buf[1024 * 1024 + 1] = {0};

  wait_prompt("open(\"concurrent.txt\", O_RDWR)");
  int read_fd = open("concurrent.txt", O_RDWR);
  check();

  wait_prompt("full_read(read_fd, buf)");
  ssize_t reads = full_read(read_fd, buf);
  printf("client %d reads content=%s\n", id, buf);

  wait_prompt("close(read_fd)");
  close(read_fd);

  wait_prompt("open(\"concurrent.txt\", O_RDWR)");
  int write_fd = open("concurrent.txt", O_RDWR);
  check();

  memset(buf, 0, sizeof(buf));
  sprintf(buf, "client %d writes dominate\n", id);

  wait_prompt("write(write_fd, buf, strlen(buf))");
  ssize_t writes = write(write_fd, buf, strlen(buf));

  wait_prompt("close(write_fd)");
  close(write_fd);

  wait_prompt("open(\"concurrent.txt\", O_RDWR)");
  read_fd = open("concurrent.txt", O_RDWR);
  check();

  wait_prompt("full_read(read_fd, buf)");
  reads = full_read(read_fd, buf);
  printf("client %d reads content=%s\n", id, buf);

  exit(0);
}

/**
    @ test basic lru workflow when not overflow
    @ pre: server side has A.txt B.txt C.txt
*/
void test_lru_0() {
  int fd_A = open("A.txt", O_RDONLY);
  close(fd_A);

  int fd_B = open("B.txt", O_RDONLY);
  close(fd_B);

  int fd_C = open("C.txt", O_RDONLY);
  close(fd_C);
}

/**
    @ test basic lru workflow when a file is unlinked
    @ pre: server side has A.txt
*/
void test_lru_1() {
  int fd_A = open("A.txt", O_RDONLY);
  close(fd_A);

  unlink("A.txt");
}

/**
    @ test basic lru workflow when a writer version covers reader version
      and only reserve space in write when going out of original file length
   boundary
    @ pre: server side has A.txt
*/
void test_lru_3() {
  int fd_write = open("A.txt", O_WRONLY);
  // since initial pointer is at 0, this write should not reserve space
  write(fd_write, "abcdefg", 7);
  // this should reserve 4 bytes
  write(fd_write, "abcdefg", 7);
  close(fd_write);
}

/**
    @ test basic lru workflow when multiple reader and one writer interact on
   one single file
    @ pre: server side has A.txt
*/
void test_lru_4() {
  char buf[20] = {0};
  int fd_write = open("A.txt", O_WRONLY);
  int fd_read = open("A.txt", O_RDONLY);
  write(fd_write, "abcdefghijkl", 12);
  read(fd_read, buf, 20);
  printf("original read=%s\n", buf);
  memset(buf, 0, 20);
  close(fd_write);
  close(fd_read);

  int fd_2nd_read = open("A.txt", O_RDONLY);
  read(fd_2nd_read, buf, 20);
  printf("second read=%s\n", buf);
  close(fd_2nd_read);
}

/*
    @ piaza: Test: Basic LRU operation (5.5meg cache, 1 meg files)
 */
void test_lru_5() {
  int fd_a, fd_b, fd_c, fd_d, fd_e, fd_f, fd_g;
  fd_a = open("A.txt", O_RDONLY);
  close(fd_a);
  fd_b = open("B.txt", O_RDONLY);
  close(fd_b);
  fd_c = open("C.txt", O_RDONLY);
  close(fd_c);

  wait_prompt("cache should be .A1.B1.C1 at this point");

  fd_b = open("B.txt", O_RDONLY);
  close(fd_b);
  fd_d = open("D.txt", O_RDONLY);
  close(fd_d);
  fd_e = open("E.txt", O_RDONLY);
  close(fd_e);
  fd_b = open("B.txt", O_RDONLY);
  close(fd_b);
  wait_prompt("cache should be .A1.B1.C1.D1.E1 at this point");

  fd_f = open("F.txt", O_RDONLY);
  close(fd_f);
  fd_g = open("G.txt", O_RDONLY);
  close(fd_g);
  wait_prompt("cache should be .B1.D1.E1.F1.G1 at this point");

  wait_prompt("Go and Modify A.txt and F.txt on the server");

  fd_f = open("F.txt", O_RDONLY);
  close(fd_f);
  fd_a = open("A.txt", O_RDONLY);
  close(fd_a);
  fd_c = open("C.txt", O_RDONLY);
  close(fd_c);

  wait_prompt("cache should be .A .B .E .F .G at this point");
}

/*
    @ piaza: Test: Advanced LRU operation (5.5meg cache, 1 meg files)
 */
void test_lru_6() {
  int fd_a, fd_b, fd_c, fd_d, fd_e, fd_f, fd_g, fd_h;
  fd_a = open("A.txt", O_RDONLY);
  wait_prompt("slow read of A happening. cache should be .A at this point");

  fd_b = open("B.txt", O_RDONLY);
  close(fd_b);
  fd_c = open("C.txt", O_RDONLY);
  close(fd_c);
  fd_d = open("D.txt", O_RDONLY);
  close(fd_d);
  fd_e = open("E.txt", O_RDONLY);
  close(fd_e);
  fd_f = open("F.txt", O_RDONLY);
  close(fd_f);
  fd_g = open("G.txt", O_RDONLY);
  close(fd_g);
  fd_h = open("H.txt", O_RDONLY);
  close(fd_h);
  wait_prompt(
      "After read B C D E F G H. cache should be .A .E .F .G .H at this point");

  fd_g = open("G.txt", O_WRONLY);
  fd_h = open("H.txt", O_WRONLY);
  wait_prompt(
      "slow write of G, H happening. cache should be .A .G .Gx .H .Hx at this "
      "point");

  close(fd_g);
  close(fd_h);
  fd_g = open("G.txt", O_RDONLY);
  close(fd_g);
  fd_h = open("H.txt", O_RDONLY);
  close(fd_h);
  wait_prompt(
      "slow write of G, H finishes and they are read again. cache should be .A "
      ".G .H at this point");

  close(fd_a);
  fd_c = open("C.txt", O_RDONLY);
  close(fd_c);
  fd_d = open("D.txt", O_RDONLY);
  close(fd_d);
  fd_e = open("E.txt", O_RDONLY);
  close(fd_e);
  wait_prompt(
      "slow read of A finishes and READ C D E. cache should be .C .D .E .G .H "
      "at this point");
}

void directory_test0() {
  int fd = open("ctest1", O_RDWR);
  write(fd, "abcdefgh", 8);
  int fd2 = open("ctest1", O_RDONLY);
  close(fd2);
  close(fd);
}

int main(int argc, char* argv[]) {
  directory_test0();

  exit(0);
  int fd_0 = open(argv[1], O_RDWR);
  // write some junk to the end and try upload to server
  lseek(fd_0, 0, SEEK_END);  // to the end
  const char* msg = "hello from client";
  write(fd_0, msg, strlen(msg));
  close(fd_0);
  exit(0);
  char buf[100] = {0};
  errno = 0;
  int fd1 = open(argv[1], O_RDONLY);

  check();
  int fd2 = open(argv[1], O_RDONLY);
  check();
  int fd3 = open(argv[1], O_RDONLY);
  check();
  int fd4 = open(argv[1], O_RDONLY);
  check();
  ssize_t readed;
  memset(buf, 0, sizeof(buf));
  readed = full_read(fd1, buf);
  printf("read content=%s\n", buf);

  memset(buf, 0, sizeof(buf));
  readed = full_read(fd2, buf);
  printf("read content=%s\n", buf);

  memset(buf, 0, sizeof(buf));
  readed = full_read(fd3, buf);
  printf("read content=%s\n", buf);

  memset(buf, 0, sizeof(buf));
  readed = full_read(fd4, buf);
  printf("read content=%s\n", buf);

  close(fd1);
  close(fd2);
  close(fd3);
  close(fd4);
  exit(0);
  //  int id = atoi(argv[1]);
  //  test_concurrent_proxy(id);
  exit(0);
  errno = 0;

  int fd_1 = open(argv[1], O_RDONLY);
  if (fd_1 == -1) {
    printf("failed 1st to open; fd_1=%d, errno=%d\n", fd_1, errno);
    exit(0);
  } else {
    printf("success 1st to open: fd_1=%d, errno=%d\n", fd_1, errno);
  }
  errno = 0;

  int fd_2 = open(argv[1], O_WRONLY, S_IRWXU);
  if (fd_2 == -1) {
    printf("failed 2nd to open; fd_2=%d, errno=%d\n", fd_2, errno);
    exit(0);
  } else {
    printf("success 2nd to open: fd_2=%d, errno=%d\n", fd_2, errno);
  }

  errno = 0;
  ssize_t writes = write(fd_2, msgs[0], strlen(msgs[0]));
  if (writes >= 0) {
    printf("succeeds on 3rd to write; writes=%d, errno=%d\n", writes, errno);
  } else {
    printf("failed 3rd to write; writes=%d, errno=%d\n", writes, errno);
    exit(0);
  }

  // has not yet close, reader should read original text
  ssize_t reads = read(fd_1, buf, 100);
  if (reads > 0) {
    printf("succeeds on 1st read; reads=%d, errno=%d, content=%s\n", reads,
           errno, buf);
  } else {
    printf("fails on 1st read; reads=%d, errno=%d", reads, errno, buf);
  }
  memset(buf, 0, 100);
  close(fd_1);
  close(fd_2);

  errno = 0;
  int fd_3 = open(argv[1], O_RDONLY);
  if (fd_3 == -1) {
    printf("failed 3rd to open; fd_3=%d, errno=%d\n", fd_3, errno);
    exit(0);
  } else {
    printf("success 3rd to open: fd_3=%d, errno=%d\n", fd_3, errno);
  }

  int fd_4 = open(argv[1], O_WRONLY);
  if (fd_4 == -1) {
    printf("failed 4th to open; fd_4=%d, errno=%d\n", fd_4, errno);
    exit(0);
  } else {
    printf("success 4th to open: fd_4=%d, errno=%d\n", fd_4, errno);
  }
  errno = 0;

  writes = write(fd_4, msgs[1], strlen(msgs[1]));
  if (writes >= 0) {
    printf("succeeds on 4th to write; writes=%d, errno=%d\n", writes, errno);
  } else {
    printf("failed 4th to write; writes=%d, errno=%d\n", writes, errno);
    exit(0);
  }

  // fd_2 has closed writing , reader should read updated text by fd_2 but not
  // fd_4
  reads = read(fd_3, buf, 100);
  if (reads > 0) {
    printf("succeeds on 2nd read; reads=%d, errno=%d, content=%s\n", reads,
           errno, buf);
  } else {
    printf("fails on 2nd read; reads=%d, errno=%d", reads, errno, buf);
  }

  close(fd_3);
  close(fd_4);

  // fd_5 should be able to read what fd_4 writes
  int fd_5 = open(argv[1], O_RDWR, S_IRWXU);
  if (fd_5 == -1) {
    printf("failed 5th to open; fd_5=%d, errno=%d\n", fd_5, errno);
    exit(0);
  } else {
    printf("success 5th to open: fd_5=%d, errno=%d\n", fd_5, errno);
  }
  errno = 0;
  memset(buf, 0, 100);
  reads = read(fd_5, buf, 100);
  if (reads > 0) {
    printf("succeeds on 5th read; reads=%d, errno=%d, content=%s\n", reads,
           errno, buf);
  } else {
    printf("fails on 5th read; reads=%d, errno=%d", reads, errno, buf);
  }
  close(fd_5);
}