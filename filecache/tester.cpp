#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

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
int main(int argc, char* argv[]) {
  // T1
  errno = 0;
  int p1_f = open("F.txt", O_RDWR);
  check();
  int p1_g = open("G.txt", O_RDWR);
  check();
  int p2_g = open("G.txt", O_RDWR);
  check();
  write(p1_g, "hello", 5);
  check();
  close(p1_g);
  check();
  write(p1_f, "15440/640", 9);
  check();
  close(p2_g);
  check();
  p2_g = open("G.txt", O_RDWR);
  check();
  p1_g = open("G.txt", O_RDWR);
  check();
  write(p2_g, "hello world", 11);
  close(p2_g);
  close(p1_g);
  exit(1);
  //  errno = 0;
  //  int me = open(argv[1], O_WRONLY);
  //  printf("me open fd=%d errno=%d\n", me, errno);
  //  errno = 0;
  //  int res = unlink(argv[1]);
  //  printf("unlink=%d errno=%d\n", res, errno);
  //  errno = 0;
  //  write(me, "hello from me", strlen("hello from me"));
  //  int han = open(argv[1], O_RDONLY);
  //  printf("han open fd=%d errno=%d\n", han, errno);
  //  errno = 0;
  //  close(me);
  //  int leo = open(argv[1], O_RDONLY);
  //  printf("leo open fd=%d errno=%d\n", leo, errno);
  //  char buf[100] = {0};
  //  ssize_t reads = read(leo, buf, 100);
  //  printf("leo reads=%d content=%s\n", reads, buf);
  //  close(leo);
  //  exit(0);

  errno = 0;
  char buf[100] = {0};

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