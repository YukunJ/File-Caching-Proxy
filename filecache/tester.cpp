#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
  errno = 0;
  int fd_1 = open(argv[1], O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
  if (fd_1 == -1) {
    printf("failed 1st to open; fd_1=%d, errno=%d\n", fd_1, errno);
    exit(0);
  } else {
    printf("success 1st to open: fd_1=%d, errno=%d\n", fd_1, errno);
  }

  errno = 0;

  int fd_2 = open(argv[1], O_RDWR, S_IRWXU);
  if (fd_2 == -1) {
    printf("failed 2nd to open; fd_2=%d, errno=%d\n", fd_2, errno);
    exit(0);
  } else {
    printf("success 2nd to open: fd_2=%d, errno=%d\n", fd_2, errno);
  }

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

  int fd_4 = open(argv[1], O_RDONLY);
  if (fd_4 == -1) {
    printf("failed 4th to open; fd_4=%d, errno=%d\n", fd_4, errno);
    exit(0);
  } else {
    printf("success 4th to open: fd_4=%d, errno=%d\n", fd_4, errno);
  }
  errno = 0;
  int fd_5 = open(argv[1], O_RDWR, S_IRWXU);
  if (fd_5 == -1) {
    printf("failed 5th to open; fd_5=%d, errno=%d\n", fd_5, errno);
    exit(0);
  } else {
    printf("success 5th to open: fd_5=%d, errno=%d\n", fd_5, errno);
  }

  close(fd_5);

  errno = 0;
  int fd_6 = open(argv[1], O_RDONLY);
  if (fd_6 == -1) {
    printf("failed 6th to open; fd_5=%d, errno=%d\n", fd_6, errno);
    exit(0);
  } else {
    printf("success 6th to open: fd_5=%d, errno=%d\n", fd_6, errno);
  }

  close(fd_3);
  close(fd_4);
  close(fd_6);
}