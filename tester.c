

#include <stdio.h>
#include <stdlib.h>

main(){
 FILE *fp;
 fp=fopen("sample.txt", "w+");
 fprintf(fp, "10");

 fclose(fp);
 return 0;
}
