#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "mpi.h"
#include "time.h"
#include "expand.h"

#define LFNAME 128 
#define KB 1024
#define MB (KB*KB)

#define LBUFMIN (128)
#define LBUFMAX (2*MB)
#define TAMFILE (100 * MB)
#define TRUE 1
#define FALSE 0


#define TASA_TRANSF(t) ((float)TAMFILE/(float) (t.tv_sec * USECPSEC + t.tv_usec))

char *  TAKE_SAMPLE_DIR; 
char buffer_basura[512*KB];
char buffer_esc[LBUFMAX];
char buffer_lec[LBUFMAX];
int errno;
char str[200];
int n_oper = 0;

/*
 * If you want, you can use this function to get the parameters:
 *                    -dir : path of the work file.
 *                    -miid
 *                    -n_cli: number of virtual clients.
 *                    -niter: numero of iterations.
 */
static int GetCheckArgs(int argc, char **argv, char *dir, int *cid, int *ncid)
{
    if (argc==2)
    {
       strcpy(dir,argv[1]); 
       (*cid)=0;
       (*ncid)=1;
       return(1);
    }
    else if (argc==4)
    {
       strcpy(dir,argv[1]); 
       (*cid)=atoi(argv[2]);
       (*ncid)=atoi(argv[3]);
       return(1);
    }

    printf("Uso: %s Dir; tu das %d\n", argv[0], argc );
    exit(1);
}


/*
 * Write TAMFILE bytes in blocks of lb bytes.
 */
static void ForwWriting
(
	int cid, int ncid,
	int f, int lb, char *buf, 
	struct timeval *tim
)
{
    int iter,ret;
    struct timeval ti, tf;
    int offset;



    //offset = 0;
    offset=cid*lb;
    iter= TAMFILE/lb;
    iter= iter/ncid;

    Timer(&ti);    

    for ( ; iter>0; iter--)
    {

       if (xpn_lseek(f,offset,SEEK_SET) < 0)
	       perror("ERROR EN LSEEK\n");

       //memset(buf, 'a', lb);

       if ((ret = xpn_write(f,buf,lb))!= lb)
       {
          sprintf(str, "IOC.ForwWriting: Error en escritura  errno = %d\n", ret);
	  printf(str);
          exit(1);
       }

       offset = offset + ncid*lb; 
       //offset = offset + lb; 


    }

    Timer(&tf);    
    DiffTime(&ti, &tf, tim);
}

/*
 * Read TAMFILE bytes in blocks of lb bytes and then compares it with
 * Bufe.
 */
static void ForwReading
(
	int cid, int ncid, 
	int f, int lb, char *bufl, char *bufe, 
	struct timeval *tim
)
{
    int iter;
    struct timeval ti, tf;
    int offset;
    int count;

    offset=cid*lb;
    iter= TAMFILE/lb;
    iter= iter/ncid;

    Timer(&ti);   

    for ( ; iter>0; iter--)
    {

       if (xpn_lseek(f,offset,SEEK_SET) < 0)
	       perror("ERROR EN LSEEK\n");

        //memset(bufl, 'a', lb);

        if ( (count = xpn_read(f, bufl, lb)) != lb)
	{
		printf(str, "IOC.ForwReading: Read %d bytes (%d expected)\n", count, lb);
		exit(1);
	}
        offset = offset + ncid*lb;
        //offset = offset + lb;
    }

    Timer(&tf);   
    DiffTime(&ti, &tf, tim);
}

/* 
 * This function is similar to ForwReading, but here the reads are
 * beginning in the end of the file.
 */
static void BackReading
(
	int cid, int ncid, 
	int f, int lb, char *bufl, char *bufe, 
	struct timeval *tim
)
{
    struct timeval ti, tf;
    int iter;
    int offset;

    offset=TAMFILE;
    iter= TAMFILE/lb;
    iter= iter/ncid;
    
    offset = offset - (lb*(cid+1));
    
    Timer(&ti);   
    
    for(;iter>0;iter--){
       
       xpn_lseek(f,offset,SEEK_SET) ;

       //memset(bufl, 'a', lb);

       if (xpn_read(f,bufl,lb)!= lb)
       {
          sprintf(str, "error IOC.BackReading: Error en lectura  errno = %d\n", errno);
	  printf(str);
          exit(1);
       }

       offset = offset - (lb*ncid);

    }

    Timer(&tf);   
    DiffTime(&ti, &tf, tim);
}

/* 
 * This function creats a new file, and is performanced a Writing, 
 * ForwReading and BackReading function.
 */
static void TakeSample
(
	int cid, int ncid,
	int lbuf, char *dir, 
	struct timeval *timew, 
	struct timeval *timefr, 
	struct timeval *timebr,
	int type
)
{
    int f;
    char fname[LFNAME];
    int ret;
    dir=TAKE_SAMPLE_DIR;
    sprintf (fname, "%s/%s.%d.%d", dir, "IOP", ncid, lbuf);
 
    if (( f=xpn_open(fname,O_CREAT|O_RDWR,0777)) < 0)
    {
    	sprintf(str, "IOC.TakeSample: failed to create file %s\n", fname);
    	printf(str);
    }	
    printf("%d > creo %s\n", cid, fname);
    

   /* lo abren todos */
   if(type == 0){
	   printf("%d > escribiendo %s\n", cid, fname);
	   ForwWriting(cid, ncid, f, lbuf, buffer_esc, timew);   
   }

   if(type == 1){
	   printf("%d > leo %s\n", cid, fname);
	   ForwReading(cid, ncid, f, lbuf, buffer_lec, buffer_esc, timefr);
   }

   if(type == 2){
	   printf("%d > leo atras %s\n", cid, fname);
	   BackReading(cid, ncid, f, lbuf, buffer_lec, buffer_esc, timefr);
   }
   xpn_close(f);

   MPI_Barrier(MPI_COMM_WORLD);
}

static void PrintHeader(void)
{
    printf("L_BUF N_BUF TOT(MB) E(MB/s) L_S(MB/s) L_NS(MB/s) T.TOTAL\n");
    printf("----------------------------------------------------------\n");
}

static void PrintResult(int cid, int lb, struct timeval *timet, float trw, float trfr, float trbr, int flag)
{
	/*
	 sprintf(str, "%d> %6d %6d %6d %f %f %f %f s.\n", cid, lb, TAMFILE/lb, TAMFILE/MB, trw,
			trfr, trbr, ((float)timet->tv_sec + (float)timet->tv_usec/USECPSEC));
			*/
        if(flag == 0){
	        sprintf(str, "%d\t%6d\t%f\t%f\tw\t\n", cid, lb, trw,
	                        ((float)timet->tv_sec + (float)timet->tv_usec/USECPSEC));
        }
        if(flag == 1){
	        sprintf(str, "%d\t%6d\t%f\t%f\tRD\t\n", cid, lb, trfr,
                        ((float)timet->tv_sec + (float)timet->tv_usec/USECPSEC));
        }
        if(flag == 2){
                 sprintf(str, "%d\t%6d\t%f\t%f\tb\t\n", cid, lb, trfr,
						                        ((float)timet->tv_sec + (float)timet->tv_usec/USECPSEC));
				        }
	     printf(str);
}

static void PrintSummary(struct timeval *ttot, int n, float med_w, float med_fr, float med_br)
{
    int n_users;

    n_users=1;
    printf("==================================================\n");
    sprintf(str, "Bandwidth. (Write):  %f MB/s \n",med_w/n);
    printf(str);
    sprintf(str, "Bandwidth. (Sec. Read):  %f MB/s \n",med_fr/n);
    printf(str);
    sprintf(str, "Bandwidth. (Rand. Read):  %f MB/s \n",med_br/n);
    printf(str);
    sprintf(str, "Average Bandwidth:  %f MB/s \n",(med_w+med_fr+med_br)/(3*n));
    printf(str);
    sprintf(str, "Total time:  %f s. \n\n",((float)ttot->tv_sec + 
		(float)ttot->tv_usec/USECPSEC));
    printf(str);

    sprintf(str, "BW %2.4f; BSR %2.4f; BRR %2.4f;  AVG %2.4f \n",
        n_users * (med_w/n), n_users * (med_fr/n), n_users * (med_br/n),
       n_users * (med_w+med_fr+med_br)/(3*n));
    printf(str);
    sprintf(str,"T. Time %2.4f \n", ((float)ttot->tv_sec +  
		(float)ttot->tv_usec/USECPSEC));
    printf(str);
    sprintf(str, "BW %2.4f; BSR %2.4f; BRR %2.4f;  AVG %2.4f \n",
        (med_w/n), (med_fr/n), (med_br/n), (med_w+med_fr+med_br)/(3*n));
    printf(str);


}

int main(int argc, char **argv)
{
    int lbuf, nit=0;
    struct timeval timei, timef, timedif, tini, tfin, tdif;
    char dir[LFNAME];
    float trw, trfr,  trbr; 
    float trw_med=0, trfr_med=0,  trbr_med=0;
    struct timeval timew, timefr, timebr;
    int ncid, cid;
    int ret;
    int  namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];

    setbuf(stdout, NULL);
    setenv("XPN_CONF","/export/home/pato11-1/proyectos/xpn/expand-2.0/test/pruebas/",1);
    ret = MPI_Init(&argc, &argv);
    if (ret < 0) {
	    printf("Error en MPI_Init \n");
	    exit(0);
    }

    MPI_Comm_size(MPI_COMM_WORLD,&ncid);
    MPI_Comm_rank(MPI_COMM_WORLD,&cid);
    MPI_Get_processor_name(processor_name,&namelen);

    printf("PROCESO %s -  %d de %d \n", processor_name, cid, ncid);
    
    if(xpn_init()<0){
	    printf("error en el xpn_init()\n");
	    exit(0);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
    lbuf = atoi(argv[2]);
    memset (buffer_esc, '7', LBUFMAX);
    TAKE_SAMPLE_DIR = argv[1];

        
    
    //PrintHeader();

    Timer(&tini);

      MPI_Barrier(MPI_COMM_WORLD);
      Timer(&timei);
      TakeSample(cid, ncid, lbuf, dir, &timew, &timefr, &timebr, 1);
      Timer(&timef);
      DiffTime(&timei, &timef, &timedif);
      trw=TASA_TRANSF(timew); 
      trfr=TASA_TRANSF(timefr); 
      trbr=TASA_TRANSF(timebr);
      trw_med+=trw; trfr_med+=trfr; trbr_med+=trbr;
      PrintResult(cid, lbuf, &timedif, trw, trfr, trbr,1);
      nit++;
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    Timer(&tfin);
    DiffTime(&tini, &tfin, &tdif);
    
    //PrintSummary(&tdif, nit, trw_med, trfr_med, trbr_med);
    //
    xpn_destroy();
    //printf("acabo.....\n");
    MPI_Finalize(); 

    exit(0);
}

