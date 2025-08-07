/*
** DuckDB Key-Value Performance Test
** Based on SQLite kvtest.c but adapted for DuckDB C API
**
** This file implements "key-value" performance test for DuckDB.  The
** purpose is to compare the speed of DuckDB for accessing large BLOBs
** versus reading those same BLOB values out of individual files in the
** filesystem.
**
** Run "ducktest" with no arguments for on-line help, or see comments below.
**
** HOW TO COMPILE:
**
** (1) Gather this source file and DuckDB library/header
**
** (2) Compile with:
**     gcc -O2 ducktest.c -lduckdb -o ducktest
**
** USAGE:
**
** (1) Create a test database by running "ducktest init" with appropriate
**     options.  See the help message for available options.
**
** (2) Construct the corresponding pile-of-files database on disk using
**     the "ducktest export" command.
**
** (3) Run tests using "ducktest run" against either the DuckDB database or
**     the pile-of-files database and with appropriate options.
**
** For example:
**
**       ./ducktest init x1.duckdb --count 100000 --size 10000
**       mkdir x1
**       ./ducktest export x1.duckdb x1
**       ./ducktest run x1.duckdb --count 10000 --max-id 1000000
**       ./ducktest run x1 --count 10000 --max-id 1000000
*/

#define _POSIX_C_SOURCE 200112L
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include "duckdb.h"

#ifndef _WIN32
# include <unistd.h>
# include <strings.h>
# include <sys/socket.h>
# include <stdio.h>
# include <stdlib.h>
#else
  /* Provide Windows equivalent for the needed parts of unistd.h */
# include <direct.h>
# include <io.h>
# define R_OK 2
# define S_ISREG(m) (((m) & S_IFMT) == S_IFREG)
# define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
# define access _access
#endif

#if !defined(_MSC_VER)
# include <stdint.h>
#endif

static const char zHelp[] = 
"Usage: ducktest COMMAND ARGS...\n"
"\n"
"   ducktest init DBFILE --count N --size M\n"
"\n"
"        Generate a new test database file named DBFILE containing N\n"
"        BLOBs each of size M bytes. Additional options:\n"
"\n"
"           --variance V           Randomly vary M by plus or minus V\n"
"\n"
"   ducktest export DBFILE DIRECTORY [--tree]\n"
"\n"
"        Export all the blobs in the kv table of DBFILE into separate\n"
"        files in DIRECTORY.  DIRECTORY is created if it does not previously\n"
"        exist.  If the --tree option is used, then the blobs are written\n"
"        into a hierarchy of directories, using names like 00/00/00,\n"
"        00/00/01, 00/00/02, and so forth.  Without the --tree option, all\n"
"        files are in the top-level directory with names like 000000, 000001,\n"
"        000002, and so forth.\n"
"\n"
"   ducktest stat DBFILE\n"
"\n"
"        Display summary information about DBFILE.\n"
"\n"
"   ducktest run DBFILE [options]\n"
"\n"
"        Run a performance test.  DBFILE can be either the name of a\n"
"        database or a directory containing sample files.  Options:\n"
"\n"
"           --asc                  Read blobs in ascending order\n"
"           --count N              Read N blobs\n"
"           --desc                 Read blobs in descending order\n"
"           --random               Read blobs in a random order\n"
"           --max-id N             Maximum blob key to use\n"
"           --start N              Start reading with this blob key\n"
"           --update               Do an overwrite test\n"
;

/* Show the help text and quit. */
static void showHelp(void){
  fprintf(stdout, "%s", zHelp);
  exit(1);
}

/* Show an error message an quit. */
static void fatalError(const char *zFormat, ...){
  va_list ap;
  fprintf(stdout, "ERROR: ");
  va_start(ap, zFormat);
  vfprintf(stdout, zFormat, ap);
  va_end(ap);
  fprintf(stdout, "\n");
  exit(1);
}

/* Return the value of a hexadecimal digit.  Return -1 if the input
** is not a hex digit. */
static int hexDigitValue(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  if( c>='A' && c<='F' ) return c - 'A' + 10;
  return -1;
}

/* Interpret zArg as an integer value, possibly with suffixes. */
static int integerValue(const char *zArg){
  int v = 0;
  static const struct { char *zSuffix; int iMult; } aMult[] = {
    { "KiB", 1024 },
    { "MiB", 1024*1024 },
    { "GiB", 1024*1024*1024 },
    { "KB",  1000 },
    { "MB",  1000000 },
    { "GB",  1000000000 },
    { "K",   1000 },
    { "M",   1000000 },
    { "G",   1000000000 },
  };
  int i;
  int isNeg = 0;
  if( zArg[0]=='-' ){
    isNeg = 1;
    zArg++;
  }else if( zArg[0]=='+' ){
    zArg++;
  }
  if( zArg[0]=='0' && zArg[1]=='x' ){
    int x;
    zArg += 2;
    while( (x = hexDigitValue(zArg[0]))>=0 ){
      v = (v<<4) + x;
      zArg++;
    }
  }else{
    while( zArg[0]>='0' && zArg[0]<='9' ){
      v = v*10 + zArg[0] - '0';
      zArg++;
    }
  }
  for(i=0; i<sizeof(aMult)/sizeof(aMult[0]); i++){
    if( strcasecmp(aMult[i].zSuffix, zArg)==0 ){
      v *= aMult[i].iMult;
      break;
    }
  }
  return isNeg? -v : v;
}

/* Path types */
#define PATH_DIR     1
#define PATH_TREE    2
#define PATH_DB      3
#define PATH_NEXIST  0
#define PATH_OTHER   99

static int pathType(const char *zPath){
  struct stat x;
  int rc;
  if( access(zPath,R_OK) ) return PATH_NEXIST;
  memset(&x, 0, sizeof(x));
  rc = stat(zPath, &x);
  if( rc<0 ) return PATH_OTHER;
  if( S_ISDIR(x.st_mode) ){
    char *zLayer1 = malloc(strlen(zPath) + 10);
    sprintf(zLayer1, "%s/00", zPath);
    memset(&x, 0, sizeof(x));
    rc = stat(zLayer1, &x);
    free(zLayer1);
    if( rc<0 ) return PATH_DIR;
    if( S_ISDIR(x.st_mode) ) return PATH_TREE;
    return PATH_DIR;
  }
  return PATH_DB;
}

/* Return the size of a file in bytes.  Or return -1 if the
** named object is not a regular file or does not exist. */
static long long fileSize(const char *zPath){
  struct stat x;
  int rc;
  memset(&x, 0, sizeof(x));
  rc = stat(zPath, &x);
  if( rc<0 ) return -1;
  if( !S_ISREG(x.st_mode) ) return -1;
  return x.st_size;
}

/* A pseudo-random number generator with a fixed seed. */
static unsigned int randInt(void){
  static unsigned int x = 0x333a13cd;
  static unsigned int y = 0xecb2adea;
  x = (x>>1) ^ ((1+~(x&1)) & 0xd0000001);
  y = y*1103515245 + 12345;
  return x^y;
}

/* Make sure a directory named zDir exists. */
static void ducktest_mkdir(const char *zDir){
#if defined(_WIN32)
  (void)mkdir(zDir);
#else
  (void)mkdir(zDir, 0755);
#endif
}

/* Read the content of file zName into memory obtained from malloc() */
static unsigned char *readFile(const char *zName, long long *pnByte){
  FILE *in;
  long long nIn;
  size_t nRead;
  unsigned char *pBuf;

  nIn = fileSize(zName);
  if( nIn<0 ) return 0;
  in = fopen(zName, "rb");
  if( in==0 ) return 0;
  pBuf = malloc(nIn + 1);
  if( pBuf==0 ) return 0;
  nRead = fread(pBuf, 1, nIn, in);
  fclose(in);
  if( nRead!=(size_t)nIn ){
    free(pBuf);
    return 0;
  }
  if( pnByte ) *pnByte = nIn;
  pBuf[nIn] = 0;
  return pBuf;
}

/* Overwrite a file with randomness. */
static void updateFile(const char *zName, long long *pnByte, int doFsync){
  FILE *out;
  long long sz;
  size_t nWritten;
  unsigned char *pBuf;
  const char *zMode = "wb";

  sz = fileSize(zName);
  if( sz<0 ){
    fatalError("No such file: \"%s\"", zName);
  }
  *pnByte = sz;
  if( sz==0 ) return;
  pBuf = malloc(sz);
  if( pBuf==0 ){
    fatalError("Cannot allocate %lld bytes\n", sz);
  }
  for(int i = 0; i < sz; i++) {
    pBuf[i] = rand() & 0xFF;
  }
  out = fopen(zName, zMode);
  if( out==0 ){
    fatalError("Cannot open \"%s\" for writing\n", zName);
  }
  nWritten = fwrite(pBuf, 1, sz, out);
  if( doFsync ){
#if !defined(_WIN32)
    fsync(fileno(out));
#endif
  }
  fclose(out);
  if( nWritten!=(size_t)sz ){
    fatalError("Wrote only %d of %d bytes to \"%s\"\n",
               (int)nWritten, (int)sz, zName);
  }
  free(pBuf);
}

/* Return the current time in milliseconds */
static long long timeOfDay(void){
#ifdef CLOCK_MONOTONIC
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

/* Do database initialization */
static int initMain(int argc, char **argv){
  char *zDb;
  int i;
  int nCount = 1000;
  int sz = 10000;
  int iVariance = 0;
  duckdb_database db;
  duckdb_connection con;
  duckdb_result result;
  char *zSql;
  char zErr[1024];

  assert( strcmp(argv[1],"init")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  for(i=3; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) fatalError("unknown argument: \"%s\"", z);
    if( z[1]=='-' ) z++;
    if( strcmp(z, "-count")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      nCount = integerValue(argv[++i]);
      if( nCount<1 ) fatalError("the --count must be positive");
      continue;
    }
    if( strcmp(z, "-size")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      sz = integerValue(argv[++i]);
      if( sz<1 ) fatalError("the --size must be positive");
      continue;
    }
    if( strcmp(z, "-variance")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iVariance = integerValue(argv[++i]);
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }

  if( duckdb_open(zDb, &db) != DuckDBSuccess ){
    fatalError("cannot open database \"%s\"", zDb);
  }
  
  if( duckdb_connect(db, &con) != DuckDBSuccess ){
    fatalError("cannot connect to database");
  }

  zSql = malloc(2048);
  sprintf(zSql, 
    "DROP TABLE IF EXISTS kv;\n"
    "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB);\n"
    "INSERT INTO kv SELECT i, "
    "CASE WHEN %d=0 THEN (SELECT string_agg(chr(cast(random() as integer) %% 256), '') FROM range(%d)) "
    "ELSE (SELECT string_agg(chr(cast(random() as integer) %% 256), '') FROM range(%d + (cast(random() as integer) %% %d))) END "
    "FROM range(1, %d + 1) t(i);",
    iVariance, sz, sz, iVariance + 1, nCount
  );

  if( duckdb_query(con, zSql, &result) != DuckDBSuccess ){
    sprintf(zErr, "database create failed: %s", duckdb_result_error(&result));
    duckdb_destroy_result(&result);
    duckdb_disconnect(&con);
    duckdb_close(&db);
    free(zSql);
    fatalError("%s", zErr);
  }
  
  duckdb_destroy_result(&result);
  duckdb_disconnect(&con);
  duckdb_close(&db);
  free(zSql);
  return 0;
}

/* Analyze an existing database file */
static int statMain(int argc, char **argv){
  char *zDb;
  duckdb_database db;
  duckdb_connection con;
  duckdb_result result;

  assert( strcmp(argv[1],"stat")==0 );
  assert( argc>=3 );
  zDb = argv[2];

  if( duckdb_open(zDb, &db) != DuckDBSuccess ){
    fatalError("cannot open database \"%s\"", zDb);
  }
  
  if( duckdb_connect(db, &con) != DuckDBSuccess ){
    fatalError("cannot connect to database");
  }

  if( duckdb_query(con, "SELECT count(*), min(length(v)), max(length(v)), avg(length(v)) FROM kv", &result) == DuckDBSuccess ){
    if( duckdb_row_count(&result) > 0 ){
      printf("Number of entries:  %8lld\n", duckdb_value_int64(&result, 0, 0));
      printf("Average value size: %8lld\n", (long long)duckdb_value_double(&result, 0, 3));
      printf("Minimum value size: %8lld\n", duckdb_value_int64(&result, 0, 1));
      printf("Maximum value size: %8lld\n", duckdb_value_int64(&result, 0, 2));
    }
    duckdb_destroy_result(&result);
  }

  duckdb_disconnect(&con);
  duckdb_close(&db);
  return 0;
}

/* Export the kv table to individual files in the filesystem */
static int exportMain(int argc, char **argv){
  char *zDb;
  char *zDir;
  duckdb_database db;
  duckdb_connection con;
  duckdb_result result;
  int ePathType;
  int nFN;
  char *zFN;
  char *zTail;
  size_t nWrote;
  int i;

  assert( strcmp(argv[1],"export")==0 );
  assert( argc>=3 );
  if( argc<4 ) fatalError("Usage: ducktest export DATABASE DIRECTORY [OPTIONS]");
  zDb = argv[2];
  zDir = argv[3];
  ducktest_mkdir(zDir);
  for(i=4; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' && z[1]=='-' ) z++;
    if( strcmp(z,"-tree")==0 ){
      zFN = malloc(strlen(zDir) + 10);
      sprintf(zFN, "%s/00", zDir);
      ducktest_mkdir(zFN);
      free(zFN);
      continue;
    }
    fatalError("unknown argument: \"%s\"\n", argv[i]);
  }
  ePathType = pathType(zDir);
  if( ePathType!=PATH_DIR && ePathType!=PATH_TREE ){
    fatalError("object \"%s\" is not a directory", zDir);
  }
  
  if( duckdb_open(zDb, &db) != DuckDBSuccess ){
    fatalError("cannot open database \"%s\"", zDb);
  }
  
  if( duckdb_connect(db, &con) != DuckDBSuccess ){
    fatalError("cannot connect to database");
  }

  if( duckdb_query(con, "SELECT k, v FROM kv ORDER BY k", &result) != DuckDBSuccess ){
    fatalError("query failed: %s\n", duckdb_result_error(&result));
  }

  nFN = strlen(zDir);
  zFN = malloc(nFN + 50);
  zTail = zFN + nFN + 1;
  
  for(idx_t row_idx = 0; row_idx < duckdb_row_count(&result); row_idx++){
    int iKey = duckdb_value_int32(&result, row_idx, 0);
    duckdb_blob blob = duckdb_value_blob(&result, row_idx, 1);
    FILE *out;
    
    if( ePathType==PATH_DIR ){
      sprintf(zTail, "%06d", iKey);
    }else{
      sprintf(zTail, "%02d", iKey/10000);
      sprintf(zFN, "%s/%s", zDir, zTail);
      ducktest_mkdir(zFN);
      sprintf(zTail, "%02d/%02d", iKey/10000, (iKey/100)%100);
      sprintf(zFN, "%s/%s", zDir, zTail);
      ducktest_mkdir(zFN);
      sprintf(zTail, "%02d/%02d/%02d", iKey/10000, (iKey/100)%100, iKey%100);
    }
    sprintf(zFN, "%s/%s", zDir, zTail);
    
    out = fopen(zFN, "wb");      
    nWrote = fwrite(blob.data, 1, blob.size, out);
    fclose(out);
    printf("\r%s   ", zTail); fflush(stdout);
    if( nWrote!=blob.size ){
      fatalError("Wrote only %d of %d bytes to %s\n",
                  (int)nWrote, (int)blob.size, zFN);
    }
  }
  
  duckdb_destroy_result(&result);
  duckdb_disconnect(&con);
  duckdb_close(&db);
  free(zFN);
  printf("\n");
  return 0;
}

/* Blob access order */
#define ORDER_ASC     1
#define ORDER_DESC    2
#define ORDER_RANDOM  3

/* Run a performance test */
static int runMain(int argc, char **argv){
  int eType;
  char *zDb;
  int i;
  int nCount = 1000;
  int nExtra = 0;
  int iKey = 1;
  int iMax = 0;
  int bStats = 0;
  int eOrder = ORDER_ASC;
  int isUpdateTest = 0;
  int doFsync = 0;
  duckdb_database db = NULL;
  duckdb_connection con = NULL;
  duckdb_result result;
  duckdb_prepared_statement stmt = NULL;
  long long tmStart;
  long long tmElapsed;
  long long nData = 0;
  long long nTotal = 0;
  unsigned char *pData = NULL;
  long long nAlloc = 0;

  assert( strcmp(argv[1],"run")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  eType = pathType(zDb);
  if( eType==PATH_OTHER ) fatalError("unknown object type: \"%s\"", zDb);
  if( eType==PATH_NEXIST ) fatalError("object does not exist: \"%s\"", zDb);
  
  for(i=3; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) fatalError("unknown argument: \"%s\"", z);
    if( z[1]=='-' ) z++;
    if( strcmp(z, "-asc")==0 ){
      eOrder = ORDER_ASC;
      continue;
    }
    if( strcmp(z, "-count")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      nCount = integerValue(argv[++i]);
      if( nCount<1 ) fatalError("the --count must be positive");
      continue;
    }
    if( strcmp(z, "-desc")==0 ){
      eOrder = ORDER_DESC;
      continue;
    }
    if( strcmp(z, "-fsync")==0 ){
      doFsync = 1;
      continue;
    }
    if( strcmp(z, "-max-id")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iMax = integerValue(argv[++i]);
      continue;
    }
    if( strcmp(z, "-random")==0 ){
      eOrder = ORDER_RANDOM;
      continue;
    }
    if( strcmp(z, "-start")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iKey = integerValue(argv[++i]);
      if( iKey<1 ) fatalError("the --start must be positive");
      continue;
    }
    if( strcmp(z, "-update")==0 ){
      isUpdateTest = 1;
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  
  if( eType==PATH_DB ){
    if( duckdb_open(zDb, &db) != DuckDBSuccess ){
      fatalError("cannot open database \"%s\"", zDb);
    }
    
    if( duckdb_connect(db, &con) != DuckDBSuccess ){
      fatalError("cannot connect to database");
    }
    
    if( iMax<=0 ){
      if( duckdb_query(con, "SELECT max(k) FROM kv", &result) == DuckDBSuccess ){
        if( duckdb_row_count(&result) > 0 ){
          iMax = duckdb_value_int32(&result, 0, 0);
        }
        duckdb_destroy_result(&result);
      }
    }
    
    if( isUpdateTest ){
      if( duckdb_prepare(con, "UPDATE kv SET v=(SELECT string_agg(chr(cast(random() as integer) %% 256), '') FROM range(length(v))) WHERE k=?1", &stmt) != DuckDBSuccess ){
        fatalError("cannot prepare update query");
      }
    }else{
      if( duckdb_prepare(con, "SELECT v FROM kv WHERE k=?1", &stmt) != DuckDBSuccess ){
        fatalError("cannot prepare select query");
      }
    }
  }
  
  if( iMax<=0 ) iMax = 1000;
  tmStart = timeOfDay();
  
  for(i=0; i<nCount; i++){
    if( eType==PATH_DIR || eType==PATH_TREE ){
      char *zKey = malloc(strlen(zDb) + 20);
      if( eType==PATH_DIR ){
        sprintf(zKey, "%s/%06d", zDb, iKey);
      }else{
        sprintf(zKey, "%s/%02d/%02d/%02d", zDb, iKey/10000,
                               (iKey/100)%100, iKey%100);
      }
      nData = 0;
      if( isUpdateTest ){
        updateFile(zKey, &nData, doFsync);
      }else{
        pData = readFile(zKey, &nData);
        free(pData);
      }
      free(zKey);
    }else{
      duckdb_bind_int32(stmt, 1, iKey);
      nData = 0;
      
      if( duckdb_execute_prepared(stmt, &result) == DuckDBSuccess ){
        if( isUpdateTest ){
          nData = duckdb_rows_changed(&result);
        }else{
          if( duckdb_row_count(&result) > 0 ){
            duckdb_blob blob = duckdb_value_blob(&result, 0, 0);
            nData = blob.size;
          }
        }
        duckdb_destroy_result(&result);
      }
      
      duckdb_clear_bindings(stmt);
    }
    
    if( eOrder==ORDER_ASC ){
      iKey++;
      if( iKey>iMax ) iKey = 1;
    }else if( eOrder==ORDER_DESC ){
      iKey--;
      if( iKey<=0 ) iKey = iMax;
    }else{
      iKey = (randInt()%iMax)+1;
    }
    nTotal += nData;
    if( nData==0 ){ nCount++; nExtra++; }
  }
  
  if( nAlloc ) free(pData);
  if( stmt ) duckdb_destroy_prepare(&stmt);
  if( con ) duckdb_disconnect(&con);
  if( db ) duckdb_close(&db);
  
  tmElapsed = timeOfDay() - tmStart;
  if( nExtra ){
    printf("%d cycles due to %d misses\n", nCount, nExtra);
  }
  printf("--count %d --max-id %d", nCount-nExtra, iMax);
  switch( eOrder ){
    case ORDER_RANDOM:  printf(" --random\n");  break;
    case ORDER_DESC:    printf(" --desc\n");    break;
    default:            printf(" --asc\n");     break;
  }
  printf("Total elapsed time: %.3f\n", tmElapsed/1000.0);
  if( isUpdateTest ){
    printf("Microseconds per BLOB write: %.3f\n", tmElapsed*1000.0/nCount);
    printf("Content write rate: %.1f MB/s\n", nTotal/(1000.0*tmElapsed));
  }else{
    printf("Microseconds per BLOB read: %.3f\n", tmElapsed*1000.0/nCount);
    printf("Content read rate: %.1f MB/s\n", nTotal/(1000.0*tmElapsed));
  }
  return 0;
}

int main(int argc, char **argv){
  if( argc<3 ) showHelp();
  if( strcmp(argv[1],"init")==0 ){
    return initMain(argc, argv);
  }
  if( strcmp(argv[1],"export")==0 ){
    return exportMain(argc, argv);
  }
  if( strcmp(argv[1],"run")==0 ){
    return runMain(argc, argv);
  }
  if( strcmp(argv[1],"stat")==0 ){
    return statMain(argc, argv);
  }
  showHelp();
  return 0;
}