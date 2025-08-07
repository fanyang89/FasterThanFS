/*
** 2016-12-28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
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
** (1) Gather this source file and DuckDB headers and library.
**     You should have:
**
**          ducktest.c     >--- this file
**          duckdb.hpp     \___ DuckDB
**          duckdb.lib     /    headers & library (or libduckdb.so on Linux)
**
** (2) Run your compiler against the source file.
**
**    (a) On linux or mac:
**
**        g++ -std=c++11 -O2 ducktest.c -lduckdb -o ducktest
**
**    (b) Windows with MSVC:
**
**        cl /EHsc ducktest.c duckdb.lib
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
**       ./ducktest init x1.db --count 100000 --size 10000
**       mkdir x1
**       ./ducktest export x1.db x1
**       ./ducktest run x1.db --count 10000 --max-id 1000000
**       ./ducktest run x1 --count 10000 --max-id 1000000
*/
static const char zHelp[] = 
"Usage: ducktest COMMAND ARGS...\n"
"\n"
"   ducktest init DBFILE --count N --size M\n"
"\n"
"        Generate a new test database file named DBFILE containing N\n"
"        BLOBs each of size M bytes.  Additional options:\n"
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
"   ducktest stat DBFILE [options]\n"
"\n"
"        Display summary information about DBFILE.  Options:\n"
"\n"
"           --vacuum               Run VACUUM on the database file\n"
"\n"
"   ducktest run DBFILE [options]\n"
"\n"
"        Run a performance test.  DBFILE can be either the name of a\n"
"        database or a directory containing sample files.  Options:\n"
"\n"
"           --asc                  Read blobs in ascending order\n"
"           --count N              Read N blobs\n"
"           --desc                 Read blobs in descending order\n"
"           --fsync                Synchronous file writes\n"
"           --max-id N             Maximum blob key to use\n"
"           --random               Read blobs in a random order\n"
"           --start N              Start reading with this blob key\n"
"           --update               Do an overwrite test\n"
;

/* Reference resources used */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <string.h>
#include <string>
#include <vector>
#include <cstdint>
#include <stdarg.h>
#include <unistd.h>
#include <sys/time.h>
#include <dirent.h>
#include "duckdb.hpp"

#ifndef _WIN32
# include <unistd.h>
#else
  /* Provide Windows equivalent for the needed parts of unistd.h */
# include <direct.h>
# include <io.h>
# include <windows.h>
# define R_OK 2
# define S_ISREG(m) (((m) & S_IFMT) == S_IFREG)
# define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
# define access _access
# define mkdir(path, mode) _mkdir(path)
# define snprintf _snprintf
# define getpid() _getpid()
#endif

#if !defined(_MSC_VER)
# include <stdint.h>
#endif

// Define missing types and functions for cross-platform compatibility
#ifndef _WIN32
#define SQLITE_INT_TO_PTR(X)  ((void*)(intptr_t)(X))
#define SQLITE_PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else
#define SQLITE_INT_TO_PTR(X)  ((void*)(X))
#define SQLITE_PTR_TO_INT(X)  ((int)(X))
#endif

// Forward declarations for SQLite3 types that are referenced but not used
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef struct sqlite3_blob sqlite3_blob;
typedef uint64_t sqlite3_int64;

// Define SQLite3 constants that are referenced
#define SQLITE_UTF8     1
#define SQLITE_OK       0
#define SQLITE_ROW      100

/*
** DuckDB doesn't need pointer-to-integer casting macros like SQLite
** since we'll use C++ features and proper type handling
*/

/*
** Show the help text and quit.
*/
static void showHelp(void){
  fprintf(stdout, "%s", zHelp);
  exit(1);
}

/*
** Show an error message an quit.
*/
static void fatalError(const char *zFormat, ...){
  va_list ap;
  fprintf(stdout, "ERROR: ");
  va_start(ap, zFormat);
  vfprintf(stdout, zFormat, ap);
  va_end(ap);
  fprintf(stdout, "\n");
  exit(1);
}

/*
** Return the value of a hexadecimal digit.  Return -1 if the input
** is not a hex digit.
*/
static int hexDigitValue(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  if( c>='A' && c<='F' ) return c - 'A' + 10;
  return -1;
}

/*
** Interpret zArg as an integer value, possibly with suffixes.
*/
static int integerValue(const char *zArg){
  int v = 0;
  static const struct { const char *zSuffix; int iMult; } aMult[] = {
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


/*
** Check the filesystem object zPath.  Determine what it is:
**
**    PATH_DIR     A single directory holding many files
**    PATH_TREE    A directory hierarchy with files at the leaves
**    PATH_DB      A DuckDB database
**    PATH_NEXIST  Does not exist
**    PATH_OTHER   Something else
**
** PATH_DIR means all of the separate files are grouped together
** into a single directory with names like 000000, 000001, 000002, and
** so forth.  PATH_TREE means there is a hierarchy of directories so
** that no single directory has too many entries.  The files have names
** like 00/00/00, 00/00/01, 00/00/02 and so forth.  The decision between
** PATH_DIR and PATH_TREE is determined by the presence of a subdirectory
** named "00" at the top-level.
*/
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
    char zLayer1[4096];
    snprintf(zLayer1, sizeof(zLayer1), "%s/00", zPath);
    memset(&x, 0, sizeof(x));
    rc = stat(zLayer1, &x);
    if( rc<0 ) return PATH_DIR;
    if( S_ISDIR(x.st_mode) ) return PATH_TREE;
    return PATH_DIR;
  }
  /* Check if it's a DuckDB database by file extension and basic structure */
  if( S_ISREG(x.st_mode) ){
    const char *ext = strrchr(zPath, '.');
    if( ext && strcmp(ext, ".db")==0 ) return PATH_DB;
    if( ext && strcmp(ext, ".duckdb")==0 ) return PATH_DB;
  }
  return PATH_OTHER;
}

/*
** Return the size of a file in bytes.  Or return -1 if the
** named object is not a regular file or does not exist.
*/
static sqlite3_int64 fileSize(const char *zPath){
  struct stat x;
  int rc;
  memset(&x, 0, sizeof(x));
  rc = stat(zPath, &x);
  if( rc<0 ) return -1;
  if( !S_ISREG(x.st_mode) ) return -1;
  return x.st_size;
}

/*
** A Pseudo-random number generator with a fixed seed.  Use this so
** that the same sequence of "random" numbers are generated on each
** run, for repeatability.
*/
static unsigned int randInt(void){
  static unsigned int x = 0x333a13cd;
  static unsigned int y = 0xecb2adea;
  x = (x>>1) ^ ((1+~(x&1)) & 0xd0000001);
  y = y*1103515245 + 12345;
  return x^y;
}

/*
** Do database initialization.
*/
static int initMain(int argc, char **argv){
  char *zDb;
  int i;
  int nCount = 1000;
  int sz = 10000;
  int iVariance = 0;

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
    if( strcmp(z, "-pagesize")==0 ){
      printf("Warning: --pagesize option is ignored in DuckDB\n");
      i++;
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  
  try {
    duckdb::DuckDB db(zDb);
    duckdb::Connection con(db);
    
    // Create table and insert data
    std::string sql = "DROP TABLE IF EXISTS kv; CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB);";
    con.Query(sql);
    
    // Generate and insert data in batches for better performance
    const int batch_size = 1000;
    for(int batch_start = 1; batch_start <= nCount; batch_start += batch_size) {
      int batch_end = batch_start + batch_size - 1;
      if (batch_end > nCount) batch_end = nCount;
      
      std::string insert_sql = "INSERT INTO kv VALUES ";
      for(int j = batch_start; j <= batch_end; j++) {
        if (j > batch_start) insert_sql += ", ";
        insert_sql += "(" + std::to_string(j) + ", randomblob(" + 
                      std::to_string(sz + (rand() % (iVariance + 1))) + "))";
      }
      con.Query(insert_sql);
    }
    
    printf("Created database with %d entries\n", nCount);
    return 0;
  } catch (std::exception& e) {
    fatalError("database create failed: %s", e.what());
    return 1;
  }
}

/*
** Analyze an existing database file.  Report its content.
*/
static int statMain(int argc, char **argv){
  char *zDb;
  int i;
  int doVacuum = 0;

  assert( strcmp(argv[1],"stat")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  for(i=3; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) fatalError("unknown argument: \"%s\"", z);
    if( z[1]=='-' ) z++;
    if( strcmp(z, "-vacuum")==0 ){
      doVacuum = 1;
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  
  try {
    duckdb::DuckDB db(zDb);
    duckdb::Connection con(db);
    
    if( doVacuum ){
      printf("Vacuuming...."); fflush(stdout);
      con.Query("VACUUM");
      printf("       done\n");
    }
    
    auto result = con.Query("SELECT count(*), min(length(v)), max(length(v)), avg(length(v)) FROM kv");
    if (result->HasError()) {
      fatalError("query failed: %s", result->GetError());
    }
    
    for (auto &row : *result) {
      printf("Number of entries:  %8d\n", row.GetValue<int64_t>(0));
      printf("Average value size: %8d\n", (int)row.GetValue<double>(3));
      printf("Minimum value size: %8d\n", row.GetValue<int64_t>(1));
      printf("Maximum value size: %8d\n", row.GetValue<int64_t>(2));
    }
    
    // DuckDB doesn't have page_size, page_count, or freelist_count pragmas like SQLite
    printf("Note: DuckDB does not expose page-level statistics like SQLite\n");
    
    return 0;
  } catch (std::exception& e) {
    fatalError("cannot open database \"%s\": %s", zDb, e.what());
    return 1;
  }
}


/*
** Make sure a directory named zDir exists.
*/
static void kvtest_mkdir(const char *zDir){
#if defined(_WIN32)
  (void)mkdir(zDir);
#else
  (void)mkdir(zDir, 0755);
#endif
}

/*
** Export the kv table to individual files in the filesystem
*/
static int exportMain(int argc, char **argv){
  char *zDb;
  char *zDir;
  int ePathType;
  char zFN[4096];
  char zTail[256];
  size_t nWrote;
  int i;

  assert( strcmp(argv[1],"export")==0 );
  assert( argc>=3 );
  if( argc<4 ) fatalError("Usage: ducktest export DATABASE DIRECTORY [OPTIONS]");
  zDb = argv[2];
  zDir = argv[3];
  kvtest_mkdir(zDir);
  for(i=4; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' && z[1]=='-' ) z++;
    if( strcmp(z,"-tree")==0 ){
      snprintf(zFN, sizeof(zFN), "%s/00", zDir);
      kvtest_mkdir(zFN);
      continue;
    }
    fatalError("unknown argument: \"%s\"\n", argv[i]);
  }
  ePathType = pathType(zDir);
  if( ePathType!=PATH_DIR && ePathType!=PATH_TREE ){
    fatalError("object \"%s\" is not a directory", zDir);
  }
  
  try {
    duckdb::DuckDB db(zDb);
    duckdb::Connection con(db);
    
    auto result = con.Query("SELECT k, v FROM kv ORDER BY k");
    if (result->HasError()) {
      fatalError("query failed: %s", result->GetError());
    }
    
    int nFN = (int)strlen(zDir);
    char *zTailPtr = zFN + nFN + 1;
    
    for (auto &row : *result) {
      int iKey = row.GetValue<int32_t>(0);
      auto blob_value = row.GetValue<duckdb::string_t>(1);
      const void *pData = blob_value.GetData();
      size_t nData = blob_value.GetSize();
      
      FILE *out;
      if( ePathType==PATH_DIR ){
        snprintf(zTailPtr, sizeof(zFN) - (zTailPtr - zFN), "%06d", iKey);
      }else{
        snprintf(zTailPtr, sizeof(zFN) - (zTailPtr - zFN), "%02d", iKey/10000);
        kvtest_mkdir(zFN);
        snprintf(zTailPtr, sizeof(zFN) - (zTailPtr - zFN), "%02d/%02d", iKey/10000, (iKey/100)%100);
        kvtest_mkdir(zFN);
        snprintf(zTailPtr, sizeof(zFN) - (zTailPtr - zFN), "%02d/%02d/%02d",
                 iKey/10000, (iKey/100)%100, iKey%100);
      }
      out = fopen(zFN, "wb");      
      nWrote = fwrite(pData, 1, nData, out);
      fclose(out);
      printf("\r%s   ", zTailPtr); fflush(stdout);
      if( nWrote!=nData ){
        fatalError("Wrote only %d of %d bytes to %s\n",
                    (int)nWrote, (int)nData, zFN);
      }
    }
    printf("\n");
    return 0;
  } catch (std::exception& e) {
    fatalError("cannot open database \"%s\": %s", zDb, e.what());
    return 1;
  }
}

/*
** Read the content of file zName into memory obtained from malloc()
** and return a pointer to the buffer. The caller is responsible for freeing 
** the memory. 
**
** If parameter pnByte is not NULL, (*pnByte) is set to the number of bytes
** read.
**
** For convenience, a nul-terminator byte is always appended to the data read
** from the file before the buffer is returned. This byte is not included in
** the final value of (*pnByte), if applicable.
**
** NULL is returned if any error is encountered. The final value of *pnByte
** is undefined in this case.
*/
static unsigned char *readFile(const char *zName, sqlite3_int64 *pnByte){
  FILE *in;               /* FILE from which to read content of zName */
  sqlite3_int64 nIn;      /* Size of zName in bytes */
  size_t nRead;           /* Number of bytes actually read */
  unsigned char *pBuf;    /* Content read from disk */

  nIn = fileSize(zName);
  if( nIn<0 ) return 0;
  in = fopen(zName, "rb");
  if( in==0 ) return 0;
  pBuf = (unsigned char*)malloc( nIn );
  if( pBuf==0 ) return 0;
  nRead = fread(pBuf, (size_t)nIn, 1, in);
  fclose(in);
  if( nRead!=1 ){
    free(pBuf);
    return 0;
  }
  if( pnByte ) *pnByte = nIn;
  return pBuf;
}

/*
** Overwrite a file with randomness.  Do not change the size of the
** file.
*/
static void updateFile(const char *zName, sqlite3_int64 *pnByte, int doFsync){
  FILE *out;              /* FILE from which to read content of zName */
  sqlite3_int64 sz;       /* Size of zName in bytes */
  size_t nWritten;        /* Number of bytes actually read */
  unsigned char *pBuf;    /* Content to store on disk */
  const char *zMode = "wb";   /* Mode for fopen() */

  sz = fileSize(zName);
  if( sz<0 ){
    fatalError("No such file: \"%s\"", zName);
  }
  *pnByte = sz;
  if( sz==0 ) return;
  pBuf = (unsigned char*)malloc( sz );
  if( pBuf==0 ){
    fatalError("Cannot allocate %lld bytes\n", sz);
  }
  // Generate random data using standard C rand()
  for(int i = 0; i < sz; i++) {
    pBuf[i] = rand() % 256;
  }
#if defined(_WIN32)
  if( doFsync ) zMode = "wbc";
#endif
  out = fopen(zName, zMode);
  if( out==0 ){
    fatalError("Cannot open \"%s\" for writing\n", zName);
  }
  nWritten = fwrite(pBuf, 1, (size_t)sz, out);
  if( doFsync ){
#if defined(_WIN32)
    fflush(out);
#else
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

/*
** Return the current time in milliseconds since the beginning of
** the Julian epoch.
*/
static sqlite3_int64 timeOfDay(void){
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (sqlite3_int64)(tv.tv_sec) * 1000 + (sqlite3_int64)(tv.tv_usec) / 1000;
}

#ifdef __linux__
/*
** Attempt to display I/O stats on Linux using /proc/PID/io
*/
static void displayLinuxIoStats(FILE *out){
  FILE *in;
  char z[200];
  snprintf(z, sizeof(z), "/proc/%d/io", getpid());
  in = fopen(z, "rb");
  if( in==0 ) return;
  while( fgets(z, sizeof(z), in)!=0 ){
    static const struct {
      const char *zPattern;
      const char *zDesc;
    } aTrans[] = {
      { "rchar: ",                  "Bytes received by read():" },
      { "wchar: ",                  "Bytes sent to write():"    },
      { "syscr: ",                  "Read() system calls:"      },
      { "syscw: ",                  "Write() system calls:"     },
      { "read_bytes: ",             "Bytes read from storage:"  },
      { "write_bytes: ",            "Bytes written to storage:" },
      { "cancelled_write_bytes: ",  "Cancelled write bytes:"    },
    };
    int i;
    for(i=0; i<sizeof(aTrans)/sizeof(aTrans[0]); i++){
      int n = (int)strlen(aTrans[i].zPattern);
      if( strncmp(aTrans[i].zPattern, z, n)==0 ){
        fprintf(out, "%-36s %s", aTrans[i].zDesc, &z[n]);
        break;
      }
    }
  }
  fclose(in);
}
#endif

/*
** Display memory stats.
*/
static int display_stats(
  duckdb::Connection *con,       /* Database connection */
  int bReset                      /* True to reset stats (ignored in DuckDB) */
){
  FILE *out = stdout;

  fprintf(out, "\n");
  fprintf(out, "Note: DuckDB does not expose detailed memory statistics like SQLite\n");
  
  // Try to get some basic information if available
  try {
    auto result = con->Query("SELECT COUNT(*) FROM kv");
    if (!result->HasError()) {
      for (auto &row : *result) {
        fprintf(out, "Total entries in kv table:           %lld\n", row.GetValue<int64_t>(0));
      }
    }
  } catch (...) {
    // Ignore errors
  }

#ifdef __linux__
  displayLinuxIoStats(out);
#endif

  return 0;
}

/* Blob access order */
#define ORDER_ASC     1
#define ORDER_DESC    2
#define ORDER_RANDOM  3


/*
** Run a performance test
*/
static int runMain(int argc, char **argv){
  int eType;                  /* Is zDb a database or a directory? */
  char *zDb;                  /* Database or directory name */
  int i;                      /* Loop counter */
  int nCount = 1000;          /* Number of blob fetch operations */
  int nExtra = 0;             /* Extra cycles */
  int iKey = 1;               /* Next blob key */
  int iMax = 0;               /* Largest allowed key */
  int bStats = 0;             /* Print stats before exiting */
  int eOrder = ORDER_ASC;     /* Access order */
  int isUpdateTest = 0;       /* Do in-place updates rather than reads */
  int doFsync = 0;            /* Update disk files synchronously */
  sqlite3_int64 tmStart;      /* Start time */
  sqlite3_int64 tmElapsed;    /* Elapsed time */
  sqlite3_int64 nData = 0;    /* Bytes of data */
  sqlite3_int64 nTotal = 0;   /* Total data read */
  unsigned char *pData = 0;   /* Content of the blob */
  sqlite3_int64 nAlloc = 0;   /* Space allocated for pData[] */
  
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
    if( strcmp(z, "-stats")==0 ){
      bStats = 1;
      continue;
    }
    if( strcmp(z, "-update")==0 ){
      isUpdateTest = 1;
      continue;
    }
    // Ignore SQLite-specific options with warnings
    if( strcmp(z, "-blob-api")==0 || strcmp(z, "-cache-size")==0 || 
        strcmp(z, "-integrity-check")==0 || strcmp(z, "-jmode")==0 ||
        strcmp(z, "-mmap")==0 || strcmp(z, "-multitrans")==0 ||
        strcmp(z, "-nocheckpoint")==0 || strcmp(z, "-nosync")==0 ){
      printf("Warning: %s option is ignored in DuckDB\n", z);
      i++; // Skip argument value if present
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  
  // For DuckDB, open connection first to get max key
  if( eType==PATH_DB ){
    try {
      duckdb::DuckDB db(zDb);
      duckdb::Connection con(db);
      
      if( iMax<=0 ){
        auto result = con.Query("SELECT max(k) FROM kv");
        if (!result->HasError() && result->RowCount() > 0) {
          for (auto &row : *result) {
            iMax = row.GetValue<int32_t>(0);
          }
        }
      }
    } catch (std::exception& e) {
      fatalError("cannot open database \"%s\": %s", zDb, e.what());
    }
  }
  
  if( iMax<=0 ) iMax = 1000;
  tmStart = timeOfDay();
  
  if( eType==PATH_DB ){
    try {
      duckdb::DuckDB db(zDb);
      duckdb::Connection con(db);
      
      // Prepare query based on operation type
      std::string base_query;
      if( isUpdateTest ){
        base_query = "UPDATE kv SET v=randomblob(length(v)) WHERE k=";
      } else {
        base_query = "SELECT v FROM kv WHERE k=";
      }
      
      for(i=0; i<nCount; i++){
        std::string query = base_query + std::to_string(iKey);
        auto result = con.Query(query);
        
        if (result->HasError()) {
          fatalError("query failed: %s", result->GetError());
        }
        
        nData = 0;
        if( isUpdateTest ){
          // For updates, we need to get the size that was updated
          auto size_result = con.Query("SELECT length(v) FROM kv WHERE k=" + std::to_string(iKey));
          if (!size_result->HasError() && size_result->RowCount() > 0) {
            for (auto &row : *size_result) {
              nData = row.GetValue<int64_t>(0);
            }
          }
        } else {
          // For reads, get the blob data
          if (result->RowCount() > 0) {
            for (auto &row : *result) {
              auto blob_value = row.GetValue<duckdb::string_t>(0);
              nData = blob_value.GetSize();
              // Copy data if needed for consistency with original
              if( nAlloc < nData+1 ){
                nAlloc = nData + 100;
                pData = (unsigned char*)realloc(pData, nAlloc);
              }
              if( pData ) {
                memcpy(pData, blob_value.GetData(), nData);
              }
            }
          }
        }
        
        // Update key for next iteration
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
      
      if( bStats ){
        display_stats(&con, 0);
      }
      
    } catch (std::exception& e) {
      fatalError("database error: %s", e.what());
    }
  } else {
    // File-based operations (PATH_DIR or PATH_TREE)
    for(i=0; i<nCount; i++){
      char *zKey;
      if( eType==PATH_DIR ){
        zKey = (char*)malloc(strlen(zDb) + 8);
        sprintf(zKey, "%s/%06d", zDb, iKey);
      }else{
        zKey = (char*)malloc(strlen(zDb) + 12);
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
  }
  
  if( nAlloc ) free(pData);
  tmElapsed = timeOfDay() - tmStart;
  
  if( nExtra ){
    printf("%d cycles due to %d misses\n", nCount, nExtra);
  }
  
  if( eType==PATH_DB ){
    printf("DuckDB database\n");
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
