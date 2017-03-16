#ifdef __cplusplus
extern "C" {
#endif

typedef struct DB DB;
typedef struct Batch Batch;

DB*    NewDB();
Batch* NewBatch(DB* db);
void   FreeBatch(Batch *batch);
int    CommitBatch(DB* db, Batch* batch);
void   ApplyBatch(Batch* batch);
  
#ifdef __cplusplus
}  // extern "C"
#endif

