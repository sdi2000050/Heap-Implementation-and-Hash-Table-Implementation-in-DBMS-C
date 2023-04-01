#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }



int SHT_CreateSecondaryIndex(char *sfileName, int buckets, char* fileName) {

  // Create new file using BF_CreateFile
  CALL_OR_DIE(BF_CreateFile(sfileName));

  // Open file and get file descriptor
  int file_desc;
  CALL_OR_DIE(BF_OpenFile(sfileName, &file_desc));

  // Allocate and initialize the first block for the secondary hash table
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_AllocateBlock(file_desc, block));

  // Write the number of buckets to the first block
  char* data = BF_Block_GetData(block);
  memcpy(data, &buckets, sizeof(int));
  
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));

  // Close file using BF_CloseFile
  CALL_OR_DIE(BF_CloseFile(file_desc));
  return 0;
}


SHT_info* SHT_OpenSecondaryIndex(char *indexName){
  // Open file and get file descriptor
  int file_desc;
  CALL_OR_DIE(BF_OpenFile(indexName, &file_desc));

  // Allocate and initialize SHT_info structure
  SHT_info *info = malloc(sizeof(SHT_info));

  // Read information from first block
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(file_desc, 0, block));
  char* data = BF_Block_GetData(block);
  int num_buckets;
  memcpy(&num_buckets, data, sizeof(int));
  info->num_buckets=num_buckets;

  // Allocate and initialize SHT_header_info->hash_table[bucket] structures
  info->hash_table = malloc(info->num_buckets * sizeof(SHT_block_info));
  for (int i = 0; i < info->num_buckets; i++) {
    info->hash_table[i].p=malloc(sizeof(pair));

  }

  return info;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
  // Free memory allocated for secondary hash table
  for (int i = 0; i < SHT_info->num_buckets; i++) {
    free(SHT_info->hash_table[i].p);
  }
  free(SHT_info->hash_table);
  //Free memory for the SHT_info
  free(SHT_info);

  return 0;
}

unsigned int hash_function(char* name, int num_buckets) {
  unsigned int hash = 5381;
  int c;
  while ((c = *name++))
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  return hash % num_buckets;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){

  int bucket = hash_function(record.name,sht_info->num_buckets);

  sht_info->hash_table[bucket].p->blockid=block_id;
  sht_info->hash_table[bucket].p->name=record.name;

  return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
  
  int bucket = hash_function(name,sht_info->num_buckets);
 
  int block_id=sht_info->hash_table[bucket].p->blockid;

  int count=0;
  for(int i=0; i<ht_info->num_buckets; i++){
    for (int block_num = ht_info->hash_table[i].num_blocks; block_num <= ht_info->hash_table[i].num_blocks; block_num++) {
      if(block_id==block_num){
        BF_Block *block;
        char* data;
        // Allocate a block
        BF_Block_Init(&block);
        CALL_OR_DIE(BF_GetBlock(ht_info->hash_table[i].fileDesc, block_num, block));

        // Get the data in the block
        data=BF_Block_GetData(block);
        int record_size=ht_info->hash_table[i].record_size;
        for(int r=0; r<ht_info->hash_table[i].num_records; r++){
          Record record;
          memcpy(&record, data + sizeof(int) + r * record_size, record_size);

          int equal=strcmp(name,record.name);
          if(equal==0){
            count=count + HT_GetAllEntries(ht_info,&record.id);
          }
        }
      }
    }
  }
  return count;
}
