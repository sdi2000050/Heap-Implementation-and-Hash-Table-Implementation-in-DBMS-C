#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
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

int HT_CreateFile(char *fileName, int buckets) {
  // Create new file using BF_CreateFile
  CALL_OR_DIE(BF_CreateFile(fileName));

  // Open file and get file descriptor
  int file_desc;
  CALL_OR_DIE(BF_OpenFile(fileName, &file_desc));

  // Allocate and initialize the first block for the hash table
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_AllocateBlock(file_desc, block));

  // Write the number of buckets to the first block
  char* data = BF_Block_GetData(block);
  memcpy(data, &buckets, sizeof(int));
  
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));

  // Allocate and initialize the rest of the blocks for the hash table
  for (int i = 0; i < buckets; i++) {
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(file_desc, block));
    data = BF_Block_GetData(block);
    int num_records = 0;
    int record_size = sizeof(char) * 15 + sizeof(int) + sizeof(char) * 15 + sizeof(char) * 20 + sizeof(char) * 20;
    int num_blocks=i+1;
    memcpy(data , &num_records, sizeof(int));
    memcpy(data + sizeof(int), &record_size,sizeof(int));
    memcpy(data +sizeof(int) + sizeof(int), &num_blocks, sizeof(int));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
  }

  // Close file using BF_CloseFile
  CALL_OR_DIE(BF_CloseFile(file_desc));
  return 0;
}


HT_info* HT_OpenFile(char *fileName) {
  // Open file and get file descriptor
  int file_desc;
  CALL_OR_DIE(BF_OpenFile(fileName, &file_desc));

  // Allocate and initialize HT_info structure
  HT_info *info = malloc(sizeof(HT_info));

  // Read information from first block
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(file_desc, 0, block));
  char* data = BF_Block_GetData(block);
  int num_buckets;
  memcpy(&num_buckets, data, sizeof(int));
  info->num_buckets=num_buckets;

  // Allocate and initialize HT_header_info->hash_table[bucket] structures
  info->hash_table = malloc(info->num_buckets * sizeof(HT_block_info));
  for (int i = 0; i < info->num_buckets; i++) {
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(file_desc, i + 1, block));
    data = BF_Block_GetData(block);
    int record_size, num_records, num_blocks;
    memcpy(&num_records, data, sizeof(int));
    memcpy(&record_size, data + sizeof(int), sizeof(int));
    memcpy(&num_blocks, data + 2 * sizeof(int), sizeof(int));
    info->hash_table[i].fileDesc=file_desc;
    info->hash_table[i].block=block;
    info->hash_table[i].num_blocks=num_blocks;
    info->hash_table[i].num_records=num_records;
    info->hash_table[i].record_size=record_size;
    CALL_OR_DIE(BF_UnpinBlock(block));

  }

  return info;
}


int HT_CloseFile(HT_info* header_info) {
  // Free memory allocated for hash table
  for (int i = 0; i < header_info->num_buckets; i++) {
    CALL_OR_DIE(BF_UnpinBlock(header_info->hash_table[i].block));
    BF_Block_Destroy(&header_info->hash_table[i].block);
  }
  free(header_info->hash_table);
  //Free memory for the header_info
  free(header_info);

  return 0;
}

int HT_InsertEntry(HT_info* header_info, Record record) {
  int blockId;
  int bucket=record.id % header_info->num_buckets;

  int block_num;
  int record_size=header_info->hash_table[bucket].record_size;

  BF_Block *block;
  char* data;
  for (block_num=header_info->hash_table[bucket].num_blocks; block_num <= header_info->hash_table[bucket].num_blocks; block_num++) {
    // Allocate a block
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(header_info->hash_table[bucket].fileDesc, block_num, block));

    // Check if the block has enough space to insert the record
    data=BF_Block_GetData(block);
    int num_records;
    memcpy(&num_records, data, sizeof(int));
    if (BF_BLOCK_SIZE - sizeof(int) - num_records * record_size >= record_size) {
      // Insert the record in the block
      memcpy(data + sizeof(int) + num_records * record_size, &record, record_size);

      // Update the number of records in the block
      num_records++;
      memcpy(data, &num_records, sizeof(int));

      // Set the block as dirty
      BF_Block_SetDirty(block);

      // Unpin the block
      CALL_OR_DIE(BF_UnpinBlock(block));

      // Update the header to reflect the new record
      BF_Block_Init(&block);
      CALL_OR_DIE(BF_GetBlock(header_info->hash_table[bucket].fileDesc, 0, block));
      data=BF_Block_GetData(block);
      header_info->hash_table[bucket].num_records++;
      memcpy(data + sizeof(int), &header_info->hash_table[bucket].num_records, sizeof(int));
      BF_Block_SetDirty(block);
      CALL_OR_DIE(BF_UnpinBlock(block));

      // Destroy the block
      BF_Block_Destroy(&block);

      blockId=block_num;
      return blockId;
    }

    // Unpin the block if there was not enough space to insert the record
    CALL_OR_DIE(BF_UnpinBlock(block));

    // Destroy the block
    BF_Block_Destroy(&block);
  }

  // Allocate a new block if there was no block with enough space to insert the record
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_AllocateBlock(header_info->hash_table[bucket].fileDesc, block));

  // Insert the record in the new block
  data=BF_Block_GetData(block);
  int num_records = 1;
  memcpy(data, &num_records, sizeof(int));
  memcpy(data + sizeof(int), &record, record_size);
  BF_Block_SetDirty(block);

  // Unpin the new block
  CALL_OR_DIE(BF_UnpinBlock(block));

  // Update the header to reflect the new block and record
  bucket++;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(header_info->hash_table[bucket].fileDesc, 0, block));
  data=BF_Block_GetData(block);
  header_info->hash_table[bucket].num_blocks++;
  header_info->hash_table[bucket].num_records++;
  memcpy(data + 2 * sizeof(int), &header_info->hash_table[bucket].num_blocks, sizeof(int));
  memcpy(data + sizeof(int), &header_info->hash_table[bucket].num_records, sizeof(int));
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));

  // Destroy the block
  BF_Block_Destroy(&block);

  blockId=header_info->hash_table[bucket].num_blocks;
  return blockId;
}

int HT_GetAllEntries(HT_info* header_info, 	void * value ) {
  int id=*(int*) value;
  int key= id % header_info->num_buckets;

  BF_Block *block;
  char* data;

  // Initialize a count to keep track of the number of entries found
  int count = 0;

  // Iterate through all the blocks in the file
  int block_num;
  int record_size = header_info->hash_table[key].record_size;

  for (block_num = header_info->hash_table[key].num_blocks; block_num <= header_info->hash_table[key].num_blocks; block_num++) {
    // Allocate a block
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(header_info->hash_table[key].fileDesc, block_num, block));

    // Get the data in the block
    data=BF_Block_GetData(block);

    // Iterate through all the records in the block
    int num_records;
    memcpy(&num_records, data, sizeof(int));
    int i;
    for (i = 0; i < num_records; i++) {
      Record record;
      memcpy(&record, data + sizeof(int) + i * record_size, record_size);
      if (record.id == id) {
        // Print the record if the id matches
        printf("\nRecord found:");
        printRecord(record);
      }
    }
    count++;
    // Unpin the block
    CALL_OR_DIE(BF_UnpinBlock(block));

    // Destroy the block
    BF_Block_Destroy(&block);
  }

  // Return the number of entries found
  return count;
}
