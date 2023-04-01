#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"

#include "hp_file.h"
#include "record.h"
#define HP_ERROR -1

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

int HP_CreateFile(char *fileName){
  // create the file using the BF library
  CALL_BF(BF_CreateFile(fileName));
  // open the file and store the file descriptor in a local variable
  int fd;
  CALL_BF(BF_OpenFile(fileName,&fd));

  // initialize a block and allocate it to the file
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(fd, block));

  // write some data to the block
  char* data;
  data=BF_Block_GetData(block);
  int num_records = 0;
  int record_size = sizeof(char) * 15 + sizeof(int) + sizeof(char) * 15 + sizeof(char) * 20 + sizeof(char) * 20;
  int num_blocks = 1;
  memcpy(data, &record_size, sizeof(int));
  memcpy(data + sizeof(int), &num_records, sizeof(int));
  memcpy(data + 2 * sizeof(int), &num_blocks, sizeof(int));
  // Unpin the header block
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  // Allocate a block for the empty block
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(fd, block));
  CALL_BF(BF_UnpinBlock(block));
  
  BF_Block_Destroy(&block);

  CALL_BF(BF_CloseFile(fd));

  return 0;
}

HP_info* HP_OpenFile(char *fileName) {
  // Open the file
  int fd;
  BF_OpenFile(fileName, &fd);

  // initialize a block and read the first block of the file
  BF_Block *block;
  BF_Block_Init(&block);
  BF_GetBlock(fd, 0, block);
  char *data; 
  data= BF_Block_GetData(block);
  int record_size, num_records, num_blocks;
  memcpy(&record_size, data, sizeof(int));
  memcpy(&num_records, data + sizeof(int), sizeof(int));
  memcpy(&num_blocks, data + 2 * sizeof(int), sizeof(int));
  BF_UnpinBlock(block);

  // Initialize the HP_info struct
  HP_info *hp_info = malloc(sizeof(HP_info));
  hp_info->fileDesc = fd;
  hp_info->block = block;
  hp_info->num_records = num_records;
  hp_info->record_size = record_size;
  hp_info->num_blocks = num_blocks;
  
  return hp_info;
}

int HP_CloseFile( HP_info* hp_info ){
  // unpin the block
  CALL_BF(BF_UnpinBlock(hp_info->block));
  // destroy the block and close the file
  CALL_BF(BF_CloseFile(hp_info->fileDesc));
  BF_Block_Destroy(&hp_info->block);
  free(hp_info);
  return 0;
}

int HP_InsertEntry(HP_info *hp_info, Record record) {
  BF_Block *block;
  char* data;

  // Check if there is a block with enough space to insert the record
  int block_num;
  int record_size = hp_info->record_size;
  for (block_num = 1; block_num <= hp_info->num_blocks; block_num++) {
    // Allocate a block
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(hp_info->fileDesc, block_num, block));

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
      CALL_BF(BF_UnpinBlock(block));

      // Update the header to reflect the new record
      BF_Block_Init(&block);
      CALL_BF(BF_GetBlock(hp_info->fileDesc, 0, block));
      data=BF_Block_GetData(block);
      hp_info->num_records++;
      memcpy(data + sizeof(int), &hp_info->num_records, sizeof(int));
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));

      // Destroy the block
      BF_Block_Destroy(&block);

      // Return BF_OK if the record was successfully inserted
      return 0;
    }

    // Unpin the block if there was not enough space to insert the record
    CALL_BF(BF_UnpinBlock(block));

    // Destroy the block
    BF_Block_Destroy(&block);
  }

  // Allocate a new block if there was no block with enough space to insert the record
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(hp_info->fileDesc, block));

  // Insert the record in the new block
  data=BF_Block_GetData(block);
  int num_records = 1;
  memcpy(data, &num_records, sizeof(int));
  memcpy(data + sizeof(int), &record, record_size);
  BF_Block_SetDirty(block);

  // Unpin the new block
  CALL_BF(BF_UnpinBlock(block));

  // Update the header to reflect the new block and record
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(hp_info->fileDesc, 0, block));
  data=BF_Block_GetData(block);
  hp_info->num_blocks++;
  hp_info->num_records++;
  memcpy(data + 2 * sizeof(int), &hp_info->num_blocks, sizeof(int));
  memcpy(data + sizeof(int), &hp_info->num_records, sizeof(int));
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  // Destroy the block
  BF_Block_Destroy(&block);

  // Return BF_OK if the record was successfully inserted
  return 0;
}

int HP_GetAllEntries(HP_info *hp_info, int id) {
  BF_Block *block;
  char* data;

  // Initialize a count to keep track of the number of entries found
  int count = 0;

  // Iterate through all the blocks in the file
  int block_num;
  int record_size = hp_info->record_size;
  for (block_num = 1; block_num <= hp_info->num_blocks; block_num++) {
    // Allocate a block
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(hp_info->fileDesc, block_num, block));

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
        count++;
      }
    }

    // Unpin the block
    CALL_BF(BF_UnpinBlock(block));

    // Destroy the block
    BF_Block_Destroy(&block);
  }

  // Return the number of entries found
  return count;
}
