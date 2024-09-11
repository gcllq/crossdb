//
// Created by Administrator on 2024/9/11.
//

#ifndef CDF_METADATA_H
#define CDF_METADATA_H

#endif //CDF_METADATA_H
#pragma pack(push, 1) // 禁用对齐
struct column_metadata {
    char name[50];
    int offset;
    uint8_t type;
    int length;
    bool isPK;
    int index;

};
#pragma pack(pop) // 恢复对齐
typedef struct column_metadata column_metadata;
