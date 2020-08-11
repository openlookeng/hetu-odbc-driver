/*
  Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
  
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; version 2 of the License.
  
  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
  for more details.
  
  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <ma_odbc.h>

typedef struct {
    MA_MEM_ROOT fields_ma_alloc_root;
}MADB_STMT_EXTENSION;

extern void *ma_alloc_root(MA_MEM_ROOT * mem_root, size_t Size);
extern int str_to_TIME(const char * str, size_t length, MYSQL_TIME * tm);

static void update_field_max_length(MYSQL_STMT *stmt, unsigned int field_index, unsigned long field_len) {
    enum enum_field_types field_type = stmt->fields[field_index].type;
    MYSQL_PS_CONVERSION  *fetch_func = &mysql_ps_fetch_functions[0];
    MYSQL_FIELD          *field      = &stmt->fields[field_index];
    unsigned long         len;

    if (!stmt->update_max_length) {
        return;
    }

    if (fetch_func[field_type].pack_len < 0) {
        switch (field_type) {
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_TIMESTAMP:
                field->max_length = fetch_func[field_type].max_len;
                break;

            default:
                if (field_len > field->max_length) {
                    field->max_length = field_len;
                }
                break;
        }
    }
    else
    {
        if (field->flags & ZEROFILL_FLAG) {
            len = (field->length >= fetch_func[field->type].max_len)? field->length : fetch_func[field->type].max_len;
            if (len > field->max_length) {
                field->max_length = len;
            }
        }
        else if (!field->max_length) {
            field->max_length = fetch_func[field->type].max_len;
        }
        else
        {
            //do nothing
        }
    }

    return;
}

static void fill_data_len_info(char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long len_value) {
    unsigned long long index;

    /*
        impemented according to MySQL c/s protocal;
        case len_value < 251: 1 byte length
        case len_value < 0x10000L: 0xfc + 2 bytes length
        case len_value < 0x1000000L: 0xfd + 3 bytes length
        case len_value < 0 or len_value >= 0x1000000L: 0xfe + 8 bytes length
    */   
    index = *pos;
    if ((len_value < 0) || (len_value >= 0x1000000L)) {
        buffer[index++] = 0xfe;

        buffer[index++] = (char)(len_value & 0xff);
        buffer[index++] = (char)((len_value >> 8) & 0xff);
        buffer[index++] = (char)((len_value >> 16) & 0xff);
        buffer[index++] = (char)((len_value >> 24) & 0xff);
        buffer[index++] = (char)((len_value >> 32) & 0xff);
        buffer[index++] = (char)((len_value >> 40) & 0xff);
        buffer[index++] = (char)((len_value >> 48) & 0xff);
        buffer[index++] = (char)((len_value >> 56) & 0xff);

        *pos = index;
        return;
    }

    if (len_value < 251) {
        buffer[index++] = (char)(len_value & 0xff);
    } else if (len_value < 0x10000L) {
        buffer[index++] = 0xfc;

        buffer[index++] = (char)(len_value & 0xff);
        buffer[index++] = (char)((len_value >> 8) & 0xff);
    } else { //len_value < 0x1000000L
        buffer[index++] = 0xfd;

        buffer[index++] = (char)(len_value & 0xff);
        buffer[index++] = (char)((len_value >> 8) & 0xff);
        buffer[index++] = (char)((len_value >> 16) & 0xff);
    }
    *pos = index;
    
    return;
}

static int convert_fill_long_long(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    unsigned long long index;
    long long value = strtoll(field_data, NULL, 10);

    index = *pos;
    buffer[index++] = (char)(value & 0xff);
    buffer[index++] = (char)((value >> 8) & 0xff);
    buffer[index++] = (char)((value >> 16) & 0xff);
    buffer[index++] = (char)((value >> 24) & 0xff);
    buffer[index++] = (char)((value >> 32) & 0xff);
    buffer[index++] = (char)((value >> 40) & 0xff);
    buffer[index++] = (char)((value >> 48) & 0xff);
    buffer[index++] = (char)((value >> 56) & 0xff);
    *pos = index;

    *fill_len = sizeof(long long);
    
    return 0;
}

static int convert_fill_int(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    unsigned long long index;
    int value = atol(field_data);

    index = *pos;
    buffer[index++] = (char)(value & 0xff);
    buffer[index++] = (char)((value >> 8) & 0xff);
    buffer[index++] = (char)((value >> 16) & 0xff);
    buffer[index++] = (char)((value >> 24) & 0xff);
    *pos = index;

    *fill_len = sizeof(int);
    
    return 0;
}

static int convert_fill_short(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    short value = (short)atoi(field_data);

    buffer[*pos] = (char)(value & 0xff);
    *pos += 1;
    buffer[*pos] = (char)((value >> 8) & 0xff);
    *pos += 1;

    *fill_len = sizeof(short);
    
    return 0;
}

static int convert_fill_tiny(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    short value = (short)atoi(field_data);

    buffer[*pos] = (char)(value & 0xff);
    *pos += 1;

    *fill_len = sizeof(char);
    
    return 0;
}

static int convert_fill_double(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    unsigned long long index;
    double value = atof(field_data);
    long long *temp = (long long*)&value; //double does not support bit operate, so use a long long pointer to replace it

    index = *pos;
    buffer[index++] = (char)(*temp & 0xff);
    buffer[index++] = (char)((*temp >> 8) & 0xff);
    buffer[index++] = (char)((*temp >> 16) & 0xff);
    buffer[index++] = (char)((*temp >> 24) & 0xff);
    buffer[index++] = (char)((*temp >> 32) & 0xff);
    buffer[index++] = (char)((*temp >> 40) & 0xff);
    buffer[index++] = (char)((*temp >> 48) & 0xff);
    buffer[index++] = (char)((*temp >> 56) & 0xff);
    *pos = index;

    *fill_len = sizeof(double);
    
    return 0;
}

static int convert_fill_float(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    unsigned long long index;
    float value = (float)atof(field_data);
    int *temp = (int*)&value; //float does not support bit operate, so use an int pointer to replace it

    index = *pos;
    buffer[index++] = (char)(*temp & 0xff);
    buffer[index++] = (char)((*temp >> 8) & 0xff);
    buffer[index++] = (char)((*temp >> 16) & 0xff);
    buffer[index++] = (char)((*temp >> 24) & 0xff);
    *pos = index;

    *fill_len = sizeof(float);
    
    return 0;
}

static int convert_fill_date(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    int    ret;
    unsigned int len;
    unsigned long long index;
    MYSQL_TIME tm;

    memset(&tm, 0, sizeof(MYSQL_TIME));
    ret = str_to_TIME(field_data, strlen(field_data), &tm);
    if (ret != 0) {
        return 1;
    }

    index = *pos;

    index++; //skip length byte

    buffer[index++] = (char)(tm.year & 0xff);
    buffer[index++] = (char)((tm.year >> 8) & 0xff);
    buffer[index++] = (char)(tm.month & 0xff);
    buffer[index++] = (char)(tm.day & 0xff);
    buffer[index++] = (char)(tm.hour & 0xff);
    buffer[index++] = (char)(tm.minute & 0xff);
    buffer[index++] = (char)(tm.second & 0xff);

    // str_to_TIME will process second_part to microseconds, eg '.32' to 320000
    // but we just need 32 here, so remove the excrescent 0
    while (tm.second_part && ((tm.second_part % 10) == 0)) {
        tm.second_part = tm.second_part / 10;
    }
    
    buffer[index++] = (char)(tm.second_part & 0xff);
    buffer[index++] = (char)((tm.second_part >> 8) & 0xff);
    buffer[index++] = (char)((tm.second_part >> 16) & 0xff);
    buffer[index++] = (char)((tm.second_part >> 24) & 0xff);

    if (tm.second_part) {
        len = 11;
    } else if (tm.hour || tm.minute || tm.second) {
        len = 7;
    } else if (tm.year || tm.month || tm.day) {
        len = 4;
    } else {
        len = 0;
    }

    buffer[*pos] = len & 0xff;
    *pos += 1 + len; //1 byte len info + len bytes content
    *fill_len = 1 + len;
    return 0;
}

static int convert_fill_time(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    unsigned long long index;
    unsigned int days;
    unsigned int len;
    int    ret;
    MYSQL_TIME tm;

    memset(&tm, 0, sizeof(MYSQL_TIME));
    ret = str_to_TIME(field_data, strlen(field_data), &tm);
    if (ret != 0) {
        return 1;
    }

    index = *pos;

    index++; //skip length byte
    buffer[index++] = tm.neg? 1 : 0;

    if (tm.hour >= 24) {
        days = tm.hour / 24;
        tm.hour -= days * 24;
        tm.day += days;
    }
    
    buffer[index++] = (char)(tm.day & 0xff);
    buffer[index++] = (char)((tm.day >> 8) & 0xff);
    buffer[index++] = (char)((tm.day >> 16) & 0xff);
    buffer[index++] = (char)((tm.day >> 24) & 0xff);
    buffer[index++] = (char)(tm.hour & 0xff);
    buffer[index++] = (char)(tm.minute & 0xff);
    buffer[index++] = (char)(tm.second & 0xff);

    // str_to_TIME will process second_part to microseconds, eg '.32' to 320000
    // but we just need 32 here, so remove the excrescent 0
    while (tm.second_part && ((tm.second_part % 10) == 0)) {
        tm.second_part = tm.second_part / 10;
    }
    
    buffer[index++] = (char)(tm.second_part & 0xff);
    buffer[index++] = (char)((tm.second_part >> 8) & 0xff);
    buffer[index++] = (char)((tm.second_part >> 16) & 0xff);
    buffer[index++] = (char)((tm.second_part >> 24) & 0xff);

    if (tm.second_part) {
        len = 12;
    } else if (tm.hour || tm.minute || tm.second || tm.day) {
        len = 8;
    } else {
        len = 0;
    }

    buffer[*pos] = len & 0xff;
    *pos += 1 + len; //1 byte len info + len bytes content
    *fill_len = 1 + len;
    return 0;

}

static int convert_fill_string(char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len) {
    unsigned int len;
    len = (unsigned int)strlen(field_data);

    fill_data_len_info(buffer, buffer_len, pos, len);
    memcpy(&buffer[*pos], field_data, len);
    *pos += len;
    *fill_len = len;
    
    return 0;
}

typedef int (*fill_type_data_func) (char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos, unsigned long long *fill_len);
typedef struct {
    enum enum_field_types data_type;
    fill_type_data_func   type_func;
}TYPE_FILL_FUNC_MAP;

static TYPE_FILL_FUNC_MAP g_type2FillDataFunc[] = {
    {MYSQL_TYPE_LONGLONG,    convert_fill_long_long},
    {MYSQL_TYPE_LONG,        convert_fill_int},
    {MYSQL_TYPE_INT24,       convert_fill_int},
    {MYSQL_TYPE_SHORT,       convert_fill_short},
    {MYSQL_TYPE_YEAR,        convert_fill_short},
    {MYSQL_TYPE_TINY,        convert_fill_tiny},
    {MYSQL_TYPE_DOUBLE,      convert_fill_double},
    {MYSQL_TYPE_FLOAT,       convert_fill_float},
    {MYSQL_TYPE_DATE,        convert_fill_date},
    {MYSQL_TYPE_DATETIME,    convert_fill_date},
    {MYSQL_TYPE_TIMESTAMP,   convert_fill_date},
    {MYSQL_TYPE_TIME,        convert_fill_time},
    {MYSQL_TYPE_VARCHAR,     convert_fill_string},
};

static int convert_and_fill_data(MYSQL_STMT *stmt, unsigned int field_index, char *field_data, char *buffer, unsigned long long buffer_len, unsigned long long *pos) {
    int    ret = 0;
    unsigned long long     fill_len = 0;
    fill_type_data_func    fill_func = convert_fill_string;
    int       array_size = sizeof(g_type2FillDataFunc) / sizeof(g_type2FillDataFunc[0]);

    for (int i = 0; i < array_size; i++) {
        if (stmt->fields[field_index].type == g_type2FillDataFunc[i].data_type) {
            fill_func = g_type2FillDataFunc[i].type_func;
            break;
        }
    }

    ret = fill_func(field_data, buffer, buffer_len, pos, &fill_len);

    // update_max_length
    update_field_max_length(stmt, field_index, (unsigned long)fill_len);

    return ret;
}

static void calc_row_buffer_len(MYSQL_STMT *stmt, MYSQL_ROW text_row, unsigned long long *packet_len) {
    unsigned long long text_len; //max bytes of field string
    unsigned long long data_len;
    unsigned long long length_len; //max indicator bytes
    unsigned int       i;

    for (i = 0; i < stmt->field_count; i++) {
        if (text_row[i] == NULL) {
            continue;
        }
        
        text_len = strlen((char*)text_row[i]);

        data_len = text_len;
        //for details please see "Binary Protocal Value" in MySQL c/s protocal
        switch (stmt->fields[i].type) {
            case MYSQL_TYPE_SHORT:
                data_len = sizeof(short);
                break;

            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
                data_len = sizeof(int);
                break;

            case MYSQL_TYPE_LONGLONG:
                data_len = sizeof(long long);
                break;

            case MYSQL_TYPE_YEAR:
                data_len = sizeof(short);
                break;

            case MYSQL_TYPE_TINY:
                data_len = sizeof(char); //1 byte for tiny
                break;

            case MYSQL_TYPE_DOUBLE:
                data_len = sizeof(double);
                break;

            case MYSQL_TYPE_FLOAT:
                data_len = sizeof(float);
                break;

            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_TIMESTAMP:
                data_len = 11; //(valid values: 0, 4, 7, 11), so max is 11
                break;

            case MYSQL_TYPE_TIME:
                data_len = 12; //(valid values: 0, 8, 12 ), so max is 12
                break;
            default:
                //data store in string format, data_len is equal to text_len
                break;
        }

        /*
        implemented according to MySQL c/s protocal:
        case text_len < 251: 1 byte length
        case text_len < 0x10000L: 1 byte flag + 2 bytes length
        case text_len < 0x1000000L: 1 byte flag + 3 bytes length
        case text_len < 0 or len_value >= 0x1000000L: 1 byte flag + 8 bytes length
        */
        if (data_len < 0) {
            length_len = 1 + sizeof(unsigned long long);
        } else if (data_len < 251) {
            length_len =  1;
        } else if (data_len < 0x10000L) {
            length_len =  1 + sizeof(short);
        } else if (data_len < 0x1000000L) {
            length_len =  1 + 3;
        } else {
            length_len =  1 + sizeof(unsigned long long);
        }

        *packet_len += data_len + length_len;
    }
}

static my_bool store_result_as_binary(MYSQL_STMT *stmt, MYSQL_RES *resultSet) {
    MYSQL_ROW text_row = NULL;
    MYSQL_DATA *result = &stmt->result;
    MYSQL_ROWS *current = NULL;
    MYSQL_ROWS **pprevious = NULL;
    unsigned long long packet_len;
    unsigned long long data_start_pos;
    unsigned int i;
    unsigned long long binary_len;
    char *binary_buffer = NULL;
    char *fill_buffer = NULL; //pointer in binary_buffer to fill binary message
    char *null_bit_ptr = NULL;
    unsigned int bit_offset;

    result->fields = stmt->field_count;
    pprevious = &result->data;

    while ((text_row = mysql_fetch_row(resultSet)) != NULL) {
        /* data format: status(1byte)+null_bits()+content
           content format: a.len+content for normal field;
                           b.null for empty field
        */
        packet_len = 0;
        calc_row_buffer_len(stmt, text_row, &packet_len); //first calc bytes to store binary message of row data
        data_start_pos = 1 + (stmt->field_count + 2 + 7) / 8; //status bit(1 bytes) and bytes of null_bits
        packet_len += data_start_pos; //plus status bytes and null bytes

        binary_buffer = (char*)calloc(1, packet_len);
        if (binary_buffer == NULL) {
            //caller to free memory of stmt->result.alloc
            SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
            return 1;
        }

        null_bit_ptr = binary_buffer + 1; //skip status byte
        bit_offset = 2; // start from first 2 bit of first null_bit byte

        fill_buffer = binary_buffer + data_start_pos; //skip status byte and null bit bytes
        for (i = 0; i < stmt->field_count; i++) {
            if (text_row[i] == NULL) {
                *null_bit_ptr |= (1 << bit_offset);                
            } else {
                convert_and_fill_data(stmt, i, text_row[i], binary_buffer, packet_len, &data_start_pos);
            }

            bit_offset++;
            if (bit_offset >= 8) {
                bit_offset = 0; /* To next byte */
                null_bit_ptr++;
            }
        }

        // now data_start_pos is the actual binary_buffer message len
        binary_len = data_start_pos;
        if (!(current = (MYSQL_ROWS *)ma_alloc_root(&stmt->result.alloc, sizeof(MYSQL_ROWS) + binary_len)))
        {
            SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
            free(binary_buffer);
            return (1);
        }

        current->data = (MYSQL_ROW)(current + 1); //we store data at offset of sizeof(MYSQL_ROWS)
        *pprevious = current;
        pprevious  = &current->next;
        *pprevious = NULL;

        memcpy_s((char*)current->data, binary_len, binary_buffer, binary_len);
        free(binary_buffer);
        binary_buffer = NULL;
        result->rows++;
    }

    stmt->result_cursor = result->data;

    return 0;
}

static int statement_buffered_fetch(MYSQL_STMT *stmt, unsigned char **row) {
    if (!stmt->result_cursor) {
        *row = NULL;
        stmt->state = MYSQL_STMT_FETCH_DONE;
        return MYSQL_NO_DATA;
    }

    stmt->state = MYSQL_STMT_USER_FETCHING;
    *row = (unsigned char *)stmt->result_cursor->data;

    stmt->result_cursor = stmt->result_cursor->next;

    return 0;
}

static int copy_field_param(MA_MEM_ROOT *mem_root, char *source, char **dest) {
    unsigned int    len;

    if (source != NULL) {
        len = (unsigned int)strlen(source) + 1;
        *dest = (char*)ma_alloc_root(mem_root, len);

        if (*dest == NULL) {
            return 1;
        }

        memcpy(*dest, source, len);
    }

    return 0;
}

static my_bool store_statement_fields(MYSQL_STMT *stmt, MYSQL_RES *resultSet) {
    MADB_STMT_EXTENSION *extension = stmt->extension;
    MA_MEM_ROOT *fields_root = &extension->fields_ma_alloc_root;
    size_t alloc_size = sizeof(MYSQL_FIELD) * stmt->field_count;
    int    ret;

    stmt->fields = (MYSQL_FIELD*)ma_alloc_root(fields_root, alloc_size);
    if (stmt->fields == NULL) {
        return 1;
    }

    memset(stmt->fields, 0, alloc_size);
    for (unsigned int i = 0; i < stmt->field_count; i++) {
        ret = memcpy_s(&stmt->fields[i], sizeof(MYSQL_FIELD), &resultSet->fields[i], sizeof(MYSQL_FIELD));

        ret = 0;
        ret |= copy_field_param(fields_root, resultSet->fields[i].name, &stmt->fields[i].name);

        ret |= copy_field_param(fields_root, resultSet->fields[i].org_name, &stmt->fields[i].org_name);
        
        ret |= copy_field_param(fields_root, resultSet->fields[i].table, &stmt->fields[i].table);
    
        ret |= copy_field_param(fields_root, resultSet->fields[i].org_table, &stmt->fields[i].org_table);
    
        ret |= copy_field_param(fields_root, resultSet->fields[i].db, &stmt->fields[i].db);
    
        ret |= copy_field_param(fields_root, resultSet->fields[i].catalog, &stmt->fields[i].catalog);
    
        ret |= copy_field_param(fields_root, resultSet->fields[i].def, &stmt->fields[i].def);
        
        if (ret != 0) {
            // caller to free memory allocated from fields_root
            return 1;
        }
    }

    return 0;
}

static void store_result_error_exit(MYSQL_STMT *stmt) {
    if (stmt->params != NULL) {
        ma_free_root(&stmt->result.alloc, 0);
        stmt->params = NULL;
    }

    if (stmt->fields != NULL) {
        ma_free_root(&((MADB_STMT_EXTENSION*)stmt->extension)->fields_ma_alloc_root, 0);
        stmt->fields = NULL;
    }

    if (stmt->result.data != NULL) {
        /* error during read - reset stmt->data */
        ma_free_root(&stmt->result.alloc, 0);
        stmt->result.data = NULL;
        stmt->result.rows = 0;
    }

    if (stmt->bind != NULL) {
        ma_free_root(&((MADB_STMT_EXTENSION*)stmt->extension)->fields_ma_alloc_root, 0);
        stmt->bind = NULL;
    }

    stmt->mysql->status = MYSQL_STATUS_READY;
    SET_CLIENT_STMT_ERROR(stmt, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);

    return;
}

SQLRETURN MADB_StoreQueryResult(MADB_Stmt * Stmt, MYSQL_RES * resultSet) {
    unsigned int last_server_status;
    size_t       alloc_size;
    my_bool      ret;
    MYSQL_STMT  *stmt                 = NULL;
    MA_MEM_ROOT *fields_ma_alloc_root = NULL;

    MADB_STMT_CLOSE_STMT(Stmt);
    Stmt->stmt = MADB_NewStmtHandle(Stmt);
    if (Stmt->stmt == NULL) {
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, "Reinit statement handle error.", 0);
        return SQL_ERROR;
    }

    Stmt->stmt->field_count = Stmt->Connection->mariadb->field_count;
    stmt                    = Stmt->stmt;
    stmt->field_count       = resultSet->field_count;

    /* 1. first build stmt->fields */
    ret = store_statement_fields(stmt, resultSet);
    if (ret != 0) {
        store_result_error_exit(stmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, "Store statement result field error.", 0);
        return SQL_ERROR;
    }

    /* 2. convert result set to binary format and store in stmt->result */
    ret = store_result_as_binary(stmt, resultSet);
    if (ret != 0) {
        store_result_error_exit(stmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, "Store result to binary format error.", 0);
        return SQL_ERROR;
    }

    last_server_status = stmt->mysql->server_status;
    if ((last_server_status & SERVER_PS_OUT_PARAMS) && !(last_server_status & SERVER_MORE_RESULTS_EXIST)) {
        stmt->mysql->server_status |= SERVER_MORE_RESULTS_EXIST;
    }

    stmt->result_cursor = stmt->result.data;
    stmt->fetch_row_func = statement_buffered_fetch;
    stmt->mysql->status = MYSQL_STATUS_READY;

    stmt->state = (stmt->result.rows != 0)? MYSQL_STMT_USE_OR_STORE_CALLED: MYSQL_STMT_FETCH_DONE;
    
    stmt->upsert_status.affected_rows = stmt->result.rows;
    stmt->mysql->affected_rows = stmt->result.rows;

    /* 3. set IRD metaDatas to support accessing */
    MADB_StmtResetResultStructures(Stmt);
    ret = MADB_DescSetIrdMetadata(Stmt, mysql_fetch_fields(FetchMetadata(Stmt)), mysql_stmt_field_count(Stmt->stmt));
    if (ret != 0) {
        store_result_error_exit(stmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, "Store result: set IRD metadata error.", 0);
        return SQL_ERROR;
    }

    /* 4. for param bind works */
    fields_ma_alloc_root = &((MADB_STMT_EXTENSION*)stmt->extension)->fields_ma_alloc_root;
    alloc_size = stmt->field_count * sizeof(MYSQL_BIND);
    if (!(stmt->bind = (MYSQL_BIND *)ma_alloc_root(fields_ma_alloc_root, alloc_size))) {
        store_result_error_exit(stmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, "Store result: alloc bind memory error.", 0);
        return SQL_ERROR;

    }

    memset(stmt->bind, 0, alloc_size);
    
    return SQL_SUCCESS;
}
