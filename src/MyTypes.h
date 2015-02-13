#ifndef __RMYSQL_MY_TYPES__
#define __RMYSQL_MY_TYPES__

enum MyFieldType {
  MY_INT32,
  MY_INT64,
  MY_DBL,
  MY_STR,
  MY_DATE,
  MY_DATE_TIME,
  MY_TIME,
  MY_RAW,
  MY_FACTOR
};

// http://dev.mysql.com/doc/refman/5.7/en/c-api-data-structures.html
inline MyFieldType variableType(enum_field_types type) {
  switch(type) {
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
    return MY_INT32;

  case MYSQL_TYPE_LONGLONG:
    return MY_INT64;

  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    return MY_DBL;
  case MYSQL_TYPE_BIT:
    return MY_STR;
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_NEWDATE:
    return MY_DATE_TIME;
  case MYSQL_TYPE_DATE:
    return MY_DATE;
  case MYSQL_TYPE_TIME:
    return MY_TIME;
  case MYSQL_TYPE_YEAR:
    return MY_INT32;
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_VARCHAR:
    return MY_STR;
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
    return MY_RAW;
  case MYSQL_TYPE_SET:
    return MY_STR;
  case MYSQL_TYPE_ENUM:
    return MY_FACTOR;
  case MYSQL_TYPE_GEOMETRY:
    return MY_RAW;
  case MYSQL_TYPE_NULL:
    return MY_INT32;
  }
}

inline std::string typeName(MyFieldType type) {
  switch(type) {
  case MY_INT32:       return "integer";
  case MY_INT64:       return "integer64";
  case MY_DBL:         return "double";
  case MY_STR:         return "string";
  case MY_DATE:        return "Date";
  case MY_DATE_TIME:   return "POSIXct";
  case MY_TIME:        return "time";
  case MY_RAW:         return "raw";
  case MY_FACTOR:      return "factor";
  }
}

inline SEXPTYPE typeSEXP(MyFieldType type) {
  switch(type) {
  case MY_INT32:       return INTSXP;
  case MY_INT64:       return STRSXP;
  case MY_DBL:         return REALSXP;
  case MY_STR:         return STRSXP;
  case MY_DATE:        return INTSXP;
  case MY_DATE_TIME:   return REALSXP;
  case MY_TIME:        return INTSXP;
  case MY_RAW:         return VECSXP;
  case MY_FACTOR:      return INTSXP;
  }
}


#endif
